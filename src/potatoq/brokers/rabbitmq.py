"""RabbitMQ broker (RabbitMQ 4.x; uses 4.3 features when available).

Design (research in docs/backends.md):

* **Quorum queues** (replicated, at-least-once) for every task queue, with
  ``x-delivery-limit`` as the poison-message guard and an at-least-once dead-letter
  exchange into ``<queue>.dlq``. Classic mirrored queues no longer exist in 4.x.
* **Publisher confirms + mandatory** on every publish: a task is only considered
  enqueued once the broker has replicated it (Celery leaves confirms off).
* **Delays without the (archived) delayed-message plugin and without holding
  messages in worker memory:** the 28-level TTL + dead-letter cascade used by
  NServiceBus and Celery 5.5 (``potatoq.delay.L27`` ... ``L00``). Delays up to ~8.5
  years, whole-second precision, replicated.
* **One message per idle process**: each worker child consumes with prefetch 1, so
  no task waits behind a long one. ``x-consumer-timeout`` is set above the task time
  limit so RabbitMQ's 30 minute default never kills a healthy long task.
* A dedicated I/O thread per worker process owns the connection, so heartbeats keep
  flowing while a task runs (pika is not thread-safe; everything is marshalled).
* RabbitMQ can't store results: set ``result_backend`` (Redis/Postgres/SQLite) if you
  need them. Periodic tasks are deduplicated with a single-active-consumer "token"
  queue: only the process holding the token schedules.
"""

from __future__ import annotations

import logging
import math
import os
import queue as queue_mod
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from .. import serialization, states
from ..exceptions import ImproperlyConfigured
from ..message import Message
from .base import Broker, Consumer, Delivery, ResultRecord

try:
    import pika
    from pika.exceptions import AMQPChannelError, AMQPConnectionError, AMQPError, UnroutableError
except ImportError as exc:  # pragma: no cover
    raise ImportError("RabbitMQ support requires pika: pip install 'potatoq[rabbitmq]'") from exc

logger = logging.getLogger("potatoq.rabbitmq")
if logging.getLogger("pika").level == logging.NOTSET:
    # pika logs every failed address of a connection attempt (e.g. IPv6 ::1) at ERROR.
    logging.getLogger("pika").setLevel(logging.CRITICAL)

DELAY_LEVELS = 28
DELAY_EXCHANGE = "potatoq.delay.L{:02d}"
DELIVERY_EXCHANGE = "potatoq.delivery"
DLX = "potatoq.dlx"
LEADER_QUEUE = "potatoq.scheduler.leader"
_INHERITED: list[Any] = []


def _params(url: str, timeout: float) -> pika.URLParameters:
    if url.startswith("pyamqp://"):
        url = "amqp://" + url[len("pyamqp://") :]
    # Celery style "amqp://host//" and "amqp://host/" both mean vhost "/".
    scheme, rest = url.split("://", 1)
    host, sep, path = rest.partition("/")
    query = ""
    if "?" in path:
        path, query = path.split("?", 1)
        query = "?" + query
    if sep and path in ("", "/"):
        url = f"{scheme}://{host}/%2F{query}"
    params = pika.URLParameters(url)
    params.socket_timeout = timeout
    params.blocked_connection_timeout = max(30.0, timeout)
    params.connection_attempts = 2
    params.retry_delay = 0.5
    return params


def _check_queue_name(name: str) -> None:
    # Queue names become topic routing-key words for delayed delivery.
    if "." in name or "#" in name or "*" in name:
        raise ImproperlyConfigured(f"RabbitMQ queue names can't contain '.', '#' or '*': {name!r}")


def delay_route(delay_s: int, queue: str) -> tuple[str, str]:
    """Exchange and routing key for a delay of ``delay_s`` whole seconds."""
    delay_s = max(1, min(delay_s, (1 << DELAY_LEVELS) - 1))
    level = delay_s.bit_length() - 1
    bits = ".".join("1" if (delay_s >> lvl) & 1 else "0" for lvl in range(DELAY_LEVELS - 1, -1, -1))
    return DELAY_EXCHANGE.format(level), f"{bits}.{queue}"


class RabbitMQBroker(Broker):
    schemes = ("amqp", "amqps")
    supports_results = False
    transactional = False
    needs_revoke_check = True

    def __init__(self, url: str, app: Any, **options: Any):
        super().__init__(url, app, **options)
        self.params = _params(url, float(app.conf.broker_connection_timeout))
        self._local = threading.local()
        self._pid = os.getpid()
        self._declared: set[str] = set()
        self._delay_ready = False
        self._leader: tuple[Any, Any] | None = None
        self._holding_token = False
        self._fired: dict[tuple[str, float], None] = {}
        self.delivery_limit = int(options.get("delivery_limit", app.conf.task_max_deliveries))

    # --- connections -------------------------------------------------------------

    def _channel(self) -> Any:
        if self._pid != os.getpid():
            self.after_fork()
        ch = getattr(self._local, "channel", None)
        if ch is None or not ch.is_open or not ch.connection.is_open:
            conn = pika.BlockingConnection(self.params)
            ch = conn.channel()
            ch.confirm_delivery()
            self._local.channel = ch
            self._declared = set()
            self._delay_ready = False
        return ch

    def after_fork(self) -> None:
        _INHERITED.append((self._local, self._leader))
        self._local = threading.local()
        self._leader = None
        self._holding_token = False
        self._declared = set()
        self._delay_ready = False
        self._pid = os.getpid()

    def close(self) -> None:
        ch = getattr(self._local, "channel", None)
        if ch is not None and self._pid == os.getpid():
            try:
                ch.connection.close()
            except Exception:
                pass
        self._local = threading.local()
        if self._leader is not None and self._pid == os.getpid():
            try:
                self._leader[0].close()
            except Exception:
                pass
        self._leader = None

    def setup(self) -> None:
        ch = self._channel()
        self.declare_queue(ch, self.app.conf.task_default_queue)

    # --- topology ------------------------------------------------------------------

    def queue_arguments(self) -> dict[str, Any]:
        return {
            "x-queue-type": "quorum",
            "x-delivery-limit": self.delivery_limit,
            "x-dead-letter-exchange": DLX,
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        }

    def declare_queue(self, ch: Any, name: str) -> None:
        if name in self._declared:
            return
        _check_queue_name(name)
        ch.exchange_declare(DLX, "direct", durable=True)
        ch.exchange_declare(DELIVERY_EXCHANGE, "topic", durable=True)
        args = {**self.queue_arguments(), "x-dead-letter-routing-key": name}
        try:
            ch.queue_declare(name, durable=True, arguments=args)
        except AMQPChannelError as exc:
            self._reset_channel()
            raise ImproperlyConfigured(
                f"RabbitMQ queue {name!r} already exists with different arguments (for example a classic "
                f"queue created by Celery): {exc}. potatoq needs its own quorum queues; use a new queue name "
                "(task_default_queue / -Q) or delete the old queue once it's drained."
            ) from exc
        ch.queue_declare(f"{name}.dlq", durable=True, arguments={"x-queue-type": "quorum", "x-delivery-limit": -1})
        ch.queue_bind(f"{name}.dlq", DLX, routing_key=name)
        ch.queue_bind(name, DELIVERY_EXCHANGE, routing_key=f"#.{name}")
        self._declared.add(name)

    def _reset_channel(self) -> Any:
        self._local.channel = None
        return self._channel()

    def declare_delay_infrastructure(self, ch: Any) -> None:
        """NServiceBus-style binary cascade of TTL queues (idempotent)."""
        if self._delay_ready:
            return
        ch.exchange_declare(DELIVERY_EXCHANGE, "topic", durable=True)
        key = "1.#"
        for level in range(DELAY_LEVELS - 1, -1, -1):
            name = DELAY_EXCHANGE.format(level)
            ch.exchange_declare(name, "topic", durable=True)
            ch.queue_declare(
                name,
                durable=True,
                arguments={
                    "x-queue-type": "quorum",
                    "x-dead-letter-strategy": "at-least-once",
                    "x-overflow": "reject-publish",
                    "x-message-ttl": (1 << level) * 1000,
                    "x-dead-letter-exchange": DELAY_EXCHANGE.format(level - 1) if level else DELIVERY_EXCHANGE,
                },
            )
            ch.queue_bind(name, name, routing_key=key)
            key = "*." + key
        key = "0.#"
        for level in range(DELAY_LEVELS - 1, 0, -1):
            ch.exchange_bind(
                destination=DELAY_EXCHANGE.format(level - 1), source=DELAY_EXCHANGE.format(level), routing_key=key
            )
            key = "*." + key
        ch.exchange_bind(destination=DELIVERY_EXCHANGE, source=DELAY_EXCHANGE.format(0), routing_key=key)
        self._delay_ready = True

    # --- publishing ------------------------------------------------------------------

    @staticmethod
    def properties(message: Message, headers: dict[str, Any] | None = None) -> pika.BasicProperties:
        return pika.BasicProperties(
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=2,
            message_id=message.id,
            # Quorum queues treat "no priority" as 4; always set it explicitly.
            priority=max(0, min(31, int(message.priority))),
            headers={**({"potatoq-eta": int(message.eta * 1000)} if message.eta else {}), **(headers or {})},
        )

    def publish_on(self, ch: Any, message: Message, headers: dict[str, Any] | None = None) -> None:
        """Publish with confirms; route through the delay cascade when needed."""
        self.declare_queue(ch, message.queue)
        body = message.encode().encode()
        props = self.properties(message, headers)
        delay = message.eta - time.time() if message.eta else 0
        if delay >= 1.0:
            self.declare_delay_infrastructure(ch)
            exchange, routing_key = delay_route(math.ceil(delay), message.queue)
        else:
            exchange, routing_key = "", message.queue
        try:
            ch.basic_publish(exchange, routing_key, body, props, mandatory=True)
        except UnroutableError:
            self._declared.discard(message.queue)
            self.declare_queue(ch, message.queue)
            ch.basic_publish(exchange, routing_key, body, props, mandatory=True)

    def enqueue(self, messages: list[Message], connection: Any = None) -> None:
        for attempt in range(2):
            try:
                ch = self._channel()
                for message in messages:
                    self.publish_on(ch, message)
                return
            except (AMQPConnectionError, AMQPChannelError) as exc:
                if attempt:
                    raise
                logger.info("RabbitMQ connection lost (%s); reconnecting", exc)
                self._local.channel = None

    def consumer(self, queues: list[str], worker_id: str, pid: int | None = None) -> RabbitMQConsumer:
        return RabbitMQConsumer(self, queues, worker_id, pid)

    # --- periodic tasks: single-active-consumer token ------------------------------

    def _leader_poll(self) -> bool:
        try:
            if self._leader is None:
                conn = pika.BlockingConnection(self.params)
                ch = conn.channel()
                ch.queue_declare(
                    LEADER_QUEUE,
                    durable=True,
                    arguments={
                        "x-queue-type": "quorum",
                        "x-single-active-consumer": True,
                        "x-max-length": 1,
                        "x-overflow": "drop-head",
                    },
                )
                ch.basic_publish("", LEADER_QUEUE, b"token", pika.BasicProperties(delivery_mode=2))
                ch.basic_qos(prefetch_count=1)

                def on_token(*args: Any) -> None:
                    # Never acked: holding it unacked is what makes us the leader. If
                    # we die it is redelivered to the next active consumer.
                    if not self._holding_token:
                        logger.info("This worker now runs the periodic task scheduler")
                    self._holding_token = True

                ch.basic_consume(LEADER_QUEUE, on_token, auto_ack=False)
                self._leader = (conn, ch)
            self._leader[0].process_data_events(time_limit=0)
        except AMQPError:
            logger.warning("Lost scheduler leadership connection; will retry", exc_info=True)
            self._leader = None
            self._holding_token = False
        return self._holding_token

    def tick(self) -> None:
        if self._leader is not None:
            self._leader_poll()

    def enqueue_periodic(self, name: str, fire_at: float, message: Message) -> bool:
        if not self._leader_poll():
            return False
        key = (name, fire_at)
        if key in self._fired:
            return False
        self._fired[key] = None
        while len(self._fired) > 10_000:
            self._fired.pop(next(iter(self._fired)))
        self.enqueue([message])
        return True

    # --- results / coordination via the result backend -------------------------------

    def _backend(self) -> Broker | None:
        backend = self.app.backend
        return backend if backend is not None and backend is not self else None

    def store_result(self, record: ResultRecord, expires: float | None) -> None:
        raise ImproperlyConfigured("RabbitMQ can't store results; set result_backend")

    def get_result(self, task_id: str) -> ResultRecord | None:
        raise ImproperlyConfigured("RabbitMQ can't store results; set result_backend")

    def heartbeat(self, worker_id: str, info: dict[str, Any]) -> None:
        backend = self._backend()
        if backend is not None:
            backend.heartbeat(worker_id, info)

    def unregister(self, worker_id: str) -> None:
        backend = self._backend()
        if backend is not None:
            backend.unregister(worker_id)

    def workers(self) -> list[dict[str, Any]]:
        backend = self._backend()
        return backend.workers() if backend is not None else []

    def chord_part_done(self, group_id: str, index: int, size: int, result: Any) -> list[Any] | None:
        backend = self._backend()
        if backend is None:
            raise ImproperlyConfigured("Chords need a result backend with RabbitMQ; set result_backend")
        return backend.chord_part_done(group_id, index, size, result)

    def revoke(self, task_ids: list[str], expires: float) -> None:
        """Messages can't be removed from a RabbitMQ queue; mark them revoked in the
        result backend instead and workers skip them when they come up."""
        backend = self._backend()
        if backend is None:
            raise ImproperlyConfigured("Revoking tasks with RabbitMQ needs a result backend; set result_backend")
        for task_id in task_ids:
            backend.store_result(ResultRecord(task_id=task_id, state=states.REVOKED, date_done=time.time()), expires)

    # --- inspection --------------------------------------------------------------

    def _known_queues(self) -> set[str]:
        names = {self.app.conf.task_default_queue, *self._declared}
        routes = self.app.conf.task_routes
        if isinstance(routes, dict):
            for opts in routes.values():
                q = opts if isinstance(opts, str) else opts.get("queue")
                if q:
                    names.add(q)
        return names

    def queue_sizes(self) -> dict[str, int]:
        sizes = {}
        for name in sorted(self._known_queues()):
            try:
                ok = self._channel().queue_declare(name, passive=True)
                if ok.method.message_count:
                    sizes[name] = ok.method.message_count
            except AMQPChannelError:
                self._local.channel = None
        return sizes

    def purge(self, queue: str) -> int:
        return self._channel().queue_purge(queue).method.message_count

    def _drain_dlq(self, visit: Callable[[Any, Any, Any, bytes], bool], limit: int) -> None:
        """Look at dead letters; ``visit`` returns True to ack (remove) the message."""
        ch = self._channel()
        for name in sorted(self._known_queues()):
            seen = 0
            tags = []
            while seen < limit:
                try:
                    method, props, body = ch.basic_get(f"{name}.dlq", auto_ack=False)
                except AMQPChannelError:
                    ch = self._reset_channel()
                    break
                if method is None:
                    break
                seen += 1
                if visit(ch, method, props, body):
                    ch.basic_ack(method.delivery_tag)
                else:
                    tags.append(method.delivery_tag)
            for tag in tags:
                ch.basic_nack(tag, requeue=True)

    def dead_letters(self, limit: int = 100) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []

        def visit(ch: Any, method: Any, props: Any, body: bytes) -> bool:
            message = serialization.loads(body)
            headers = props.headers or {}
            reason = headers.get("potatoq-reason")
            if reason is None and headers.get("x-death"):
                reason = str(headers["x-death"][0].get("reason"))
            died = (headers.get("potatoq-died") or time.time() * 1000) / 1000
            out.append({"id": props.message_id or message.get("id"), "queue": message.get("queue"), "task": message.get("task"),
                        "reason": reason, "died_at": float(died), "message": message})  # fmt: skip
            return False

        self._drain_dlq(visit, limit)
        return sorted(out, key=lambda e: -e["died_at"])[:limit]

    def requeue_dead(self, task_id: str) -> bool:
        found = []

        def visit(ch: Any, method: Any, props: Any, body: bytes) -> bool:
            if found or (props.message_id != task_id):
                return False
            message = Message.decode(body)
            message.eta = None
            self.publish_on(ch, message)
            found.append(message)
            return True

        self._drain_dlq(visit, 100_000)
        return bool(found)


class RabbitMQConsumer(Consumer):
    """Consumes in a background I/O thread; the worker's main thread runs tasks."""

    broker: RabbitMQBroker
    can_settle_foreign = False

    def __init__(self, broker: RabbitMQBroker, queues: list[str], worker_id: str, pid: int | None = None):
        super().__init__(broker, queues, worker_id, pid)
        self._inbox: queue_mod.Queue[tuple[str, Any, Any, bytes]] = queue_mod.Queue()
        self._conn: Any = None
        self._ch: Any = None
        self._thread: threading.Thread | None = None
        self._closing = False
        self._broken = False
        self._consumer_tags: dict[str, str] = {}
        self._paused: set[str] = set()
        self._interrupted = False

    # --- I/O thread ------------------------------------------------------------------

    def _ensure_started(self) -> None:
        if self._thread is not None and self._thread.is_alive() and not self._broken:
            return
        self._broken = False
        self._inbox = queue_mod.Queue()
        self._conn = pika.BlockingConnection(self.broker.params)
        self._ch = self._conn.channel()
        self._ch.confirm_delivery()
        self._ch.basic_qos(prefetch_count=1)
        self.broker._declared = set()
        self.broker._delay_ready = False
        for name in self.queues:
            self.broker.declare_queue(self._ch, name)
        self._consumer_tags = {}
        self._paused = set()
        for name in self.queues:
            self._consume(name)
        self._thread = threading.Thread(target=self._io_loop, name="potatoq-amqp-io", daemon=True)
        self._thread.start()

    def _consume_arguments(self) -> dict[str, Any]:
        limit = self.broker.app.conf.task_time_limit
        timeout_ms = int(((float(limit) if limit else 24 * 3600) + 300) * 1000)
        return {"x-consumer-timeout": timeout_ms}

    def _consume(self, name: str) -> None:
        def on_message(ch: Any, method: Any, props: Any, body: bytes) -> None:
            self._inbox.put((name, method, props, body))

        try:
            tag = self._ch.basic_consume(name, on_message, auto_ack=False, arguments=self._consume_arguments())
        except AMQPChannelError:
            # Older servers may reject consumer arguments.
            self._ch = self._conn.channel()
            self._ch.confirm_delivery()
            self._ch.basic_qos(prefetch_count=1)
            tag = self._ch.basic_consume(name, on_message, auto_ack=False)
        self._consumer_tags[name] = tag

    def _io_loop(self) -> None:
        try:
            while not self._closing:
                self._conn.process_data_events(time_limit=0.2)
        except Exception as exc:
            if not self._closing:
                logger.warning("RabbitMQ consumer connection lost: %s", exc)
            self._broken = True

    def _call(self, fn: Callable[[], Any]) -> Any:
        """Run ``fn`` on the I/O thread and wait for it."""
        if self._broken or self._thread is None or not self._thread.is_alive():
            raise AMQPConnectionError("consumer connection is down")
        done = threading.Event()
        box: dict[str, Any] = {}

        def run() -> None:
            try:
                box["value"] = fn()
            except BaseException as exc:
                box["error"] = exc
            finally:
                done.set()

        self._conn.add_callback_threadsafe(run)
        if not done.wait(timeout=60):
            raise AMQPConnectionError("timed out talking to RabbitMQ")
        if "error" in box:
            raise box["error"]
        return box.get("value")

    # --- consumer API ------------------------------------------------------------

    def fetch(self, timeout: float) -> Delivery | None:
        self._interrupted = False
        try:
            self._ensure_started()
            if self._paused:
                self._call(self._resume_all)
        except AMQPError as exc:
            logger.warning("Can't connect to RabbitMQ: %s", exc)
            self._broken = True
            time.sleep(min(timeout, 1.0))
            return None
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self._interrupted or self._broken:
                return None
            try:
                name, method, props, body = self._inbox.get(timeout=min(remaining, 0.25))
            except queue_mod.Empty:
                continue
            if len(self.queues) > 1:
                # Stop the other queues from handing us messages we can't run yet.
                try:
                    self._call(lambda keep=name: self._pause_others(keep))
                except AMQPError:
                    pass
            headers = props.headers or {}
            count = 1 + max(int(headers.get("x-delivery-count") or 0), int(headers.get("x-acquired-count") or 0) - 1, 0)
            try:
                message = Message.decode(body)
            except Exception:
                logger.exception("Undecodable message in %s; dead-lettering it", name)
                self._call(lambda tag=method.delivery_tag: self._ch.basic_reject(tag, requeue=False))
                continue
            return Delivery(message, delivery_count=count, handle=method.delivery_tag)

    def _pause_others(self, keep: str) -> None:
        for name, tag in list(self._consumer_tags.items()):
            if name != keep and name not in self._paused:
                self._ch.basic_cancel(tag)
                self._paused.add(name)
        # Give back anything the paused consumers already delivered.
        leftovers = deque()
        while True:
            try:
                leftovers.append(self._inbox.get_nowait())
            except queue_mod.Empty:
                break
        for _, method, _, _ in leftovers:
            self._ch.basic_nack(method.delivery_tag, requeue=True)

    def _resume_all(self) -> None:
        for name in list(self._paused):
            self._consume(name)
        self._paused.clear()

    def interrupt(self) -> None:
        self._interrupted = True

    def _publish_and_ack(
        self, delivery: Delivery, messages: list[Message], headers: dict[str, Any] | None = None
    ) -> None:
        def run() -> None:
            for m in messages:
                self.broker.publish_on(self._ch, m, headers)
            self._ch.basic_ack(delivery.handle)

        self._call(run)

    def complete(self, delivery: Delivery, record: ResultRecord | None, followups: list[Message]) -> None:
        self._publish_and_ack(delivery, followups)

    def retry(self, delivery: Delivery, message: Message, record: ResultRecord | None) -> None:
        self._publish_and_ack(delivery, [message])

    def requeue(self, delivery: Delivery, count: bool = False) -> None:
        tag = delivery.handle
        if count:
            self._call(lambda: self._ch.basic_reject(tag, requeue=True))
        else:
            # On RabbitMQ >= 4.3 a nack does not count towards the delivery limit.
            self._call(lambda: self._ch.basic_nack(tag, requeue=True))

    def dead_letter(
        self, delivery: Delivery, reason: str, record: ResultRecord | None, followups: list[Message] | None = None
    ) -> None:
        message = delivery.message

        def run() -> None:
            for m in followups or []:
                self.broker.publish_on(self._ch, m)
            props = self.broker.properties(
                message, {"potatoq-reason": reason[-2000:], "potatoq-died": int(time.time() * 1000)}
            )
            self._ch.basic_publish(DLX, message.queue, message.encode().encode(), props, mandatory=True)
            self._ch.basic_ack(delivery.handle)

        self._call(run)

    def close(self) -> None:
        self._closing = True
        if self._thread is not None:
            self._thread.join(timeout=2)
        try:
            if self._conn is not None and self._conn.is_open:
                # Return anything delivered but never started.
                while True:
                    try:
                        _, method, _, _ = self._inbox.get_nowait()
                    except queue_mod.Empty:
                        break
                    self._ch.basic_nack(method.delivery_tag, requeue=True)
                self._conn.close()
        except Exception:
            pass
