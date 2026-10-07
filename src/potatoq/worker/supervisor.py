"""The worker's main process: forks children, watches them, keeps the node alive.

The supervisor is deliberately single-threaded (so forking replacement children is
always safe) and never runs tasks. Its loop:

* reads "started"/"finished" events from each child's pipe, so it always knows which
  task every child is running and when its hard time limit expires;
* SIGKILLs children that blow their hard time limit and records the failure;
* when a child dies unexpectedly (OOM killer, segfault), immediately requeues its
  task, or dead-letters it if it already crashed workers ``task_max_deliveries`` times;
* heartbeats the node and extends task leases, recovers tasks of dead nodes,
  promotes scheduled tasks and runs the deduplicated periodic-task scheduler;
* on SIGTERM stops fetching, waits ``worker_shutdown_timeout`` for running tasks,
  then interrupts and requeues whatever is left (Kubernetes/Heroku friendly).
"""

from __future__ import annotations

import errno
import json
import logging
import math
import os
import random
import selectors
import signal
import socket
import sys
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .. import signals
from ..brokers.base import Delivery
from ..exceptions import TimeLimitExceeded, WorkerLostError
from ..message import Message
from . import executor
from .child import child_main
from .scheduler import Scheduler

if TYPE_CHECKING:
    from ..app import Potatoq

logger = logging.getLogger("potatoq.worker")


def default_concurrency() -> int:
    """CPUs actually available to this process: honours affinity and cgroup quotas
    (Celery uses the host's CPU count, which over-subscribes containers)."""
    if hasattr(os, "process_cpu_count"):
        n = os.process_cpu_count() or 1
    elif hasattr(os, "sched_getaffinity"):
        n = len(os.sched_getaffinity(0))
    else:
        n = os.cpu_count() or 1
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            quota, period = f.read().split()
        if quota != "max":
            n = min(n, max(1, math.ceil(int(quota) / int(period))))
    except (OSError, ValueError):
        pass
    return max(1, n)


def parse_memory(value: Any) -> int | None:
    """``"512MB"``/``"2GiB"``/``262144`` (KiB, like Celery) -> KiB."""
    if value in (None, "", 0):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().upper().replace("IB", "B")
    units = {"KB": 1, "K": 1, "MB": 1024, "M": 1024, "GB": 1024**2, "G": 1024**2}
    for suffix, factor in sorted(units.items(), key=lambda kv: -len(kv[0])):
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * factor)
    return int(text)


@dataclass
class ChildProc:
    pid: int
    index: int
    read_fd: int
    started_at: float = field(default_factory=time.monotonic)
    buffer: bytes = b""
    delivery: Delivery | None = None
    hard_deadline: float | None = None
    task_started: float | None = None
    killed_for_timeout: bool = False
    term_sent: bool = False
    abort_sent: bool = False


class Supervisor:
    def __init__(
        self,
        app: Potatoq,
        *,
        concurrency: int | None = None,
        queues: list[str] | str | None = None,
        hostname: str | None = None,
        loglevel: str | None = None,
        logfile: str | None = None,
        max_tasks_per_child: int | None = -1,
        max_memory_per_child: Any = -1,
        scheduler: bool | None = None,
        shutdown_timeout: float | None = None,
        **kwargs: Any,
    ):
        self.app = app
        conf = app.conf
        self.concurrency = int(concurrency or conf.worker_concurrency or default_concurrency())
        if isinstance(queues, str):
            queues = [q.strip() for q in queues.split(",") if q.strip()]
        self.queues = list(queues or [conf.task_default_queue])
        self.hostname = hostname or f"potatoq@{socket.gethostname()}"
        self.node_id = f"{self.hostname}:{os.getpid()}:{random.randrange(16**6):06x}"
        self.loglevel = loglevel
        self.logfile = logfile
        self.max_tasks_per_child = conf.worker_max_tasks_per_child if max_tasks_per_child == -1 else max_tasks_per_child
        self.max_memory_kib = parse_memory(
            conf.worker_max_memory_per_child if max_memory_per_child == -1 else max_memory_per_child
        )
        self.scheduler_enabled = conf.worker_enable_scheduler if scheduler is None else scheduler
        self.shutdown_timeout = conf.worker_shutdown_timeout if shutdown_timeout is None else shutdown_timeout
        self.children: dict[int, ChildProc] = {}
        self.selector = selectors.DefaultSelector()
        self.shutting_down = False
        self.cold = False
        self.shutdown_deadline: float | None = None
        self._wake_r, self._wake_w = os.pipe()
        self._recent_crashes: list[float] = []
        self.consumer: Any = None
        self.scheduler: Scheduler | None = None
        self.exitcode = 0

    # --- lifecycle -----------------------------------------------------------------

    def start(self) -> int:
        app = self.app
        if self.loglevel or not logging.getLogger().handlers:
            from ..log import setup_logging

            setup_logging(app, self.loglevel or "INFO", self.logfile)
        app.loader_import_default_modules()
        signals.worker_init.send(sender=self)
        broker = app.broker  # connects and creates the schema
        self.consumer = broker.consumer(self.queues, self.node_id, pid=0)
        broker.heartbeat(self.node_id, self._info())
        self._install_signals()
        if self.scheduler_enabled and app.conf.beat_schedule:
            self.scheduler = Scheduler(app)
            self.scheduler.start()
        self._banner()
        # Children must not inherit our broker connections.
        broker.close()
        for index in range(self.concurrency):
            self._spawn(index)
        signals.worker_ready.send(sender=self)
        try:
            self._loop()
        finally:
            self._finish()
        return self.exitcode

    def _info(self) -> dict[str, Any]:
        return {
            "hostname": self.hostname,
            "pid": os.getpid(),
            "queues": self.queues,
            "concurrency": self.concurrency,
            "running": [c.delivery.message.id for c in self.children.values() if c.delivery],
        }

    def _banner(self) -> None:
        from .. import __version__

        broker_url = _redact(self.app.broker.url)
        backend = self.app.backend
        logger.info(
            "potatoq %s worker %s ready: broker=%s results=%s queues=%s concurrency=%d (prefork) "
            "time_limit=%ss max_tasks_per_child=%s scheduler=%s",
            __version__, self.hostname, broker_url, _redact(backend.url) if backend else "disabled",
            ",".join(self.queues), self.concurrency, self.app.conf.task_time_limit, self.max_tasks_per_child,
            "on" if self.scheduler else "off",
        )  # fmt: skip
        tasks = sorted(n for n in self.app.tasks if not n.startswith("potatoq."))
        logger.info("Registered tasks: %s", ", ".join(tasks) or "(none)")

    def _install_signals(self) -> None:
        os.set_blocking(self._wake_w, False)
        os.set_blocking(self._wake_r, False)
        signal.set_wakeup_fd(self._wake_w, warn_on_full_buffer=False)
        self.selector.register(self._wake_r, selectors.EVENT_READ, None)
        signal.signal(signal.SIGTERM, self._on_term)
        signal.signal(signal.SIGINT, self._on_int)
        signal.signal(signal.SIGQUIT, self._on_cold)
        signal.signal(signal.SIGCHLD, lambda *a: None)  # wakes select via the wakeup fd

    def _on_term(self, signum: int, frame: Any) -> None:
        self._begin_shutdown(cold=False)

    def _on_int(self, signum: int, frame: Any) -> None:
        self._begin_shutdown(cold=self.shutting_down)  # second Ctrl-C = cold

    def _on_cold(self, signum: int, frame: Any) -> None:
        self._begin_shutdown(cold=True)

    def _begin_shutdown(self, cold: bool) -> None:
        if not self.shutting_down:
            logger.info("Warm shutdown: waiting up to %ss for running tasks", self.shutdown_timeout)
            signals.worker_shutting_down.send(sender=self.hostname, sig="SIGTERM", how="Warm", exitcode=0)
            self.shutting_down = True
            self.shutdown_deadline = time.monotonic() + self.shutdown_timeout
            for child in self.children.values():
                self._kill(child, signal.SIGTERM)
                child.term_sent = True
        if cold and not self.cold:
            logger.info("Cold shutdown: interrupting running tasks")
            self.cold = True
            self.shutdown_deadline = time.monotonic()

    # --- children ------------------------------------------------------------------

    def _spawn(self, index: int) -> None:
        read_fd, write_fd = os.pipe()
        sys.stdout.flush()
        sys.stderr.flush()
        pid = os.fork()
        if pid == 0:  # child
            os.close(read_fd)
            os.close(self._wake_r)
            os.close(self._wake_w)
            signal.set_wakeup_fd(-1)
            child_main(
                self.app, node_id=self.node_id, queues=self.queues, write_fd=write_fd, index=index,
                hostname=self.hostname, max_tasks=self.max_tasks_per_child, max_memory_kib=self.max_memory_kib,
            )  # fmt: skip
        os.close(write_fd)
        os.set_blocking(read_fd, False)
        child = ChildProc(pid=pid, index=index, read_fd=read_fd)
        self.children[pid] = child
        self.selector.register(read_fd, selectors.EVENT_READ, child)

    def _kill(self, child: ChildProc, sig: int) -> None:
        try:
            os.kill(child.pid, sig)
        except ProcessLookupError:
            pass

    def _read_child(self, child: ChildProc) -> None:
        while True:
            try:
                chunk = os.read(child.read_fd, 65536)
            except BlockingIOError:
                break
            except OSError as exc:
                if exc.errno == errno.EINTR:
                    continue
                break
            if not chunk:
                break
            child.buffer += chunk
        *lines, child.buffer = child.buffer.split(b"\n")
        for line in lines:
            if not line:
                continue
            event = json.loads(line)
            if event["e"] == "start":
                child.delivery = Delivery(
                    Message.from_dict(event["message"]),
                    delivery_count=event["count"],
                    handle=_to_handle(event["handle"]),
                )
                child.task_started = event["started"]
                hard = event.get("hard")
                # A little grace so the soft limit's exception can be handled first.
                child.hard_deadline = time.monotonic() + (hard - event["started"]) + 1.0 if hard else None
            elif event["e"] == "done":
                child.delivery = None
                child.hard_deadline = None
                child.task_started = None

    def _reap(self) -> None:
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return
            if pid == 0:
                return
            child = self.children.pop(pid, None)
            if child is None:
                continue
            self._read_child(child)  # final events
            self.selector.unregister(child.read_fd)
            os.close(child.read_fd)
            self._on_child_exit(child, status)

    def _on_child_exit(self, child: ChildProc, status: int) -> None:
        code = os.waitstatus_to_exitcode(status)
        lost: list[Delivery] = []
        if child.delivery is not None:
            lost.append(child.delivery)
        if self.consumer.can_settle_foreign:
            known = {d.message.id for d in lost}
            try:
                lost += [
                    d for d in self.app.broker.lost_deliveries(self.node_id, child.pid) if d.message.id not in known
                ]
            except Exception:
                logger.exception("Could not look up tasks of dead child %d", child.pid)
        for delivery in lost:
            self._handle_lost(child, delivery, code)
        if code != 0 and not child.killed_for_timeout and not (self.shutting_down and child.abort_sent):
            logger.error("Child %d exited unexpectedly (code %s)", child.pid, code)
            self._recent_crashes = [t for t in self._recent_crashes if time.monotonic() - t < 10] + [time.monotonic()]
        if not self.shutting_down:
            if len(self._recent_crashes) > 3 * self.concurrency:
                logger.critical("Children keep crashing; backing off for 1s")
                time.sleep(1)
                self._recent_crashes.clear()
            self._spawn(child.index)

    def _handle_lost(self, child: ChildProc, delivery: Delivery, code: int) -> None:
        app = self.app
        message = delivery.message
        consumer = self.consumer
        if child.killed_for_timeout:
            hard = (time.time() - child.task_started) if child.task_started else None
            exc: BaseException = TimeLimitExceeded(
                f"Task {message.task}[{message.id}] exceeded its time limit ({hard:.0f}s) and was killed"
                if hard
                else "time limit exceeded"
            )
            logger.error("%s", exc)
            outcome = executor.failure_outcome(app, message, exc, self.hostname)
        elif self.shutting_down and child.abort_sent:
            logger.warning("Requeueing %s[%s] interrupted by shutdown", message.task, message.id)
            if consumer.can_settle_foreign:
                consumer.requeue(delivery, count=False)
            return
        else:
            limit = int(app.conf.task_max_deliveries)
            if delivery.delivery_count < limit and app.conf.task_reject_on_worker_lost:
                logger.warning(
                    "Process running %s[%s] died (code %s); requeueing (delivery %d of %d)",
                    message.task, message.id, code, delivery.delivery_count, limit,
                )  # fmt: skip
                if consumer.can_settle_foreign:
                    consumer.requeue(delivery, count=True)
                return
            exc = WorkerLostError(
                f"Worker exited prematurely (code {code}) while running {message.task}[{message.id}], {delivery.delivery_count} time(s)"
            )
            logger.error("%s; dead-lettering", exc)
            outcome = executor.failure_outcome(app, message, exc, self.hostname)
        if consumer.can_settle_foreign:
            executor.settle(app, consumer, delivery, outcome)
        elif outcome.record is not None and app.backend is not None:
            # e.g. RabbitMQ: the broker redelivers by itself; the stored final result
            # makes the redelivered copy a no-op (see executor.execute).
            app.backend.store_result(outcome.record, expires=app.conf.result_expires)

    # --- main loop -----------------------------------------------------------------

    def _loop(self) -> None:
        conf = self.app.conf
        broker = self.app.broker
        now = time.monotonic()
        heartbeat_every = float(conf.worker_heartbeat_interval)
        timers = {
            "heartbeat": now + heartbeat_every,
            "recover": now + random.uniform(1, 5),
            "tick": now + 1.0,
            "maintenance": now + random.uniform(30, 90),
            "scheduler": now + 0.5,
        }
        while True:
            now = time.monotonic()
            if self.shutting_down:
                if not self.children:
                    return
                self._shutdown_step(now)
            timeout = max(0.0, min(min(timers.values()) - now, 0.5))
            for key, _ in self.selector.select(timeout):
                if key.data is None:
                    try:
                        while os.read(self._wake_r, 512):
                            pass
                    except BlockingIOError:
                        pass
                else:
                    self._read_child(key.data)
            self._reap()
            now = time.monotonic()
            self._enforce_time_limits(now)
            try:
                if now >= timers["heartbeat"]:
                    timers["heartbeat"] = now + heartbeat_every
                    broker.heartbeat(self.node_id, self._info())
                    running = [c.delivery for c in self.children.values() if c.delivery is not None]
                    if running:
                        broker.extend(running)
                if self.shutting_down:
                    continue
                if now >= timers["tick"]:
                    timers["tick"] = now + 1.0
                    broker.tick()
                if now >= timers["recover"]:
                    timers["recover"] = now + random.uniform(5, 15)
                    for delivery in broker.recover(float(conf.worker_dead_after)):
                        exc = WorkerLostError(
                            f"Worker node died while running {delivery.message.task}[{delivery.message.id}] {delivery.delivery_count} time(s)"
                        )
                        outcome = executor.failure_outcome(self.app, delivery.message, exc, self.hostname)
                        if outcome.record is not None and self.app.backend is not None:
                            self.app.backend.store_result(outcome.record, expires=conf.result_expires)
                        if outcome.followups:
                            self.app.publish(outcome.followups)
                if now >= timers["maintenance"]:
                    timers["maintenance"] = now + random.uniform(45, 75)
                    broker.maintenance()
                if self.scheduler is not None and now >= timers["scheduler"]:
                    self.scheduler.tick()
                    timers["scheduler"] = now + min(1.0, max(0.05, self.scheduler.seconds_until_next()))
            except Exception:
                logger.exception("Supervisor housekeeping failed (will retry)")

    def _enforce_time_limits(self, now: float) -> None:
        for child in list(self.children.values()):
            if child.hard_deadline is not None and now >= child.hard_deadline and not child.killed_for_timeout:
                child.killed_for_timeout = True
                logger.error("Hard time limit exceeded; killing child %d", child.pid)
                self._kill(child, signal.SIGKILL)

    def _shutdown_step(self, now: float) -> None:
        if self.shutdown_deadline is None or now < self.shutdown_deadline:
            return
        for child in self.children.values():
            if not child.abort_sent:
                child.abort_sent = True
                self._kill(child, signal.SIGUSR1)  # interrupt the task; it gets requeued
        if now >= self.shutdown_deadline + 5.0:
            for child in self.children.values():
                self._kill(child, signal.SIGKILL)

    def _finish(self) -> None:
        for child in list(self.children.values()):
            self._kill(child, signal.SIGKILL)
        deadline = time.monotonic() + 5
        while self.children and time.monotonic() < deadline:
            self._reap()
            time.sleep(0.05)
        try:
            self.app.broker.unregister(self.node_id)
        except Exception:
            logger.exception("Could not unregister worker")
        signals.worker_shutdown.send(sender=self)
        logger.info("Worker %s stopped", self.hostname)


def _to_handle(handle: Any) -> Any:
    return tuple(handle) if isinstance(handle, list) else handle


def _redact(url: str) -> str:
    if "@" in url and "://" in url:
        scheme, rest = url.split("://", 1)
        creds, host = rest.rsplit("@", 1)
        user = creds.split(":", 1)[0]
        return f"{scheme}://{user}:***@{host}" if ":" in creds else f"{scheme}://{user}@{host}"
    return url
