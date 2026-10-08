"""The interface every backend implements.

A broker does three jobs:

* **queueing** - enqueue (now or later), reliably fetch, ack, retry, dead-letter
* **results**  - store and wait for task results (optional, ``supports_results``)
* **coordination** - worker liveness, recovery of tasks held by dead workers,
  chord counters, revocation, and deduplicated periodic-task fire times

Each backend implements these with whatever its native primitives are best at
(Redis Lua + sorted sets, RabbitMQ quorum queues and dead-lettering, Postgres
``SKIP LOCKED`` + ``LISTEN/NOTIFY``, SQLite ``BEGIN IMMEDIATE`` + WAL).
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .. import states
from ..message import Message

if TYPE_CHECKING:
    from ..app import Potatoq


@dataclass(slots=True)
class Delivery:
    """A message handed to a worker. ``handle`` is broker specific."""

    message: Message
    #: 1 on first delivery; incremented every time the task is redelivered because
    #: the process running it died. Not incremented by retries.
    delivery_count: int = 1
    handle: Any = None
    received_at: float = field(default_factory=time.time)


@dataclass(slots=True)
class ResultRecord:
    task_id: str
    state: str = states.PENDING
    result: Any = None
    traceback: str | None = None
    meta: dict[str, Any] | None = None
    date_done: float | None = None
    task_name: str | None = None
    args: Any = None
    kwargs: Any = None
    retries: int = 0
    worker: str | None = None
    date_started: float | None = None
    enqueued_at: float | None = None

    @property
    def ready(self) -> bool:
        return self.state in states.READY_STATES

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.state,
            "result": self.result,
            "traceback": self.traceback,
            "meta": self.meta,
            "date_done": self.date_done,
            "name": self.task_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "retries": self.retries,
            "worker": self.worker,
            "date_started": self.date_started,
            "enqueued_at": self.enqueued_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ResultRecord:
        return cls(
            task_id=data["task_id"],
            state=data.get("status", states.PENDING),
            result=data.get("result"),
            traceback=data.get("traceback"),
            meta=data.get("meta"),
            date_done=data.get("date_done"),
            task_name=data.get("name"),
            args=data.get("args"),
            kwargs=data.get("kwargs"),
            retries=data.get("retries", 0),
            worker=data.get("worker"),
            date_started=data.get("date_started"),
            enqueued_at=data.get("enqueued_at"),
        )


class Consumer:
    """A worker process's connection to the broker. Not thread-safe; one per process.

    ``worker_id`` identifies the worker node (supervisor); ``pid`` the child process.
    Brokers that track ownership use both so a dead child's tasks can be recovered
    immediately and a dead node's tasks once its heartbeat goes stale.
    """

    #: False when only the process that fetched a delivery can settle it (RabbitMQ:
    #: unacked messages belong to the channel). The broker then redelivers tasks of
    #: dead processes by itself.
    can_settle_foreign = True

    def __init__(self, broker: Broker, queues: list[str], worker_id: str, pid: int | None = None):
        self.broker = broker
        self.queues = queues
        self.worker_id = worker_id
        self.pid = pid if pid is not None else os.getpid()

    def fetch(self, timeout: float) -> Delivery | None:
        """Block up to ``timeout`` seconds for the next due task and claim it."""
        raise NotImplementedError

    def complete(self, delivery: Delivery, record: ResultRecord | None, followups: list[Message]) -> None:
        """The task finished (successfully or terminally).

        Remove the message, store ``record`` (only passed when this broker is also the
        result backend) and enqueue ``followups``, atomically where the broker allows.
        """
        raise NotImplementedError

    def retry(self, delivery: Delivery, message: Message, record: ResultRecord | None) -> None:
        """Atomically replace the delivered message with ``message`` (which may have an eta)."""
        raise NotImplementedError

    def requeue(self, delivery: Delivery, count: bool = False) -> None:
        """Give the message back so another worker runs it.

        ``count=False`` (graceful shutdown) does not count as a delivery attempt.
        """
        raise NotImplementedError

    def dead_letter(
        self, delivery: Delivery, reason: str, record: ResultRecord | None, followups: list[Message] | None = None
    ) -> None:
        """Park the message for inspection; it will not run again automatically."""
        raise NotImplementedError

    def interrupt(self) -> None:
        """Called from a signal handler to wake a blocking ``fetch`` (best effort)."""

    def close(self) -> None:
        pass


class Broker:
    #: URL schemes handled by this broker.
    schemes: tuple[str, ...] = ()
    #: Whether results can be stored in this broker.
    supports_results: bool = True
    #: Whether tasks can be enqueued inside the caller's database transaction.
    transactional: bool = False
    #: Exceptions that mean "the broker is unavailable"; publishing wraps them in
    #: :class:`~potatoq.exceptions.OperationalError`.
    connection_errors: tuple[type[BaseException], ...] = (OSError,)
    #: Queued messages can't be deleted, so revocations are checked at run time.
    needs_revoke_check: bool = False
    #: ``enqueue_periodic`` claims outlive a worker restart, so a starting scheduler can
    #: safely send runs that fell due just before it started.
    durable_periodic_claims: bool = True

    def __init__(self, url: str, app: Potatoq, **options: Any):
        self.url = url
        self.app = app
        self.options = options

    # --- lifecycle ----------------------------------------------------------------

    def setup(self) -> None:
        """Create tables / declare queues. Idempotent."""

    def close(self) -> None:
        pass

    def after_fork(self) -> None:
        """Drop connections inherited from the parent process."""

    # --- producing ----------------------------------------------------------------

    def enqueue(self, messages: list[Message], connection: Any = None) -> None:
        raise NotImplementedError

    def consumer(self, queues: list[str], worker_id: str, pid: int | None = None) -> Consumer:
        raise NotImplementedError

    def enqueue_periodic(self, name: str, fire_at: float, message: Message) -> bool:
        """Enqueue ``message`` unless ``(name, fire_at)`` was already enqueued by any
        scheduler. This is what makes it safe to run the scheduler on every worker."""
        raise NotImplementedError

    def last_periodic_runs(self) -> dict[str, float]:
        """The latest fire time each periodic entry was sent for (epoch seconds), as far
        as the broker remembers (claims are kept for a week, a day on Redis)."""
        return {}

    # --- results ------------------------------------------------------------------

    def store_result(self, record: ResultRecord, expires: float | None) -> None:
        raise NotImplementedError

    def get_result(self, task_id: str) -> ResultRecord | None:
        raise NotImplementedError

    def wait_for_result(self, task_id: str, timeout: float | None) -> ResultRecord | None:
        """Block until the task is ready (or ``timeout`` elapses); default polls."""
        deadline = None if timeout is None else time.monotonic() + timeout
        interval = 0.01
        while True:
            record = self.get_result(task_id)
            if record is not None and record.ready:
                return record
            if deadline is not None and time.monotonic() >= deadline:
                return record
            time.sleep(interval if deadline is None else min(interval, max(0.0, deadline - time.monotonic())))
            interval = min(interval * 1.5, 0.5)

    def forget(self, task_id: str) -> None:
        raise NotImplementedError

    def peek(self, task_id: str) -> tuple[Message, str] | None:
        """A task that hasn't finished yet: its message and ``"scheduled"``,
        ``"ready"`` or ``"running"``. None if unknown or not supported (RabbitMQ)."""
        return None

    # --- coordination -------------------------------------------------------------

    def heartbeat(self, worker_id: str, info: dict[str, Any]) -> None:
        """Mark a worker node as alive."""
        raise NotImplementedError

    def unregister(self, worker_id: str) -> None:
        """Clean shutdown of a worker node."""

    def recover(self, worker_dead_after: float) -> list[Delivery]:
        """Requeue tasks held by dead workers. Tasks that already used up their
        deliveries are dead-lettered and returned so the caller can record the
        failure. Safe to call concurrently from every node."""
        return []

    def extend(self, deliveries: list[Delivery]) -> None:
        """Extend the claims (leases) of tasks that are still running."""

    def tick(self) -> None:
        """Frequent housekeeping (about once a second): promote due scheduled tasks."""

    def lost_deliveries(self, worker_id: str, pid: int) -> list[Delivery]:
        """Tasks still claimed by a child process that died."""
        return []

    def maintenance(self) -> None:
        """Periodic housekeeping: expire results, trim dead letters, vacuum..."""

    def workers(self) -> list[dict[str, Any]]:
        return []

    def chord_part_done(self, group_id: str, index: int, size: int, result: Any) -> list[Any] | None:
        """Record a finished chord header task. Returns all results, in order, exactly
        once: to the caller that completed the chord."""
        raise NotImplementedError

    def revoke(self, task_ids: list[str], expires: float) -> None:
        raise NotImplementedError

    # --- inspection ---------------------------------------------------------------

    def queue_sizes(self) -> dict[str, int]:
        return {}

    def purge(self, queue: str) -> int:
        raise NotImplementedError

    def dead_letters(self, limit: int = 100) -> list[dict[str, Any]]:
        return []
