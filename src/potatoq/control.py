"""``app.control``: the parts of Celery's remote control that matter in practice."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .app import Potatoq

logger = logging.getLogger("potatoq")

_REGISTERED = "potatoq-registered-"
_REGISTERED_TTL = 7 * 86400


def publish_registered(app: Potatoq) -> str | None:
    """Store the worker's task names once, under a digest that heartbeats carry instead
    of the full list (workers running the same code share one copy). Returns the digest."""
    import hashlib

    from . import states
    from .brokers.base import ResultRecord

    backend = app.backend
    if backend is None:
        return None
    names = sorted(n for n in app.tasks if not n.startswith("potatoq."))
    digest = hashlib.sha256("\n".join(names).encode()).hexdigest()[:16]
    try:
        backend.store_result(ResultRecord(_REGISTERED + digest, states.SUCCESS, names), expires=_REGISTERED_TTL)
    except Exception:
        logger.warning("Couldn't store the registered task names", exc_info=True)
    return digest


class Inspect:
    """Read-only view of workers, built from broker heartbeats (no broadcast round trips)."""

    def __init__(self, app: Potatoq, destination: list[str] | None = None, timeout: float = 1.0):
        self.app = app
        self.destination = destination

    def _workers(self) -> list[dict[str, Any]]:
        workers = self.app.broker.workers()
        cutoff = time.time() - float(self.app.conf.worker_dead_after)
        workers = [w for w in workers if float(w.get("heartbeat", 0)) >= cutoff]
        if self.destination:
            workers = [w for w in workers if w.get("hostname") in self.destination or w["id"] in self.destination]
        return workers

    def ping(self) -> dict[str, Any] | None:
        return {w["id"]: {"ok": "pong"} for w in self._workers()} or None

    def active(self) -> dict[str, Any] | None:
        return {w["id"]: [{"id": tid} for tid in w.get("running", [])] for w in self._workers()} or None

    def active_queues(self) -> dict[str, Any] | None:
        return {w["id"]: [{"name": q} for q in w.get("queues", [])] for w in self._workers()} or None

    def stats(self) -> dict[str, Any] | None:
        return {w["id"]: w for w in self._workers()} or None

    def registered(self) -> dict[str, Any] | None:
        backend = self.app.backend
        names: dict[str, list[str]] = {}
        out = {}
        for w in self._workers():
            digest = w.get("registered")
            if digest and backend is not None and digest not in names:
                record = backend.get_result(_REGISTERED + digest)
                names[digest] = list(record.result) if record is not None else []
            out[w["id"]] = names.get(digest, []) if digest else []
        return out or None

    def scheduled(self) -> dict[str, Any] | None:
        return {w["id"]: [] for w in self._workers()} or None

    reserved = scheduled


class Control:
    def __init__(self, app: Potatoq):
        self.app = app

    def inspect(self, destination: list[str] | None = None, timeout: float = 1.0, **kwargs: Any) -> Inspect:
        return Inspect(self.app, destination, timeout)

    def revoke(
        self,
        task_id: str | list[str],
        destination: Any = None,
        terminate: bool = False,
        signal: Any = None,
        **kwargs: Any,
    ) -> None:
        """Prevent tasks that haven't started from running.

        Database and Redis brokers delete the waiting task outright; RabbitMQ marks it
        revoked in the result backend so workers skip it. ``terminate`` (killing a
        running task) is not supported: use time limits instead.
        """
        ids = [task_id] if isinstance(task_id, str) else list(task_id)
        if terminate:
            logger.warning(
                "revoke(terminate=True) isn't supported yet: tasks that already started keep running "
                "until they finish or hit their time limit. Waiting tasks are revoked."
            )
        self.app.broker.revoke(ids, expires=float(self.app.conf.result_expires))

    def last_periodic_runs(self) -> dict[str, datetime]:
        """When each ``beat_schedule`` entry was last sent: its fire time, as an aware
        UTC datetime. Use it to alert on periodic tasks that stopped running. Brokers keep
        claims for a week (Redis: the latest one per entry); RabbitMQ keeps none."""
        return {
            name: datetime.fromtimestamp(ts, UTC) for name, ts in sorted(self.app.broker.last_periodic_runs().items())
        }

    def purge(self) -> int:
        return sum(self.app.broker.purge(q) for q in self.app.broker.queue_sizes())

    def ping(self, destination: list[str] | None = None, timeout: float = 1.0, **kwargs: Any) -> list[dict[str, Any]]:
        return [{wid: v} for wid, v in (self.inspect(destination).ping() or {}).items()]

    def shutdown(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("Send SIGTERM to the worker process instead")

    def rate_limit(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def add_consumer(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("Restart the worker with -Q instead")

    cancel_consumer = add_consumer
