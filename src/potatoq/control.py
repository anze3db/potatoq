"""``app.control``: the parts of Celery's remote control that matter in practice."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .app import Potatoq


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
        return {w["id"]: w.get("registered", []) for w in self._workers()} or None

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
        self.app.broker.revoke(ids, expires=float(self.app.conf.result_expires))

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
