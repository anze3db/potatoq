"""In-process broker for tests (``memory://``). Single process only; not for production.

Pair it with ``potatoq.testing.drain(app)`` to run whatever was enqueued, exactly as a
worker would (serialization, retries, chains and chords included), but synchronously.
"""

from __future__ import annotations

import heapq
import itertools
import threading
import time
from typing import Any

from .. import serialization, states
from ..message import Message
from .base import Broker, Consumer, Delivery, ResultRecord


class MemoryBroker(Broker):
    schemes = ("memory",)
    supports_results = True
    transactional = False

    def __init__(self, url: str, app: Any, **options: Any):
        super().__init__(url, app, **options)
        self.lock = threading.Condition()
        self.ready: dict[str, list[tuple[int, int, str]]] = {}
        self.delayed: list[tuple[float, int, str]] = []
        self.jobs: dict[str, dict[str, Any]] = {}
        self.results: dict[str, tuple[str, float | None]] = {}
        self.dead: dict[str, dict[str, Any]] = {}
        self.chords: dict[str, dict[int, Any]] = {}
        self.periodic: set[tuple[str, float]] = set()
        self.workers_: dict[str, dict[str, Any]] = {}
        self._seq = itertools.count()

    def _push_ready(self, job_id: str, front: bool = False) -> None:
        job = self.jobs[job_id]
        seq = -next(self._seq) if front else next(self._seq)
        heapq.heappush(self.ready.setdefault(job["queue"], []), (-job["priority"], seq, job_id))

    def enqueue(self, messages: list[Message], connection: Any = None) -> None:
        now = time.time()
        with self.lock:
            for m in messages:
                if m.id in self.jobs:
                    continue
                self.jobs[m.id] = {
                    "queue": m.queue,
                    "priority": m.priority,
                    "payload": m.encode(),
                    "deliveries": 0,
                    "state": "ready",
                }
                if m.eta is not None and m.eta > now:
                    self.jobs[m.id]["state"] = "delayed"
                    heapq.heappush(self.delayed, (m.eta, next(self._seq), m.id))
                else:
                    self._push_ready(m.id)
            self.lock.notify_all()

    def enqueue_periodic(self, name: str, fire_at: float, message: Message) -> bool:
        with self.lock:
            if (name, fire_at) in self.periodic:
                return False
            self.periodic.add((name, fire_at))
        self.enqueue([message])
        return True

    def promote(self, now: float | None = None) -> None:
        now = time.time() if now is None else now
        with self.lock:
            while self.delayed and self.delayed[0][0] <= now:
                _, _, job_id = heapq.heappop(self.delayed)
                if job_id in self.jobs and self.jobs[job_id]["state"] == "delayed":
                    self.jobs[job_id]["state"] = "ready"
                    self._push_ready(job_id)

    def consumer(self, queues: list[str], worker_id: str, pid: int | None = None) -> MemoryConsumer:
        return MemoryConsumer(self, queues, worker_id, pid)

    def store_result(self, record: ResultRecord, expires: float | None) -> None:
        with self.lock:
            self.results[record.task_id] = (
                serialization.dumps(record.to_dict()),
                time.time() + expires if expires else None,
            )
            self.lock.notify_all()

    def get_result(self, task_id: str) -> ResultRecord | None:
        with self.lock:
            entry = self.results.get(task_id)
            if entry is not None:
                return ResultRecord.from_dict(serialization.loads(entry[0]))
            job = self.jobs.get(task_id)
            if job is not None and job["state"] == "active":
                return ResultRecord(task_id=task_id, state=states.STARTED)
        return None

    def forget(self, task_id: str) -> None:
        with self.lock:
            self.results.pop(task_id, None)

    def heartbeat(self, worker_id: str, info: dict[str, Any]) -> None:
        self.workers_[worker_id] = {**info, "heartbeat": time.time()}

    def unregister(self, worker_id: str) -> None:
        self.workers_.pop(worker_id, None)

    def workers(self) -> list[dict[str, Any]]:
        return [{"id": k, **v} for k, v in sorted(self.workers_.items())]

    def tick(self) -> None:
        self.promote()

    def chord_part_done(self, group_id: str, index: int, size: int, result: Any) -> list[Any] | None:
        with self.lock:
            parts = self.chords.setdefault(group_id, {})
            parts.setdefault(index, serialization.loads(serialization.dumps(result)))
            if len(parts) < size:
                return None
            return [parts[i] for i in sorted(parts)]

    def revoke(self, task_ids: list[str], expires: float) -> None:
        with self.lock:
            for task_id in task_ids:
                job = self.jobs.get(task_id)
                if job is not None and job["state"] != "active":
                    del self.jobs[task_id]
                self.results[task_id] = (
                    serialization.dumps(ResultRecord(task_id, states.REVOKED, date_done=time.time()).to_dict()),
                    None,
                )

    def queue_sizes(self) -> dict[str, int]:
        with self.lock:
            sizes: dict[str, int] = {}
            for job in self.jobs.values():
                if job["state"] in ("ready", "delayed"):
                    sizes[job["queue"]] = sizes.get(job["queue"], 0) + 1
            return sizes

    def purge(self, queue: str) -> int:
        with self.lock:
            ids = [k for k, j in self.jobs.items() if j["queue"] == queue and j["state"] in ("ready", "delayed")]
            for job_id in ids:
                del self.jobs[job_id]
            self.ready.pop(queue, None)
            return len(ids)

    def dead_letters(self, limit: int = 100) -> list[dict[str, Any]]:
        entries = sorted(self.dead.values(), key=lambda e: -e["died_at"])
        return entries[:limit]

    def requeue_dead(self, task_id: str) -> bool:
        entry = self.dead.pop(task_id, None)
        if entry is None:
            return False
        message = Message.from_dict(entry["message"])
        message.eta = None
        self.jobs.pop(task_id, None)
        self.results.pop(task_id, None)
        self.enqueue([message])
        return True


class MemoryConsumer(Consumer):
    broker: MemoryBroker

    def __init__(self, broker: MemoryBroker, queues: list[str], worker_id: str, pid: int | None = None):
        super().__init__(broker, queues, worker_id, pid)
        self._interrupted = False

    def _claim(self) -> Delivery | None:
        b = self.broker
        b.promote()
        for queue in self.queues:
            heap = b.ready.get(queue)
            while heap:
                _, _, job_id = heapq.heappop(heap)
                job = b.jobs.get(job_id)
                if job is None or job["state"] != "ready":
                    continue
                job["state"] = "active"
                job["deliveries"] += 1
                return Delivery(
                    Message.decode(job["payload"]), delivery_count=job["deliveries"], handle=(job_id, job["deliveries"])
                )
        return None

    def fetch(self, timeout: float) -> Delivery | None:
        deadline = time.monotonic() + timeout
        self._interrupted = False
        with self.broker.lock:
            while True:
                delivery = self._claim()
                if delivery is not None:
                    return delivery
                remaining = deadline - time.monotonic()
                if remaining <= 0 or self._interrupted:
                    return None
                self.broker.lock.wait(min(remaining, 0.05))

    def interrupt(self) -> None:
        self._interrupted = True

    def _owned(self, delivery: Delivery) -> dict[str, Any] | None:
        job_id, deliveries = delivery.handle
        job = self.broker.jobs.get(job_id)
        if job is None or job["state"] != "active" or job["deliveries"] != deliveries:
            return None
        return job

    def complete(self, delivery: Delivery, record: ResultRecord | None, followups: list[Message]) -> None:
        with self.broker.lock:
            if self._owned(delivery) is None:
                return
            del self.broker.jobs[delivery.handle[0]]
        if record is not None:
            self.broker.store_result(record, self.broker.app.conf.result_expires)
        self.broker.enqueue(followups)

    def retry(self, delivery: Delivery, message: Message, record: ResultRecord | None) -> None:
        with self.broker.lock:
            if self._owned(delivery) is not None:
                del self.broker.jobs[delivery.handle[0]]
        if record is not None:
            self.broker.store_result(record, self.broker.app.conf.result_expires)
        self.broker.enqueue([message])

    def requeue(self, delivery: Delivery, count: bool = False) -> None:
        with self.broker.lock:
            job = self._owned(delivery)
            if job is None:
                return
            job["state"] = "ready"
            if not count:
                job["deliveries"] -= 1
            self.broker._push_ready(delivery.handle[0], front=True)
            self.broker.lock.notify_all()

    def dead_letter(
        self, delivery: Delivery, reason: str, record: ResultRecord | None, followups: list[Message] | None = None
    ) -> None:
        m = delivery.message
        with self.broker.lock:
            if self._owned(delivery) is None:
                return
            del self.broker.jobs[delivery.handle[0]]
            self.broker.dead[m.id] = {
                "id": m.id,
                "queue": m.queue,
                "task": m.task,
                "reason": reason,
                "died_at": time.time(),
                "message": m.to_dict(),
            }
        if record is not None:
            self.broker.store_result(record, self.broker.app.conf.result_expires)
        self.broker.enqueue(followups or [])
