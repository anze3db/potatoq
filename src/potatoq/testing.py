"""Test helpers.

Prefer running tasks through the real machinery over ``task_always_eager``: messages
are serialized, retries are rescheduled, chains/chords go through the broker, and
``transaction.on_commit`` behaves like production.

    app = Potatoq("tests", broker="memory://")

    def test_signup():
        signup(...)                        # code under test calls send_welcome.delay(...)
        results = drain(app)               # run everything that was enqueued
        assert results[0].state == "SUCCESS"
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .worker import executor

if TYPE_CHECKING:
    from .app import Potatoq

__all__ = ["DrainedTask", "drain"]


@dataclass
class DrainedTask:
    id: str
    name: str
    args: list[Any]
    kwargs: dict[str, Any]
    state: str
    result: Any
    exception: BaseException | None


def drain(
    app: Potatoq,
    queues: list[str] | None = None,
    *,
    include_scheduled: bool = True,
    max_tasks: int = 10_000,
    raise_on_failure: bool = False,
) -> list[DrainedTask]:
    """Run queued tasks in this process until the queues are empty.

    ``include_scheduled`` also runs tasks with an ETA/countdown and retries (time is
    skipped ahead, they are not waited for).
    """
    broker = app.broker
    if queues is None:
        queues = sorted({*broker.queue_sizes(), app.conf.task_default_queue})
    consumer = broker.consumer(queues, "potatoq-testing")
    done: list[DrainedTask] = []
    for _ in range(max_tasks):
        delivery = consumer.fetch(timeout=0)
        if delivery is None and include_scheduled:
            delivery = _next_scheduled(app, consumer)
        if delivery is None:
            break
        message = delivery.message
        outcome = executor.execute(app, message, delivery_count=delivery.delivery_count, hostname="testing")
        executor.settle(app, consumer, delivery, outcome)
        done.append(
            DrainedTask(
                message.id, message.task, message.args, message.kwargs, outcome.state, outcome.retval, outcome.exc
            )
        )
        if raise_on_failure and outcome.state == "FAILURE" and outcome.exc is not None:
            raise outcome.exc
    return done


def _next_scheduled(app: Potatoq, consumer: Any) -> Any:
    """Make the earliest scheduled task due now and fetch it (memory and SQL brokers)."""
    broker = app.broker
    from .brokers.memory import MemoryBroker

    if isinstance(broker, MemoryBroker):
        with broker.lock:
            if not broker.delayed:
                return None
            eta = broker.delayed[0][0]
        broker.promote(now=eta)
        return consumer.fetch(timeout=0)
    from .brokers.sqlite import SQLiteBroker

    if isinstance(broker, SQLiteBroker):
        with broker._write() as conn:
            conn.execute(
                "UPDATE potatoq_jobs SET state = 1, run_at = ? WHERE seq = (SELECT seq FROM potatoq_jobs WHERE state = 0 ORDER BY run_at LIMIT 1)",
                (time.time(),),
            )
        consumer._data_version = None
        return consumer.fetch(timeout=0)
    return None
