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

import dataclasses
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from .worker import executor

if TYPE_CHECKING:
    from .app import Potatoq

__all__ = ["DrainedTask", "drain", "due", "tick"]


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


def _aware(app: Potatoq, when: datetime) -> datetime:
    """Naive datetimes are in the app's timezone, the one crontabs use."""
    if when.tzinfo is not None:
        return when
    return when.replace(tzinfo=ZoneInfo(app.conf.timezone) if app.conf.timezone else UTC)


def due(app: Potatoq, start: datetime, end: datetime) -> list[tuple[str, datetime]]:
    """The ``beat_schedule`` runs that fall due after ``start``, up to and including ``end``.

    Returns ``(entry name, fire time)`` pairs in order, fire times in the app's
    timezone. Nothing is sent: use it to check that a schedule fires when you expect.

        assert due(app, datetime(2026, 1, 1), datetime(2026, 1, 2)) == [
            ("nightly-report", datetime(2026, 1, 1, 3, 0, tzinfo=ZoneInfo("Europe/Ljubljana"))),
        ]
    """
    from .worker.scheduler import load_entries

    start, end = _aware(app, start), _aware(app, end)
    tz = ZoneInfo(app.conf.timezone) if app.conf.timezone else UTC
    runs = [
        (entry.name, fire.astimezone(tz))
        for entry in load_entries(app)
        for fire in entry.schedule.fire_times_between(start, end)
    ]
    return sorted(runs, key=lambda run: (run[1], run[0]))


def tick(app: Potatoq, at: datetime | None = None) -> list[str]:
    """Send the periodic tasks a worker's scheduler would send at ``at`` (default: now).

    That's every ``beat_schedule`` entry with a fire time in the minute before ``at``,
    claimed through the broker as in production, so ticking twice at the same time
    sends nothing the second time. Returns the names of the entries sent; ``drain(app)``
    then runs them.

        tick(app, at=datetime(2026, 1, 1, 3, 0))   # naive: the app's timezone
        [task] = drain(app)
        assert task.name == "reports.nightly"
    """
    from datetime import timedelta

    from .worker.scheduler import Scheduler

    now = _aware(app, at or datetime.now(UTC)).astimezone(UTC)
    sent: list[str] = []

    class Recording(Scheduler):
        def _send(self, entry: Any, fire_at: datetime) -> bool:
            if "expires" not in entry.options:
                # Expire when the next run is due, counted from ``at``, not the real clock.
                expires = max(1.0, (entry.schedule.next_after(fire_at) - now).total_seconds())
                entry = dataclasses.replace(entry, options={**entry.options, "expires": expires})
            claimed = super()._send(entry, fire_at)
            if claimed:
                sent.append(entry.name)
            return claimed

    scheduler = Recording(app)
    scheduler.last_check = now - timedelta(seconds=scheduler.max_catch_up)
    scheduler.tick(now)
    return sent
