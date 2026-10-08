"""Periodic tasks (``beat_schedule``) without a single ``beat`` process.

Every worker runs the scheduler. Fire times are deterministic (see
``potatoq.schedules``) and each ``(entry, fire time)`` is enqueued at most once
through ``Broker.enqueue_periodic`` (a unique row on SQL brokers, ``SET NX`` on Redis,
a single-active-consumer leader on RabbitMQ). Running zero extra processes and having
no "two beats double-schedule everything" failure mode are the point.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from .. import signals
from ..schedules import BaseSchedule, crontab, maybe_schedule

if TYPE_CHECKING:
    from ..app import Potatoq

logger = logging.getLogger("potatoq.scheduler")


@dataclass
class Entry:
    name: str
    task: str
    schedule: BaseSchedule
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)


def load_entries(app: Potatoq) -> list[Entry]:
    entries = []
    tz = ZoneInfo(app.conf.timezone) if app.conf.timezone else UTC
    for name, spec in (app.conf.beat_schedule or {}).items():
        schedule = maybe_schedule(spec["schedule"])
        if isinstance(schedule, crontab) and schedule.tz is None:
            schedule.tz = tz
        entries.append(
            Entry(
                name=name,
                task=spec["task"],
                schedule=schedule,
                args=tuple(spec.get("args", ())),
                kwargs=dict(spec.get("kwargs", {})),
                options=dict(spec.get("options", {})),
            )
        )
    return entries


def local_time(app: Potatoq, when: datetime) -> str:
    """``2026-10-09 10:00:00 America/Chicago``: in the timezone crontabs use."""
    tz = ZoneInfo(app.conf.timezone) if app.conf.timezone else UTC
    return f"{when.astimezone(tz):%Y-%m-%d %H:%M:%S} {app.conf.timezone or 'UTC'}"


def describe(app: Potatoq, entries: list[Entry], now: datetime | None = None) -> list[tuple[Entry, str]]:
    """Each entry with its next fire time in the app's timezone, e.g. ``2026-10-09 10:00 America/Chicago``."""
    now = now or datetime.now(UTC)
    out = []
    for entry in entries:
        try:
            out.append((entry, local_time(app, entry.schedule.next_after(now))))
        except Exception as exc:
            out.append((entry, f"unknown ({exc})"))
    return out


class Scheduler:
    #: Fire times missed by more than this (e.g. all workers were down) are skipped
    #: rather than replayed in a burst.
    max_catch_up = 60.0

    def __init__(self, app: Potatoq):
        self.app = app
        self.entries = load_entries(app)
        # Look back on start, so a run due while the workers restarted is still sent (if
        # it's less than max_catch_up late). Claims make sure it's sent only once.
        lookback = self.max_catch_up if app.broker.durable_periodic_claims else 0.0
        self.last_check = datetime.fromtimestamp(time.time() - lookback, UTC)
        self.checkpoints: dict[str, datetime] = {}

    def __bool__(self) -> bool:
        return bool(self.entries)

    def start(self) -> None:
        signals.beat_init.send(sender=self)
        for entry, next_run in describe(self.app, self.entries):
            logger.info("Scheduler: %s -> %s (%r), next run %s", entry.name, entry.task, entry.schedule, next_run)
            if entry.task not in self.app.tasks:
                logger.warning(
                    "Scheduler: %s runs %r, which isn't registered in this worker. If that's a typo, "
                    "every run will be dead-lettered.",
                    entry.name, entry.task,
                )  # fmt: skip

    def tick(self, now: datetime | None = None) -> int:
        """Enqueue everything due since the last tick. Returns the number enqueued."""
        now = now or datetime.now(UTC)
        floor = datetime.fromtimestamp(now.timestamp() - self.max_catch_up, UTC)
        sent = 0
        for entry in self.entries:
            # Each entry keeps its own checkpoint, advanced only once its due run was
            # enqueued: a broker blip delays a run (within max_catch_up) instead of
            # silently skipping it.
            start = max(self.checkpoints.setdefault(entry.name, self.last_check), floor)
            try:
                times = entry.schedule.fire_times_between(start, now)
            except Exception:
                logger.exception("Scheduler: bad schedule for %s", entry.name)
                continue
            ok = True
            for fire_at in times[-1:]:  # never send a backlog of the same entry
                try:
                    if self._send(entry, fire_at):
                        sent += 1
                except Exception:
                    logger.exception("Scheduler: failed to enqueue %s; will retry", entry.name)
                    ok = False
            if ok:
                self.checkpoints[entry.name] = now
        self.last_check = now
        return sent

    def _send(self, entry: Entry, fire_at: datetime) -> bool:
        app = self.app
        task = app.tasks.get(entry.task)
        options = dict(entry.options)
        if "expires" not in options:
            # A periodic run that couldn't start before the next one is due is dropped
            # instead of piling up behind a stuck queue.
            next_fire = entry.schedule.next_after(fire_at)
            options["expires"] = max(1.0, next_fire.timestamp() - time.time())
        options.setdefault("headers", {})["periodic"] = entry.name
        if task is None:
            task = app._task_from_fun(lambda *a, **k: None, name=entry.task, typing=False)
            app.tasks.unregister(entry.task)
        message = task.build_message(list(entry.args), dict(entry.kwargs), **options)
        claimed = app.broker.enqueue_periodic(entry.name, fire_at.timestamp(), message)
        if claimed:
            logger.info("Scheduler: sending due task %s (%s)", entry.name, entry.task)
        return claimed

    def seconds_until_next(self, now: datetime | None = None) -> float:
        now = now or datetime.now(UTC)
        upcoming = [e.schedule.next_after(now) for e in self.entries]
        if not upcoming:
            return 3600.0
        return max(0.0, (min(upcoming) - now).total_seconds())
