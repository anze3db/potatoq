"""Scheduler edge cases (periodic tasks run by every worker)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from potatoq import signals
from potatoq.schedules import BaseSchedule, crontab
from potatoq.testing import drain
from potatoq.worker.scheduler import Scheduler

START = datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC)


def test_empty_schedule(memory_app):
    scheduler = Scheduler(memory_app)
    assert not scheduler
    assert scheduler.seconds_until_next() == 3600.0
    assert scheduler.tick() == 0


def test_crontab_without_timezone_uses_app_timezone(memory_app):
    memory_app.conf.timezone = "Europe/Ljubljana"
    explicit = crontab(minute=0, tz="UTC")
    memory_app.conf.beat_schedule = {
        "local": {"task": "x", "schedule": crontab(minute=0)},
        "explicit": {"task": "x", "schedule": explicit},
    }
    local, other = Scheduler(memory_app).entries
    assert local.schedule.tz == ZoneInfo("Europe/Ljubljana")
    assert other.schedule is explicit and other.schedule.tz != ZoneInfo("Europe/Ljubljana")


def test_start_sends_beat_init_and_logs_entries(memory_app, caplog):
    memory_app.conf.beat_schedule = {"r": {"task": "some.task", "schedule": 60.0}}
    scheduler = Scheduler(memory_app)
    seen = []

    def on_init(sender, **kwargs):
        seen.append(sender)

    signals.beat_init.connect(on_init)
    caplog.set_level(logging.INFO, logger="potatoq.scheduler")
    try:
        scheduler.start()
    finally:
        signals.beat_init.disconnect(on_init)
    assert seen == [scheduler]
    assert "Scheduler: r -> some.task" in caplog.text


def test_bad_schedule_is_logged_and_others_still_run(memory_app, caplog):
    class Broken(BaseSchedule):
        def next_after(self, after):
            raise RuntimeError("broken schedule")

    @memory_app.task
    def report():
        pass

    memory_app.conf.beat_schedule = {
        "bad": {"task": report.name, "schedule": Broken()},
        "good": {"task": report.name, "schedule": 60.0},
    }
    scheduler = Scheduler(memory_app)
    scheduler.last_check = START
    assert scheduler.tick(START + timedelta(seconds=40)) == 1
    assert "bad schedule for bad" in caplog.text
    assert scheduler.checkpoints["bad"] == START  # not advanced past the failure


def test_unregistered_task_is_sent_by_name(memory_app):
    memory_app.conf.beat_schedule = {
        "remote": {
            "task": "remote.report",
            "schedule": 60.0,
            "args": [1],
            "kwargs": {"k": 2},
            "options": {"queue": "r"},
        }
    }
    scheduler = Scheduler(memory_app)
    scheduler.last_check = START
    assert scheduler.tick(START + timedelta(seconds=40)) == 1
    assert "remote.report" not in memory_app.tasks
    assert memory_app.broker.queue_sizes() == {"r": 1}
    [drained] = drain(memory_app, ["r"])
    assert (drained.name, drained.args, drained.kwargs) == ("remote.report", [1], {"k": 2})


def test_seconds_until_next(memory_app):
    memory_app.conf.beat_schedule = {
        "minutely": {"task": "x", "schedule": 60.0},
        "every 10s": {"task": "x", "schedule": 10.0},
    }
    scheduler = Scheduler(memory_app)
    assert scheduler
    assert scheduler.seconds_until_next(START) == 10.0  # 00:00:30 -> 00:00:40
    assert scheduler.seconds_until_next(START + timedelta(seconds=5)) == 5.0
