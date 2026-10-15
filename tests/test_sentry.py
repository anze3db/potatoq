"""Sentry: errors, transactions, trace propagation and cron check-ins for tasks."""

from __future__ import annotations

from datetime import timedelta

import pytest
import sentry_sdk
from sentry_sdk.transport import Transport

from potatoq import Potatoq, signals
from potatoq.contrib.sentry import PotatoqIntegration
from potatoq.exceptions import SoftTimeLimitExceeded
from potatoq.schedules import crontab
from potatoq.testing import drain
from potatoq.worker import executor


class Capture(Transport):
    def __init__(self, options=None):
        super().__init__(options)
        self.envelopes = []

    def capture_envelope(self, envelope):
        self.envelopes.append(envelope)

    def items(self, kind):
        return [item.payload.json for e in self.envelopes for item in e.items if item.type == kind]


@pytest.fixture
def sentry(request):
    options = getattr(request, "param", {})
    transport = Capture()
    sentry_sdk.init(
        dsn="https://key@sentry.example.com/1",
        transport=transport,
        traces_sample_rate=1.0,
        default_integrations=False,
        integrations=[PotatoqIntegration(**options)],
    )
    try:
        yield transport
    finally:
        sentry_sdk.init(default_integrations=False)  # no DSN: disabled


@pytest.fixture
def app():
    app = Potatoq("sentry", broker="memory://", set_as_current=True)
    yield app
    app.close()


def test_failures_are_reported_once_with_the_task(sentry, app):
    @app.task(name="shop.flaky", autoretry_for=(ConnectionError,), max_retries=1, retry_backoff=False)
    def flaky(order_id):
        sentry_sdk.add_breadcrumb(message=f"attempt for {order_id}")
        raise ConnectionError("down")

    with sentry_sdk.start_transaction(name="POST /checkout") as request:
        flaky.delay(42)
    drain(app)

    [event] = sentry.items("event")  # the retried attempt isn't reported, the last one is
    assert event["exception"]["values"][0]["type"] == "ConnectionError"
    assert event["exception"]["values"][0]["mechanism"] == {"type": "potatoq", "handled": False}
    assert event["tags"]["potatoq_task"] == "shop.flaky"
    job = event["extra"]["potatoq-job"]
    assert job["task_name"] == "shop.flaky" and job["retries"] == 1 and job["queue"] == "default"
    assert job["args"] == "[Filtered]"  # no PII by default
    assert [b["message"] for b in event["breadcrumbs"]["values"]] == ["attempt for 42"]  # this attempt's only
    assert event["contexts"]["trace"]["trace_id"] == request.trace_id

    transactions = sentry.items("transaction")
    tasks = [t for t in transactions if t["transaction"] == "shop.flaky"]
    assert [t["contexts"]["trace"]["status"] for t in tasks] == ["aborted", "internal_error"]
    assert {t["contexts"]["trace"]["trace_id"] for t in tasks} == {request.trace_id}
    assert tasks[0]["contexts"]["trace"]["op"] == "queue.task.potatoq"
    [process] = [s for s in tasks[0]["spans"] if s["op"] == "queue.process"]
    assert process["data"]["messaging.system"] == "potatoq"
    assert process["data"]["messaging.destination.name"] == "default"
    assert process["data"]["messaging.message.retry.count"] == 0
    assert process["data"]["messaging.message.receive.latency"] >= 0
    [checkout] = [t for t in transactions if t["transaction"] == "POST /checkout"]
    [publish] = [s for s in checkout["spans"] if s["op"] == "queue.publish"]
    assert publish["description"] == "shop.flaky"


@pytest.mark.parametrize("sentry", [{"propagate_traces": False}], indirect=True)
def test_trace_propagation_can_be_turned_off(sentry, app):
    @app.task(name="shop.ok")
    def ok():
        return 1

    with sentry_sdk.start_transaction(name="request"):
        ok.delay()
    [task] = drain(app)
    assert task.state == "SUCCESS"
    assert "sentry-trace" not in str(sentry.items("transaction"))
    request, run = sorted(sentry.items("transaction"), key=lambda t: t["transaction"])
    assert request["contexts"]["trace"]["trace_id"] != run["contexts"]["trace"]["trace_id"]


def test_baggage_from_other_tracers_is_kept_and_pii_is_sent_when_allowed(sentry, app):
    sentry_sdk.get_client().options["send_default_pii"] = True

    @app.task(name="shop.boom")
    def boom(x):
        raise SoftTimeLimitExceeded()

    with sentry_sdk.start_transaction(name="request"):
        message = boom.build_message([7], {}, headers={"baggage": "otel=1"})
    assert "otel=1" in message.headers["baggage"] and "sentry-trace_id=" in message.headers["baggage"]
    assert "sentry-trace" in message.headers
    executor.execute(app, message)
    [event] = sentry.items("event")
    assert event["extra"]["potatoq-job"]["args"] == [7]
    assert event["fingerprint"] == ["potatoq", "SoftTimeLimitExceeded", "shop.boom"]


def test_dead_processes_and_unknown_tasks_are_reported(sentry, app):
    @app.task(name="shop.big")
    def big():
        return 1

    message = big.build_message([], {})
    executor.failure_outcome(app, message, MemoryError("killed"))
    message.task = "gone.task"
    executor.execute(app, message)
    first, second = sentry.items("event")
    assert first["exception"]["values"][0]["type"] == "MemoryError"
    assert first["extra"]["potatoq-job"]["process_died"] is True
    assert second["exception"]["values"][0]["type"] == "NotRegistered"


@pytest.mark.parametrize(
    "sentry", [{"monitor_periodic_tasks": True, "exclude_periodic_tasks": ["^skip"]}], indirect=True
)
def test_periodic_tasks_send_cron_check_ins(sentry, app):
    @app.task(name="shop.report")
    def report(fail=False):
        if fail:
            raise ValueError("no")

    app.conf.beat_schedule = {
        "Nightly Report!": {"task": "shop.report", "schedule": crontab(minute=30, hour=3, day_of_week="mon-fri")},
        "every hour": {"task": "shop.report", "schedule": timedelta(hours=1)},
        "every day": {"task": "shop.report", "schedule": timedelta(days=1)},
        "every 15 minutes": {"task": "shop.report", "schedule": 900},
        "every 30 seconds": {"task": "shop.report", "schedule": 30},
        "skip me": {"task": "shop.report", "schedule": 900},
    }
    names = ["Nightly Report!", "every hour", "every day", "every 15 minutes", "every 30 seconds", "skip me", "gone"]
    for name in names:
        message = report.build_message([], {"fail": name == "every hour"}, headers={"periodic": name})
        assert "sentry-trace" not in message.headers  # sent by the scheduler: its own trace
        executor.execute(app, message)

    checkins = sentry.items("check_in")
    by_slug = {}
    for checkin in checkins:
        by_slug.setdefault(checkin["monitor_slug"], []).append(checkin)
    assert set(by_slug) == {"nightly-report", "every-hour", "every-day", "every-15-minutes", "every-30-seconds", "gone"}
    started, done = by_slug["nightly-report"]
    assert started["status"] == "in_progress" and done["status"] == "ok"
    assert done["check_in_id"] == started["check_in_id"] and done["duration"] >= 0
    assert started["monitor_config"] == {
        "schedule": {"type": "crontab", "value": "30 3 * * mon-fri"},
        "timezone": "UTC",
    }
    assert by_slug["every-hour"][1]["status"] == "error"
    assert by_slug["every-hour"][0]["monitor_config"]["schedule"] == {"type": "interval", "value": 1, "unit": "hour"}
    assert by_slug["every-day"][0]["monitor_config"]["schedule"] == {"type": "interval", "value": 1, "unit": "day"}
    assert by_slug["every-15-minutes"][0]["monitor_config"]["schedule"]["value"] == 15
    assert "monitor_config" not in by_slug["every-30-seconds"][0]  # too short for Sentry
    assert "monitor_config" not in by_slug["gone"][0]  # no longer in the schedule


@pytest.mark.parametrize("sentry", [{"monitor_periodic_tasks": True}], indirect=True)
def test_a_failing_check_in_doesnt_stop_the_task(sentry, app, monkeypatch):
    def broken(**kwargs):
        raise ConnectionError("sentry is down")

    monkeypatch.setattr("potatoq.contrib.sentry.capture_checkin", broken)

    @app.task(name="shop.report")
    def report():
        return 1

    message = report.build_message([], {}, headers={"periodic": "nightly"})
    assert executor.execute(app, message).state == "SUCCESS"


def test_disabled_sentry_does_nothing(app):
    sentry_sdk.init(default_integrations=False)

    @app.task(name="shop.ok")
    def ok():
        return 1

    message = ok.build_message([], {})
    assert "sentry-trace" not in message.headers
    assert executor.execute(app, message).state == "SUCCESS"
    signals.worker_process_shutdown.send(sender=None, pid=1, exitcode=0)  # nothing to flush


def test_flushes_before_a_worker_process_exits(sentry, monkeypatch):
    flushed = []
    monkeypatch.setattr(sentry_sdk, "flush", lambda *a, **k: flushed.append(1))
    signals.worker_process_shutdown.send(sender=None, pid=1, exitcode=0)
    assert flushed == [1]
