"""OpenTelemetry: spans and metrics, and trace context from .delay() into the task."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

from potatoq import Potatoq, chain, instrumentation, signals
from potatoq.contrib.opentelemetry import PotatoqInstrumentor
from potatoq.exceptions import Reject, WorkerTerminate
from potatoq.testing import drain
from potatoq.worker import executor

TESTS = Path(__file__).parent


@pytest.fixture
def otel():
    spans = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(spans))
    metrics = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[metrics])
    instrumentor = PotatoqInstrumentor()
    instrumentor.instrument(tracer_provider=tracer_provider, meter_provider=meter_provider)
    try:
        yield tracer_provider.get_tracer("test"), spans, metrics
    finally:
        instrumentor.uninstrument()


@pytest.fixture
def app():
    app = Potatoq("otel", broker="memory://", set_as_current=True)
    yield app
    app.close()


def by_name(spans):
    return {span.name: span for span in spans.get_finished_spans()}


def metric_points(metrics, name):
    data = metrics.get_metrics_data()
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                if metric.name == name:
                    return list(metric.data.data_points)
    return []


def test_trace_context_goes_from_delay_into_the_task(otel, app):
    tracer, spans, metrics = otel
    seen = {}

    @app.task(name="shop.charge")
    def charge(order_id):
        seen["span"] = trace.get_current_span().get_span_context()
        return order_id

    with tracer.start_as_current_span("POST /checkout") as request:
        charge.delay(1)

    [done] = drain(app)
    assert done.state == "SUCCESS"

    found = by_name(spans)
    send, process = found["send shop.charge"], found["process shop.charge"]
    assert send.kind == SpanKind.PRODUCER and process.kind == SpanKind.CONSUMER
    assert send.parent.span_id == request.get_span_context().span_id
    assert process.parent.span_id == send.context.span_id
    assert process.context.trace_id == request.get_span_context().trace_id
    assert seen["span"].span_id == process.context.span_id  # the task body runs inside it
    assert process.attributes["messaging.system"] == "potatoq"
    assert process.attributes["messaging.destination.name"] == "default"
    assert process.attributes["messaging.message.id"] == done.id
    assert process.attributes["potatoq.task.state"] == "SUCCESS"
    assert process.status.status_code == StatusCode.UNSET

    [sent] = metric_points(metrics, "messaging.client.sent.messages")
    assert sent.value == 1
    assert sent.attributes["potatoq.task.name"] == "shop.charge"
    [duration] = metric_points(metrics, "messaging.process.duration")
    assert duration.count == 1 and duration.attributes["potatoq.task.state"] == "SUCCESS"
    [wait] = metric_points(metrics, "potatoq.task.wait.duration")
    assert wait.count == 1


def test_failures_retries_and_rejections(otel, app):
    _, spans, metrics = otel
    attempts = []

    @app.task(name="shop.flaky", autoretry_for=(ConnectionError,), max_retries=1, retry_backoff=False)
    def flaky():
        attempts.append(1)
        raise ConnectionError("down")

    @app.task(name="shop.picky")
    def picky():
        raise Reject("no", requeue=False)

    flaky.delay()
    picky.delay()
    drain(app)

    process = [s for s in spans.get_finished_spans() if s.name == "process shop.flaky"]
    retry, failure = sorted(process, key=lambda s: s.attributes["potatoq.task.retries"])
    assert retry.attributes["potatoq.task.state"] == "RETRY"
    assert retry.status.status_code == StatusCode.UNSET
    assert [e.name for e in retry.events] == ["exception"]
    assert "potatoq.task.retry_eta" in retry.attributes
    # The retry is sent from inside the first attempt: one trace, attempt after attempt.
    resend = next(s for s in spans.get_finished_spans() if s.name == "send shop.flaky" and s.parent)
    assert resend.parent.span_id == retry.context.span_id
    assert failure.parent.span_id == resend.context.span_id
    assert failure.status.status_code == StatusCode.ERROR
    assert failure.attributes["error.type"] == "ConnectionError"

    rejected = by_name(spans)["process shop.picky"]
    assert rejected.status.status_code == StatusCode.ERROR
    assert rejected.attributes["potatoq.task.reason"] == "no"

    states = {p.attributes["potatoq.task.state"] for p in metric_points(metrics, "messaging.process.duration")}
    assert states == {"RETRY", "FAILURE", "REJECTED"}


def test_a_chain_is_one_trace(otel, app):
    tracer, spans, _ = otel

    @app.task(name="shop.add")
    def add(x, y):
        return x + y

    with tracer.start_as_current_span("job"):
        chain(add.s(1, 2), add.s(3)).delay()
    drain(app)
    traces = {s.context.trace_id for s in spans.get_finished_spans()}
    assert len(traces) == 1
    first, second = sorted(
        (s for s in spans.get_finished_spans() if s.name == "process shop.add"), key=lambda s: s.start_time
    )
    step = next(
        s for s in spans.get_finished_spans() if s.name == "send shop.add" and s.parent.span_id == first.context.span_id
    )
    assert second.parent.span_id == step.context.span_id


def test_eager_periodic_and_dead_processes(otel, app):
    _, spans, metrics = otel

    @app.task(name="shop.report")
    def report():
        return "ok"

    app.conf.task_always_eager = True
    report.delay()
    eager = by_name(spans)["process shop.report"]
    assert eager.attributes["potatoq.task.eager"] is True
    spans.clear()

    app.conf.task_always_eager = False
    message = report.build_message([], {}, headers={"periodic": "nightly"})
    assert by_name(spans)["send shop.report"].attributes["potatoq.task.periodic"] == "nightly"
    outcome = executor.failure_outcome(app, message, MemoryError("killed"), hostname="w1")
    assert outcome.state == "FAILURE"
    died = by_name(spans)["process shop.report"]
    assert died.status.status_code == StatusCode.ERROR
    assert died.attributes["potatoq.task.periodic"] == "nightly"
    assert died.attributes["potatoq.worker.hostname"] == "w1"
    assert died.attributes["error.type"] == "MemoryError"
    assert died.parent.span_id == by_name(spans)["send shop.report"].context.span_id
    # Only attempts that ran here are timed.
    assert [p.count for p in metric_points(metrics, "messaging.process.duration")] == [1]


def test_tasks_sent_by_name_and_interrupted_attempts(otel, app):
    _, spans, _ = otel
    app.send_task("elsewhere.task", [1])
    assert by_name(spans)["send elsewhere.task"].attributes["potatoq.task.name"] == "elsewhere.task"

    @app.task(name="shop.slow")
    def slow():
        raise WorkerTerminate()

    message = slow.build_message([], {})
    with pytest.raises(WorkerTerminate):
        executor.execute(app, message)
    interrupted = by_name(spans)["process shop.slow"]
    assert interrupted.status.status_code == StatusCode.ERROR
    assert interrupted.attributes["error.type"] == "WorkerTerminate"


def test_unregistered_tasks_and_no_eta_or_retries_on_plain_sends(otel, app):
    _, spans, _ = otel
    message = app.tasks["potatoq.accumulate"].build_message([], {}, countdown=60)
    message.task = "gone.task"
    outcome = executor.execute(app, message)
    assert outcome.state == "FAILURE"
    gone = by_name(spans)["process gone.task"]
    assert gone.attributes["error.type"] == "NotRegistered"
    send = by_name(spans)["send potatoq.accumulate"]
    assert "potatoq.task.eta" in send.attributes
    assert "potatoq.task.retries" not in send.attributes


def test_flushes_before_a_worker_process_exits(otel):
    with mock.patch.object(trace, "get_tracer_provider") as provider:
        signals.worker_process_shutdown.send(sender=None, pid=1, exitcode=0)
    provider.return_value.force_flush.assert_called_once()
    with mock.patch.object(trace, "get_tracer_provider", return_value=object()):
        signals.worker_process_shutdown.send(sender=None, pid=1, exitcode=0)  # no force_flush: fine


def test_uninstrument_removes_it(app):
    instrumentor = PotatoqInstrumentor()
    instrumentor.instrument()
    assert any(type(i).__name__ == "_OpenTelemetry" for i in instrumentation.registered())
    assert instrumentor.instrumentation_dependencies() == ()
    instrumentor.uninstrument()
    assert not any(type(i).__name__ == "_OpenTelemetry" for i in instrumentation.registered())


def test_worker_processes_export_their_spans_before_exiting(otel, tmp_path):
    """Children exit with os._exit(), skipping the SDK's atexit flush: potatoq flushes."""
    tracer, _, _ = otel
    broker = f"sqlite:///{tmp_path}/broker.db"
    spans_file = tmp_path / "spans.jsonl"
    env = {
        **os.environ,
        "TEST_BROKER": broker,
        "TEST_SPANS": str(spans_file),
        "PYTHONPATH": str(TESTS),
    }
    env.pop("DJANGO_SETTINGS_MODULE", None)
    producer = Potatoq("otelworkerapp", broker=broker, set_as_current=False)
    add = producer.task(name="otelworkerapp.add")(lambda x, y: x + y)
    with tracer.start_as_current_span("request") as request:
        results = [add.delay(i, i) for i in range(3)]
    log = open(tmp_path / "worker.log", "w")
    worker = subprocess.Popen(
        [sys.executable, "-m", "potatoq.cli", "-A", "otelworkerapp", "worker", "-c", "1"],
        env=env, cwd=TESTS, stdout=log, stderr=subprocess.STDOUT,
    )  # fmt: skip
    try:
        assert [r.get(timeout=30) for r in results] == [0, 2, 4]
    finally:
        worker.send_signal(signal.SIGTERM)
        assert worker.wait(timeout=30) == 0, (tmp_path / "worker.log").read_text()
        log.close()
        producer.close()
    rows = [json.loads(line) for line in spans_file.read_text().splitlines()]
    process = [r for r in rows if r["name"] == "process otelworkerapp.add"]
    assert len(process) == 3
    assert {r["trace"] for r in process} == {request.get_span_context().trace_id}
    assert len({r["pid"] for r in process}) == 2  # recycled after two tasks, and both flushed
