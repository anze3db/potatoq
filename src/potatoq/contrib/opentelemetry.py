"""OpenTelemetry: traces and metrics for tasks, with trace context carried from
``.delay()`` into the worker.

    from potatoq.contrib.opentelemetry import PotatoqInstrumentor

    PotatoqInstrumentor().instrument()

Or let ``opentelemetry-instrument`` find it (it's registered as an instrumentor entry
point). Every message gets a ``send <task>`` span (``PRODUCER``) in the code that sends
it, whose context travels in ``message.headers`` (W3C ``traceparent`` by default, or
whatever propagators are configured). Every attempt at running it gets a
``process <task>`` span (``CONSUMER``), a child of that span, with the task's logs,
queries and HTTP calls inside. Retries, a chain's next step and a chord's callback are
sent from inside the task's span, so the whole workflow is one trace.

Spans follow the messaging semantic conventions (``messaging.system`` is ``potatoq``),
plus ``potatoq.task.*`` attributes. Metrics: ``messaging.client.sent.messages``,
``messaging.process.duration`` and ``potatoq.task.wait.duration`` (time spent in the
queue after the task was due).

Task arguments and results are never recorded.
"""

from __future__ import annotations

import time
from collections.abc import Collection, Iterator
from contextlib import contextmanager
from importlib.metadata import version
from typing import TYPE_CHECKING, Any

from opentelemetry import context, propagate, trace
from opentelemetry._logs import get_logger_provider
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor  # type: ignore[attr-defined]
from opentelemetry.metrics import get_meter, get_meter_provider
from opentelemetry.trace import SpanKind, Status, StatusCode

from .. import instrumentation, signals, states
from ..instrumentation import Instrumentation, TaskRun

if TYPE_CHECKING:
    from ..message import Message

__all__ = ["PotatoqInstrumentor"]

SYSTEM = "potatoq"

#: Seconds: from a few milliseconds to the default 30-minute time limit.
_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800)


def _base(message: Message, operation: str) -> dict[str, Any]:
    """Low-cardinality attributes, shared by spans and metrics."""
    return {
        "messaging.system": SYSTEM,
        "messaging.operation.name": operation,
        "messaging.operation.type": operation,
        "messaging.destination.name": message.queue,
        "potatoq.task.name": message.task,
    }


class _OpenTelemetry(Instrumentation):
    def __init__(self, tracer_provider: Any = None, meter_provider: Any = None) -> None:
        self.tracer = trace.get_tracer(__name__, version("potatoq"), tracer_provider)
        meter = get_meter(__name__, version("potatoq"), meter_provider)
        self.sent = meter.create_counter(
            "messaging.client.sent.messages", unit="{message}", description="Task messages sent"
        )
        self.duration = meter.create_histogram(
            "messaging.process.duration",
            unit="s",
            description="Time taken to run a task (one attempt)",
            explicit_bucket_boundaries_advisory=_BUCKETS,
        )
        self.wait = meter.create_histogram(
            "potatoq.task.wait.duration",
            unit="s",
            description="Time a task waited in the queue after it was due",
            explicit_bucket_boundaries_advisory=_BUCKETS,
        )

    def message_created(self, message: Message) -> None:
        attributes = _base(message, "send")
        span_attributes = {
            **attributes,
            "messaging.message.id": message.id,
            "messaging.message.conversation_id": message.root_id or message.id,
        }
        if message.eta is not None:
            span_attributes["potatoq.task.eta"] = message.eta
        if message.retries:
            span_attributes["potatoq.task.retries"] = message.retries
        if message.headers.get("periodic"):
            span_attributes["potatoq.task.periodic"] = message.headers["periodic"]
        with self.tracer.start_as_current_span(
            f"send {message.task}", kind=SpanKind.PRODUCER, attributes=span_attributes
        ):
            propagate.inject(message.headers)
        self.sent.add(1, attributes)

    @contextmanager
    def task_run(self, run: TaskRun) -> Iterator[None]:
        message = run.message
        parent = propagate.extract(message.headers)
        attributes = {
            **_base(message, "process"),
            "messaging.message.id": message.id,
            "messaging.message.conversation_id": message.root_id or message.id,
            "potatoq.task.retries": message.retries,
            "potatoq.task.delivery_count": run.delivery_count,
        }
        if run.is_eager:
            attributes["potatoq.task.eager"] = True
        if run.periodic:
            attributes["potatoq.task.periodic"] = run.periodic
        if run.hostname:
            attributes["potatoq.worker.hostname"] = run.hostname
        span = self.tracer.start_span(
            f"process {message.task}", context=parent, kind=SpanKind.CONSUMER, attributes=attributes
        )
        token = context.attach(trace.set_span_in_context(span, parent))
        started = time.monotonic()
        error: BaseException | None = None
        try:
            yield
        except BaseException as exc:  # worker shutdown: the attempt never finished
            error = exc
            raise
        finally:
            context.detach(token)
            self._finish(span, run, error, time.monotonic() - started)

    def _finish(self, span: Any, run: TaskRun, error: BaseException | None, elapsed: float) -> None:
        metric = _base(run.message, "process")
        state = run.state or states.FAILURE
        span.set_attribute("potatoq.task.state", state)
        metric["potatoq.task.state"] = state
        exc = error or run.exception
        if run.state == states.RETRY:  # not an error (yet): the task runs again
            if exc is not None:
                span.record_exception(exc)
            if run.retry_eta is not None:
                span.set_attribute("potatoq.task.retry_eta", run.retry_eta)
        elif state in (states.FAILURE, states.REJECTED):
            error_type = type(exc).__qualname__ if exc is not None else state
            span.set_attribute("error.type", error_type)
            metric["error.type"] = error_type
            if exc is not None:
                span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, repr(exc) if exc is not None else run.reason))
        if run.reason:
            span.set_attribute("potatoq.task.reason", run.reason)
        span.end()
        if not run.process_died:
            self.duration.record(run.runtime or elapsed, metric)
            if not run.is_eager:
                self.wait.record(run.wait_time, _base(run.message, "process"))


def _flush(**kwargs: Any) -> None:
    """Export what's buffered before a worker process exits: children end with
    ``os._exit()``, which skips the SDK's own atexit flush."""
    for provider in (trace.get_tracer_provider(), get_meter_provider(), get_logger_provider()):
        flush = getattr(provider, "force_flush", None)
        if flush is not None:
            flush()


class PotatoqInstrumentor(BaseInstrumentor):
    """``instrument(tracer_provider=..., meter_provider=...)``: both default to the
    global providers."""

    def instrumentation_dependencies(self) -> Collection[str]:
        return ()

    def _instrument(self, **kwargs: Any) -> None:
        self._instrumentation = _OpenTelemetry(kwargs.get("tracer_provider"), kwargs.get("meter_provider"))
        instrumentation.register(self._instrumentation)
        signals.worker_process_shutdown.connect(_flush, dispatch_uid="potatoq.opentelemetry.flush")

    def _uninstrument(self, **kwargs: Any) -> None:
        instrumentation.unregister(self._instrumentation)
        signals.worker_process_shutdown.disconnect(_flush, dispatch_uid="potatoq.opentelemetry.flush")
