"""Sentry: errors, performance and cron monitoring for tasks.

    import sentry_sdk
    from potatoq.contrib.sentry import PotatoqIntegration

    sentry_sdk.init(dsn=..., traces_sample_rate=0.1, integrations=[PotatoqIntegration()])

What it does, like Sentry's own Celery integration:

* **Errors.** A task that fails for good is reported, with the task's name, id, queue
  and retry count. Attempts that will be retried aren't: only the last one is. A task
  whose process died (hard time limit, out of memory) is reported too.
* **Tracing.** Each attempt is a transaction (op ``queue.task.potatoq``) with a
  ``queue.process`` span, continuing the trace of the code that sent the task, which
  gets a ``queue.publish`` span. The trace headers (``sentry-trace``, ``baggage``)
  travel in ``message.headers``. Sentry's Queues page shows the queue's latency and
  failure rate from these spans.
* **Breadcrumbs and tags** are per task: each attempt runs in its own isolation scope.
* **Crons**, with ``monitor_periodic_tasks=True``: each ``beat_schedule`` run sends
  check-ins, and the monitor is created from the schedule (crontabs and intervals of a
  minute or more). ``exclude_periodic_tasks`` takes regexes of entry names to skip.

Task arguments are only sent with ``send_default_pii=True``.
"""

from __future__ import annotations

import re
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import sentry_sdk
from sentry_sdk.consts import OP, SPANDATA, SPANSTATUS
from sentry_sdk.crons import MonitorStatus, capture_checkin
from sentry_sdk.integrations import Integration
from sentry_sdk.scope import should_send_default_pii
from sentry_sdk.tracing import BAGGAGE_HEADER_NAME, TransactionSource
from sentry_sdk.tracing_utils import Baggage
from sentry_sdk.utils import SENSITIVE_DATA_SUBSTITUTE, capture_internal_exceptions, event_from_exception

from .. import instrumentation, signals, states
from ..exceptions import SoftTimeLimitExceeded
from ..instrumentation import Instrumentation, TaskRun
from ..message import Message
from ..schedules import crontab
from ..worker.scheduler import load_entries

if TYPE_CHECKING:
    from sentry_sdk._types import MonitorConfig

__all__ = ["PotatoqIntegration"]

SYSTEM = "potatoq"

_STATUS = {
    states.SUCCESS: SPANSTATUS.OK,
    states.IGNORED: SPANSTATUS.OK,
    states.FAILURE: SPANSTATUS.INTERNAL_ERROR,
    states.RETRY: SPANSTATUS.ABORTED,
    states.REJECTED: SPANSTATUS.ABORTED,
    states.REVOKED: SPANSTATUS.CANCELLED,
}


class PotatoqIntegration(Integration):
    identifier = "potatoq"
    origin = f"auto.queue.{identifier}"

    def __init__(
        self,
        propagate_traces: bool = True,
        monitor_periodic_tasks: bool = False,
        exclude_periodic_tasks: list[str] | None = None,
    ) -> None:
        self.propagate_traces = propagate_traces
        self.monitor_periodic_tasks = monitor_periodic_tasks
        self.exclude_periodic_tasks = [re.compile(pattern) for pattern in exclude_periodic_tasks or ()]

    @staticmethod
    def setup_once() -> None:
        instrumentation.register(_Sentry())
        signals.worker_process_shutdown.connect(_flush, dispatch_uid="potatoq.sentry.flush")


def _integration() -> PotatoqIntegration | None:
    return sentry_sdk.get_client().get_integration(PotatoqIntegration)


class _Sentry(Instrumentation):
    def message_created(self, message: Message) -> None:
        integration = _integration()
        if integration is None or not integration.propagate_traces:
            return
        if message.headers.get("periodic") and sentry_sdk.get_current_span() is None:
            return  # the scheduler's: each run starts its own trace
        with capture_internal_exceptions():
            with sentry_sdk.start_span(
                op=OP.QUEUE_PUBLISH, name=message.task, origin=PotatoqIntegration.origin
            ) as span:
                span.set_data(SPANDATA.MESSAGING_MESSAGE_ID, message.id)
                span.set_data(SPANDATA.MESSAGING_DESTINATION_NAME, message.queue)
                span.set_data(SPANDATA.MESSAGING_SYSTEM, SYSTEM)
                for key, value in sentry_sdk.get_current_scope().iter_trace_propagation_headers(span=span):
                    if key == BAGGAGE_HEADER_NAME and message.headers.get(key):
                        value = _merge_baggage(value, message.headers[key])
                    message.headers[key] = value

    @contextmanager
    def task_run(self, run: TaskRun) -> Iterator[None]:
        integration = _integration()
        if integration is None:
            yield
            return
        with sentry_sdk.isolation_scope() as scope:
            scope.clear_breadcrumbs()
            scope.add_event_processor(_event_processor(run))
            scope.set_tag("potatoq_task", run.name)
            transaction = sentry_sdk.continue_trace(
                run.message.headers,
                op=f"queue.task.{SYSTEM}",
                name=run.name,
                source=TransactionSource.TASK,
                origin=PotatoqIntegration.origin,
            )
            sampling = {"potatoq_job": {"task": run.name, "queue": run.queue, "retries": run.retries}}
            checkin = _start_checkin(integration, run)
            with sentry_sdk.start_transaction(transaction, custom_sampling_context=sampling):
                with sentry_sdk.start_span(
                    op=OP.QUEUE_PROCESS, name=run.name, origin=PotatoqIntegration.origin
                ) as span:
                    span.set_data(SPANDATA.MESSAGING_MESSAGE_ID, run.message.id)
                    span.set_data(SPANDATA.MESSAGING_DESTINATION_NAME, run.queue)
                    span.set_data(SPANDATA.MESSAGING_SYSTEM, SYSTEM)
                    span.set_data(SPANDATA.MESSAGING_MESSAGE_RETRY_COUNT, run.retries)
                    if not run.is_eager and not run.process_died:
                        span.set_data(SPANDATA.MESSAGING_MESSAGE_RECEIVE_LATENCY, run.wait_time * 1000)
                    try:
                        yield
                    finally:
                        status = _STATUS.get(run.state or "", SPANSTATUS.ABORTED)  # None: shutdown
                        span.set_status(status)
                        transaction.set_status(status)
                        if run.state == states.FAILURE and run.exception is not None:
                            _capture(run.exception)
                        if checkin is not None:
                            _finish_checkin(checkin, run)


def _merge_baggage(sentry: str, existing: str) -> str:
    """Sentry's own entries, plus the other ones already there (OpenTelemetry's...)."""
    combined = Baggage.from_incoming_header(sentry)
    other = Baggage.from_incoming_header(existing)
    combined.third_party_items = ",".join(i for i in (combined.third_party_items, other.third_party_items) if i)
    return combined.serialize(include_third_party=True)


def _capture(exc: BaseException) -> None:
    with capture_internal_exceptions():
        client = sentry_sdk.get_client()
        event, hint = event_from_exception(
            (type(exc), exc, exc.__traceback__),
            client_options=client.options,
            mechanism={"type": PotatoqIntegration.identifier, "handled": False},
        )
        sentry_sdk.capture_event(event, hint=hint)


def _event_processor(run: TaskRun) -> Any:
    def process(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any]:
        with capture_internal_exceptions():
            event.setdefault("tags", {})["potatoq_task_id"] = run.message.id
            job: dict[str, Any] = {
                "task_name": run.name,
                "task_id": run.message.id,
                "queue": run.queue,
                "retries": run.retries,
                "delivery_count": run.delivery_count,
                "root_id": run.message.root_id,
            }
            if run.periodic:
                job["periodic"] = run.periodic
            if run.process_died:
                job["process_died"] = True
            if should_send_default_pii():
                job["args"], job["kwargs"] = run.message.args, run.message.kwargs
            else:
                job["args"] = job["kwargs"] = SENSITIVE_DATA_SUBSTITUTE
            event.setdefault("extra", {})["potatoq-job"] = job
            exc_info = hint.get("exc_info")
            if exc_info and issubclass(exc_info[0], SoftTimeLimitExceeded):
                # One issue per task, not one per place the limit happened to hit.
                event["fingerprint"] = ["potatoq", "SoftTimeLimitExceeded", run.name]
        return event

    return process


def _monitor_slug(name: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", name.lower()).strip("-")[:50]


def _monitor_config(run: TaskRun) -> MonitorConfig | None:
    entry = next((e for e in load_entries(run.app) if e.name == run.periodic), None)
    if entry is None:
        return None
    schedule = entry.schedule
    if isinstance(schedule, crontab):
        minute, hour, day_of_week, day_of_month, month = schedule._orig
        cron = f"{minute} {hour} {day_of_month} {month} {day_of_week}"
        return {"schedule": {"type": "crontab", "value": cron}, "timezone": str(schedule.tz or "UTC")}
    seconds = getattr(schedule, "seconds", None)
    if not seconds or seconds < 60:
        return None  # Sentry's smallest interval is a minute
    minutes = round(seconds / 60)
    if minutes % 1440 == 0:
        return {"schedule": {"type": "interval", "value": minutes // 1440, "unit": "day"}}
    if minutes % 60 == 0:
        return {"schedule": {"type": "interval", "value": minutes // 60, "unit": "hour"}}
    return {"schedule": {"type": "interval", "value": minutes, "unit": "minute"}}


def _start_checkin(integration: PotatoqIntegration, run: TaskRun) -> tuple[str, str | None, float] | None:
    name = run.periodic
    if not integration.monitor_periodic_tasks or not name or run.is_eager:
        return None
    if any(pattern.match(name) for pattern in integration.exclude_periodic_tasks):
        return None
    with capture_internal_exceptions():
        slug = _monitor_slug(name)
        checkin_id = capture_checkin(
            monitor_slug=slug, status=MonitorStatus.IN_PROGRESS, monitor_config=_monitor_config(run)
        )
        return slug, checkin_id, time.monotonic()
    return None  # couldn't send it


def _finish_checkin(checkin: tuple[str, str | None, float], run: TaskRun) -> None:
    slug, checkin_id, started = checkin
    status = MonitorStatus.OK if run.state in (states.SUCCESS, states.IGNORED) else MonitorStatus.ERROR
    with capture_internal_exceptions():
        capture_checkin(monitor_slug=slug, check_in_id=checkin_id, status=status, duration=time.monotonic() - started)


def _flush(**kwargs: Any) -> None:
    """Send what's queued before a worker process exits (with ``os._exit()``)."""
    if _integration() is not None:
        sentry_sdk.flush()
