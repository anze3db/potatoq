"""New Relic: tasks as background transactions, with distributed tracing.

    import newrelic.agent
    newrelic.agent.initialize()           # or start the process with newrelic-admin run-program

    from potatoq.contrib import newrelic as potatoq_newrelic
    potatoq_newrelic.instrument()

Each attempt at running a task is a background transaction in the ``potatoq`` group,
named after the task, that continues the distributed trace of the code that sent it
(``traceparent``/``tracestate`` travel in ``message.headers``; sending is recorded as a
``MessageBroker/potatoq/Queue/Produce`` segment). A task run eagerly inside another
transaction, such as a web request, is a function trace in that transaction instead.

A task that fails for good is reported with ``notice_error``; attempts that will be
retried aren't. The transaction carries ``potatoq.*`` attributes: task id, queue,
retries, deliveries, final state and the time it waited in the queue.

The agent is activated in each worker process when it starts, and shut down (which
sends what it has collected) before the process exits.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import newrelic.agent

from .. import instrumentation, signals, states
from ..instrumentation import Instrumentation, TaskRun
from ..message import Message

__all__ = ["instrument", "uninstrument"]

GROUP = "potatoq"


class _NewRelic(Instrumentation):
    def message_created(self, message: Message) -> None:
        transaction = newrelic.agent.current_transaction()
        if transaction is None:
            return
        with newrelic.agent.MessageTrace(GROUP, "Produce", "Queue", message.queue):
            headers: list[tuple[str, str]] = []
            newrelic.agent.insert_distributed_trace_headers(headers)
            message.headers.update(headers)

    @contextmanager
    def task_run(self, run: TaskRun) -> Iterator[None]:
        if newrelic.agent.current_transaction() is not None:
            # Eager, inside a web request or another task: part of that transaction.
            with newrelic.agent.FunctionTrace(run.name, group=GROUP):
                try:
                    yield
                finally:
                    _notice_failure(run)
            return
        with newrelic.agent.BackgroundTask(newrelic.agent.application(), run.name, group=GROUP) as transaction:
            transaction.accept_distributed_trace_headers(run.message.headers, transport_type="Queue")
            attributes: list[tuple[str, Any]] = [
                ("potatoq.task_id", run.message.id),
                ("potatoq.queue", run.queue),
                ("potatoq.retries", run.retries),
                ("potatoq.delivery_count", run.delivery_count),
            ]
            if not run.is_eager and not run.process_died:
                attributes.append(("potatoq.wait_time", run.wait_time))
            if run.periodic:
                attributes.append(("potatoq.periodic", run.periodic))
            newrelic.agent.add_custom_attributes(attributes)
            try:
                yield
            finally:
                newrelic.agent.add_custom_attribute("potatoq.state", run.state or "interrupted")
                if run.process_died:
                    newrelic.agent.add_custom_attribute("potatoq.process_died", True)
                _notice_failure(run)


def _notice_failure(run: TaskRun) -> None:
    exc = run.exception
    if run.state == states.FAILURE and exc is not None:
        newrelic.agent.notice_error(error=(type(exc), exc, exc.__traceback__))


def _activate(**kwargs: Any) -> None:
    """Start connecting the agent in a new worker process, without waiting for it."""
    newrelic.agent.application().activate()


def _shutdown(**kwargs: Any) -> None:
    """Send what the agent has collected: worker processes end with ``os._exit()``."""
    newrelic.agent.shutdown_agent()


_instrumentation = _NewRelic()


def instrument() -> None:
    """Trace potatoq tasks in this process (and the worker processes it starts)."""
    instrumentation.register(_instrumentation)
    signals.worker_init.connect(_activate, dispatch_uid="potatoq.newrelic.activate")
    signals.worker_process_init.connect(_activate, dispatch_uid="potatoq.newrelic.activate.process")
    signals.worker_process_shutdown.connect(_shutdown, dispatch_uid="potatoq.newrelic.shutdown")


def uninstrument() -> None:
    instrumentation.unregister(_instrumentation)
    signals.worker_init.disconnect(_activate, dispatch_uid="potatoq.newrelic.activate")
    signals.worker_process_init.disconnect(_activate, dispatch_uid="potatoq.newrelic.activate.process")
    signals.worker_process_shutdown.disconnect(_shutdown, dispatch_uid="potatoq.newrelic.shutdown")
