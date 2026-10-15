"""Hooks for tracing, APM and error tracking.

The integrations in ``potatoq.contrib`` (OpenTelemetry, Sentry, New Relic) are built on
two hooks, and your own can be too:

* :meth:`Instrumentation.message_created` is called whenever a task message is built:
  at ``.delay()``, for a workflow's next step, a retry or a periodic run. It runs in the
  code that sends the task, so it can copy the current trace context into
  ``message.headers``. A message deferred to the end of a transaction is built (and
  carries the context of) the ``.delay()`` call, not the commit.
* :meth:`Instrumentation.task_run` wraps every attempt at running a task, in workers and
  in eager mode. The task's body, its hooks and signals, and the building of its
  follow-up messages all run inside it. When a task's process dies (hard time limit,
  out of memory), the worker's supervisor reports that attempt instead, with
  ``run.process_died`` set and nothing in between.

Instrumentations are process-wide, like the SDKs they wrap. One that raises is logged
and skipped: it never breaks sending or running a task.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from typing import TYPE_CHECKING, Any

from . import states

if TYPE_CHECKING:
    from .app import Potatoq
    from .message import Message
    from .task import Task
    from .worker.executor import Outcome

logger = logging.getLogger("potatoq.instrumentation")

__all__ = ["Instrumentation", "TaskRun", "register", "registered", "unregister"]


class TaskRun:
    """One attempt at running a task, as instrumentation sees it.

    The outcome attributes (``state`` and below) are set when the attempt ends, before
    the instrumentation's ``task_run`` context exits."""

    #: The final state of this attempt: ``SUCCESS``, ``FAILURE``, ``RETRY``, ``IGNORED``,
    #: ``REVOKED`` or ``REJECTED``. ``None`` if it was interrupted (worker shutdown).
    state: str | None = None
    #: What the task raised (for ``FAILURE`` and ``RETRY``).
    exception: BaseException | None = None
    #: For ``RETRY``: when the next attempt is due (Unix time), or ``None`` for now.
    retry_eta: float | None = None
    #: Seconds the attempt took.
    runtime: float = 0.0
    #: Why it ended without running, failing or succeeding (``expired``, ``revoked``...).
    reason: str | None = None

    def __init__(
        self,
        app: Potatoq,
        message: Message,
        task: Task | None,
        *,
        delivery_count: int = 1,
        is_eager: bool = False,
        hostname: str | None = None,
        process_died: bool = False,
    ) -> None:
        #: The app running the task.
        self.app = app
        #: The task message (``message.headers`` carries the trace context).
        self.message = message
        #: The task, or ``None`` if this worker doesn't know it (it is dead-lettered).
        self.task = task
        #: How many times the broker delivered this message (``> 1`` after a crash).
        self.delivery_count = delivery_count
        #: ``True`` for ``task.apply()`` and ``task_always_eager``.
        self.is_eager = is_eager
        self.hostname = hostname
        #: ``True`` when the supervisor reports a task whose process died running it.
        self.process_died = process_died
        #: When the attempt started (Unix time).
        self.started_at = time.time()

    @property
    def name(self) -> str:
        return self.message.task

    @property
    def queue(self) -> str:
        return self.message.queue

    @property
    def retries(self) -> int:
        return self.message.retries

    @property
    def periodic(self) -> str | None:
        """The ``beat_schedule`` entry that sent this run, if any."""
        return self.message.headers.get("periodic")

    @property
    def wait_time(self) -> float:
        """Seconds the task waited in the queue after it was due."""
        due = max(self.message.enqueued_at, self.message.eta or 0.0)
        return max(0.0, self.started_at - due)

    @property
    def failed(self) -> bool:
        return self.state == states.FAILURE

    def finish(self, outcome: Outcome) -> None:
        self.state = outcome.state
        self.exception = outcome.exc
        self.runtime = outcome.runtime
        self.reason = outcome.reason
        if outcome.retry_message is not None:
            self.retry_eta = outcome.retry_message.eta

    def __repr__(self) -> str:
        return f"<TaskRun {self.name}[{self.message.id}] {self.state or 'running'}>"


class Instrumentation:
    """Base class for instrumentations: override the hooks you need, then
    :func:`register` an instance."""

    def message_created(self, message: Message) -> None:
        """A task message was built, in the sending code's context."""

    def task_run(self, run: TaskRun) -> AbstractContextManager[Any]:
        """A context manager around one attempt at running a task."""
        return nullcontext()


_registry: list[Instrumentation] = []


def register(instrumentation: Instrumentation) -> None:
    """Install ``instrumentation`` in this process, replacing one of the same class."""
    _registry[:] = [i for i in _registry if type(i) is not type(instrumentation)]
    _registry.append(instrumentation)


def unregister(instrumentation: Instrumentation) -> None:
    if instrumentation in _registry:
        _registry.remove(instrumentation)


def registered() -> list[Instrumentation]:
    return list(_registry)


def message_created(message: Message) -> None:
    for instrumentation in list(_registry):
        try:
            instrumentation.message_created(message)
        except Exception:
            logger.exception("%r failed on message %s[%s]", instrumentation, message.task, message.id)


@contextmanager
def task_run(run: TaskRun) -> Iterator[TaskRun]:
    """Enter every registered instrumentation's ``task_run`` around one attempt."""
    managers = []
    for instrumentation in list(_registry):
        try:
            manager = instrumentation.task_run(run)
            manager.__enter__()
        except Exception:
            logger.exception("%r failed to start %r", instrumentation, run)
            continue
        managers.append((instrumentation, manager))
    exc_info: tuple[Any, Any, Any] = (None, None, None)
    try:
        yield run
    except BaseException as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        raise
    finally:
        for instrumentation, manager in reversed(managers):
            try:
                manager.__exit__(*exc_info)
            except Exception:
                logger.exception("%r failed to finish %r", instrumentation, run)
