"""Exceptions, named after their ``celery.exceptions`` counterparts."""

from __future__ import annotations

import builtins
from datetime import datetime
from typing import Any


class PotatoqError(Exception):
    """Base class for all Potatoq errors."""


class ImproperlyConfigured(PotatoqError):
    """Potatoq is configured incorrectly."""


class NotRegistered(PotatoqError, KeyError):
    """The task is not registered with the app."""

    def __str__(self) -> str:
        return f"Task {self.args[0]!r} is not registered. Did you import the module that defines it?"


class TaskError(PotatoqError):
    """Base class for errors raised from inside tasks."""


class Retry(TaskError):
    """Raised by ``Task.retry()`` to signal that the task should be retried."""

    def __init__(
        self,
        message: str | None = None,
        exc: BaseException | None = None,
        when: float | datetime | None = None,
        **kwargs: Any,
    ):
        self.message = message
        self.exc = exc
        self.when = when
        super().__init__(message or (repr(exc) if exc else "Retry"), exc, when)

    def humanize(self) -> str:
        if isinstance(self.when, (int, float)):
            return f"in {round(self.when, 2):g}s"
        return f"at {self.when}"

    def __str__(self) -> str:
        if self.message:
            return self.message
        if self.exc:
            return f"Retry {self.humanize()}: {self.exc!r}"
        return f"Retry {self.humanize()}"


RetryTaskError = Retry


class MaxRetriesExceededError(TaskError):
    """The task has been retried the maximum number of times."""

    def __init__(self, *args: Any, task_args: Any = None, task_kwargs: Any = None, **kwargs: Any):
        self.task_args = task_args
        self.task_kwargs = task_kwargs
        super().__init__(*args)


class Ignore(TaskError):
    """Raise from a task to stop processing without recording a state."""


class Replace(Ignore):
    """Raised by ``Task.replace()``; carries the replacement signature."""

    def __init__(self, sig: Any):
        self.sig = sig
        super().__init__(sig)


class Reject(TaskError):
    """Raise from a task to reject the message (optionally requeueing it)."""

    def __init__(self, reason: Any = None, requeue: bool = False):
        self.reason = reason
        self.requeue = requeue
        super().__init__(reason, requeue)


class TimeoutError(PotatoqError, builtins.TimeoutError):
    """Waiting for a result timed out."""


class TimeLimitExceeded(PotatoqError):
    """The hard time limit of a task was exceeded and the worker process was killed."""


class SoftTimeLimitExceeded(PotatoqError):
    """Raised inside a task when its soft time limit is exceeded.

    Deliberately inherits from ``Exception`` (via PotatoqError) like Celery, so tasks
    can catch it to clean up.
    """


class WorkerLostError(PotatoqError):
    """The worker process executing the task exited unexpectedly."""


class TaskRevokedError(PotatoqError):
    """The task was revoked and will not run."""


class ResultBackendDisabled(ImproperlyConfigured):
    """Results were requested but no result backend is available."""


class DeadLettered(PotatoqError):
    """The task was delivered too many times without completing and was dead-lettered."""


class RemoteError(PotatoqError):
    """Stand-in for an exception raised by a task that could not be reconstructed locally."""

    def __init__(self, exc_type: str, exc_message: str, exc_module: str | None = None):
        self.exc_type = exc_type
        self.exc_module = exc_module
        self.exc_message = exc_message
        super().__init__(f"{exc_type}: {exc_message}")


class ChordError(PotatoqError):
    """A task in a chord's header failed, so the chord callback can't run."""


class WorkerTerminate(BaseException):
    """Raised inside a running task when the worker is forced to stop (shutdown
    timeout). The task is requeued, not failed. BaseException so tasks don't catch it."""
