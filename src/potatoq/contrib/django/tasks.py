"""A ``django.tasks`` backend (Django 6.0+): run Django's built-in Tasks on potatoq.

.. code-block:: python

    TASKS = {
        "default": {
            "BACKEND": "potatoq.contrib.django.tasks.PotatoqBackend",
            "QUEUES": ["default", "emails"],  # Django validates queue_name against this
            # "OPTIONS": {"APP": "proj.potatoq:app"},  # default: the Django-configured app
        }
    }

.. code-block:: python

    from django.tasks import task

    @task(priority=10, time_limit=120, autoretry_for=(ConnectionError,))  # extra options: Django 6.1+
    def send_email(to, subject): ...

    result = send_email.enqueue("ann@example.com", "Hi")
    result.refresh(); result.status, result.return_value

Every ``django.tasks`` feature is supported: priorities (-100..100, higher first),
``run_after`` (stored by the broker), async tasks, ``takes_context``, ``get_result()``
with READY/RUNNING/SUCCESSFUL/FAILED, and the ``task_enqueued``/``task_started``/
``task_finished`` signals. On top of that, Django tasks get potatoq's guarantees:
at-least-once delivery, time limits, retries, dead letters and enqueue-on-commit.
"""

from __future__ import annotations

import inspect
import traceback
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from django.tasks import Task, TaskContext, TaskResult, TaskResultStatus
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import TaskError
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils import timezone
from django.utils.json import normalize_json
from django.utils.module_loading import import_string

from ... import states
from ...config import load_object

if TYPE_CHECKING:
    from ...app import Potatoq
    from ...task import Task as PotatoqTaskImpl

__all__ = ["PotatoqBackend", "PotatoqTask", "task"]

_STATUS = {
    states.PENDING: TaskResultStatus.READY,
    states.RECEIVED: TaskResultStatus.READY,
    states.RETRY: TaskResultStatus.READY,
    states.STARTED: TaskResultStatus.RUNNING,
    states.SUCCESS: TaskResultStatus.SUCCESSFUL,
    states.FAILURE: TaskResultStatus.FAILED,
    states.REVOKED: TaskResultStatus.FAILED,
}

#: potatoq task options accepted by ``@task(...)`` (Django 6.1 forwards extra kwargs).
_OPTIONS = (
    "time_limit",
    "soft_time_limit",
    "max_retries",
    "autoretry_for",
    "dont_autoretry_for",
    "retry_backoff",
    "retry_backoff_max",
    "retry_jitter",
    "default_retry_delay",
    "ignore_result",
    "enqueue_on_commit",
    "expires",
)


@dataclass(frozen=True, slots=True, kw_only=True)
class PotatoqTask(Task):
    """``django.tasks.Task`` plus potatoq's per-task options."""

    time_limit: float | None = None
    soft_time_limit: float | None = None
    max_retries: int | None = None
    autoretry_for: tuple[type[BaseException], ...] = ()
    dont_autoretry_for: tuple[type[BaseException], ...] = ()
    retry_backoff: Any = None
    retry_backoff_max: float | None = None
    retry_jitter: bool | None = None
    default_retry_delay: float | None = None
    ignore_result: bool | None = None
    enqueue_on_commit: bool | None = None
    expires: Any = None


def task(
    function: Any = None,
    *,
    priority: int = 0,
    queue_name: str = "default",
    backend: str = "default",
    takes_context: bool = False,
    **options: Any,
) -> Any:
    """``django.tasks.task`` that also accepts potatoq options (``time_limit``,
    ``autoretry_for``, ...) on Django 6.0, where the built-in decorator doesn't
    forward extra arguments. On Django 6.1+ both decorators are equivalent."""
    from django.tasks import task_backends

    unknown = set(options) - set(_OPTIONS)
    if unknown:
        raise TypeError(f"Unknown potatoq task option(s): {', '.join(sorted(unknown))}")

    def wrapper(f: Any) -> Task:
        return task_backends[backend].task_class(
            func=f,
            priority=priority,
            queue_name=queue_name,
            backend=backend,
            takes_context=takes_context,
            run_after=None,  # required on Django 6.0
            **options,
        )

    return wrapper(function) if function is not None else wrapper


def _aware(ts: float | None) -> datetime | None:
    return datetime.fromtimestamp(ts, UTC) if ts is not None else None


def _exception_path(result: Any) -> str:
    if isinstance(result, dict) and "exc_type" in result:
        return f"{result.get('exc_module') or 'builtins'}.{result['exc_type']}"
    return "builtins.Exception"


class PotatoqBackend(BaseTaskBackend):
    task_class = PotatoqTask
    supports_defer = True
    supports_async_task = True
    supports_priority = True

    def __init__(self, alias: str, params: dict[str, Any]):
        super().__init__(alias, params)
        self._app: Potatoq | None = None

    # --- the potatoq app -------------------------------------------------------------

    @property
    def app(self) -> Potatoq:
        if self._app is None:
            from ...app import current_app
            from . import install

            target = self.options.get("APP")
            app = (load_object(target) if isinstance(target, str) else target) if target else current_app()
            self.bind(app)
            install(app)
        return self._app  # type: ignore[return-value]

    def bind(self, app: Potatoq) -> None:
        """Run this backend's tasks on ``app``."""
        self._app = app
        if self._resolve not in app._task_resolvers:
            app._task_resolvers.append(self._resolve)

    @property
    def supports_get_result(self) -> bool:
        return self.app.backend is not None

    # --- registration ----------------------------------------------------------------

    def validate_task(self, task: Task) -> None:
        super().validate_task(task)
        self._register(task)

    def _register(self, task: Task) -> PotatoqTaskImpl:
        """The potatoq task that runs ``task`` (registered under its module path)."""
        app = self.app
        name = task.module_path
        existing = app.tasks.get(name)
        if existing is not None and getattr(existing, "_django_func", None) is task.func:
            return existing
        options = {opt: getattr(task, opt) for opt in _OPTIONS if getattr(task, opt, None) not in (None, ())}
        backend_cls = type(self)

        def run(self_: PotatoqTaskImpl, *args: Any, **kwargs: Any) -> Any:
            return _execute(backend_cls, self_, args, kwargs)

        run.__module__ = task.func.__module__
        run.__name__ = run.__qualname__ = task.func.__name__
        run.__doc__ = task.func.__doc__
        ptask = app._task_from_fun(run, name=name, bind=True, typing=False, **options)
        type(ptask)._django_task = task  # type: ignore[attr-defined]
        type(ptask)._django_func = task.func  # type: ignore[attr-defined]
        type(ptask)._django_alias = self.alias  # type: ignore[attr-defined]
        return ptask

    def _resolve(self, name: str) -> PotatoqTaskImpl | None:
        """Load a Django task on a worker that hasn't imported its module yet. Only
        modules inside INSTALLED_APPS are imported: names come from the broker."""
        from django.apps import apps

        module = name.rpartition(".")[0]
        if not any(module == cfg.name or module.startswith(cfg.name + ".") for cfg in apps.get_app_configs()):
            return None
        try:
            obj = import_string(name)
        except (ImportError, AttributeError):
            return None
        if isinstance(obj, Task) and obj.backend == self.alias:
            return self._register(obj)
        return None

    # --- enqueueing --------------------------------------------------------------------

    def enqueue(self, task: Task, args: Any, kwargs: Any) -> TaskResult:
        self.validate_task(task)
        ptask = self._register(task)
        args, kwargs = list(args), dict(kwargs)
        # Fail at enqueue time, not on the worker.
        call_args = [None, *args] if task.takes_context else args
        try:
            inspect.signature(task.func).bind(*call_args, **kwargs)
        except TypeError as exc:
            raise TypeError(f"{task.module_path}: {exc}") from None
        normalized_args, normalized_kwargs = normalize_json(args), normalize_json(kwargs)

        options: dict[str, Any] = {"queue": task.queue_name, "priority": task.priority}
        if task.run_after is not None:
            run_after = task.run_after
            options["eta"] = run_after if timezone.is_aware(run_after) else timezone.make_aware(run_after)
        async_result = ptask.apply_async(args, kwargs, **options)
        result = TaskResult(
            task=task,
            id=async_result.id,
            status=TaskResultStatus.READY,
            enqueued_at=timezone.now(),
            started_at=None,
            finished_at=None,
            last_attempted_at=None,
            args=normalized_args,
            kwargs=normalized_kwargs,
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )
        task_enqueued.send(type(self), task_result=result)
        return result

    # --- results -------------------------------------------------------------------

    def get_result(self, result_id: str) -> TaskResult:
        app = self.app
        record = app.backend.get_result(result_id) if app.backend is not None else None
        peeked = app.broker.peek(result_id)
        if record is not None and (record.ready or peeked is None):
            name, args, kwargs = record.task_name, record.args or [], record.kwargs or {}
            status = _STATUS.get(record.state, TaskResultStatus.READY)
            enqueued_at = _aware(record.enqueued_at)
            retries = record.retries
        elif peeked is not None:
            message, state = peeked
            name, args, kwargs = message.task, message.args, message.kwargs
            status = TaskResultStatus.RUNNING if state == "running" else TaskResultStatus.READY
            enqueued_at = _aware(message.enqueued_at)
            retries = message.retries
        else:
            raise TaskResultDoesNotExist(result_id)

        task = self._django_task(name)
        started = _aware(record.date_started) if record is not None else None
        attempts = retries + 1 if status != TaskResultStatus.READY else retries
        worker = (record.worker if record is not None else None) or "potatoq"
        result = TaskResult(
            task=task,
            id=result_id,
            status=status,
            enqueued_at=enqueued_at,
            started_at=started,
            finished_at=_aware(record.date_done) if record is not None and record.ready else None,
            last_attempted_at=started,
            args=args,
            kwargs=kwargs,
            backend=self.alias,
            errors=[],
            worker_ids=[worker] * attempts,
        )
        if status == TaskResultStatus.SUCCESSFUL:
            object.__setattr__(result, "_return_value", record.result)  # type: ignore[union-attr]
        elif status == TaskResultStatus.FAILED and record is not None:
            path = (
                "potatoq.exceptions.TaskRevokedError"
                if record.state == states.REVOKED
                else _exception_path(record.result)
            )
            result.errors.append(TaskError(exception_class_path=path, traceback=record.traceback or ""))
        return result

    def _django_task(self, name: str | None) -> Task:
        ptask = self.app.resolve_task(name) if name else None
        task = getattr(ptask, "_django_task", None) if ptask is not None else None
        if task is None:
            raise TaskResultDoesNotExist(f"{name!r} is not a django.tasks task")
        return task


def _execute(backend_cls: type, ptask: PotatoqTaskImpl, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    """Run a Django task inside a potatoq worker, with Django's context and signals."""
    from ...task import run_coroutine

    task: Task = type(ptask)._django_task  # type: ignore[attr-defined]
    request = ptask.request
    now = timezone.now()
    result = TaskResult(
        task=task,
        id=request.id,
        status=TaskResultStatus.RUNNING,
        enqueued_at=_aware(request.message.enqueued_at) if request.message else None,
        started_at=now,
        finished_at=None,
        last_attempted_at=now,
        args=list(args),
        kwargs=dict(kwargs),
        backend=type(ptask)._django_alias,  # type: ignore[attr-defined]
        errors=[],
        worker_ids=[request.hostname or "potatoq"] * (request.retries + 1),
    )
    task_started.send(sender=backend_cls, task_result=result)
    try:
        call_args = (TaskContext(task_result=result), *args) if task.takes_context else args
        value = task.func(*call_args, **kwargs)
        if inspect.iscoroutine(value):
            value = run_coroutine(value, request.timelimit[1] if request.timelimit else None)
        value = normalize_json(value)
    except BaseException as exc:
        object.__setattr__(result, "status", TaskResultStatus.FAILED)
        object.__setattr__(result, "finished_at", timezone.now())
        exc_type = type(exc)
        result.errors.append(
            TaskError(
                exception_class_path=f"{exc_type.__module__}.{exc_type.__qualname__}",
                traceback="".join(traceback.format_exception(exc)),
            )
        )
        task_finished.send(sender=backend_cls, task_result=result)
        raise
    object.__setattr__(result, "status", TaskResultStatus.SUCCESSFUL)
    object.__setattr__(result, "finished_at", timezone.now())
    object.__setattr__(result, "_return_value", value)
    task_finished.send(sender=backend_cls, task_result=result)
    return value
