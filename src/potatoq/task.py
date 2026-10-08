"""The ``Task`` class: Celery-compatible decorator target."""

from __future__ import annotations

import inspect
import logging
import random
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar

from . import serialization, states
from .exceptions import MaxRetriesExceededError, Retry
from .message import Message, new_id

if TYPE_CHECKING:
    from .app import Potatoq
    from .canvas import Signature
    from .result import AsyncResult, EagerResult

logger = logging.getLogger("potatoq.task")


class Context:
    """``self.request`` inside a running task."""

    id: str | None = None
    task: str | None = None
    args: Any = None
    kwargs: Any = None
    retries: int = 0
    delivery_count: int = 1
    eta: float | None = None
    expires: float | None = None
    is_eager: bool = False
    called_directly: bool = True
    hostname: str | None = None
    root_id: str | None = None
    parent_id: str | None = None
    group: str | None = None
    group_index: int | None = None
    headers: dict[str, Any] | None = None
    delivery_info: dict[str, Any] | None = None
    timelimit: tuple[float | None, float | None] = (None, None)
    message: Message | None = None
    ignore_result: bool = False
    chord: Any = None
    callbacks: Any = None
    errbacks: Any = None
    origin: str | None = None
    started_at: float | None = None

    @property
    def correlation_id(self) -> str | None:
        return self.id

    def __init__(self, **kwargs: Any):
        self.__dict__.update(kwargs)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def __repr__(self) -> str:
        return f"<Context: {self.__dict__!r}>"


_loop_state = threading.local()


def run_coroutine(coro: Any, soft_time_limit: float | None = None) -> Any:
    """Run ``coro`` to completion on this thread's persistent event loop."""
    import asyncio

    from .exceptions import SoftTimeLimitExceeded

    loop = getattr(_loop_state, "loop", None)
    if loop is None or loop.is_closed():
        loop = _loop_state.loop = asyncio.new_event_loop()
    if soft_time_limit:
        coro = asyncio.wait_for(coro, soft_time_limit)
    fut = loop.create_task(coro)
    try:
        return loop.run_until_complete(fut)
    except TimeoutError as exc:
        raise SoftTimeLimitExceeded(f"soft time limit ({soft_time_limit}s) exceeded") from exc
    except BaseException:
        if not fut.done():
            fut.cancel()
            try:
                loop.run_until_complete(fut)
            except BaseException:
                pass
        raise


class _RequestStack(threading.local):
    def __init__(self) -> None:
        self.stack: list[Context] = []


def _to_seconds(value: float | timedelta | None) -> float | None:
    if isinstance(value, timedelta):
        return value.total_seconds()
    return value


def _to_timestamp(value: float | datetime | timedelta | None, now: float) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError(
                "Naive datetimes are ambiguous; pass a timezone-aware datetime "
                "(e.g. datetime.now(UTC) + timedelta(...)) or use countdown=."
            )
        return value.timestamp()
    if isinstance(value, timedelta):
        return now + value.total_seconds()
    return now + float(value)


class Task:
    """Base class for tasks. Usually created with ``@app.task``."""

    #: Set by the decorator.
    name: str = None  # type: ignore[assignment]
    app: Potatoq = None  # type: ignore[assignment]
    run: Callable[..., Any]

    # Options (None means "use the app-level default").
    typing: bool = True
    max_retries: int | None = None
    default_retry_delay: float | None = None
    autoretry_for: tuple[type[BaseException], ...] = ()
    dont_autoretry_for: tuple[type[BaseException], ...] = ()
    retry_kwargs: dict[str, Any] = {}
    retry_backoff: bool | float | None = None
    retry_backoff_max: float | None = None
    retry_jitter: bool | None = None
    acks_late: bool | None = None
    enqueue_on_commit: bool | None = None
    reject_on_worker_lost: bool | None = None
    ignore_result: bool | None = None
    store_errors_even_if_ignored: bool = False
    track_started: bool | None = None
    time_limit: float | None = None
    soft_time_limit: float | None = None
    rate_limit: str | None = None
    queue: str | None = None
    priority: int | None = None
    expires: float | timedelta | None = None
    serializer: str = "json"
    bind: bool = False
    abstract: bool = False

    #: Task options that can be passed to the decorator.
    OPTION_NAMES: ClassVar[frozenset[str]] = frozenset(
        {
            "name", "typing", "max_retries", "default_retry_delay", "autoretry_for",
            "dont_autoretry_for", "retry_kwargs", "retry_backoff", "retry_backoff_max",
            "retry_jitter", "acks_late", "reject_on_worker_lost", "ignore_result",
            "store_errors_even_if_ignored", "track_started", "time_limit",
            "soft_time_limit", "rate_limit", "queue", "priority", "expires", "serializer",
            "bind", "shared", "base", "lazy", "enqueue_on_commit", "trail", "send_events", "routing_key",
            "exchange", "pydantic", "throws", "resultrepr_maxsize", "acks_on_failure_or_timeout",
        }
    )  # fmt: skip

    _request_stack: _RequestStack

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._request_stack = _RequestStack()

    # --- direct call -------------------------------------------------------------

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        stack = self._request_stack.stack
        if not stack:
            # Called directly as a function: give it a request like Celery does.
            stack.append(Context(called_directly=True, args=args, kwargs=kwargs, task=self.name))
            try:
                return self.run(*args, **kwargs)
            finally:
                stack.pop()
        result = self.run(*args, **kwargs)
        if inspect.iscoroutine(result):
            # ``async def`` tasks run on a per-process event loop that is kept
            # between tasks, so async clients/pools can be reused.
            result = run_coroutine(result, self.request.timelimit[1] if self.request.timelimit else None)
        return result

    @property
    def request(self) -> Context:
        stack = self._request_stack.stack
        return stack[-1] if stack else Context()

    def push_request(self, **kwargs: Any) -> Context:
        ctx = Context(**kwargs)
        self._request_stack.stack.append(ctx)
        return ctx

    def pop_request(self) -> None:
        self._request_stack.stack.pop()

    # --- option resolution -------------------------------------------------------

    def _opt(self, name: str, conf_name: str) -> Any:
        value = getattr(self, name)
        return self.app.conf[conf_name] if value is None else value

    def resolved_max_retries(self) -> int | None:
        if self.max_retries is not None:
            return self.max_retries
        if "max_retries" in self.retry_kwargs:
            return self.retry_kwargs["max_retries"]
        return self.app.conf.task_max_retries

    def resolved_ignore_result(self) -> bool:
        value = self._opt("ignore_result", "task_ignore_result")
        if value is None:
            return not self.app.results_enabled_by_default()
        return bool(value)

    def resolved_time_limits(self, options: dict[str, Any] | None = None) -> tuple[float | None, float | None]:
        options = options or {}
        hard = options.get("time_limit", self.time_limit)
        if hard is None:
            hard = self.app.conf.task_time_limit
        soft = options.get("soft_time_limit", self.soft_time_limit)
        if soft is None:
            soft = self.app.conf.task_soft_time_limit
        hard, soft = _to_seconds(hard), _to_seconds(soft)
        if soft is None and hard:
            # Give tasks a chance to clean up before the process is killed.
            soft = max(hard - min(hard * 0.1, 30.0), hard * 0.5)
        if soft and hard and soft >= hard:
            soft = None
        return hard, soft

    # --- publishing --------------------------------------------------------------

    def delay(self, *args: Any, **kwargs: Any) -> AsyncResult:
        return self.apply_async(args, kwargs)

    def apply_async(
        self,
        args: Any = None,
        kwargs: dict[str, Any] | None = None,
        task_id: str | None = None,
        producer: Any = None,
        link: Any = None,
        link_error: Any = None,
        shadow: str | None = None,
        **options: Any,
    ) -> AsyncResult:
        args = list(args or ())
        kwargs = dict(kwargs or {})
        if self.typing:
            self._check_arguments(args, kwargs)
        if self.app.conf.task_always_eager or options.pop("always_eager", False):
            return self.apply(args, kwargs, task_id=task_id, link=link, link_error=link_error, **options)
        connection = options.pop("connection", None)
        using = options.pop("using", None)
        on_commit = options.pop("enqueue_on_commit", None)
        if on_commit is None:
            on_commit = self.enqueue_on_commit
        message = self.build_message(args, kwargs, task_id, link=link, link_error=link_error, **options)
        self.app.publish([message], connection=connection, on_commit=on_commit, using=using)
        return self.AsyncResult(message.id, ignored=message.ignore_result)

    def build_message(
        self,
        args: list[Any],
        kwargs: dict[str, Any],
        task_id: str | None = None,
        *,
        link: Any = None,
        link_error: Any = None,
        **options: Any,
    ) -> Message:
        app = self.app
        now = time.time()
        countdown = options.pop("countdown", None)
        eta = options.pop("eta", None)
        if countdown is not None and eta is not None:
            raise ValueError("Pass either countdown or eta, not both")
        eta_ts = _to_timestamp(eta if eta is not None else countdown, now)
        if eta_ts is not None and eta_ts <= now:
            eta_ts = None
        expires = options.pop("expires", None)
        if expires is None:
            expires = self.expires
        expires_ts = _to_timestamp(expires, now)

        route = app.route_for(self.name)
        queue = options.pop("queue", None) or self.queue or route.get("queue") or app.conf.task_default_queue
        priority = options.pop("priority", None)
        if priority is None:
            priority = self.priority if self.priority is not None else route.get("priority")
        if priority is None:
            priority = app.conf.task_default_priority

        ignore_result = options.pop("ignore_result", None)
        if ignore_result is None:
            ignore_result = self.resolved_ignore_result()

        parent = app.current_task_request()
        root_id = options.pop("root_id", None) or (parent.root_id or parent.id if parent else None)
        parent_id = options.pop("parent_id", None) or (parent.id if parent else None)

        from .canvas import signatures_to_list

        overrides: dict[str, Any] = {
            k: options.pop(k)
            for k in ("time_limit", "soft_time_limit", "max_retries", "retry_policy_delay")
            if k in options and options[k] is not None
        }
        headers = dict(options.pop("headers", None) or {})
        message = Message(
            task=self.name,
            args=args,
            kwargs=kwargs,
            id=task_id or new_id(),
            queue=queue,
            priority=int(priority),
            eta=eta_ts,
            expires=expires_ts,
            retries=int(options.pop("retries", 0)),
            root_id=root_id,
            parent_id=parent_id,
            group_id=options.pop("group_id", None),
            group_index=options.pop("group_index", None),
            options=overrides,
            link=signatures_to_list(link),
            link_error=signatures_to_list(link_error),
            chord=options.pop("chord", None),
            ignore_result=bool(ignore_result),
            headers=headers,
            enqueued_at=now,
        )
        if message.root_id is None:
            message.root_id = message.id
        serialization.check_serializable(message.args)
        serialization.check_serializable(message.kwargs)
        return message

    def _check_arguments(self, args: list[Any], kwargs: dict[str, Any]) -> None:
        """Fail fast at ``.delay()`` time on a signature mismatch (Celery's ``typing``)."""
        try:
            sig = inspect.signature(self.run)
        except (TypeError, ValueError):
            return
        try:
            sig.bind(*args, **kwargs)
        except TypeError as exc:
            raise TypeError(f"{self.name}{sig}: {exc}") from None

    def delay_on_commit(self, *args: Any, **kwargs: Any) -> None:
        """Enqueue when the current database transaction commits (Celery 5.4 API).

        With Potatoq this is what ``delay()`` already does by default inside a
        transaction (``task_enqueue_on_commit``); this method forces it.
        """
        self.apply_async(args, kwargs, enqueue_on_commit=True)

    def apply_async_on_commit(self, args: Any = None, kwargs: dict[str, Any] | None = None, **options: Any) -> None:
        options["enqueue_on_commit"] = True
        self.apply_async(args, kwargs, **options)

    async def adelay(self, *args: Any, **kwargs: Any) -> AsyncResult:
        """``delay`` for async code: the broker round trip runs in a thread."""
        import asyncio

        return await asyncio.to_thread(self.apply_async, args, kwargs)

    async def aapply_async(self, args: Any = None, kwargs: dict[str, Any] | None = None, **options: Any) -> AsyncResult:
        import asyncio

        return await asyncio.to_thread(lambda: self.apply_async(args, kwargs, **options))

    def enqueue(self, *args: Any, **kwargs: Any) -> AsyncResult:
        """django.tasks style alias for ``delay``."""
        return self.apply_async(args, kwargs)

    def apply(
        self,
        args: Any = None,
        kwargs: dict[str, Any] | None = None,
        task_id: str | None = None,
        link: Any = None,
        link_error: Any = None,
        **options: Any,
    ) -> EagerResult:
        """Run the task in this process and return an ``EagerResult``."""
        from .worker.executor import execute_eagerly

        args = list(args or ())
        kwargs = dict(kwargs or {})
        throw = options.pop("throw", None)
        if throw is None:
            throw = self.app.conf.task_eager_propagates
        message = self.build_message(args, kwargs, task_id, link=link, link_error=link_error, **options)
        message.eta = None
        # Round-trip through the serializer so eager tests see what a worker would.
        message = Message.decode(message.encode())
        return execute_eagerly(self.app, message, throw=throw)

    def send(self, *args: Any, **kwargs: Any) -> AsyncResult:
        return self.delay(*args, **kwargs)

    # --- signatures --------------------------------------------------------------

    def signature(self, args: Any = None, *starargs: Any, **starkwargs: Any) -> Signature:
        from .canvas import Signature

        kwargs = starkwargs.pop("kwargs", None)
        options = starkwargs.pop("options", None) or {}
        options.update(starkwargs)
        if starargs:
            args = (args, *starargs)
        return Signature(self.name, args, kwargs, options, app=self.app)

    subtask = signature

    def s(self, *args: Any, **kwargs: Any) -> Signature:
        from .canvas import Signature

        return Signature(self.name, args, kwargs, app=self.app)

    def si(self, *args: Any, **kwargs: Any) -> Signature:
        from .canvas import Signature

        return Signature(self.name, args, kwargs, immutable=True, app=self.app)

    def map(self, it: Any) -> Any:
        from .canvas import group

        return group(self.s(item) for item in it)

    def starmap(self, it: Any) -> Any:
        from .canvas import group

        return group(self.s(*item) for item in it)

    def chunks(self, it: Any, n: int) -> Any:
        from .canvas import group

        items = list(it)
        return group(self.app.tasks["potatoq.starmap"].s(self.name, items[i : i + n]) for i in range(0, len(items), n))

    # --- retry -------------------------------------------------------------------

    def retry(
        self,
        args: Any = None,
        kwargs: dict[str, Any] | None = None,
        exc: BaseException | None = None,
        throw: bool = True,
        eta: datetime | None = None,
        countdown: float | None = None,
        max_retries: int | None = None,
        **options: Any,
    ) -> Retry:
        """Retry the current task. Raises ``Retry`` (or ``exc`` once retries are exhausted)."""
        request = self.request
        if request.called_directly:
            # Celery re-raises the exception when a task is called as a function.
            if exc is not None:
                raise exc
            raise MaxRetriesExceededError("Task can't be retried when called directly")

        if max_retries is None:
            max_retries = request.message.options.get("max_retries") if request.message else None
        if max_retries is None:
            max_retries = self.resolved_max_retries()
        if max_retries is not None and request.retries >= max_retries:
            if exc is not None:
                raise exc
            raise MaxRetriesExceededError(
                f"Can't retry {self.name}[{request.id}] args:{request.args} kwargs:{request.kwargs}",
                task_args=request.args,
                task_kwargs=request.kwargs,
            )

        if eta is None and countdown is None:
            countdown = self.backoff_delay(request.retries)
        when: float | datetime = eta if eta is not None else float(countdown or 0)
        retry = Retry(exc=exc, when=when)
        retry.args_override = args  # type: ignore[attr-defined]
        retry.kwargs_override = kwargs  # type: ignore[attr-defined]
        retry.options = options  # type: ignore[attr-defined]
        retry.max_retries = max_retries  # type: ignore[attr-defined]
        if throw:
            raise retry
        return retry

    def backoff_delay(self, retries: int) -> float:
        """Delay before the next automatic retry (``autoretry_for`` + ``retry_backoff``)."""
        backoff = self.retry_kwargs.get("retry_backoff", self.retry_backoff)
        if backoff is None:
            if self.default_retry_delay is not None or "countdown" in self.retry_kwargs:
                backoff = False  # an explicit fixed delay on the task wins over the app default
            else:
                backoff = self.app.conf.task_retry_backoff
        if not backoff:
            countdown = self.retry_kwargs.get("countdown", self.default_retry_delay)
            if countdown is None:
                countdown = self.app.conf.task_default_retry_delay
            return float(countdown)
        factor = 1.0 if backoff is True else float(backoff)
        maximum = self.retry_backoff_max
        if maximum is None:
            maximum = self.app.conf.task_retry_backoff_max
        delay = min(maximum, factor * (2**retries))
        jitter = self.retry_jitter if self.retry_jitter is not None else self.app.conf.task_retry_jitter
        if jitter:
            # "Equal jitter": spread retries out without ever retrying immediately.
            delay = random.uniform(delay / 2, delay)
        return delay

    # --- results / state ---------------------------------------------------------

    def replace(self, sig: Any) -> None:
        """Replace this task with ``sig``, which inherits its id, callbacks and chord.

        The current task ends and the replacement is enqueued atomically with its
        acknowledgement.
        """
        from .canvas import maybe_signature
        from .exceptions import Replace

        request = self.request
        if request.called_directly or request.message is None:
            raise RuntimeError("replace() only works inside a running task")
        sig = maybe_signature(sig, self.app)
        if sig is None:
            raise TypeError("replace() needs a signature")
        sig = sig.clone()
        message = request.message
        sig.set(task_id=request.id)
        if message.link:
            sig["options"]["link"] = [*sig["options"].get("link", []), *message.link]
        if message.link_error:
            sig["options"]["link_error"] = [*sig["options"].get("link_error", []), *message.link_error]
        if message.chord:
            sig.set(chord=message.chord, group_id=message.group_id, group_index=message.group_index)
        raise Replace(sig)

    @property
    def backend(self) -> Any:
        return self.app.backend

    def AsyncResult(self, task_id: str, **kwargs: Any) -> AsyncResult:
        from .result import AsyncResult

        return AsyncResult(task_id, app=self.app, task_name=self.name, **kwargs)

    def update_state(
        self, task_id: str | None = None, state: str | None = None, meta: Any = None, **kwargs: Any
    ) -> None:
        task_id = task_id or self.request.id
        if not task_id:
            return
        self.app.store_result(task_id, state or states.STARTED, meta, task_name=self.name)

    # --- hooks (override in subclasses) -------------------------------------------

    def before_start(self, task_id: str, args: Any, kwargs: Any) -> None:
        pass

    def on_success(self, retval: Any, task_id: str, args: Any, kwargs: Any) -> None:
        pass

    def on_failure(self, exc: BaseException, task_id: str, args: Any, kwargs: Any, einfo: Any) -> None:
        pass

    def on_retry(self, exc: BaseException, task_id: str, args: Any, kwargs: Any, einfo: Any) -> None:
        pass

    def after_return(self, status: str, retval: Any, task_id: str, args: Any, kwargs: Any, einfo: Any) -> None:
        pass

    def __repr__(self) -> str:
        return f"<@task: {self.name}>"

    def __reduce__(self) -> Any:
        # Tasks pickle by name so they can be passed to multiprocessing etc.
        return (_unpickle_task, (self.name,))


def _unpickle_task(name: str) -> Task:
    from .app import current_app

    return current_app().tasks[name]
