"""The application object: ``Potatoq`` (also importable as ``Celery``)."""

from __future__ import annotations

import fnmatch
import importlib
import logging
import os
import re
import threading
import time
import warnings
from collections.abc import Callable, Iterable
from functools import cached_property
from typing import TYPE_CHECKING, Any

from . import signals, states
from .brokers import broker_for_url
from .config import Settings, load_object
from .exceptions import ImproperlyConfigured, NotRegistered, ResultBackendDisabled
from .message import Message
from .task import Context, Task
from .task import Task as BaseTask

if TYPE_CHECKING:
    from .brokers.base import Broker, ResultRecord
    from .result import AsyncResult, GroupResult

logger = logging.getLogger("potatoq")

_state = threading.local()
_default_app: Potatoq | None = None
_current_app: Potatoq | None = None
#: The app ``_get_default_app()`` created on its own (no app created explicitly yet).
_implicit_app: Potatoq | None = None
_shared_tasks: list[Callable[[Potatoq], Task]] = []
_apps: list[Potatoq] = []
_inherited: list[Any] = []


def current_app() -> Potatoq:
    global _current_app
    if _current_app is None:
        _current_app = _get_default_app()
    return _current_app


def _get_default_app() -> Potatoq:
    """The app used by ``@shared_task`` when none was created explicitly.

    With Django this needs no ``celery.py``: settings are read from Django settings.
    """
    global _default_app, _implicit_app
    if _default_app is None:
        _default_app = _implicit_app = Potatoq("default", set_as_current=False)
        if os.environ.get("DJANGO_SETTINGS_MODULE"):
            from .contrib.django import configure_app

            configure_app(_default_app)
    return _default_app


class TaskRegistry(dict[str, Task]):
    def __missing__(self, key: str) -> Task:
        raise NotRegistered(key)

    def register(self, task: Task) -> Task:
        self[task.name] = task
        return task

    def unregister(self, name: str) -> None:
        self.pop(name, None)


class Potatoq:
    """The task queue application.

    ``Potatoq("proj")`` with no other arguments works: the broker comes from
    ``$POTATOQ_BROKER_URL`` (or ``$CELERY_BROKER_URL``), Django's database when running
    under Django, or a local SQLite file for development.
    """

    Task: type[BaseTask] = BaseTask

    def __init__(
        self,
        main: str | None = None,
        broker: str | None = None,
        backend: str | None = None,
        *,
        include: Iterable[str] | None = None,
        config_source: Any = None,
        set_as_current: bool = True,
        task_cls: type[BaseTask] | str | None = None,
        namespace: str | None = None,
        autofinalize: bool = True,
        **kwargs: Any,
    ):
        self.main = main
        self.conf = Settings()
        self.tasks = TaskRegistry()
        self._lock = threading.RLock()
        self._broker: Broker | None = None
        #: Set by the Django integration when the broker is a Django database.
        self._broker_follows: Any = None
        self._backend: Broker | bool | None = False  # False = not resolved yet
        self._transaction_hooks: list[Any] = []
        self._task_resolvers: list[Callable[[str], BaseTask | None]] = []
        self._autodiscover: list[tuple[Any, str]] = []
        self._finalized = False
        self._pid = os.getpid()
        self.on_configure = signals.Signal("on_configure")
        self.on_after_configure = signals.Signal("on_after_configure")
        self.on_after_finalize = signals.Signal("on_after_finalize")
        self.on_after_fork = signals.Signal("on_after_fork")
        if task_cls is not None:
            self.Task = load_object(task_cls) if isinstance(task_cls, str) else task_cls
        if config_source is not None:
            self.config_from_object(config_source, namespace=namespace)
        if broker:
            self.conf.broker_url = broker
        if backend:
            self.conf.result_backend = backend
        if include:
            self.conf.include = tuple(include)
        for key, value in kwargs.items():
            self.conf[key] = value
        if set_as_current:
            self.set_current()
        _apps.append(self)
        self._register_builtin_tasks()

    # --- configuration -----------------------------------------------------------

    def set_current(self) -> None:
        global _current_app
        _current_app = self

    def set_default(self) -> None:
        global _default_app
        _default_app = self

    def config_from_object(
        self, obj: Any, silent: bool = False, force: bool = False, namespace: str | None = None
    ) -> None:
        try:
            self.conf.update_from_object(obj, namespace=namespace)
        except ImportError:
            if not silent:
                raise
        if isinstance(obj, str) and obj.startswith("django.conf"):
            from .contrib.django import install

            install(self)
        self._reset_connections()

    def config_from_envvar(self, variable_name: str, silent: bool = False, force: bool = False) -> None:
        module = os.environ.get(variable_name)
        if not module:
            if silent:
                return
            raise ImproperlyConfigured(f"Environment variable {variable_name!r} is not set")
        self.config_from_object(module, silent=silent, force=force)

    def add_defaults(self, mapping: Any) -> None:
        if callable(mapping):
            mapping = mapping()
        for key, value in dict(mapping).items():
            if key not in self.conf.changed():
                self.conf[key] = value

    def _reset_connections(self) -> None:
        if self._broker is not None:
            self._broker.close()
        self._broker = None
        self._backend = False
        self.__dict__.pop("_results_default", None)

    # --- tasks -------------------------------------------------------------------

    def task(self, *args: Any, **opts: Any) -> Any:
        """Decorator: ``@app.task`` or ``@app.task(bind=True, ...)``."""

        def decorator(fun: Callable[..., Any]) -> BaseTask:
            return self._task_from_fun(fun, **opts)

        if len(args) == 1 and callable(args[0]) and not opts:
            return decorator(args[0])
        if args:
            raise TypeError("@app.task() takes keyword arguments only")
        return decorator

    def _task_from_fun(
        self, fun: Callable[..., Any], name: str | None = None, base: Any = None, bind: bool = False, **options: Any
    ) -> BaseTask:
        name = name or self.gen_task_name(fun.__name__, fun.__module__)
        if name in self.tasks and getattr(self.tasks[name], "__wrapped__", None) is fun:
            return self.tasks[name]
        unknown = set(options) - Task.OPTION_NAMES
        if unknown:
            raise TypeError(f"Unknown task option(s) for {name}: {', '.join(sorted(unknown))}")
        if options.get("rate_limit"):
            logger.warning("Task %s: rate_limit is not enforced yet by Potatoq (accepted for compatibility)", name)
        base = base or self.Task
        if isinstance(base, str):
            base = load_object(base)
        for ignored in (
            "shared",
            "lazy",
            "trail",
            "send_events",
            "routing_key",
            "exchange",
            "pydantic",
            "throws",
            "resultrepr_maxsize",
            "acks_on_failure_or_timeout",
        ):
            options.pop(ignored, None)
        run = fun if bind else staticmethod(fun)
        attrs: dict[str, Any] = {
            "app": self,
            "name": name,
            "run": run,
            "bind": bind,
            "_decorated": True,
            "__doc__": fun.__doc__,
            "__module__": fun.__module__,
            "__qualname__": fun.__qualname__,
            "__wrapped__": staticmethod(fun),
            "__annotations__": getattr(fun, "__annotations__", {}),
        }
        attrs.update(options)
        task_cls = type(fun.__name__, (base,), attrs)
        task = task_cls()
        self.tasks.register(task)
        return task

    def resolve_task(self, name: str) -> BaseTask | None:
        """The registered task ``name``, or one an integration can load on demand
        (e.g. a ``django.tasks`` task whose module the worker hasn't imported yet).
        A failing resolver counts as not finding it: workers call this for every message."""
        task = self.tasks.get(name)
        if task is not None:
            return task
        for resolver in self._task_resolvers:
            try:
                task = resolver(name)
            except Exception:
                logger.exception("Task resolver %r failed for %r", resolver, name)
                continue
            if task is not None:
                return task
        return None

    def gen_task_name(self, name: str, module: str) -> str:
        if module == "__main__" and self.main:
            module = self.main
        return f"{module}.{name}"

    def register_task(self, task: BaseTask | type[BaseTask], **options: Any) -> BaseTask:
        if isinstance(task, type):
            task = task()
        assert isinstance(task, BaseTask)
        if task.app is None:
            task.app = self
        if not task.name:
            task.name = self.gen_task_name(type(task).__name__, type(task).__module__)
        self.tasks.register(task)
        return task

    def _register_builtin_tasks(self) -> None:
        @self.task(name="potatoq.starmap", ignore_result=False)
        def starmap(task: str, it: list[Any]) -> list[Any]:
            fun = self.tasks[task]
            return [fun(*item) for item in it]

        @self.task(name="potatoq.accumulate")
        def accumulate(*args: Any, **kwargs: Any) -> Any:
            index = kwargs.get("index")
            return args[index] if index is not None else args

    def finalize(self, auto: bool = False) -> None:
        with self._lock:
            if self._finalized:
                return
            self._finalized = True
            for factory in list(_shared_tasks):
                factory(self)

    def add_periodic_task(
        self, schedule: Any, sig: Any, args: Any = (), kwargs: Any = (), name: str | None = None, **opts: Any
    ) -> str:
        """Celery's ``sender.add_periodic_task(10.0, my_task.s(), name="...")`` idiom."""
        from .canvas import maybe_signature

        sig = maybe_signature(sig, self)
        key = name or repr(sig)
        self.conf.beat_schedule = {
            **self.conf.beat_schedule,
            key: {
                "task": sig.task,
                "schedule": schedule,
                "args": tuple(args) or sig.args,
                "kwargs": dict(kwargs) or sig.kwargs,
                "options": {**sig.options, **opts},
            },
        }
        return key

    def _import_includes(self) -> None:
        for module in (*self.conf.imports, *self.conf.include):
            importlib.import_module(module)
        signals.import_modules.send(sender=self)
        for packages, related_name in self._autodiscover:
            self._do_autodiscover(packages, related_name)

    def loader_import_default_modules(self) -> None:
        """Import task modules and run configuration hooks (worker / beat startup)."""
        self.finalize()
        self._import_includes()
        if not getattr(self, "_configured_signals_sent", False):
            self._configured_signals_sent = True
            self.on_after_configure.send(sender=self, source=self.conf)
            self.on_after_finalize.send(sender=self)

    def autodiscover_tasks(self, packages: Any = None, related_name: str = "tasks", force: bool = False) -> None:
        """Import ``<package>.tasks`` for each package (Django apps when ``packages`` is None)."""
        if force:
            self._do_autodiscover(packages, related_name)
        else:
            self._autodiscover.append((packages, related_name))

    def _do_autodiscover(self, packages: Any, related_name: str) -> None:
        if callable(packages):
            packages = packages()
        if packages is None:
            try:
                from django.apps import apps as django_apps

                packages = [config.name for config in django_apps.get_app_configs()]
            except Exception:
                packages = []
        for package in packages:
            module = f"{package}.{related_name}"
            try:
                importlib.import_module(module)
            except ModuleNotFoundError as exc:
                if exc.name not in (module, package):
                    raise

    # --- broker / results ----------------------------------------------------------

    def _broker_url(self) -> str:
        url = self.conf.broker_url or self.conf.get_env("POTATOQ_BROKER_URL", "CELERY_BROKER_URL")
        if url:
            return url
        url = self._django_database_url()
        if url:
            return url
        logger.warning(
            "No broker configured; using sqlite:///potatoq.sqlite3. "
            "Set POTATOQ_BROKER_URL (redis://, amqp://, postgresql://, sqlite://) for production."
        )
        return "sqlite:///potatoq.sqlite3"

    def _django_database_url(self) -> str | None:
        if not os.environ.get("DJANGO_SETTINGS_MODULE"):
            return None
        try:
            from .contrib.django import database_url

            return database_url()
        except Exception:
            logger.debug("Could not derive broker from Django DATABASES", exc_info=True)
            return None

    def _follow_database(self) -> None:
        if self._broker_follows is not None:
            url = self._broker_follows.changed()
            if url is not None:
                with self._lock:
                    self._reset_connections()
                    self.conf.broker_url = url

    @property
    def broker(self) -> Broker:
        self._follow_database()
        if self._broker is None or self._pid != os.getpid():
            with self._lock:
                if self._pid != os.getpid():
                    self._after_fork()
                if self._broker is None:
                    url = self._broker_url()
                    broker = broker_for_url(url)(url, self, **self.conf.broker_transport_options)
                    if self.conf.database_auto_create_schema:
                        broker.setup()
                    self._broker = broker
        return self._broker

    @property
    def backend(self) -> Broker | None:
        """Where results are stored (the broker itself unless ``result_backend`` is set)."""
        self._follow_database()
        if self._backend is False or self._pid != os.getpid():
            with self._lock:
                if self._pid != os.getpid():
                    self._after_fork()
                if self._backend is False:
                    self._backend = self._resolve_backend()
        return self._backend  # type: ignore[return-value]

    def _resolve_backend(self) -> Broker | None:
        url = self.conf.result_backend
        if url in (None, "", "broker"):
            return self.broker if self.broker.supports_results else None
        if url in ("rpc://", "rpc", "disabled", "none"):
            return None
        if url in ("django-db", "django-cache"):
            url = self._django_database_url()
            if not url:
                raise ImproperlyConfigured("result_backend='django-db' requires Django")
        if url == self._broker_url():
            return self.broker
        backend = broker_for_url(url)(url, self, **self.conf.get("result_backend_transport_options", {}))
        if not backend.supports_results:
            raise ImproperlyConfigured(f"{url} can't store results")
        if self.conf.database_auto_create_schema:
            backend.setup()
        return backend

    def _after_fork(self) -> None:
        # Connections opened by the parent must never be used, closed, or even
        # garbage collected here: closing a psycopg connection would terminate the
        # parent's session, closing a sqlite3 one can corrupt the parent's locks.
        # Keep them referenced forever and start fresh.
        _inherited.append((self._broker, self._backend))
        self._broker = None
        self._backend = False
        self._pid = os.getpid()

    def results_enabled_by_default(self) -> bool:
        """Results are stored unless ignored when a result backend was configured
        explicitly (as in Celery) or the broker is a database, where storing the
        result is part of the same transaction as the ack and costs nothing."""
        cached = self.__dict__.get("_results_default")
        if cached is None:
            if self.conf.result_backend not in (None, "", "broker"):
                cached = self.conf.result_backend not in ("rpc://", "rpc", "disabled", "none")
            else:
                cached = bool(self.conf.result_backend == "broker" or broker_for_url(self._broker_url()).transactional)
            self.__dict__["_results_default"] = cached
        return cached

    def require_backend(self) -> Broker:
        backend = self.backend
        if backend is None:
            raise ResultBackendDisabled(
                "No result backend: the broker can't store results. Set result_backend "
                "(e.g. 'redis://...' or 'postgresql://...') to use AsyncResult.get()."
            )
        return backend

    def publish(
        self,
        messages: list[Message],
        connection: Any = None,
        on_commit: bool | None = None,
        using: Any = None,
    ) -> None:
        """Send messages to the broker.

        Inside a database transaction (Django ``atomic()``, an SQLAlchemy session) the
        messages are sent when it commits and dropped if it rolls back, so a worker never
        sees a task before the data it needs. When the broker *is* that database, the
        rows are written inside the transaction itself, which is the same behaviour
        without the gap between COMMIT and publishing.
        """
        if not messages:
            return
        if on_commit is None:
            on_commit = self.conf.task_enqueue_on_commit
        if connection is None and on_commit:
            for hook in self._transaction_hooks:
                if hook.publish(self, messages, using):
                    return
        self.publish_now(messages, connection)

    def publish_now(self, messages: list[Message], connection: Any = None) -> None:
        for message in messages:
            signals.before_task_publish.send(
                sender=message.task, body=message.to_dict(), exchange="", routing_key=message.queue,
                headers=message.headers, properties={}, declare=[], retry_policy=None,
            )  # fmt: skip
        self.broker.enqueue(messages, connection=connection)
        for message in messages:
            signals.after_task_publish.send(
                sender=message.task, body=message.to_dict(), exchange="", routing_key=message.queue
            )

    def on_commit(self, fn: Callable[[], None], using: Any = None) -> None:
        """Run ``fn`` after the current transaction commits (or now, if there is none)."""
        for hook in self._transaction_hooks:
            if hook.on_commit(fn, using):
                return
        fn()

    def add_transaction_hook(self, hook: Any) -> None:
        if not any(type(h) is type(hook) for h in self._transaction_hooks):
            self._transaction_hooks.append(hook)

    def route_for(self, name: str) -> dict[str, Any]:
        routes = self.conf.task_routes
        if not routes:
            return {}
        for route in routes if isinstance(routes, (list, tuple)) else [routes]:
            if callable(route):
                result = route(name, (), {}, {})
                if result:
                    return {"queue": result} if isinstance(result, str) else dict(result)
                continue
            for pattern, options in route.items():
                matched = pattern.match(name) if isinstance(pattern, re.Pattern) else fnmatch.fnmatchcase(name, pattern)
                if matched:
                    return {"queue": options} if isinstance(options, str) else dict(options)
        return {}

    def send_task(
        self,
        name: str,
        args: Any = None,
        kwargs: dict[str, Any] | None = None,
        countdown: float | None = None,
        eta: Any = None,
        task_id: str | None = None,
        **options: Any,
    ) -> AsyncResult:
        """Enqueue a task by name, without needing its code."""
        task = self.tasks.get(name)
        if task is None:
            task = self._task_from_fun(_missing_task, name=name, typing=False)
            self.tasks.unregister(name)
        else:
            options.setdefault("_skip_typing", True)
        options.pop("_skip_typing", None)
        message = task.build_message(
            list(args or ()), dict(kwargs or {}), task_id, countdown=countdown, eta=eta, **options
        )
        self.publish([message], connection=options.get("connection"))
        return self.AsyncResult(message.id)

    def store_result(
        self,
        task_id: str,
        state: str,
        result: Any,
        traceback: str | None = None,
        task_name: str | None = None,
        request: Context | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        backend = self.backend
        if backend is None:
            return
        from .brokers.base import ResultRecord

        record = ResultRecord(
            task_id=task_id,
            state=state,
            result=result,
            traceback=traceback,
            meta=meta,
            date_done=time.time() if state in states.READY_STATES else None,
            task_name=task_name,
            args=request.args if request else None,
            kwargs=request.kwargs if request else None,
            retries=request.retries if request else 0,
            worker=request.hostname if request else None,
        )
        backend.store_result(record, expires=self.conf.result_expires)

    def get_result(self, task_id: str) -> ResultRecord | None:
        return self.require_backend().get_result(task_id)

    def AsyncResult(self, task_id: str, **kwargs: Any) -> AsyncResult:
        from .result import AsyncResult

        return AsyncResult(task_id, app=self, **kwargs)

    def GroupResult(self, group_id: str, results: Any = None, **kwargs: Any) -> GroupResult:
        from .result import GroupResult

        return GroupResult(group_id, results or [], app=self, **kwargs)

    def signature(self, name: Any, args: Any = None, kwargs: Any = None, **options: Any) -> Any:
        from .canvas import Signature

        if isinstance(name, dict):
            return Signature.from_dict(name, app=self)
        return Signature(name, args, kwargs, options, app=self)

    # --- control -----------------------------------------------------------------

    @cached_property
    def control(self) -> Any:
        from .control import Control

        return Control(self)

    # --- request context ---------------------------------------------------------

    def current_task_request(self) -> Context | None:
        task = getattr(_state, "current_task", None)
        if task is None:
            return None
        req = task.request
        return req if req.id else None

    @property
    def current_task(self) -> BaseTask | None:
        return getattr(_state, "current_task", None)

    @property
    def current_worker_task(self) -> BaseTask | None:
        return self.current_task

    # --- worker ------------------------------------------------------------------

    def Worker(self, **kwargs: Any) -> Any:
        from .worker.supervisor import Supervisor

        return Supervisor(self, **kwargs)

    def worker_main(self, argv: list[str] | None = None) -> None:
        from .cli import main

        main(["-A", self, *(argv or ["worker"])])

    start = worker_main

    def close(self) -> None:
        self._reset_connections()

    def __enter__(self) -> Potatoq:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<Potatoq {self.main or '__main__'}:{id(self):#x}>"

    @property
    def amqp(self) -> Any:
        raise AttributeError("Potatoq doesn't expose kombu internals")


def _missing_task(*args: Any, **kwargs: Any) -> Any:
    raise NotImplementedError("Task executed by name without its code")


def set_current_task(task: Task | None) -> None:
    _state.current_task = task


# --- shared_task -----------------------------------------------------------------


class _SharedTaskProxy:
    """Resolves to the task registered on the *current* app, like Celery's ``shared_task``."""

    def __init__(self, fun: Callable[..., Any], options: dict[str, Any]):
        self.__wrapped__ = fun
        self._options = options
        self.__name__ = fun.__name__
        self.__qualname__ = fun.__qualname__
        self.__module__ = fun.__module__
        self.__doc__ = fun.__doc__

    def _resolve(self) -> Task:
        app = current_app()
        app.finalize()
        return app._task_from_fun(self.__wrapped__, **dict(self._options))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._resolve(), name)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._resolve()(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<@shared_task: {self.__module__}.{self.__name__}>"

    def __reduce__(self) -> Any:
        return self._resolve().__reduce__()


def shared_task(*args: Any, **options: Any) -> Any:
    """Define a task that isn't bound to a specific app (for reusable apps / Django)."""

    def decorator(fun: Callable[..., Any]) -> Any:
        proxy = _SharedTaskProxy(fun, options)

        def register(app: Potatoq) -> Task:
            return app._task_from_fun(fun, **dict(options))

        _shared_tasks.append(register)
        for app in _apps:
            if app._finalized:
                register(app)
        return proxy

    if len(args) == 1 and callable(args[0]) and not options:
        return decorator(args[0])
    return decorator


Celery = Potatoq

warnings.filterwarnings("default", category=DeprecationWarning, module="potatoq")
