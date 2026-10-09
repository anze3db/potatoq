"""Django integration.

Add ``"potatoq.contrib.django"`` to ``INSTALLED_APPS`` and you're done:

* No ``celery.py`` needed. ``@shared_task`` works out of the box and ``tasks.py`` of
  every installed app is discovered automatically.
* Settings come from ``POTATOQ = {...}`` or the ``POTATOQ_*`` / ``CELERY_*`` settings
  you already have (``CELERY_BROKER_URL`` etc.).
* Without a broker setting, the broker is your default database (Postgres or SQLite):
  zero extra infrastructure, and tasks are enqueued *inside* your transactions.
* ``.delay()`` inside ``transaction.atomic()`` (or ``ATOMIC_REQUESTS``) enqueues when
  the transaction commits and never if it rolls back. Opt out per task with
  ``@shared_task(enqueue_on_commit=False)``.
* Worker processes close stale database connections around every task, like Celery's
  Django fixup.

Run workers with ``potatoq worker`` (detects ``DJANGO_SETTINGS_MODULE``) or
``python manage.py potatoq worker``.
"""

from __future__ import annotations

import logging
import os
import weakref
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

if TYPE_CHECKING:
    from ...app import Potatoq
    from ...message import Message

logger = logging.getLogger("potatoq.django")

default_app_config = "potatoq.contrib.django.apps.PotatoqConfig"

_INHERITED: list[Any] = []
#: Apps ``install()`` has run for.
_INSTALLED: weakref.WeakSet[Potatoq] = weakref.WeakSet()


def _settings() -> Any:
    from django.conf import settings

    return settings


def database_alias() -> str:
    return getattr(_settings(), "POTATOQ_DATABASE", None) or "default"


def database_url(alias: str | None = None) -> str | None:
    """Broker URL for a Django database alias (Postgres or SQLite only)."""
    from django.db import connections

    alias = alias or database_alias()
    settings_dict = connections.databases[alias]
    engine = settings_dict["ENGINE"]
    if engine.endswith(("postgresql", "postgis", "postgresql_psycopg2")):
        user = quote(settings_dict.get("USER") or "", safe="")
        password = quote(str(settings_dict.get("PASSWORD") or ""), safe="")
        host = settings_dict.get("HOST") or ""
        port = settings_dict.get("PORT") or ""
        creds = f"{user}:{password}@" if password else (f"{user}@" if user else "")
        hostpart = quote(host, safe="") if host.startswith("/") else host
        if port:
            hostpart += f":{port}"
        url = f"postgresql://{creds}{hostpart}/{quote(settings_dict['NAME'], safe='')}"
        options = settings_dict.get("OPTIONS") or {}
        params = {
            k: v
            for k, v in options.items()
            if k in ("sslmode", "sslrootcert", "sslcert", "sslkey", "options", "service")
        }
        if params:
            url += "?" + "&".join(f"{k}={quote(str(v), safe='')}" for k, v in params.items())
        return url
    if engine.endswith("sqlite3"):
        name = str(settings_dict["NAME"])
        if name == ":memory:" or name.startswith("file:"):
            return None
        return "sqlite:///" + os.path.abspath(name)
    logger.warning(
        "Database engine %s can't be used as a Potatoq broker; set POTATOQ_BROKER_URL "
        "(redis://, amqp://, postgresql:// or sqlite://)",
        engine,
    )
    return None


def configure_app(app: Potatoq) -> None:
    """Load settings (``POTATOQ`` dict, ``POTATOQ_*``, ``CELERY_*``) and install hooks."""
    settings = _settings()
    if not settings.configured:
        return
    conf = app.conf
    for name in dir(settings):
        if name.startswith("CELERY_"):
            conf.update_from_mapping({name: getattr(settings, name)}, namespace="CELERY")
    for name in dir(settings):
        if name.startswith("POTATOQ_") and name != "POTATOQ_DATABASE":
            conf.update_from_mapping({name: getattr(settings, name)}, namespace="POTATOQ")
    conf.update_from_mapping(getattr(settings, "POTATOQ", {}) or {})
    if "timezone" not in conf.changed() and getattr(settings, "TIME_ZONE", None):
        conf.timezone = settings.TIME_ZONE
    if not conf.broker_url and not os.environ.get("POTATOQ_BROKER_URL") and not os.environ.get("CELERY_BROKER_URL"):
        url = database_url()
        if url:
            conf.broker_url = url
            app._broker_follows = DatabaseFollower(database_alias(), app)
    install(app)


class DatabaseFollower:
    """Keeps a broker derived from ``DATABASES`` pointed at the database Django uses now.

    Django's test runner renames the database (``test_<name>``, or an in-memory SQLite
    database) after potatoq was configured. Without this, tests would enqueue into the
    real database, where a running development worker would pick the tasks up."""

    def __init__(self, alias: str, app: Potatoq) -> None:
        self.alias = alias
        self.app = weakref.ref(app)
        self.name = self._name()
        self._hooked = False

    def _name(self) -> Any:
        from django.db import connections

        return connections.databases[self.alias].get("NAME")

    def changed(self) -> str | None:
        """The new broker URL if the database was switched since the last call."""
        name = self._name()
        if name == self.name:
            return None
        self.name = name
        # An in-memory test database can't be shared with a separate broker connection;
        # keep tasks in this process instead (run them with potatoq.testing.drain).
        url = database_url(self.alias) or "memory://"
        logger.info("Django switched database %r to %r; potatoq now uses %s", self.alias, name, url)
        self._close_before_destroy()
        return url

    def _close_before_destroy(self) -> None:
        """Close potatoq's connections before the test runner drops the test database:
        Postgres refuses to drop a database other sessions are connected to."""
        if self._hooked:
            return
        from django.db import connections

        creation = connections[self.alias].creation
        destroy = creation.destroy_test_db

        def destroy_test_db(*args: Any, **kwargs: Any) -> Any:
            app = self.app()
            if app is not None:
                with app._lock:
                    app._reset_connections()
            return destroy(*args, **kwargs)

        creation.destroy_test_db = destroy_test_db
        self._hooked = True


def install(app: Potatoq, *, bind_backends: bool = True) -> None:
    """Hook Potatoq into Django's transactions and connection handling (idempotent).

    ``django.tasks`` backends without an ``APP`` option that aren't bound to an app yet
    run on ``app``, unless ``bind_backends`` is false."""
    from ... import signals

    app.add_transaction_hook(DjangoTransactionHook())
    _bind_task_backends(app, bind_backends)
    if not getattr(install, "_signals_connected", False):
        signals.worker_init.connect(_import_urlconf, weak=False, dispatch_uid="potatoq.django.urlconf")
        signals.task_prerun.connect(_close_old_connections, weak=False, dispatch_uid="potatoq.django.prerun")
        signals.task_postrun.connect(_close_old_connections, weak=False, dispatch_uid="potatoq.django.postrun")
        signals.worker_init.connect(_close_all, weak=False, dispatch_uid="potatoq.django.worker_init")
        signals.worker_process_init.connect(_drop_inherited, weak=False, dispatch_uid="potatoq.django.process_init")
        install._signals_connected = True  # type: ignore[attr-defined]
    if app not in _INSTALLED:
        _INSTALLED.add(app)
        app.autodiscover_tasks()


def _bind_task_backends(app: Potatoq, bind_backends: bool = True) -> None:
    """Initialise ``django.tasks`` backends that run on potatoq (TASKS setting), so a
    worker can resolve Django tasks by name even if nothing else touched them."""
    try:
        from django.tasks import task_backends
    except ImportError:  # Django < 6 (Python 3.11)
        return
    from ... import app as app_module
    from ...config import load_object
    from .tasks import PotatoqBackend, _default_apps

    backends = []
    for alias in getattr(_settings(), "TASKS", None) or {}:
        try:
            backend = task_backends[alias]
        except Exception:
            logger.exception("Could not load django.tasks backend %r", alias)
            continue
        if isinstance(backend, PotatoqBackend):
            backends.append((alias, backend))
    named = False  # some backend's APP option is this app
    for alias, backend in backends:
        target = backend.options.get("APP")
        if target:
            try:
                target = load_object(target) if isinstance(target, str) else target
            except Exception:
                # E.g. its module is still being imported; a later install() binds it.
                logger.debug("Could not load APP %r of django.tasks backend %r", target, alias, exc_info=True)
                continue
            if target is app:
                named = True
                backend.bind(app)
    if not bind_backends or named:
        return  # an app named by an APP option never takes over the other backends
    for alias, backend in backends:
        # First app wins, except over the implicit one: a celery.py app configured after
        # something touched @shared_task's fallback app still takes over.
        if not backend.options.get("APP") and _default_apps.get(alias) in (None, app_module._implicit_app):
            backend.bind(app)


def _import_urlconf(sender: Any = None, **kwargs: Any) -> None:
    """With ``worker_import_urlconf``, register tasks defined outside ``tasks.py`` (e.g.
    in views) by importing the URLconf, like Celery's worker does through its startup
    system checks. Off by default: workers shouldn't need to import the web layer."""
    from importlib import import_module

    app = getattr(sender, "app", None)
    if app is None or not app.conf.worker_import_urlconf:
        return
    urlconf = getattr(_settings(), "ROOT_URLCONF", None)
    if urlconf:
        try:
            import_module(urlconf)
        except Exception:
            logger.exception("Could not import ROOT_URLCONF %r; tasks defined in views aren't registered", urlconf)


def _close_old_connections(task: Any = None, **kwargs: Any) -> None:
    if task is not None and task.request.is_eager:
        return  # runs inside the caller's request, maybe inside its transaction
    from django.db import close_old_connections

    close_old_connections()


def _close_all(**kwargs: Any) -> None:
    """In the supervisor, before forking: close our own connections properly."""
    from django.db import connections

    connections.close_all()


def _drop_inherited(**kwargs: Any) -> None:
    """In a fresh child: forget (never close!) connections inherited from the parent."""
    from django.db import connections

    for conn in connections.all(initialized_only=True):
        if conn.connection is not None:
            _INHERITED.append(conn.connection)
            conn.connection = None


def _in_event_loop() -> bool:
    import asyncio

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def _same_database(broker: Any, alias: str) -> bool:
    if not getattr(broker, "transactional", False):
        return False
    try:
        url = database_url(alias)
    except Exception:
        return False
    if url is None:
        return False
    return _canonical(url) == _canonical(broker.url)


def _canonical(url: str) -> str:
    """Compare broker URLs loosely: scheme family, host, port, database/path."""
    from urllib.parse import unquote, urlsplit

    if url.startswith("sqlite"):
        from ...brokers.sqlite import _path_from_url

        return "sqlite:" + os.path.realpath(_path_from_url(url))
    parts = urlsplit(
        url.replace("postgres://", "postgresql://", 1).replace("postgresql+psycopg://", "postgresql://", 1)
    )
    host = unquote(parts.hostname or "localhost")
    if host in ("127.0.0.1", "::1", "") or host.startswith("/"):
        host = "localhost"
    return f"postgresql://{host}:{parts.port or 5432}/{unquote(parts.path.lstrip('/'))}"


class DjangoTransactionHook:
    """Defers (or joins) publishing to the current ``transaction.atomic()`` block."""

    def _connection(self, using: Any) -> Any:
        # The caller's transaction lives on ``using`` (default: Django's default alias,
        # like ``transaction.on_commit``), not on the broker's POTATOQ_DATABASE alias;
        # ``_same_database`` decides whether the broker is that database.
        from django.db import DEFAULT_DB_ALIAS, connections

        return connections[using or DEFAULT_DB_ALIAS]

    def publish(self, app: Potatoq, messages: list[Message], using: Any, on_commit: bool = True) -> bool:
        from django.db import transaction

        conn = self._connection(using)
        in_loop = _in_event_loop()
        if not conn.in_atomic_block and not in_loop and app._broker_follows is not None:
            # The broker is a Django database: let Django's checks (pytest-django's
            # "Database access not allowed") run before potatoq touches any database.
            conn.ensure_connection()
        same = _same_database(app.broker, conn.alias)
        if conn.in_atomic_block:
            if not on_commit:
                return False  # sent now, visible before COMMIT: potatoq's own connection
            if same:
                # The broker is this database: write the task rows in this transaction.
                conn.ensure_connection()
                app.publish_now(messages, connection=conn.connection)
                return True
            transaction.on_commit(lambda: app.publish_after_commit(messages), using=conn.alias)
            return True
        if not same or in_loop:
            return False  # Django won't hand its connection to async code
        # No transaction, but the broker is this database: write through Django's
        # connection anyway (and commit at once). Then tests write to the test database,
        # and pytest-django's "Database access not allowed" guard covers tasks too,
        # instead of a test silently queueing into the development database.
        with transaction.atomic(using=conn.alias):
            app.publish_now(messages, connection=conn.connection)
        return True

    def on_commit(self, fn: Any, using: Any) -> bool:
        from django.db import transaction

        conn = self._connection(using)
        if not conn.in_atomic_block:
            return False
        transaction.on_commit(fn, using=conn.alias)
        return True
