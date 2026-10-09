"""Django integration edge cases: database URLs, settings, hooks, apps, command, django.tasks."""

from __future__ import annotations

import logging
import time
from types import SimpleNamespace

import pytest

django = pytest.importorskip("django")


# --- database_url / _canonical / _same_database ---------------------------------------


def _has_django_tasks() -> bool:
    try:
        import django.tasks  # noqa: F401
    except ImportError:  # pragma: no cover - Django 5.2 (Python 3.11)
        return False
    return True


needs_django_tasks = pytest.mark.skipif(not _has_django_tasks(), reason="django.tasks needs Django 6 (Python 3.12+)")


def test_database_url_for_postgres_settings(django_env, monkeypatch):
    from django.db import connections

    from potatoq.contrib.django import database_url

    monkeypatch.setitem(
        connections.databases,
        "pg_full",
        {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": "my db",
            "USER": "ann",
            "PASSWORD": "p@ss/word",
            "HOST": "db.example",
            "PORT": 6432,
            "OPTIONS": {"sslmode": "require", "connect_timeout": 5, "options": "-c search_path=app"},
        },
    )
    monkeypatch.setitem(
        connections.databases,
        "pg_socket",
        {"ENGINE": "django.contrib.gis.db.backends.postgis", "NAME": "geo", "USER": "bob",
         "HOST": "/var/run/postgresql"},
    )  # fmt: skip
    monkeypatch.setitem(
        connections.databases, "pg_bare", {"ENGINE": "django.db.backends.postgresql_psycopg2", "NAME": "plain"}
    )
    assert database_url("pg_full") == (
        "postgresql://ann:p%40ss%2Fword@db.example:6432/my%20db?sslmode=require&options=-c%20search_path%3Dapp"
    )  # connect_timeout is not a libpq URL option potatoq forwards
    assert database_url("pg_socket") == "postgresql://bob@%2Fvar%2Frun%2Fpostgresql/geo"
    assert database_url("pg_bare") == "postgresql:///plain"


def test_database_url_unusable_databases(django_env, monkeypatch, caplog):
    from django.db import connections

    from potatoq.contrib.django import database_url

    monkeypatch.setitem(connections.databases, "mem", {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"})
    monkeypatch.setitem(
        connections.databases, "uri", {"ENGINE": "django.db.backends.sqlite3", "NAME": "file:x?mode=memory"}
    )
    monkeypatch.setitem(connections.databases, "my", {"ENGINE": "django.db.backends.mysql", "NAME": "shop"})
    assert database_url("mem") is None
    assert database_url("uri") is None
    with caplog.at_level(logging.WARNING, logger="potatoq.django"):
        assert database_url("my") is None
    assert "django.db.backends.mysql can't be used as a Potatoq broker" in caplog.text


def test_canonical_urls_compare_loosely(tmp_path):
    from potatoq.contrib.django import _canonical

    assert _canonical("postgresql+psycopg://u:p@127.0.0.1/db") == "postgresql://localhost:5432/db"
    assert _canonical("postgres://[::1]:5432/db") == "postgresql://localhost:5432/db"
    assert _canonical("postgresql://%2Fvar%2Frun%2Fpostgresql/db") == "postgresql://localhost:5432/db"
    assert _canonical("postgresql:///my%20db") == "postgresql://localhost:5432/my db"
    assert _canonical("postgresql://db.example:6432/x") != _canonical("postgresql://db.example/x")
    db = tmp_path / "a.db"
    assert _canonical(f"sqlite:///{db}") == _canonical(f"sqlite:///{tmp_path}/./a.db")


def test_same_database(django_env, monkeypatch):
    from django.db import connections

    from potatoq.contrib.django import _same_database

    url = django_env.conf.broker_url
    assert _same_database(SimpleNamespace(transactional=True, url=url), "default") is True
    assert _same_database(SimpleNamespace(transactional=False, url=url), "default") is False
    assert _same_database(SimpleNamespace(transactional=True, url=url), "no-such-alias") is False
    monkeypatch.setitem(connections.databases, "mem", {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"})
    assert _same_database(SimpleNamespace(transactional=True, url=url), "mem") is False


# --- configure_app / install / signal handlers ----------------------------------------


class FakeSettings:
    configured = True
    CELERY_TASK_ACKS_LATE = False
    POTATOQ_BROKER_URL = "memory://"
    POTATOQ_DATABASE = "ignored"  # potatoq's own setting, not app config
    POTATOQ = {"task_default_queue": "fake"}
    TIME_ZONE = "America/New_York"
    TASKS = {"not-configured": {}}


@needs_django_tasks
def test_configure_app_reads_potatoq_and_celery_settings(django_env, monkeypatch, caplog):
    from potatoq import Potatoq
    from potatoq.contrib import django as dj

    monkeypatch.setattr(dj, "_settings", lambda: FakeSettings())
    app = Potatoq("fake-settings", set_as_current=False)
    with caplog.at_level(logging.ERROR, logger="potatoq.django"):
        dj.configure_app(app)
    assert app.conf.broker_url == "memory://"
    assert app.conf.task_acks_late is False
    assert app.conf.task_default_queue == "fake"
    assert app.conf.timezone == "America/New_York"
    assert any(type(h) is dj.DjangoTransactionHook for h in app._transaction_hooks)
    # A TASKS alias Django can't load is logged, not fatal.
    assert "Could not load django.tasks backend 'not-configured'" in caplog.text


def test_configure_app_without_configured_settings_is_a_no_op(monkeypatch):
    from potatoq import Potatoq
    from potatoq.contrib import django as dj

    monkeypatch.setattr(dj, "_settings", lambda: SimpleNamespace(configured=False))
    app = Potatoq("unconfigured", set_as_current=False)
    dj.configure_app(app)
    assert app.conf.broker_url is None
    assert app._transaction_hooks == []


def test_worker_process_init_forgets_inherited_connections(django_env):
    from django.db import connection

    from potatoq.contrib import django as dj

    connection.ensure_connection()
    raw = connection.connection
    dj._drop_inherited()
    try:
        assert connection.connection is None
        assert dj._INHERITED[-1] is raw
    finally:
        dj._INHERITED.remove(raw)
        raw.close()
    connection.ensure_connection()  # Django reconnects on demand


def test_on_commit_follows_atomic_blocks(django_env):
    from django.db import transaction

    calls = []
    django_env.on_commit(lambda: calls.append("now"))
    assert calls == ["now"]
    with transaction.atomic():
        django_env.on_commit(lambda: calls.append("later"))
        assert calls == ["now"]
    assert calls == ["now", "later"]
    with pytest.raises(RuntimeError), transaction.atomic():
        django_env.on_commit(lambda: calls.append("never"))
        raise RuntimeError
    assert calls == ["now", "later"]


@pytest.mark.filterwarnings("ignore:The EMAIL_")
@needs_django_tasks
def test_app_config_ready_configures_an_explicit_app(django_env):
    from django.apps import apps
    from django.tasks import task_backends

    from potatoq import Potatoq

    other = Potatoq("explicit", set_as_current=True)
    try:
        apps.get_app_config("potatoq").ready()
        assert other.conf.task_default_queue == "shop"
        assert other.conf.broker_url == django_env.conf.broker_url
        # The explicit app takes django.tasks over from the implicit one, in every thread.
        assert task_backends["default"].app is other
        assert len(other._task_resolvers) == 1
        third = Potatoq("third", broker="memory://", set_as_current=False)
        third.config_from_object("django.conf:settings")  # but not from another explicit app
        assert task_backends["default"].app is other
        third.close()
    finally:
        from potatoq.contrib.django import tasks as dj_tasks

        task_backends["default"]._app = None
        dj_tasks._default_apps["default"] = django_env
        django_env.set_current()
        other.close()


def test_management_command_run_from_argv(django_env, capsys):
    from django.core.management import ManagementUtility

    with pytest.raises(SystemExit) as exc:
        ManagementUtility(["manage.py", "potatoq", "queues"]).execute()
    assert exc.value.code == 0
    assert capsys.readouterr().out

    with pytest.raises(SystemExit):
        ManagementUtility(["manage.py", "potatoq", "--help"]).execute()
    assert capsys.readouterr().out.startswith("usage: manage.py potatoq")


# --- django.tasks backend -------------------------------------------------------------


@needs_django_tasks
def test_backend_registers_each_task_once(django_env):
    from django.tasks import task_backends
    from djangoproj.shop.jobs import total

    backend = task_backends["default"]
    assert backend.supports_get_result is True
    first = backend._register(total)
    assert backend._register(total) is first
    assert django_env.tasks[total.module_path] is first


@needs_django_tasks
def test_resolver_only_imports_django_tasks_from_installed_apps(django_env):
    from django.tasks import task_backends

    backend = task_backends["default"]
    assert backend._resolve("os.path.join") is None  # not an installed app
    assert backend._resolve("djangoproj.shop.nope.job") is None  # no such module
    assert backend._resolve("djangoproj.shop.jobs.missing") is None  # no such attribute
    assert backend._resolve("djangoproj.shop.tasks.send_receipt") is None  # a potatoq task, not django.tasks
    assert backend._resolve("djangoproj.shop.jobs.total").name == "djangoproj.shop.jobs.total"


@needs_django_tasks
def test_get_result_for_revoked_and_odd_failures(django_env):
    from django.tasks import TaskResultStatus, task_backends
    from django.tasks.exceptions import TaskResultDoesNotExist
    from djangoproj.shop.jobs import total
    from djangoproj.shop.tasks import audit

    from potatoq import states
    from potatoq.brokers.base import ResultRecord

    backend = task_backends["default"]
    app = django_env
    try:
        revoked = total.enqueue(1, [1])
        app.AsyncResult(revoked.id).revoke()
        result = total.get_result(revoked.id)
        assert result.status == TaskResultStatus.FAILED
        assert result.errors[0].exception_class_path == "potatoq.exceptions.TaskRevokedError"

        app.backend.store_result(
            ResultRecord(task_id="odd-failure", state=states.FAILURE, result="not an exception dict",
                         task_name=total.module_path, date_done=time.time()),
            None,
        )  # fmt: skip
        odd = total.get_result("odd-failure")
        assert odd.status == TaskResultStatus.FAILED
        assert odd.errors[0].exception_class_path == "builtins.Exception"

        plain = audit.apply_async(("x",))  # a potatoq task, not a django.tasks one
        with pytest.raises(TaskResultDoesNotExist, match=r"not a django\.tasks task"):
            backend.get_result(plain.id)
    finally:
        app.broker.purge("shop")


@pytest.fixture
def second_backend(django_env, monkeypatch):
    """A second django.tasks backend, "second", running on its own app (OPTIONS["APP"])."""
    from django.conf import settings
    from django.tasks import task_backends

    from potatoq import Potatoq
    from potatoq.contrib.django import tasks as dj_tasks

    other = Potatoq("second-app", broker="memory://", set_as_current=False)
    other.conf.result_backend = "broker"
    backends = {
        **settings.TASKS,
        "second": {"BACKEND": "potatoq.contrib.django.tasks.PotatoqBackend", "QUEUES": ["shop"],
                   "OPTIONS": {"APP": other}},
    }  # fmt: skip
    monkeypatch.setattr(settings, "TASKS", backends)
    monkeypatch.setitem(task_backends.__dict__, "settings", backends)
    monkeypatch.setattr(dj_tasks, "_resolvers", type(dj_tasks._resolvers)())
    yield other
    del task_backends["second"]
    other.close()


@pytest.mark.filterwarnings("ignore:The EMAIL_")
@needs_django_tasks
def test_a_backend_with_its_own_app_leaves_the_others_alone(django_env, second_backend):
    from django.tasks import task_backends
    from djangoproj.shop.jobs import total

    other = second_backend
    on_other = total.using(backend="second").enqueue(1, [1])  # first use: installs `other`
    assert task_backends["second"].app is other
    assert other.broker.peek(on_other.id) is not None
    on_default = total.enqueue(2, [2])
    assert task_backends["default"].app is django_env
    assert django_env.broker.peek(on_default.id) is not None
    assert other.broker.peek(on_default.id) is None
    other.config_from_object("django.conf:settings")  # configuring it from Django doesn't either
    assert task_backends["default"].app is django_env
    django_env.broker.purge("shop")


@needs_django_tasks
def test_install_registers_resolvers_for_backends_naming_the_app(django_env, second_backend, monkeypatch):
    """A worker never touches backend.app: install() alone must let it find "second"'s tasks."""
    import threading

    from django.tasks import task
    from djangoproj.shop import jobs

    from potatoq.contrib.django import install
    from potatoq.contrib.django import tasks as dj_tasks

    other = second_backend

    def second_total(items):
        return sum(items)

    second_total.__module__, second_total.__qualname__ = jobs.__name__, "second_total"
    monkeypatch.setattr(jobs, "second_total", task(backend="second", queue_name="shop")(second_total), raising=False)
    # Forget what defining the task did: start like a fresh worker process.
    other.tasks.unregister("djangoproj.shop.jobs.second_total")
    other._task_resolvers.clear()
    dj_tasks._resolvers.clear()
    install(other)
    install(other)
    assert len(other._task_resolvers) == 1
    assert other.resolve_task("djangoproj.shop.jobs.second_total").name == "djangoproj.shop.jobs.second_total"
    assert other.resolve_task("djangoproj.shop.jobs.total") is None  # the "default" backend's task
    # New backend instances (one per thread) add no resolvers and don't install again.
    autodiscover = list(other._autodiscover)
    thread = threading.Thread(target=jobs.second_total.enqueue, args=([1],))
    thread.start()
    thread.join()
    assert len(other._task_resolvers) == 1
    assert other._autodiscover == autodiscover


@needs_django_tasks
def test_install_skips_unloadable_apps_and_other_backends(django_env, monkeypatch, caplog):
    from django.conf import settings
    from django.tasks import task_backends

    from potatoq import Potatoq
    from potatoq.contrib.django import install

    backends = {
        "dummy": {"BACKEND": "django.tasks.backends.dummy.DummyBackend"},  # not potatoq's: left alone
        "broken": {"BACKEND": "potatoq.contrib.django.tasks.PotatoqBackend", "OPTIONS": {"APP": "no_such_mod:app"}},
    }
    monkeypatch.setattr(settings, "TASKS", backends)
    monkeypatch.setitem(task_backends.__dict__, "settings", backends)
    app = Potatoq("unloadable", broker="memory://", set_as_current=False)
    try:
        with caplog.at_level(logging.DEBUG, logger="potatoq.django"):
            install(app)
        assert "Could not load APP 'no_such_mod:app' of django.tasks backend 'broken'" in caplog.text
        assert app._task_resolvers == []
    finally:
        del task_backends["broken"]
        del task_backends["dummy"]
        app.close()
