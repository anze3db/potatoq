"""The ``Potatoq`` application object: configuration, registration, routing, backends."""

from __future__ import annotations

import logging
import os
import pickle
import re
import sys
import textwrap
import types

import pytest

from potatoq import Potatoq, Task, shared_task, states
from potatoq import app as app_module
from potatoq.app import _missing_task
from potatoq.exceptions import ImproperlyConfigured, NotRegistered, ResultBackendDisabled
from potatoq.testing import drain


@pytest.fixture
def no_env_broker(monkeypatch):
    for name in ("POTATOQ_BROKER_URL", "CELERY_BROKER_URL", "DJANGO_SETTINGS_MODULE"):
        monkeypatch.delenv(name, raising=False)


# --- construction / configuration ---------------------------------------------------


class CustomTask(Task):
    custom = True


def test_constructor_arguments():
    class Config:
        task_default_queue = "from-config"

    app = Potatoq(
        "ctor",
        broker="memory://",
        backend="memory://",
        include=["json"],
        config_source=Config,
        task_cls=CustomTask,
        set_as_current=False,
        task_default_priority=4,
    )
    assert app.Task is CustomTask
    assert app.conf.task_default_queue == "from-config"
    assert app.conf.result_backend == "memory://"
    assert app.conf.include == ("json",)
    assert app.conf.task_default_priority == 4

    @app.task
    def f():
        return 1

    assert isinstance(f, CustomTask) and f.custom


def test_task_cls_as_import_path():
    app = Potatoq("ctor", broker="memory://", task_cls=f"{__name__}:CustomTask", set_as_current=False)
    assert app.Task is CustomTask


def test_set_current_and_default():
    previous_current, previous_default = app_module._current_app, app_module._default_app
    try:
        app = Potatoq("cur", broker="memory://", set_as_current=False)
        assert app_module.current_app() is not app
        app.set_current()
        assert app_module.current_app() is app
        app.set_default()
        assert app_module._get_default_app() is app
    finally:
        app_module._current_app, app_module._default_app = previous_current, previous_default


def test_config_from_object_missing_module():
    app = Potatoq("cfg", broker="memory://", set_as_current=False)
    with pytest.raises(ImportError):
        app.config_from_object("potatoq_no_such_settings_module")
    app.config_from_object("potatoq_no_such_settings_module", silent=True)
    assert app.conf.broker_url == "memory://"


def test_config_from_object_resets_connections():
    app = Potatoq("cfg", broker="memory://", set_as_current=False)
    first = app.broker
    assert app.results_enabled_by_default() is False
    app.config_from_object({"result_backend": "broker"})
    assert app.broker is not first
    assert app.results_enabled_by_default() is True


@pytest.mark.filterwarnings("ignore:The EMAIL_")
def test_config_from_object_django_settings_installs_hooks(django_env):
    from potatoq.contrib.django import DjangoTransactionHook

    app = Potatoq("djcfg", set_as_current=False)
    app.config_from_object("django.conf:settings", namespace="CELERY")
    assert app.conf.task_acks_late is True
    assert any(isinstance(h, DjangoTransactionHook) for h in app._transaction_hooks)
    assert app._autodiscover == [(None, "tasks")]


def test_config_from_envvar(monkeypatch):
    app = Potatoq("env", broker="memory://", set_as_current=False)
    monkeypatch.delenv("POTATOQ_TEST_CONFIG_MODULE", raising=False)
    app.config_from_envvar("POTATOQ_TEST_CONFIG_MODULE", silent=True)
    with pytest.raises(ImproperlyConfigured, match="POTATOQ_TEST_CONFIG_MODULE"):
        app.config_from_envvar("POTATOQ_TEST_CONFIG_MODULE")

    module = types.ModuleType("potatoq_cov_api_settings")
    module.task_default_queue = "from-env"  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setenv("POTATOQ_TEST_CONFIG_MODULE", module.__name__)
    app.config_from_envvar("POTATOQ_TEST_CONFIG_MODULE")
    assert app.conf.task_default_queue == "from-env"


def test_add_defaults_does_not_override_explicit_settings():
    app = Potatoq("defaults", broker="memory://", set_as_current=False)
    app.conf.task_default_queue = "explicit"
    app.add_defaults({"task_default_queue": "default-q", "task_default_priority": 3})
    assert app.conf.task_default_queue == "explicit"
    assert app.conf.task_default_priority == 3
    app.add_defaults(lambda: {"worker_concurrency": 7})
    assert app.conf.worker_concurrency == 7


# --- tasks --------------------------------------------------------------------------


def test_task_registry_missing_raises_not_registered(memory_app):
    with pytest.raises(NotRegistered):
        memory_app.tasks["no.such.task"]
    memory_app.tasks.unregister("no.such.task")  # no error


def test_task_decorator_rejects_positional_arguments(memory_app):
    with pytest.raises(TypeError, match="keyword arguments only"):
        memory_app.task("name")


def test_redecorating_the_same_function_returns_the_registered_task(memory_app):
    def add(x, y):
        return x + y

    first = memory_app.task(add)
    assert memory_app.task(add) is first


def test_unknown_task_options_are_rejected(memory_app):
    with pytest.raises(TypeError, match="Unknown task option"):

        @memory_app.task(not_an_option=True)
        def f():
            pass


def test_rate_limit_warns(memory_app, caplog):
    with caplog.at_level(logging.WARNING, logger="potatoq"):

        @memory_app.task(rate_limit="10/m")
        def limited():
            pass

    assert "rate_limit is not enforced" in caplog.text
    assert limited.rate_limit == "10/m"


def test_base_as_import_path_and_ignored_options(memory_app):
    @memory_app.task(base=f"{__name__}:CustomTask", shared=False, lazy=True, routing_key="x", throws=(KeyError,))
    def f():
        return 1

    assert isinstance(f, CustomTask)
    assert not hasattr(type(f), "shared") and not hasattr(type(f), "routing_key")


def test_gen_task_name_for_main_module(memory_app):
    assert memory_app.gen_task_name("add", "__main__") == "tests.add"
    nameless = Potatoq(broker="memory://", set_as_current=False)
    assert nameless.gen_task_name("add", "__main__") == "__main__.add"


def test_register_task_class_based(memory_app):
    class Multiply(Task):
        def run(self, x, y):
            return x * y

    task = memory_app.register_task(Multiply)
    assert task.app is memory_app
    assert task.name == f"{__name__}.Multiply"
    assert memory_app.tasks[task.name] is task
    result = task.delay(3, 4)
    drain(memory_app)
    assert result.get() == 12

    class Named(Task):
        name = "custom.named"

        def run(self):
            return "named"

    other = Potatoq("other", broker="memory://", set_as_current=False)
    instance = Named()
    instance.app = other
    assert memory_app.register_task(instance) is instance
    assert instance.app is other and memory_app.tasks["custom.named"] is instance


def test_builtin_tasks(memory_app):
    @memory_app.task
    def add(x, y):
        return x + y

    starmap = memory_app.tasks["potatoq.starmap"]
    assert starmap(add.name, [(1, 2), (3, 4)]) == [3, 7]
    accumulate = memory_app.tasks["potatoq.accumulate"]
    assert accumulate(1, 2, 3) == (1, 2, 3)
    assert accumulate(1, 2, 3, index=1) == 2


def test_resolve_task_uses_resolvers(memory_app):
    @memory_app.task(name="known")
    def known():
        pass

    memory_app._task_resolvers.append(lambda name: known if name == "alias" else None)
    assert memory_app.resolve_task("known") is known
    assert memory_app.resolve_task("alias") is known
    assert memory_app.resolve_task("unknown") is None


def test_a_failing_resolver_doesnt_crash_the_worker(memory_app, caplog):
    """The executor (and with it a worker child) never sees a resolver's exception: the
    message is dead-lettered as unregistered."""
    from potatoq.message import Message
    from potatoq.worker import executor

    def broken(name):
        raise LookupError("registry not ready")

    memory_app._task_resolvers.append(broken)
    memory_app._task_resolvers.append(lambda name: memory_app.tasks["potatoq.accumulate"] if name == "later" else None)
    with caplog.at_level(logging.ERROR, logger="potatoq"):
        assert memory_app.resolve_task("later") is memory_app.tasks["potatoq.accumulate"]  # next resolver still asked
        outcome = executor.execute(memory_app, Message(task="nowhere", args=[], kwargs={}), hostname="test")
    assert (outcome.action, outcome.reason) == (executor.DEAD_LETTER, "unregistered task nowhere")
    assert "Task resolver" in caplog.text and "registry not ready" in caplog.text


def test_missing_task_placeholder_raises():
    with pytest.raises(NotImplementedError):
        _missing_task(1, x=2)


def test_send_task_unknown_name_does_not_register(memory_app):
    result = memory_app.send_task("remote.task", (1,), {"y": 2}, countdown=5)
    assert "remote.task" not in memory_app.tasks
    message, _ = memory_app.broker.peek(result.id)
    assert message.args == [1] and message.kwargs == {"y": 2} and message.eta is not None


# --- periodic tasks / imports / autodiscovery -----------------------------------------


def test_add_periodic_task_defaults(memory_app):
    @memory_app.task
    def report(x):
        pass

    key = memory_app.add_periodic_task(30.0, report.s(1), queue="periodic")
    entry = memory_app.conf.beat_schedule[key]
    assert key == repr(report.s(1))
    assert entry["args"] == (1,) and entry["kwargs"] == {} and entry["options"] == {"queue": "periodic"}
    key = memory_app.add_periodic_task(60.0, report.s(), args=(2,), kwargs={"x": 3}, name="explicit")
    assert memory_app.conf.beat_schedule["explicit"]["args"] == (2,)
    assert memory_app.conf.beat_schedule["explicit"]["kwargs"] == {"x": 3}


@pytest.fixture
def task_packages(tmp_path, monkeypatch):
    root = tmp_path / "pkgs"
    for name, body in {
        "covpkg_ok": "IMPORTED = True\n",
        "covpkg_broken": "import potatoq_cov_missing_dependency  # noqa\n",
    }.items():
        (root / name).mkdir(parents=True)
        (root / name / "__init__.py").write_text("")
        (root / name / "tasks.py").write_text(textwrap.dedent(body))
    (root / "covpkg_notasks").mkdir()
    (root / "covpkg_notasks" / "__init__.py").write_text("")
    monkeypatch.syspath_prepend(str(root))
    yield
    for name in list(sys.modules):
        if name.startswith("covpkg_"):
            del sys.modules[name]


def test_loader_imports_includes_and_autodiscovers(memory_app, task_packages):
    memory_app.conf.imports = ("json",)
    memory_app.conf.include = ("covpkg_ok",)
    memory_app.autodiscover_tasks(lambda: ["covpkg_ok", "covpkg_notasks"])
    assert "covpkg_ok.tasks" not in sys.modules
    calls = []
    memory_app.on_after_configure.connect(lambda sender, **kw: calls.append("configure"), weak=False)
    memory_app.loader_import_default_modules()
    memory_app.loader_import_default_modules()
    assert "covpkg_ok" in sys.modules and sys.modules["covpkg_ok.tasks"].IMPORTED
    assert calls == ["configure"]


def test_autodiscover_force_propagates_real_import_errors(memory_app, task_packages):
    memory_app.autodiscover_tasks(["covpkg_notasks", "covpkg_ok"], force=True)
    assert "covpkg_ok.tasks" in sys.modules
    with pytest.raises(ModuleNotFoundError, match="potatoq_cov_missing_dependency"):
        memory_app.autodiscover_tasks(["covpkg_broken"], force=True)


def test_autodiscover_without_django_finds_nothing(memory_app, monkeypatch):
    from django.apps import apps as django_apps

    def not_ready():
        raise RuntimeError("Apps aren't loaded yet")

    monkeypatch.setattr(django_apps, "get_app_configs", not_ready)
    memory_app.autodiscover_tasks(force=True)  # no packages, no error


# --- broker / backend resolution ------------------------------------------------------


def test_broker_url_falls_back_to_sqlite(no_env_broker, caplog):
    app = Potatoq("fallback", set_as_current=False)
    with caplog.at_level(logging.WARNING, logger="potatoq"):
        assert app._broker_url() == "sqlite:///potatoq.sqlite3"
    assert "No broker configured" in caplog.text


def test_broker_url_from_environment(no_env_broker, monkeypatch):
    monkeypatch.setenv("CELERY_BROKER_URL", "memory://")
    assert Potatoq("env", set_as_current=False)._broker_url() == "memory://"


def test_django_database_url_errors_are_swallowed(no_env_broker, monkeypatch):
    import potatoq.contrib.django as dj

    app = Potatoq("dj", set_as_current=False)
    assert app._django_database_url() is None
    monkeypatch.setenv("DJANGO_SETTINGS_MODULE", "potatoq_no_such_settings")

    def broken() -> str:
        raise RuntimeError("no Django")

    monkeypatch.setattr(dj, "database_url", broken)
    assert app._django_database_url() is None


def test_broker_url_from_django_database(django_env, monkeypatch):
    from potatoq.contrib.django import database_url

    monkeypatch.delenv("POTATOQ_BROKER_URL", raising=False)
    monkeypatch.delenv("CELERY_BROKER_URL", raising=False)
    app = Potatoq("djbroker", set_as_current=False)
    assert app._broker_url() == database_url()
    assert app._broker_url().startswith("sqlite:///")


def test_django_db_result_backend(django_env):
    app = Potatoq("djdb", broker="memory://", backend="django-db", set_as_current=False)
    backend = app.backend
    assert type(backend).__name__ == "SQLiteBroker"
    assert app.results_enabled_by_default() is True
    app.close()


def test_django_db_result_backend_requires_django(no_env_broker):
    app = Potatoq("djdb", broker="memory://", backend="django-db", set_as_current=False)
    with pytest.raises(ImproperlyConfigured, match="requires Django"):
        _ = app.backend


@pytest.mark.parametrize("url", ["rpc://", "rpc", "disabled", "none"])
def test_disabled_result_backends(url):
    app = Potatoq("nores", broker="memory://", backend=url, set_as_current=False)
    assert app.backend is None
    assert app.results_enabled_by_default() is False
    with pytest.raises(ResultBackendDisabled):
        app.require_backend()
    app.store_result("some-id", states.SUCCESS, 1)  # silently dropped


def test_broker_without_result_support_has_no_backend():
    app = Potatoq("amqp", broker="amqp://guest:guest@localhost:5672//", set_as_current=False)
    assert app.backend is None


def test_result_backend_equal_to_broker_url_shares_the_broker():
    app = Potatoq("same", broker="memory://", backend="memory://", set_as_current=False)
    assert app.backend is app.broker
    assert app.results_enabled_by_default() is True


def test_separate_result_backend(tmp_path):
    url = f"sqlite:///{tmp_path}/results.db"
    app = Potatoq("sep", broker="memory://", backend=url, set_as_current=False)
    with app:
        backend = app.backend
        assert backend is not app.broker and type(backend).__name__ == "SQLiteBroker"
        app.store_result("tid", states.SUCCESS, 42, task_name="t")
        assert app.AsyncResult("tid").get() == 42
    assert (tmp_path / "results.db").exists()


def test_result_backend_that_cannot_store_results():
    app = Potatoq("bad", broker="memory://", backend="amqp://guest:guest@localhost:5672//", set_as_current=False)
    with pytest.raises(ImproperlyConfigured, match="can't store results"):
        _ = app.backend


def test_connections_are_reopened_after_fork(memory_app):
    broker = memory_app.broker
    memory_app._pid = -1
    assert memory_app.broker is not broker
    assert memory_app._pid == os.getpid()
    backend = memory_app.backend
    memory_app._pid = -1
    assert memory_app.backend is not backend
    assert any(b is broker for b, _ in app_module._inherited)


# --- publishing / transactions / routing ----------------------------------------------


def test_publish_nothing_is_a_noop(memory_app):
    memory_app.publish([])
    assert memory_app.broker.queue_sizes() == {}


def test_on_commit_hooks(memory_app):
    calls = []
    memory_app.on_commit(lambda: calls.append("now"))
    assert calls == ["now"]

    class Deferring:
        pending: list = []

        def on_commit(self, fn, using):
            self.pending.append(fn)
            return True

        def publish(self, app, messages, using):
            return False

    hook = Deferring()
    memory_app.add_transaction_hook(hook)
    memory_app.add_transaction_hook(Deferring())  # same type: added once
    assert memory_app._transaction_hooks == [hook]
    memory_app.on_commit(lambda: calls.append("later"))
    assert calls == ["now"] and len(hook.pending) == 1


def test_task_routes_callables_regex_and_lists(memory_app):
    seen = []

    def router(name, args, kwargs, options):
        seen.append(name)
        if name == "a.string":
            return "q-string"
        if name == "a.dict":
            return {"queue": "q-dict", "priority": 5}
        return None

    memory_app.conf.task_routes = [
        router,
        {re.compile(r"^re\..*"): {"queue": "q-regex"}},
        {"glob.*": "q-glob"},
    ]
    assert memory_app.route_for("a.string") == {"queue": "q-string"}
    assert memory_app.route_for("a.dict") == {"queue": "q-dict", "priority": 5}
    assert memory_app.route_for("re.thing") == {"queue": "q-regex"}
    assert memory_app.route_for("glob.thing") == {"queue": "q-glob"}
    assert memory_app.route_for("unrouted") == {}
    assert "unrouted" in seen

    @memory_app.task(name="a.dict")
    def routed():
        pass

    message, _ = memory_app.broker.peek(routed.delay().id)
    assert message.queue == "q-dict" and message.priority == 5

    memory_app.conf.task_routes = (router,)
    assert memory_app.route_for("a.string") == {"queue": "q-string"}
    memory_app.conf.task_routes = router
    assert memory_app.route_for("a.string") == {"queue": "q-string"}


# --- misc API surface -----------------------------------------------------------------


def test_signature_from_name_and_dict(memory_app):
    sig = memory_app.signature("some.task", (1,), {"x": 2}, countdown=3)
    assert sig.task == "some.task" and sig.args == (1,) and sig.kwargs == {"x": 2}
    assert sig.options == {"countdown": 3}
    clone = memory_app.signature(dict(sig))
    assert clone == sig and clone.app is memory_app


def test_group_result_factory(memory_app):
    empty = memory_app.GroupResult("gid")
    assert empty.id == "gid" and empty.results == [] and empty.app is memory_app


def test_current_task_properties(memory_app):
    assert memory_app.current_task is None
    assert memory_app.current_worker_task is None
    assert memory_app.current_task_request() is None

    @memory_app.task(bind=True)
    def introspect(self):
        return [memory_app.current_task is self, memory_app.current_worker_task is self]

    result = introspect.delay()
    drain(memory_app)
    assert result.get() == [True, True]


def test_worker_factory(memory_app):
    worker = memory_app.Worker(concurrency=2, queues="a,b")
    try:
        assert worker.app is memory_app and worker.concurrency == 2 and worker.queues == ["a", "b"]
    finally:
        os.close(worker._wake_r)
        os.close(worker._wake_w)


def test_worker_main_runs_cli_commands(memory_app, capsys):
    memory_app.worker_main(["queues"])
    assert "All queues are empty" in capsys.readouterr().out
    assert Potatoq.start is Potatoq.worker_main


def test_worker_main_defaults_to_worker(memory_app, monkeypatch):
    import potatoq.cli as cli

    calls = []
    monkeypatch.setattr(cli, "cmd_worker", lambda app, args: calls.append(app) or 0)
    memory_app.worker_main()
    assert calls == [memory_app]


def test_context_manager_repr_and_amqp():
    with Potatoq("ctx", broker="memory://", set_as_current=False) as app:
        broker = app.broker
        assert repr(app).startswith("<Potatoq ctx:0x")
    assert app._broker is None and broker is not None
    assert repr(Potatoq(broker="memory://", set_as_current=False)).startswith("<Potatoq __main__:")
    with pytest.raises(AttributeError, match="kombu"):
        _ = app.amqp


# --- shared_task ----------------------------------------------------------------------


def test_shared_task_proxy_resolves_on_current_app(memory_app):
    @shared_task(name="cov.shared.mul")
    def mul(x, y):
        """Multiply."""
        return x * y

    assert repr(mul) == f"<@shared_task: {__name__}.mul>"
    assert mul.__doc__ == "Multiply." and mul.__wrapped__(2, 2) == 4
    assert mul(2, 3) == 6
    assert mul.name == "cov.shared.mul"
    assert memory_app.tasks["cov.shared.mul"].run(2, 5) == 10
    restored = pickle.loads(pickle.dumps(mul))
    assert restored is memory_app.tasks["cov.shared.mul"]


def test_shared_task_registers_on_already_finalized_apps(memory_app):
    memory_app.finalize()
    memory_app.finalize()  # idempotent

    @shared_task
    def late():
        return "late"

    assert f"{__name__}.late" in memory_app.tasks
