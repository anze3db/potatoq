"""Core building blocks: config, control, logging, signals, schedules, serialization,
messages, exceptions and the ``drain`` test helper."""

from __future__ import annotations

import contextlib
import gc
import importlib
import importlib.metadata
import io
import logging
import os
import time
import types
from datetime import UTC, datetime, timedelta

import pytest

import potatoq
from potatoq import Potatoq, serialization, signals
from potatoq.config import Settings, load_object, normalize_key
from potatoq.exceptions import RemoteError, Retry
from potatoq.log import TaskContextFilter, TaskFormatter, get_logger, get_task_logger, setup_logging
from potatoq.message import Message
from potatoq.schedules import crontab, maybe_schedule, schedule
from potatoq.testing import _next_scheduled, drain


def test_version_falls_back_when_not_installed(monkeypatch):
    def not_installed(name):
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", not_installed)
    try:
        importlib.reload(potatoq)
        assert potatoq.__version__ == "0+unknown"
    finally:
        monkeypatch.undo()
        importlib.reload(potatoq)
    assert potatoq.__version__ != "0+unknown"


# --- config ---------------------------------------------------------------------


def test_normalize_key():
    assert normalize_key("CELERY_BROKER_URL", namespace="CELERY") == "broker_url"
    assert normalize_key("DEBUG", namespace="CELERY") is None  # other Django settings
    assert normalize_key("POTATOQ_TASK_TIME_LIMIT") == "task_time_limit"
    assert normalize_key("CELERYD_CONCURRENCY") == "worker_concurrency"
    assert normalize_key("SECRET_KEY") is None
    assert normalize_key("task_acks_late") == "task_acks_late"


def test_settings_is_a_mutable_mapping():
    conf = Settings({"CELERY_DEFAULT_QUEUE": "jobs"})
    assert conf.task_default_queue == "jobs"
    assert conf.changed() == {"task_default_queue": "jobs"}
    conf.custom = 1
    assert "custom" in list(conf) and len(conf) == len(set(conf))
    del conf["custom"]
    assert "custom" not in conf
    with pytest.raises(AttributeError, match="custom"):
        conf.custom  # noqa: B018


def test_settings_from_an_import_path(tmp_path, monkeypatch):
    (tmp_path / "cov_core_settings.py").write_text("CELERY_TASK_ACKS_LATE = False\nDEBUG = True\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    conf = Settings()
    conf.update_from_object("cov_core_settings", namespace="CELERY")
    assert conf.task_acks_late is False
    assert "debug" not in conf
    conf.update_from_object({"task_max_retries": 7})
    assert conf.task_max_retries == 7


def test_get_env_returns_the_first_non_empty_variable(monkeypatch):
    monkeypatch.setenv("COV_CORE_A", "")
    monkeypatch.setenv("COV_CORE_B", "b")
    assert Settings().get_env("COV_CORE_A", "COV_CORE_B") == "b"
    assert Settings().get_env("COV_CORE_A") is None


def test_load_object():
    assert load_object("os.path:join") is os.path.join
    assert load_object("os.path") is os.path
    assert load_object("os.path.join") is os.path.join
    with pytest.raises(ImportError):
        load_object("a_module_that_does_not_exist_xyz")


# --- control --------------------------------------------------------------------


@pytest.fixture
def app():
    app = Potatoq("core-tests", broker="memory://", set_as_current=False)
    app.conf.result_backend = "broker"
    yield app
    app.close()


def test_inspect_reports_live_workers_from_heartbeats(app):
    inspect = app.control.inspect()
    assert inspect.ping() is None
    app.broker.heartbeat(
        "w1@a:1", {"hostname": "w1@a", "queues": ["default"], "running": ["t1"], "registered": ["core.add"]}
    )
    app.broker.heartbeat("w2@b:2", {"hostname": "w2@b", "queues": ["other"]})
    app.broker.workers_["dead@c:3"] = {"hostname": "dead@c", "heartbeat": time.time() - 3600}

    assert inspect.ping() == {"w1@a:1": {"ok": "pong"}, "w2@b:2": {"ok": "pong"}}
    assert inspect.active() == {"w1@a:1": [{"id": "t1"}], "w2@b:2": []}
    assert inspect.active_queues()["w2@b:2"] == [{"name": "other"}]
    # What each worker registered, not this process's registry.
    assert inspect.registered() == {"w1@a:1": ["core.add"], "w2@b:2": []}
    assert inspect.scheduled() == inspect.reserved() == {"w1@a:1": [], "w2@b:2": []}
    assert inspect.stats()["w1@a:1"]["hostname"] == "w1@a"

    by_hostname = app.control.inspect(destination=["w2@b"])
    by_id = app.control.inspect(destination=["w1@a:1"])
    assert list(by_hostname.ping()) == ["w2@b:2"]
    assert list(by_id.ping()) == ["w1@a:1"]
    assert app.control.ping(destination=["w2@b"]) == [{"w2@b:2": {"ok": "pong"}}]


def test_control_purge_empties_every_queue(app):
    app.send_task("core.x")
    app.send_task("core.x", queue="other")
    app.send_task("core.x", queue="other")
    assert app.control.purge() == 3
    assert app.broker.queue_sizes() == {}


# --- logging --------------------------------------------------------------------


def test_logger_helpers():
    assert get_logger("proj.tasks") is logging.getLogger("proj.tasks")
    assert get_task_logger("proj.tasks").name == "potatoq.task.proj.tasks"
    assert get_task_logger("celery.task") is get_task_logger("potatoq.task") is logging.getLogger("potatoq.task")


def test_task_records_use_the_task_format(app):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(TaskContextFilter())
    handler.setFormatter(TaskFormatter("plain: %(message)s", "task %(task_name)s[%(task_id)s]: %(message)s"))
    logger = get_task_logger("cov_core")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    @app.task(name="core.logs")
    def logs():
        logger.info("inside")

    try:
        result = logs.delay()
        drain(app)
        logger.info("outside")
    finally:
        logger.removeHandler(handler)
    assert stream.getvalue().splitlines() == [f"task core.logs[{result.id}]: inside", "plain: outside"]


@contextlib.contextmanager
def empty_root_logger():
    """An empty root logger; pytest's own handlers are put back afterwards.

    (A context manager rather than a fixture: pytest swaps its capture handlers in and
    out around each test phase.)"""
    root = logging.getLogger()
    saved = root.handlers[:]
    levels = {name: logging.getLogger(name).level for name in (None, "potatoq", "pika")}
    root.handlers[:] = []
    try:
        yield root
    finally:
        for handler in root.handlers:
            handler.close()
        root.handlers[:] = saved
        for name, level in levels.items():
            logging.getLogger(name).setLevel(level)


def test_setup_logging_adds_a_handler_when_none_is_configured(app, tmp_path):
    configured = []

    def after_setup(sender, logger, loglevel, **kwargs):
        configured.append((logger, loglevel))

    signals.after_setup_logger.connect(after_setup)
    try:
        with empty_root_logger() as root:
            setup_logging(app, "warning", str(tmp_path / "worker.log"))
            [handler] = root.handlers
            assert isinstance(handler, logging.FileHandler)
            assert isinstance(handler.formatter, TaskFormatter)
            assert root.level == logging.WARNING
            assert logging.getLogger("pika").level == logging.CRITICAL
    finally:
        signals.after_setup_logger.disconnect(after_setup)
    assert configured == [(root, logging.WARNING)]


def test_setup_logging_keeps_an_existing_configuration(app):
    existing = logging.StreamHandler(io.StringIO())
    with empty_root_logger() as root:
        root.addHandler(existing)
        setup_logging(app, logging.DEBUG)
        assert root.handlers == [existing]
        assert any(isinstance(f, TaskContextFilter) for f in existing.filters)
        assert logging.getLogger("potatoq").level == logging.DEBUG


def test_setup_logging_hijacks_the_root_logger_when_asked(app):
    existing = logging.StreamHandler(io.StringIO())
    app.conf.worker_hijack_root_logger = True
    with empty_root_logger() as root:
        root.addHandler(existing)
        setup_logging(app)
        [handler] = root.handlers
        assert handler is not existing and isinstance(handler, logging.StreamHandler)


def test_setup_logging_signal_receivers_take_over(app):
    calls = []

    def configure(sender, loglevel, **kwargs):
        calls.append(loglevel)

    signals.setup_logging.connect(configure)
    try:
        with empty_root_logger() as root:
            setup_logging(app, "INFO")
            assert root.handlers == []
    finally:
        signals.setup_logging.disconnect(configure)
    assert calls == [logging.INFO]


# --- signals --------------------------------------------------------------------


def test_signal_connect_variants_and_sender_filtering():
    sig = signals.Signal("cov_core")
    assert repr(sig) == "<Signal: cov_core>"
    assert sig.send(sender=None) == []
    calls = []

    @sig.connect(sender="core.add")
    def by_name(sender, **kwargs):
        calls.append(("by_name", sender.name))

    @sig.connect
    def anyone(sender, **kwargs):
        calls.append(("anyone", kwargs["value"]))
        return "ok"

    sig.send(sender=types.SimpleNamespace(name="core.add"), value=1)
    sig.send(sender=types.SimpleNamespace(name="core.other"), value=2)
    assert calls == [("by_name", "core.add"), ("anyone", 1), ("anyone", 2)]
    assert len(sig.receivers) == 2 and sig.has_receivers()
    assert sig.disconnect(anyone) and not sig.disconnect(anyone)


def test_signal_handler_errors_are_logged_not_raised(caplog):
    sig = signals.Signal("cov_core_errors")

    def broken(sender, **kwargs):
        raise ValueError("bad receiver")

    sig.connect(broken)
    with caplog.at_level(logging.ERROR, logger="potatoq.signals"):
        [(fn, response)] = sig.send()
    assert fn is broken and isinstance(response, ValueError)
    assert "Signal handler" in caplog.text and "bad receiver" in caplog.text


def test_weak_receivers_are_dropped_once_garbage_collected():
    sig = signals.Signal("cov_core_weak")
    calls = []

    class Receiver:
        def method(self, sender, **kwargs):
            calls.append("method")

    def function(sender, **kwargs):
        calls.append("function")

    receiver = Receiver()
    sig.connect(receiver.method, weak=True)
    sig.connect(function, weak=True)
    sig.send()
    assert calls == ["method", "function"]

    del receiver, function
    gc.collect()
    assert sig.send() == []
    assert sig.receivers == []


# --- schedules ------------------------------------------------------------------


def test_interval_schedule():
    with pytest.raises(ValueError, match="positive"):
        schedule(0)
    assert schedule(timedelta(minutes=1)) == schedule(60)
    assert schedule(60) != schedule(30) and schedule(60) != 60
    assert len({schedule(60), schedule(60.0)}) == 1
    assert maybe_schedule(timedelta(seconds=5)) == schedule(5)


def test_crontab_field_syntax():
    every_half_hour = crontab(minute=[0, "30"])
    assert every_half_hour.minute == {0, 30}
    assert crontab(minute="1,,2").minute == {1, 2}
    assert crontab(day_of_week="fri-mon").day_of_week == {5, 6, 0, 1}
    assert crontab(hour="22-2").hour == {22, 23, 0, 1, 2}
    assert crontab(month_of_year="jan,jul").month_of_year == {1, 7}
    for bad, match in [({"minute": "*/0"}, "Invalid step"), ({"minute": []}, "Empty"), ({"hour": "x"}, "token")]:
        with pytest.raises(ValueError, match=match):
            crontab(**bad)


def test_crontab_equality_hash_and_repr():
    weekday_mornings = crontab(minute=0, hour=7, day_of_week="mon-fri")
    assert weekday_mornings == crontab.from_string("0 7 * * 1-5")
    assert weekday_mornings != crontab(minute=0, hour=8) and weekday_mornings != "0 7 * * 1-5"
    assert len({weekday_mornings, crontab.from_string("0 7 * * mon-fri")}) == 1
    assert repr(weekday_mornings) == "<crontab: 0 7 * * mon-fri (m/h/dM/MY/d)>"
    assert maybe_schedule(weekday_mornings) is weekday_mornings
    assert maybe_schedule("@hourly") == crontab(minute=0)
    with pytest.raises(TypeError, match="Unsupported schedule"):
        maybe_schedule(object())


def test_crontab_that_can_never_fire():
    feb_30 = crontab(minute=0, hour=0, day_of_month=30, month_of_year=2)
    with pytest.raises(ValueError, match="never fires"):
        feb_30.next_after(datetime(2026, 1, 1, tzinfo=UTC))


# --- serialization --------------------------------------------------------------


def test_objects_with_json_hooks_and_lazy_strings():
    class Point:
        def __json__(self):
            return {"x": 1, "y": 2}

    lazy = type("__proxy__", (), {"__str__": lambda self: "translated"})()
    assert serialization.loads(serialization.dumps([Point(), lazy])) == [{"x": 1, "y": 2}, "translated"]


def test_loads_accepts_memoryview():
    assert serialization.loads(memoryview(b'{"a": 1}')) == {"a": 1}


def test_exception_round_trip_edge_cases():
    unencodable = object()
    data = serialization.exception_to_dict(ValueError("msg", unencodable))
    assert data["exc_message"] == ["msg", repr(unencodable)]

    assert serialization.exception_from_dict(None) is None
    single = serialization.exception_from_dict({"exc_type": "KeyError", "exc_message": "k"})
    assert isinstance(single, KeyError) and single.args == ("k",)
    # UnicodeDecodeError needs five arguments: rebuilding it fails, so it is reported remotely.
    remote = serialization.exception_from_dict({"exc_type": "UnicodeDecodeError", "exc_message": ["a", "b"]})
    assert isinstance(remote, RemoteError)
    assert (remote.exc_type, remote.exc_module, remote.exc_message) == ("UnicodeDecodeError", "builtins", "a, b")


# --- messages / exceptions --------------------------------------------------------


def test_message_delay():
    assert Message("t").delay == 0.0
    assert Message("t", eta=time.time() - 10).delay == 0.0
    assert 50 < Message("t", eta=time.time() + 60).delay <= 60


def test_retry_messages():
    when = datetime(2030, 1, 1, tzinfo=UTC)
    assert str(Retry("custom message")) == "custom message"
    assert str(Retry(exc=ValueError("x"), when=when)) == f"Retry at {when}: ValueError('x')"
    assert str(Retry(when=30)) == "Retry in 30s"
    assert str(Retry(when=8.782365561677146)) == "Retry in 8.78s"


# --- testing.drain --------------------------------------------------------------


def test_drain_can_raise_the_first_failure(app):
    @app.task(name="core.fail")
    def fail():
        raise LookupError("nope")

    fail.delay()
    with pytest.raises(LookupError, match="nope"):
        drain(app, raise_on_failure=True)


def test_drain_runs_scheduled_tasks_on_sqlite(tmp_path):
    app = Potatoq("core-sqlite", broker=f"sqlite:///{tmp_path}/drain.db", set_as_current=False)
    app.conf.result_backend = "broker"

    @app.task(name="core.later")
    def later(x):
        return x * 2

    try:
        result = later.apply_async((21,), countdown=3600)
        assert drain(app, include_scheduled=False) == []
        [done] = drain(app)
        assert (done.id, done.state, done.result) == (result.id, "SUCCESS", 42)
    finally:
        app.close()


def test_scheduled_tasks_are_not_fast_forwarded_on_other_brokers():
    assert _next_scheduled(types.SimpleNamespace(broker=object()), consumer=None) is None
