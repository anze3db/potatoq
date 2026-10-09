"""The ``potatoq`` command line, driven in-process through ``potatoq.cli.main``."""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import sys
import textwrap
import time
import types
import uuid
from pathlib import Path

import pytest

from potatoq import Potatoq, cli, console
from potatoq.brokers.sqlite import SQLiteBroker
from potatoq.message import Message
from potatoq.testing import drain
from potatoq.worker import supervisor


@pytest.fixture(autouse=True)
def _restore_logging():
    """The solo worker and beat configure logging; undo it after each test."""
    root = logging.getLogger()
    handlers = {h: list(h.filters) for h in root.handlers}
    levels = {name: logging.getLogger(name).level for name in (None, "potatoq", "pika")}
    yield
    for handler, filters in handlers.items():
        handler.filters[:] = filters
    for name, level in levels.items():
        logging.getLogger(name).setLevel(level)


@pytest.fixture
def app():
    app = Potatoq("cli-tests", broker="memory://", set_as_current=False)
    app.conf.result_backend = "broker"
    yield app
    app.close()


def run(app, *argv):
    return cli.main(["-A", app, *argv])


@pytest.fixture(autouse=True)
def _plain_output(monkeypatch):
    """Assertions read plain text: no emojis (captured output has no colors anyway)."""
    monkeypatch.setenv("POTATOQ_NO_EMOJI", "1")
    yield
    console.configure()


def said(capsys) -> list[str]:
    """What the command printed, one entry per non-empty line, whitespace collapsed."""
    return [" ".join(line.split()) for line in capsys.readouterr().out.splitlines() if line.strip()]


# --- finding the app ------------------------------------------------------------


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A scratch directory on sys.path; returns a helper that writes modules into it."""
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.chdir(tmp_path)
    created: list[str] = []

    def write(relpath: str, source: str) -> str:
        path = tmp_path / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(source))
        created.append(relpath.split("/")[0].removesuffix(".py"))
        return created[-1]

    write.unique = lambda prefix: f"{prefix}_{uuid.uuid4().hex[:8]}"  # type: ignore[attr-defined]
    yield write
    for name in list(sys.modules):
        if name.split(".")[0] in created:
            del sys.modules[name]


APP_SOURCE = "from potatoq import Potatoq\n{name} = Potatoq('found', broker='memory://', set_as_current=False)\n"


def test_find_app_returns_an_instance_unchanged(app):
    assert cli.find_app(app) is app


def test_find_app_looks_for_app_potatoq_or_celery_attributes(project):
    for attr in ("app", "potatoq", "celery"):
        name = project(f"{project.unique('mod')}.py", APP_SOURCE.format(name=attr))
        assert cli.find_app(name) is getattr(sys.modules[name], attr)


def test_find_app_with_module_and_attribute(project):
    name = project(
        f"{project.unique('factory')}.py",
        """
        from potatoq import Potatoq

        def create_app():
            return Potatoq("from-factory", broker="memory://", set_as_current=False)

        class holder:
            app = Potatoq("nested", broker="memory://", set_as_current=False)
        """,
    )
    assert cli.find_app(f"{name}:create_app").main == "from-factory"  # factories are called
    assert cli.find_app(f"{name}:holder.app") is sys.modules[name].holder.app


def test_find_app_looks_in_the_celery_submodule_of_a_package(project):
    pkg = project.unique("proj")
    project(f"{pkg}/__init__.py", "")
    project(f"{pkg}/celery.py", APP_SOURCE.format(name="app"))
    assert cli.find_app(pkg) is sys.modules[f"{pkg}.celery"].app


def test_find_app_reports_broken_submodule_imports(project):
    pkg = project.unique("broken")
    project(f"{pkg}/__init__.py", "")
    project(f"{pkg}/potatoq.py", "import a_module_that_does_not_exist_xyz\n")
    with pytest.raises(ModuleNotFoundError, match="a_module_that_does_not_exist_xyz"):
        cli.find_app(pkg)


def test_find_app_falls_back_to_any_instance_in_the_module(project):
    name = project(f"{project.unique('anyname')}.py", APP_SOURCE.format(name="my_queue"))
    assert cli.find_app(name) is sys.modules[name].my_queue


def test_find_app_without_an_app_exits_with_a_hint(project):
    name = project(f"{project.unique('empty')}.py", "x = 1\n")
    with pytest.raises(SystemExit, match="Pass -A module:attribute"):
        cli.find_app(name)


def test_find_app_without_spec_uses_the_current_app(project, monkeypatch, app):
    monkeypatch.delenv("DJANGO_SETTINGS_MODULE", raising=False)
    monkeypatch.setattr(cli, "current_app", lambda: app)
    assert cli.find_app(None) is app


def test_find_app_without_spec_sets_up_django_first(project, monkeypatch, app):
    import django
    from django.apps import apps

    calls = []
    monkeypatch.setenv("DJANGO_SETTINGS_MODULE", "proj.settings")
    monkeypatch.setattr(apps, "ready", False)
    monkeypatch.setattr(django, "setup", lambda: calls.append("setup"))
    monkeypatch.setattr(cli, "current_app", lambda: calls.append("current_app") or app)
    assert cli.find_app(None) is app
    assert calls == ["setup", "current_app"]


def test_find_app_sets_up_django_after_importing_a_celery_py_module(project, monkeypatch):
    """`-A proj.celery:app`: the module sets DJANGO_SETTINGS_MODULE, then Django is set up."""
    import django
    from django.apps import apps

    monkeypatch.setenv("DJANGO_SETTINGS_MODULE", "restored-afterwards")
    monkeypatch.delenv("DJANGO_SETTINGS_MODULE")
    name = project(
        f"{project.unique('celerypy')}.py",
        "import os\nos.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proj.settings')\n" + APP_SOURCE.format(name="app"),
    )
    setups = []
    monkeypatch.setattr(apps, "ready", False)
    monkeypatch.setattr(django, "setup", lambda: setups.append(os.environ["DJANGO_SETTINGS_MODULE"]))
    assert cli.find_app(f"{name}:app") is sys.modules[name].app
    assert setups == ["proj.settings"]
    monkeypatch.setattr(apps, "ready", True)
    cli.find_app(f"{name}:app")
    assert setups == ["proj.settings"]  # already set up (e.g. `manage.py`): not again


def test_find_app_with_django_settings_but_no_django(project, monkeypatch):
    name = project(f"{project.unique('nodjango')}.py", APP_SOURCE.format(name="app"))
    monkeypatch.setenv("DJANGO_SETTINGS_MODULE", "proj.settings")
    monkeypatch.setitem(sys.modules, "django", None)
    assert cli.find_app(name) is sys.modules[name].app


def test_main_imports_the_app_by_name(project, capsys):
    name = project(f"{project.unique('named')}.py", APP_SOURCE.format(name="app"))
    assert cli.main(["-A", name, "queues"]) == 0
    assert said(capsys) == ["queues All queues are empty"]


# --- global options -------------------------------------------------------------


def test_global_options_override_configuration(app, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(Path.cwd())  # restored after the test
    url = f"sqlite:///{tmp_path}/override.db"
    assert run(app, "--workdir", str(tmp_path), "-b", url, "--result-backend", "broker", "queues") == 0
    assert Path.cwd().resolve() == tmp_path.resolve()
    assert (app.conf.broker_url, app.conf.result_backend) == (url, "broker")
    assert isinstance(app.broker, SQLiteBroker)


def test_worker_is_the_default_command(app, monkeypatch):
    monkeypatch.setattr(cli, "cmd_worker", lambda app, args: 7)
    assert run(app) == 7


# --- worker ---------------------------------------------------------------------


@pytest.fixture
def fake_supervisor(monkeypatch):
    captured: dict = {}

    class FakeSupervisor:
        def __init__(self, app, **kwargs):
            captured.update(kwargs, app=app)

        def start(self):
            return 0

    monkeypatch.setattr(supervisor, "Supervisor", FakeSupervisor)
    return captured


def test_worker_options_reach_the_supervisor_and_config(app, fake_supervisor, tmp_path):
    pidfile = tmp_path / "worker.pid"
    argv = ["worker", "-c", "3", "-Q", "a,b", "-n", "w1@%h", "-l", "debug", "--max-tasks-per-child", "50"]
    argv += ["--time-limit", "60", "--soft-time-limit", "50", "--pidfile", str(pidfile), "--no-scheduler"]
    assert run(app, *argv) == 0
    assert (app.conf.task_time_limit, app.conf.task_soft_time_limit) == (60.0, 50.0)
    assert pidfile.read_text() == str(__import__("os").getpid())
    assert fake_supervisor["concurrency"] == 3
    assert fake_supervisor["queues"] == "a,b"
    assert fake_supervisor["hostname"] == f"w1@{socket.gethostname()}"
    assert fake_supervisor["max_tasks_per_child"] == 50
    assert fake_supervisor["scheduler"] is False


def test_worker_accepts_celery_only_flags(app, fake_supervisor):
    argv = ["worker", "-B", "-O", "fair", "--autoscale", "10,3", "--prefetch-multiplier", "4", "--without-gossip", "-E"]
    assert run(app, *argv) == 0
    assert fake_supervisor["scheduler"] is None  # the scheduler stays on unless --no-scheduler


def test_worker_with_unknown_pool_falls_back_to_prefork(app, fake_supervisor, capsys):
    assert run(app, "worker", "-P", "gevent") == 0
    assert "pool 'gevent' is not supported; using prefork" in capsys.readouterr().err
    assert fake_supervisor["app"] is app


def test_threads_pool_means_one_process_with_threads(app, fake_supervisor):
    assert run(app, "worker", "-P", "threads") == 0
    assert (fake_supervisor["concurrency"], fake_supervisor["threads"]) == (1, 10)
    assert run(app, "worker", "-P", "THREADS", "-c", "4") == 0
    assert (fake_supervisor["concurrency"], fake_supervisor["threads"]) == (1, 4)


def test_solo_pool_runs_in_process(app, monkeypatch, fake_supervisor):
    calls = []
    monkeypatch.setattr(cli, "run_solo", lambda app, args: calls.append(args.hostname) or 0)
    assert run(app, "worker", "-P", "solo", "-n", "solo1") == 0
    assert calls == ["solo1"]
    assert fake_supervisor == {}


def test_run_solo_executes_tasks_until_stopped(app, monkeypatch):
    @app.task(name="cli.add")
    def add(x, y):
        return x + y

    result = add.delay(2, 3)
    handlers: dict = {}
    monkeypatch.setattr(signal, "signal", lambda sig, handler: handlers.__setitem__(sig, handler))
    real_consumer = app.broker.consumer
    seen_workers = []

    def consumer(queues, node_id):
        inner = real_consumer(queues, node_id)
        real_fetch = inner.fetch

        def fetch(timeout):
            delivery = real_fetch(timeout=0)
            if delivery is None:  # queue drained: what SIGTERM does
                seen_workers.extend(app.broker.workers())
                handlers[signal.SIGTERM]()
            return delivery

        inner.fetch = fetch
        return inner

    monkeypatch.setattr(app.broker, "consumer", consumer)
    app.conf.beat_schedule = {"every-second": {"task": add.name, "schedule": 1.0, "args": (1, 1)}}
    ticks = []
    monkeypatch.setattr("potatoq.worker.scheduler.Scheduler.tick", lambda self: ticks.append(self))
    assert run(app, "worker", "-P", "solo", "-n", "solo-test") == 0
    assert ticks  # solo workers run the scheduler too
    assert result.get(timeout=1) == 5
    assert set(handlers) == {signal.SIGTERM, signal.SIGINT}
    assert [(w["hostname"], w["concurrency"]) for w in seen_workers] == [("solo-test", 1)]
    assert app.broker.workers() == []  # unregistered on the way out


# --- beat -----------------------------------------------------------------------


def _stop_beat_on_sleep(monkeypatch):
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "time", types.SimpleNamespace(sleep=sleep))
    return sleeps


def test_beat_runs_the_scheduler_until_interrupted(app, monkeypatch, caplog):
    from potatoq import signals

    app.conf.beat_schedule = {"every-minute": {"task": "cli.noop", "schedule": 60.0}}
    sleeps = _stop_beat_on_sleep(monkeypatch)
    started = []

    def on_beat_init(sender, **kwargs):
        started.append(sender)

    signals.beat_init.connect(on_beat_init)
    try:
        with caplog.at_level(logging.INFO, logger="potatoq"):
            assert run(app, "beat") == 0
    finally:
        signals.beat_init.disconnect(on_beat_init)
    assert len(started) == 1
    assert len(sleeps) == 1 and 0.05 <= sleeps[0] <= 1.0
    assert "beat_schedule is empty" not in caplog.text


def test_beat_warns_when_there_is_nothing_to_schedule(app, monkeypatch, caplog):
    _stop_beat_on_sleep(monkeypatch)
    with caplog.at_level(logging.WARNING, logger="potatoq"):
        assert run(app, "beat") == 0
    assert "beat_schedule is empty; nothing to do" in caplog.text


# --- status / inspect -------------------------------------------------------------


def test_status(app, capsys):
    assert run(app, "status") == 1
    assert said(capsys) == ["workers No live workers"]

    app.broker.heartbeat("w1@host:1", {"hostname": "w1@host", "queues": ["a", "b"], "concurrency": 4, "running": ["t"]})
    assert run(app, "status") == 0
    assert said(capsys) == [
        "workers 1 live",
        "WORKER QUEUES CONCURRENCY RUNNING HEARTBEAT",
        "w1@host:1 a,b 4 1 just now",
    ]

    app.broker.heartbeat("w1@host:1", {"hostname": "w1@host", "queues": ["a"], "concurrency": 2, "threads": 4})
    app.broker.workers_["w1@host:1"]["heartbeat"] -= 125  # stale for a minute or two
    assert run(app, "status") == 0
    assert said(capsys)[-1] == "w1@host:1 a 8 (2×4) 0 2m ago"  # noqa: RUF001

    assert run(app, "status", "--json") == 0
    assert [w["id"] for w in json.loads(capsys.readouterr().out)] == ["w1@host:1"]


def test_inspect(app, capsys):
    assert run(app, "inspect", "ping") == 1
    assert said(capsys) == ["error No workers replied (is a worker running? see potatoq status)"]

    from potatoq.control import publish_registered

    @app.task(name="cli.x")
    def x():
        pass

    digest = publish_registered(app)
    app.broker.heartbeat("w1@host:1", {"hostname": "w1@host", "queues": ["default"], "registered": digest})
    app.broker.heartbeat("w2@host:2", {"hostname": "w2@host", "queues": ["other"]})
    assert run(app, "inspect", "active_queues", "-d", "w2@host") == 0
    assert json.loads(capsys.readouterr().out) == {"w2@host:2": [{"name": "other"}]}
    assert run(app, "inspect", "registered") == 0
    assert "cli.x" in json.loads(capsys.readouterr().out)["w1@host:1"]


# --- queues / purge / call / result / revoke ----------------------------------------


def test_queues(app, capsys):
    assert run(app, "queues") == 0
    assert said(capsys) == ["queues All queues are empty"]
    app.send_task("cli.x", queue="b")
    app.send_task("cli.x", queue="b")
    app.send_task("cli.x", queue="a")
    assert run(app, "queues") == 0
    assert said(capsys) == ["queues 3 tasks waiting", "QUEUE WAITING", "a 1", "b 2"]
    assert run(app, "queues", "--json") == 0
    assert json.loads(capsys.readouterr().out) == {"a": 1, "b": 2}


def test_purge_asks_for_confirmation(app, monkeypatch, capsys):
    app.send_task("cli.x")
    app.send_task("cli.x", queue="other")
    monkeypatch.setattr("builtins.input", lambda prompt: "n")
    assert run(app, "purge") == 1
    assert app.broker.queue_sizes() == {"default": 1, "other": 1}

    prompts = []
    monkeypatch.setattr("builtins.input", lambda prompt: prompts.append(prompt) or "y")
    assert run(app, "purge") == 0
    assert [" ".join(p.split()) for p in prompts] == ["purge Delete all waiting tasks in default? [y/N]"]
    assert said(capsys) == ["purged 1 task from default"]

    assert run(app, "purge", "-f", "-Q", "other,") == 0
    assert said(capsys) == ["purged 1 task from other"]
    assert app.broker.queue_sizes() == {}


def test_call_and_result(app, capsys):
    @app.task(name="cli.add")
    def add(x, y=0):
        return x + y

    assert run(app, "call", "cli.add", "-a", "[2]", "-k", '{"y": 3}', "-Q", "math", "--countdown", "0") == 0
    task_id = capsys.readouterr().out.strip()
    assert app.broker.peek(task_id)[0].queue == "math"

    assert run(app, "result", task_id) == 0
    assert said(capsys) == ["state PENDING", "result None"]
    assert run(app, "result", task_id, "--wait", "0.01") == 1
    assert said(capsys)[0] == "state PENDING"

    drain(app)
    assert run(app, "result", task_id, "--wait", "1") == 0
    assert said(capsys) == ["state SUCCESS", "result 5"]


def test_result_shows_the_traceback_of_a_failure(app, capsys):
    @app.task(name="cli.fail")
    def fail():
        raise ValueError("boom")

    result = fail.delay()
    drain(app)
    assert run(app, "result", result.id) == 0
    out = capsys.readouterr().out
    assert [" ".join(line.split()) for line in out.splitlines()[:2]] == ["state FAILURE", "error ValueError('boom')"]
    assert "Traceback" in out and 'raise ValueError("boom")' in out


def test_revoke(app, capsys):
    first, second = app.send_task("cli.x"), app.send_task("cli.x")
    assert run(app, "revoke", first.id, second.id) == 0
    assert said(capsys) == ["revoked 2 tasks"]
    assert app.broker.queue_sizes() == {}
    assert first.state == "REVOKED"


# --- dead letters ---------------------------------------------------------------


def test_dead_letters_list_and_retry(app, capsys):
    attempts = []

    @app.task(name="cli.flaky")
    def flaky():
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("first attempt fails")
        return "ok"

    assert run(app, "dead", "list") == 0
    assert said(capsys) == ["dead No dead-lettered tasks"]

    result = flaky.delay()
    drain(app)
    assert run(app, "dead", "list") == 0
    assert said(capsys) == [
        "dead 1 dead-lettered task, newest first",
        "TASK ID QUEUE DIED REASON",
        f"cli.flaky {result.id} default just now RuntimeError: first attempt fails",
        "tip Run one again with: potatoq dead retry <ID>",
    ]

    assert run(app, "dead", "list", "--json", "--limit", "5") == 0
    assert [e["id"] for e in json.loads(capsys.readouterr().out)] == [result.id]

    assert run(app, "dead", "retry", result.id, "missing-id") == 0
    assert said(capsys) == [f"requeued {result.id}", "missing missing-id isn't in the dead letters"]
    drain(app)
    assert result.get(timeout=1) == "ok"


def test_dead_letter_without_reason_or_time(app, monkeypatch, capsys):
    entry = {"id": "abc", "task": "cli.x", "queue": "default", "reason": None, "died_at": None}
    monkeypatch.setattr(app.broker, "dead_letters", lambda limit: [entry])
    assert run(app, "dead") == 0  # `dead` alone lists
    assert said(capsys)[2] == "cli.x abc default ?"

    entry.update(died_at=time.time() - 7200, reason="x" * 100)
    assert run(app, "dead") == 0
    assert said(capsys)[2] == f"cli.x abc default 2h ago {'x' * 69}…"


# --- migrate / shell ------------------------------------------------------------


def test_migrate_sets_up_broker_and_separate_result_backend(app, tmp_path, capsys):
    app.conf.result_backend = f"sqlite:///{tmp_path}/results.db"
    assert run(app, "migrate") == 0
    assert said(capsys) == [f"broker Set up {app.broker.url}", f"results Set up sqlite:///{tmp_path}/results.db"]
    assert isinstance(app.backend, SQLiteBroker)
    assert (tmp_path / "results.db").exists()


def test_shell_exposes_the_app_and_tasks(app, monkeypatch):
    @app.task(name="proj.tasks.add")
    def add(x, y):
        return x + y

    sessions = []
    monkeypatch.setattr("code.interact", lambda local, banner: sessions.append((local, banner)))
    assert run(app, "shell") == 0
    [(namespace, banner)] = sessions
    assert namespace["app"] is app
    assert namespace["add"] is app.tasks["proj.tasks.add"]
    assert not any(name.startswith("potatoq") for name in namespace)
    assert banner.startswith("potatoq shell (")


def test_worker_refuses_to_start_on_windows(monkeypatch):
    import pytest

    from potatoq import Potatoq, cli

    monkeypatch.setattr(cli.sys, "platform", "win32")
    app = Potatoq("win", broker="memory://", set_as_current=False)
    with pytest.raises(SystemExit, match="Windows isn't supported"):
        cli.main(["-A", app, "worker"])


# --- macOS fork safety ----------------------------------------------------------


def test_worker_reexecs_on_macos_with_fork_safety_disabled(monkeypatch):
    calls = []
    monkeypatch.setattr(os, "execv", lambda path, argv: calls.append((path, argv)))
    monkeypatch.delenv("OBJC_DISABLE_INITIALIZE_FORK_SAFETY", raising=False)
    monkeypatch.setattr(sys, "orig_argv", [sys.executable, "-m", "potatoq.cli", "worker", "-c", "2"])

    monkeypatch.setattr(sys, "platform", "linux")
    cli._macos_fork_safety()
    assert calls == []

    monkeypatch.setattr(sys, "platform", "darwin")
    cli._macos_fork_safety()
    assert calls == [(sys.executable, [sys.executable, "-m", "potatoq.cli", "worker", "-c", "2"])]
    assert os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] == "YES"

    cli._macos_fork_safety()  # after the restart: already set, carry on
    assert len(calls) == 1


def test_hostname_placeholders_like_celery(monkeypatch):
    monkeypatch.setattr(cli.socket, "gethostname", lambda: "web-1.example.com")
    assert cli.expand_hostname("w1@%h") == "w1@web-1.example.com"
    assert cli.expand_hostname("%n-worker@%d") == "web-1-worker@example.com"
    assert cli.expand_hostname("100%%@%n") == "100%@web-1"


def test_schedule_lists_entries_with_next_run(app, capsys):
    assert run(app, "schedule") == 0
    assert said(capsys) == ["schedule No periodic tasks (beat_schedule is empty)"]
    app.conf.beat_schedule = {"ping": {"task": "cli.ping", "schedule": 30.0}}
    app.broker.enqueue_periodic("ping", time.time() - 90, Message(task="cli.ping"))
    assert run(app, "schedule") == 0
    lines = said(capsys)
    assert lines[:2] == ["schedule 1 periodic task, times in UTC", "ENTRY TASK SCHEDULE NEXT RUN LAST SENT"]
    assert lines[2].startswith("ping cli.ping every 30s 20") and lines[2].endswith(" 1m ago")


def test_solo_worker_heartbeats_while_a_task_runs(app, monkeypatch, caplog):
    """A long task mustn't make a solo worker look dead: other workers would recover
    its task and run it again."""
    import threading

    app.conf.worker_heartbeat_interval = 0.01
    seen: dict = {}
    extended = threading.Event()
    real_extend = app.broker.extend

    def extend(deliveries):
        real_extend(deliveries)
        extended.set()

    monkeypatch.setattr(app.broker, "extend", extend)
    real_heartbeat = app.broker.heartbeat
    failures = iter([RuntimeError("broker blip")])

    def heartbeat(node_id, info):
        failure = next(failures, None)
        if failure:
            raise failure
        real_heartbeat(node_id, info)

    monkeypatch.setattr(app.broker, "heartbeat", heartbeat)

    @app.task(name="cli.slow")
    def slow():
        assert extended.wait(5)  # the lease is extended while we run...
        seen["running"] = [w.get("running") for w in app.broker.workers()]  # ...and we're reported

    slow.delay()
    handlers: dict = {}
    monkeypatch.setattr(signal, "signal", lambda sig, handler: handlers.__setitem__(sig, handler))
    real_consumer = app.broker.consumer

    def consumer(queues, node_id):
        inner = real_consumer(queues, node_id)
        real_fetch = inner.fetch

        def fetch(timeout):
            delivery = real_fetch(timeout=0)
            if delivery is None:
                handlers[signal.SIGTERM]()
            return delivery

        inner.fetch = fetch
        return inner

    monkeypatch.setattr(app.broker, "consumer", consumer)
    assert run(app, "worker", "-P", "solo", "--no-scheduler") == 0
    assert len(seen["running"]) == 1 and len(seen["running"][0]) == 1
    assert "Heartbeat failed" in caplog.text


def test_worker_main_sets_up_django(app, monkeypatch):
    """app.worker_main() in a Django project (DJANGO_SETTINGS_MODULE set) must set Django
    up, or worker processes fail with AppRegistryNotReady."""
    import django
    from django.apps import apps

    setups = []
    monkeypatch.setenv("DJANGO_SETTINGS_MODULE", "proj.settings")
    monkeypatch.setattr(apps, "ready", False)
    monkeypatch.setattr(django, "setup", lambda: setups.append("setup"))
    assert run(app, "queues") == 0
    assert setups == ["setup"]


def test_python_dash_m_potatoq():
    import subprocess

    out = subprocess.run([sys.executable, "-m", "potatoq", "--help"], capture_output=True, text=True, check=True)
    assert out.stdout.startswith("usage: potatoq")


def test_version(capsys):
    import potatoq

    with pytest.raises(SystemExit) as exc:
        cli.main(["--version"])
    assert exc.value.code == 0
    assert (
        capsys.readouterr().out == f"potatoq {potatoq.__version__} (Python {sys.version.split()[0]}, {sys.platform})\n"
    )
