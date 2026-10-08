"""django.tasks (Django 6+) running on potatoq."""

from __future__ import annotations

import os
import subprocess
import sys
import time

import pytest

django = pytest.importorskip("django")
if django.VERSION < (6, 0):  # pragma: no cover
    pytest.skip("django.tasks needs Django 6", allow_module_level=True)

from conftest import TESTS  # noqa: E402


def test_enqueue_and_result_lifecycle(django_env):
    from django.tasks import TaskResultStatus
    from djangoproj.shop.jobs import total

    from potatoq.testing import drain

    app = django_env
    result = total.enqueue(7, [1, 2, 3])
    assert result.status == TaskResultStatus.READY
    assert result.backend == "default"
    refreshed = total.get_result(result.id)
    assert refreshed.status == TaskResultStatus.READY
    assert refreshed.args == [7, [1, 2, 3]]

    drained = drain(app, ["shop"])
    assert [d.name for d in drained] == ["djangoproj.shop.jobs.total"]
    result.refresh()
    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == {"order": 7, "total": 6}
    assert result.is_finished and result.attempts == 1
    assert result.started_at <= result.finished_at


def test_priority_queue_run_after(django_env):
    from datetime import timedelta

    from django.tasks import TaskResultStatus
    from django.utils import timezone
    from djangoproj.shop.jobs import total, with_context

    from potatoq.testing import drain

    app = django_env
    later = total.using(run_after=timezone.now() + timedelta(hours=1)).enqueue(1, [1])
    low = total.using(priority=-10).enqueue(2, [2])
    high = with_context.enqueue(3)  # priority 50
    msg, state = app.broker.peek(later.id)
    assert state == "scheduled" and msg.eta > time.time() + 3000
    drained = drain(app, ["shop"], include_scheduled=False)
    assert [d.id for d in drained] == [high.id, low.id]  # higher priority first
    high.refresh()
    assert high.return_value == {"attempt": 1, "id": high.id, "x": 3}
    assert total.get_result(later.id).status == TaskResultStatus.READY
    app.broker.purge("shop")


def test_async_task(django_env):
    from djangoproj.shop.jobs import async_double

    from potatoq.testing import drain

    result = async_double.enqueue(21)
    drain(django_env, ["shop"])
    result.refresh()
    assert result.return_value == 42


def test_failure_and_potatoq_retry_options(django_env, tmp_path):
    from django.tasks import TaskResultStatus
    from djangoproj.shop.jobs import boom, flaky

    from potatoq.testing import drain

    app = django_env
    failed = boom.enqueue()
    retried = flaky.enqueue(str(tmp_path / "attempts"))
    drain(app, ["shop"])
    failed.refresh()
    assert failed.status == TaskResultStatus.FAILED
    assert failed.errors[0].exception_class is ValueError
    assert "boom" in failed.errors[0].traceback
    with pytest.raises(ValueError, match="Task failed"):
        failed.return_value  # noqa: B018
    assert app.broker.dead_letters()[0]["id"] == failed.id  # dead-lettered like any potatoq task
    retried.refresh()
    assert retried.status == TaskResultStatus.SUCCESSFUL
    assert retried.return_value == 3  # autoretry_for from @task(...) worked


def test_signals_and_validation(django_env):
    from django.tasks import signals
    from django.tasks.exceptions import InvalidTask, TaskResultDoesNotExist
    from djangoproj.shop.jobs import total

    from potatoq.testing import drain

    seen = []

    def record(sender, task_result, signal, **kwargs):
        seen.append((signal, task_result.status))

    for sig in (signals.task_enqueued, signals.task_started, signals.task_finished):
        sig.connect(record)
    try:
        total.enqueue(1, [1])
        drain(django_env, ["shop"])
    finally:
        for sig in (signals.task_enqueued, signals.task_started, signals.task_finished):
            sig.disconnect(record)
    assert [(s, str(st)) for s, st in seen] == [
        (signals.task_enqueued, "READY"),
        (signals.task_started, "RUNNING"),
        (signals.task_finished, "SUCCESSFUL"),
    ]
    with pytest.raises(TypeError, match="total"):
        total.enqueue(1)  # missing argument: caught at enqueue time
    with pytest.raises(InvalidTask):
        total.using(queue_name="nope")
    with pytest.raises(TaskResultDoesNotExist):
        total.get_result("does-not-exist")


def test_transaction_commit_semantics(django_env):
    from django.db import transaction
    from djangoproj.shop.jobs import total

    app = django_env
    app.broker.purge("shop")
    with transaction.atomic():
        total.enqueue(1, [1])
        assert app.broker.queue_sizes() == {}  # written in the transaction, not visible yet
    assert app.broker.queue_sizes() == {"shop": 1}
    with pytest.raises(RuntimeError), transaction.atomic():
        total.enqueue(2, [2])
        raise RuntimeError
    assert app.broker.queue_sizes() == {"shop": 1}
    app.broker.purge("shop")


def test_real_worker_runs_django_tasks(django_env, tmp_path):
    """A `potatoq worker` process finds tasks outside tasks.py and runs them."""
    from django.tasks import TaskResultStatus
    from djangoproj.shop.jobs import total

    env = {**os.environ, "PYTHONPATH": str(TESTS), "DJANGO_SETTINGS_MODULE": "djangoproj.settings"}
    out = open(tmp_path / "worker.log", "w")
    worker = subprocess.Popen(
        [sys.executable, "-m", "potatoq.cli", "worker", "-Q", "shop", "-c", "1", "--threads", "2"],
        env=env, cwd=TESTS, stdout=out, stderr=subprocess.STDOUT,
    )  # fmt: skip
    try:
        results = [total.enqueue(i, [i, i]) for i in range(3)]
        deadline = time.time() + 30
        for result in results:
            while time.time() < deadline:
                result.refresh()
                if result.is_finished:
                    break
                time.sleep(0.1)
        assert [r.status for r in results] == [TaskResultStatus.SUCCESSFUL] * 3, (tmp_path / "worker.log").read_text()
        assert [r.return_value["total"] for r in results] == [0, 2, 4]
    finally:
        worker.terminate()
        worker.wait(30)
        out.close()


@pytest.mark.parametrize("tasks_app", [None, "djangoproj.celery:app"], ids=["default-app", "app-option"])
def test_real_worker_with_a_celery_py_app(django_env, tmp_path, tasks_app):
    """`potatoq -A djangoproj.celery:app worker`, the documented celery.py pattern: the CLI
    sets Django up once the module set DJANGO_SETTINGS_MODULE, so django.tasks (also with
    OPTIONS["APP"] naming that app) and @shared_task tasks both run."""
    from django.tasks import TaskResultStatus
    from djangoproj.shop.jobs import total
    from djangoproj.shop.tasks import audit

    env = {**os.environ, "PYTHONPATH": str(TESTS)}
    env.pop("DJANGO_SETTINGS_MODULE")  # set by djangoproj/celery.py
    if tasks_app:
        env["TEST_DJANGO_TASKS_APP"] = tasks_app
    out = open(tmp_path / "worker.log", "w")
    worker = subprocess.Popen(
        [sys.executable, "-m", "potatoq.cli", "-A", "djangoproj.celery:app", "worker", "-Q", "shop", "-c", "1"],
        env=env, cwd=TESTS, stdout=out, stderr=subprocess.STDOUT,
    )  # fmt: skip
    try:
        result = total.enqueue(5, [1, 2])
        plain = audit.delay("logged")
        deadline = time.time() + 30
        while time.time() < deadline:
            result.refresh()
            if result.is_finished:
                break
            time.sleep(0.1)
        assert result.status == TaskResultStatus.SUCCESSFUL, (tmp_path / "worker.log").read_text()
        assert result.return_value == {"order": 5, "total": 3}
        assert plain.get(timeout=30) == "logged"
    finally:
        worker.terminate()
        worker.wait(30)
        out.close()


def test_a_task_module_failing_to_import_is_dead_lettered(django_env, monkeypatch, caplog):
    """An installed app's module raising on import fails the message, not the worker."""
    from potatoq.contrib.django import tasks as dj_tasks
    from potatoq.message import Message
    from potatoq.worker import executor

    def broken(path):
        raise RuntimeError("import-time failure")

    monkeypatch.setattr(dj_tasks, "import_string", broken)
    message = Message(task="djangoproj.shop.broken.job", args=[], kwargs={}, queue="shop")
    outcome = executor.execute(django_env, message, hostname="test")
    assert (outcome.action, outcome.state) == (executor.DEAD_LETTER, "FAILURE")
    assert outcome.reason == "unregistered task djangoproj.shop.broken.job"
    assert "Could not import djangoproj.shop.broken.job" in caplog.text
    assert "import-time failure" in caplog.text


def test_resolver_before_django_setup_finds_nothing(django_env, monkeypatch):
    """Without django.setup() (AppRegistryNotReady), the message is dead-lettered as unregistered."""
    from django.apps import apps
    from django.tasks import task_backends

    from potatoq.message import Message
    from potatoq.worker import executor

    monkeypatch.setattr(apps, "ready", False)
    assert task_backends["default"]._resolve("djangoproj.shop.jobs.total") is None
    message = Message(task="djangoproj.shop.jobs.not_imported_yet", args=[], kwargs={}, queue="shop")
    outcome = executor.execute(django_env, message, hostname="test")
    assert outcome.action == executor.DEAD_LETTER


@pytest.mark.skipif(django.VERSION < (6, 1), reason="Django 6.1 forwards @task(**options)")
def test_django_61_task_options_reach_potatoq(django_env):
    from djangoproj.shop.jobs import with_options

    ptask = django_env.resolve_task(with_options.module_path)
    assert ptask.time_limit == 60
    assert ptask.ignore_result is False


def test_potatoq_task_decorator_rejects_unknown_options(django_env):
    from potatoq.contrib.django.tasks import task

    with pytest.raises(TypeError, match="nonsense"):
        task(nonsense=1)
