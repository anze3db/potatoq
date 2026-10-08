"""Real worker processes against every broker."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from conftest import make_app

TESTS = Path(__file__).parent
WORKER_BROKERS = ["sqlite", "postgres", "redis", "rabbitmq"]


class Worker:
    def __init__(self, app, tmp_path, *args: str):
        self.log = tmp_path / f"events-{time.monotonic_ns()}.jsonl"
        env = {
            **os.environ,
            "TEST_BROKER": app.conf.broker_url,
            "TEST_CONF": json.dumps({k: v for k, v in app.conf.changed().items() if k != "broker_url"}),
            "TEST_LOG": str(tmp_path / "events.jsonl"),
            "PYTHONPATH": str(TESTS),
        }
        env.pop("DJANGO_SETTINGS_MODULE", None)  # left by the Django tests; workerapp isn't Django
        self.out = open(tmp_path / f"worker-{time.monotonic_ns()}.log", "w")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "potatoq.cli", "-A", "workerapp", "worker", "-l", "info", *args],
            env=env, cwd=TESTS, stdout=self.out, stderr=subprocess.STDOUT,
        )  # fmt: skip
        self.logfile = Path(self.out.name)

    def stop(self, sig=signal.SIGTERM, timeout=30) -> int:
        if self.proc.poll() is None:
            self.proc.send_signal(sig)
        try:
            return self.proc.wait(timeout=timeout)
        finally:
            self.out.close()

    def output(self) -> str:
        return self.logfile.read_text()


def events(tmp_path, name=None):
    path = tmp_path / "events.jsonl"
    if not path.exists():
        return []
    rows = [json.loads(line) for line in path.read_text().splitlines() if line]
    return [r for r in rows if name is None or r["event"] == name]


@pytest.fixture(params=WORKER_BROKERS)
def wapp(request, tmp_path):
    app, cleanup = make_app(request.param, tmp_path)
    app.kind = request.param
    sys.path.insert(0, str(TESTS))
    import workerapp

    # Producer side: same tasks, same configuration as the worker.
    workerapp.app.conf.update(app.conf.changed())
    workerapp.app.conf.broker_url = app.conf.broker_url
    workerapp.app._reset_connections()
    workerapp.app.set_current()
    workerapp.app.kind = request.param
    workerapp.app.test_token = app.test_token
    try:
        yield workerapp.app
    finally:
        workerapp.app.close()
        app.close()
        cleanup()


def test_runs_tasks_and_stores_results(wapp, tmp_path):
    import workerapp

    w = Worker(wapp, tmp_path, "-c", "2")
    try:
        results = [workerapp.add.delay(i, i) for i in range(10)]
        assert [r.get(timeout=20) for r in results] == [2 * i for i in range(10)]
    finally:
        assert w.stop() == 0, w.output()


def test_hard_time_limit_kills_and_fails(wapp, tmp_path):
    import workerapp

    from potatoq.exceptions import TimeLimitExceeded

    w = Worker(wapp, tmp_path, "-c", "1")
    try:
        result = workerapp.hang.delay()
        with pytest.raises(TimeLimitExceeded):
            result.get(timeout=20)
        # The worker replaced the killed process and keeps working.
        assert workerapp.add.delay(1, 2).get(timeout=20) == 3
    finally:
        w.stop()
    assert "Hard time limit exceeded" in w.output()


def test_crashing_task_is_dead_lettered_after_max_deliveries(wapp, tmp_path):
    import workerapp

    from potatoq.exceptions import WorkerLostError

    wapp.conf.task_max_deliveries = 3
    w = Worker(wapp, tmp_path, "-c", "1")
    try:
        result = workerapp.crash.delay()
        with pytest.raises(WorkerLostError):
            result.get(timeout=30)
        assert workerapp.add.delay(2, 2).get(timeout=20) == 4
    finally:
        w.stop()
    assert len(events(tmp_path, "crash")) == 3


def test_graceful_shutdown_finishes_running_task(wapp, tmp_path):
    import workerapp

    w = Worker(wapp, tmp_path, "-c", "1", "--shutdown-timeout", "10")
    result = workerapp.sleep.delay(1.5, tag="finish-me")
    deadline = time.time() + 20
    while not events(tmp_path, "sleep-start") and time.time() < deadline:
        time.sleep(0.05)
    assert w.stop() == 0
    assert [e["tag"] for e in events(tmp_path, "sleep-end")] == ["finish-me"]
    assert result.get(timeout=5) == "finish-me"


def test_shutdown_timeout_requeues_running_task(wapp, tmp_path):
    import workerapp

    w = Worker(wapp, tmp_path, "-c", "1", "--shutdown-timeout", "0.5")
    result = workerapp.sleep.delay(4, tag="interrupted")
    deadline = time.time() + 20
    while not events(tmp_path, "sleep-start") and time.time() < deadline:
        time.sleep(0.05)
    w.stop()
    assert events(tmp_path, "sleep-end") == []
    # A new worker picks the interrupted task up and finishes it.
    w2 = Worker(wapp, tmp_path, "-c", "1")
    try:
        assert result.get(timeout=30) == "interrupted"
    finally:
        w2.stop()


def test_max_tasks_per_child_recycles(wapp, tmp_path):
    import workerapp

    w = Worker(wapp, tmp_path, "-c", "1", "--max-tasks-per-child", "2")
    try:
        pids = [workerapp.pid.delay().get(timeout=20) for _ in range(5)]
    finally:
        w.stop()
    assert len(set(pids)) == 3


def test_scheduler_runs_once_across_workers(wapp, tmp_path):
    wapp.conf.beat_schedule = {"tick": {"task": "workerapp.tick", "schedule": 1.0}}
    w1 = Worker(wapp, tmp_path, "-c", "1")
    w2 = Worker(wapp, tmp_path, "-c", "1")
    time.sleep(6)
    w1.stop()
    w2.stop()
    ticks = events(tmp_path, "tick")
    seconds = [round(t["at"]) for t in ticks]
    assert len(ticks) >= 3, w1.output()
    assert len(seconds) - len(set(seconds)) <= 1, seconds  # no double scheduling


def test_dead_lettered_crash_keeps_argument_types(wapp, tmp_path):
    import datetime as dt

    import workerapp

    from potatoq.exceptions import WorkerLostError

    if wapp.kind == "rabbitmq":
        pytest.skip("RabbitMQ dead-letters through the broker's delivery limit")
    wapp.conf.task_max_deliveries = 1
    when = dt.datetime(2026, 1, 2, 3, 4, tzinfo=dt.UTC)
    w = Worker(wapp, tmp_path, "-c", "1")
    try:
        result = workerapp.crash_with.delay(when)
        with pytest.raises(WorkerLostError):
            result.get(timeout=30)
    finally:
        w.stop()
    [entry] = wapp.broker.dead_letters()
    assert entry["message"]["args"] == [when]
