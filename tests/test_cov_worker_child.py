"""Unit tests for forked worker children, driven in-process with a memory broker."""

from __future__ import annotations

import io
import logging
import os
import threading
import time

import pytest

from potatoq import Potatoq, serialization
from potatoq.brokers.base import Delivery
from potatoq.exceptions import SoftTimeLimitExceeded
from potatoq.worker import child as child_mod
from potatoq.worker import executor
from potatoq.worker.child import Child, child_main


@pytest.fixture
def app():
    app = Potatoq("covworkerchild", broker="memory://")
    app.conf.result_backend = "broker"

    @app.task(name="cov.add")
    def add(x, y):
        return x + y

    yield app
    app.close()


@pytest.fixture
def pipe():
    r, w = os.pipe()
    os.set_blocking(r, False)
    yield r, w
    for fd in (r, w):
        try:
            os.close(fd)
        except OSError:
            pass


def make_child(app, write_fd: int, **kw) -> Child:
    options = {"max_tasks": None, "max_memory_kib": None, "threads": 1, **kw}
    return Child(app, "node", ["default"], write_fd, 0, "cov@host", **options)


def events(read_fd: int) -> list[dict]:
    data = b""
    while True:
        try:
            chunk = os.read(read_fd, 65536)
        except BlockingIOError:
            break
        if not chunk:
            break
        data += chunk
    return [serialization.loads(line) for line in data.split(b"\n") if line]


class FakeConsumer:
    def __init__(self, fetch, close_fails: bool = False):
        self._fetch = fetch
        self.close_fails = close_fails
        self.requeued: list[tuple[str, bool]] = []
        self.closed = False

    def fetch(self, timeout: float):
        return self._fetch()

    def requeue(self, delivery, count=False):
        self.requeued.append((delivery.message.id, count))

    def close(self):
        self.closed = True
        if self.close_fails:
            raise ConnectionError("already gone")


# --- memory ----------------------------------------------------------------------------


def test_rss_from_proc_statm(monkeypatch):
    def fake_open(path, *a, **kw):
        assert path == "/proc/self/statm"
        return io.StringIO("1000 250 30 1 0 100 0\n")

    monkeypatch.setattr(child_mod, "open", fake_open, raising=False)
    assert child_mod._rss_kib() == 250 * os.sysconf("SC_PAGE_SIZE") // 1024


@pytest.mark.parametrize("content", [None, "garbage", ""])
def test_rss_falls_back_to_peak_rss(monkeypatch, content):
    def fake_open(path, *a, **kw):
        if content is None:
            raise FileNotFoundError(path)
        return io.StringIO(content)

    monkeypatch.setattr(child_mod, "open", fake_open, raising=False)
    rss = child_mod._rss_kib()
    assert 1024 < rss < 64 * 1024 * 1024  # a Python process: more than 1 MiB, less than 64 GiB


def test_recycles_when_over_the_memory_limit(app, pipe, caplog):
    child = make_child(app, pipe[1], max_memory_kib=1)
    assert child._done_one()
    assert "memory" in caplog.text and "over limit 1 KiB" in caplog.text
    caplog.clear()
    assert child._done_one()  # already stopping: logged once
    assert caplog.text == ""
    assert child.processed == 2


def test_no_recycling_under_the_limits(app, pipe):
    child = make_child(app, pipe[1], max_tasks=3, max_memory_kib=1024**3)
    assert not child._done_one()
    assert not child._done_one()
    assert child._done_one()


# --- signals and reporting ----------------------------------------------------------------


def test_soft_limit_signal_only_raises_inside_a_task_body(app, pipe, monkeypatch):
    child = make_child(app, pipe[1])
    child._on_soft_limit(14, None)  # outside a task: ignored
    monkeypatch.setattr(executor, "in_task_body", lambda: True)
    with pytest.raises(SoftTimeLimitExceeded):
        child._on_soft_limit(14, None)


def test_report_retries_interrupted_and_partial_writes(app, pipe, monkeypatch):
    r, w = pipe
    child = make_child(app, w)
    real_write = os.write
    calls: list[int] = []

    def flaky_write(fd, data):
        calls.append(len(data))
        if len(calls) == 1:
            raise InterruptedError
        return real_write(fd, data[:5])  # short writes

    monkeypatch.setattr(os, "write", flaky_write)
    child._report({"e": "done", "id": "abc"})
    assert len(calls) > 2
    assert events(r) == [{"e": "done", "id": "abc"}]


# --- slot loop ------------------------------------------------------------------------------


def test_single_slot_exits_when_the_supervisor_died(app, pipe, monkeypatch, caplog):
    child = make_child(app, pipe[1])
    child.parent_pid = -1  # our parent is no longer the supervisor that forked us
    consumer = FakeConsumer(lambda: pytest.fail("must not fetch"))
    monkeypatch.setattr(app.broker, "consumer", lambda queues, node: consumer)
    child._slot_loop(child.slots[0])
    assert "Supervisor died; child exiting" in caplog.text
    assert consumer.closed and child.exit_code == 0


def test_threaded_child_exits_when_the_supervisor_died(app, pipe, monkeypatch, caplog):
    child = make_child(app, pipe[1], threads=2)
    child.parent_pid = -1
    seen: list[int] = []

    def slot_loop(slot):
        seen.append(slot.index)
        deadline = time.monotonic() + 10
        while not child.stopping and time.monotonic() < deadline:
            time.sleep(0.01)

    monkeypatch.setattr(child, "_slot_loop", slot_loop)
    child._run_threads()
    assert child.stopping
    assert sorted(seen) == [0, 1]
    assert caplog.text.count("Supervisor died; child exiting") == 1


def test_task_fetched_while_stopping_is_given_back(app, pipe, monkeypatch):
    child = make_child(app, pipe[1])
    delivery = Delivery(app.tasks["cov.add"].build_message([1, 2], {}))

    def fetch():
        child.stopping = True  # SIGTERM arrives while we are claiming
        return delivery

    consumer = FakeConsumer(fetch)
    monkeypatch.setattr(app.broker, "consumer", lambda queues, node: consumer)
    child._slot_loop(child.slots[0])
    assert consumer.requeued == [(delivery.message.id, False)]
    assert consumer.closed
    assert events(pipe[0]) == []  # never started


def test_slot_crash_stops_the_child_with_an_error_code(app, pipe, monkeypatch, caplog):
    child = make_child(app, pipe[1])

    def fetch():
        raise ConnectionError("broker down")

    consumer = FakeConsumer(fetch, close_fails=True)
    monkeypatch.setattr(app.broker, "consumer", lambda queues, node: consumer)
    child._slot_loop(child.slots[0])
    assert "Task slot 1 crashed" in caplog.text
    assert child.exit_code == 1 and child.stopping
    assert consumer.closed and not child.slots[0].fetching


def test_failed_settle_restarts_the_process_without_reporting_done(app, pipe, monkeypatch, caplog):
    child = make_child(app, pipe[1])
    slot = child.slots[0]
    slot.consumer = app.broker.consumer(["default"], "node")
    app.tasks["cov.add"].delay(2, 3)
    delivery = slot.consumer.fetch(timeout=1)

    def settle(*a):
        raise ConnectionError("broker down")

    monkeypatch.setattr(executor, "settle", settle)
    assert child._process(slot, delivery) is False
    assert child.exit_code == 3 and child.stopping
    assert "Failed to settle task cov.add" in caplog.text
    assert [e["e"] for e in events(pipe[0])] == ["start"]  # the supervisor recovers it


def test_process_runs_a_task_and_reports_it(app, pipe):
    child = make_child(app, pipe[1])
    slot = child.slots[0]
    slot.consumer = app.broker.consumer(["default"], "node")
    result = app.tasks["cov.add"].delay(2, 3)
    delivery = slot.consumer.fetch(timeout=1)
    assert child._process(slot, delivery) is True
    assert result.get(timeout=1) == 5
    assert [e["e"] for e in events(pipe[0])] == ["start", "done"]


# --- entry point --------------------------------------------------------------------------


class Exited(Exception):
    def __init__(self, code):
        self.code = code


@pytest.mark.parametrize(
    "outcome, code",
    [(0, 0), (4, 4), (SystemExit(7), 7), (SystemExit("bye"), 1), (RuntimeError("boom"), 1)],
)
def test_child_main_always_exits_with_a_code(app, pipe, monkeypatch, caplog, outcome, code):
    def run(self):
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def fake_exit(code):
        raise Exited(code)

    monkeypatch.setattr(Child, "run", run)
    monkeypatch.setattr(os, "_exit", fake_exit)
    with pytest.raises(Exited) as info:
        child_main(app, node_id="node", queues=["default"], write_fd=pipe[1], index=0, hostname="h",
                   max_tasks=None, max_memory_kib=None)  # fmt: skip
    assert info.value.code == code
    assert ("Worker child crashed" in caplog.text) == isinstance(outcome, RuntimeError)


def test_threaded_abort_injects_worker_terminate(app, pipe, monkeypatch):
    child = make_child(app, pipe[1], threads=2)
    injected: list = []
    started = threading.Barrier(3)

    def slot_loop(slot):
        slot.inject = lambda exc, task_id=None: injected.append((slot.index, exc.__name__))
        started.wait(5)
        deadline = time.monotonic() + 10
        while len(injected) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)

    monkeypatch.setattr(child, "_slot_loop", slot_loop)
    t = threading.Thread(target=child._run_threads)
    t.start()
    started.wait(5)
    child._on_abort(10, None)
    t.join(15)
    assert sorted(injected) == [(0, "WorkerTerminate"), (1, "WorkerTerminate")]
    assert child.stopping and child.aborting


def test_logging_level_for_failed_tasks(app, pipe, caplog):
    @app.task(name="cov.fail")
    def fail():
        raise ValueError("nope")

    child = make_child(app, pipe[1])
    slot = child.slots[0]
    slot.consumer = app.broker.consumer(["default"], "node")
    fail.delay()
    with caplog.at_level(logging.INFO, logger="potatoq.worker"):
        assert child._process(slot, slot.consumer.fetch(timeout=1)) is True
    assert any(r.levelno == logging.WARNING and "cov.fail" in r.getMessage() for r in caplog.records)
