"""Unit tests for the worker supervisor: driven in-process, without forking real children."""

from __future__ import annotations

import errno
import io
import logging
import os
import selectors
import signal
import time

import pytest

from potatoq import Potatoq, serialization
from potatoq.brokers.base import Delivery
from potatoq.message import Message
from potatoq.worker import supervisor as sup_mod
from potatoq.worker.supervisor import ChildProc, Running, Supervisor, default_concurrency, parse_memory

FAKE_PID = 999_999_001  # never signalled: every test that uses it fakes _kill


class FakeConsumer:
    def __init__(self, can_settle_foreign: bool = True, fail: bool = False):
        self.can_settle_foreign = can_settle_foreign
        self.fail = fail
        self.requeued: list[tuple[str, bool]] = []

    def requeue(self, delivery: Delivery, count: bool = False) -> None:
        if self.fail:
            raise ConnectionError("broker down")
        self.requeued.append((delivery.message.id, count))


@pytest.fixture
def app():
    app = Potatoq("covworker", broker="memory://")
    app.conf.result_backend = "broker"
    app.conf.worker_heartbeat_interval = 3600

    @app.task(name="cov.boom")
    def boom():
        return None

    @app.task(name="cov.errback")
    def errback(task_id):
        return task_id

    yield app
    app.close()


@pytest.fixture
def sup(app):
    s = Supervisor(app, concurrency=2, hostname="cov@host")
    s.consumer = FakeConsumer()
    yield s
    for fd in (s._wake_r, s._wake_w):
        try:
            os.close(fd)
        except OSError:
            pass
    s.selector.close()


@pytest.fixture
def kills(sup, monkeypatch):
    sent: list[tuple[int, int]] = []
    monkeypatch.setattr(sup, "_kill", lambda child, sig: sent.append((child.pid, sig)))
    return sent


@pytest.fixture
def spawned(sup, monkeypatch):
    indexes: list[int] = []
    monkeypatch.setattr(sup, "_spawn", indexes.append)
    return indexes


def message(app, *, errback: bool = False, **kw) -> Message:
    task = app.tasks["cov.boom"]
    link_error = app.tasks["cov.errback"].s() if errback else None
    return task.build_message([], {}, link_error=link_error, **kw)


def child_with(sup, *deliveries: Delivery, **kw) -> ChildProc:
    child = ChildProc(pid=FAKE_PID, index=1, read_fd=-1, **kw)
    for d in deliveries:
        child.running[d.message.id] = Running(delivery=d, started=time.time() - 3, hard_deadline=None)
    return child


# --- module helpers ------------------------------------------------------------------


def fake_cgroup(content: str | None):
    def opener(path, *a, **kw):
        assert path == "/sys/fs/cgroup/cpu.max"
        if content is None:
            raise FileNotFoundError(path)
        return io.StringIO(content)

    return opener


@pytest.mark.parametrize(
    "cgroup, expected",
    [(None, 8), ("max 100000\n", 8), ("200000 100000\n", 2), ("50000 100000\n", 1), ("garbage\n", 8)],
)
def test_default_concurrency_honours_cgroup_quota(monkeypatch, cgroup, expected):
    monkeypatch.setattr(os, "process_cpu_count", lambda: 8, raising=False)
    monkeypatch.setattr(sup_mod, "open", fake_cgroup(cgroup), raising=False)
    assert default_concurrency() == expected


def test_default_concurrency_falls_back_to_affinity_then_cpu_count(monkeypatch):
    monkeypatch.setattr(sup_mod, "open", fake_cgroup(None), raising=False)
    monkeypatch.delattr(os, "process_cpu_count", raising=False)
    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: {0, 1, 2}, raising=False)
    assert default_concurrency() == 3
    monkeypatch.delattr(os, "sched_getaffinity")
    monkeypatch.setattr(os, "cpu_count", lambda: None)
    assert default_concurrency() == 1
    monkeypatch.setattr(os, "cpu_count", lambda: 6)
    assert default_concurrency() == 6


@pytest.mark.parametrize(
    "value, kib",
    [
        (None, None),
        ("", None),
        (0, None),
        (262144, 262144),
        (1.9, 1),
        ("512MB", 512 * 1024),
        ("512mib", 512 * 1024),
        ("2GiB", 2 * 1024**2),
        ("1.5g", int(1.5 * 1024**2)),
        ("300K", 300),
        ("64kb", 64),
        (" 2048 ", 2048),
    ],
)
def test_parse_memory_units(value, kib):
    assert parse_memory(value) == kib


def test_parse_memory_rejects_garbage():
    with pytest.raises(ValueError):
        parse_memory("lots")


def test_redact_hides_passwords():
    assert sup_mod._redact("redis://user:secret@host:6379/0") == "redis://user:***@host:6379/0"
    assert sup_mod._redact("amqp://guest@rabbit//") == "amqp://guest@rabbit//"
    assert sup_mod._redact("postgresql://u:p@ss@db/x") == "postgresql://u:***@db/x"
    assert sup_mod._redact("memory://") == "memory://"
    assert sup_mod._redact("sqlite:///tmp/a@b.db") == "sqlite:///tmp/a@b.db"


def test_to_handle_turns_json_lists_back_into_tuples():
    assert sup_mod._to_handle(["job", 2]) == ("job", 2)
    assert sup_mod._to_handle("tag") == "tag"


# --- construction and banner --------------------------------------------------------------


def test_queues_given_as_comma_separated_string(app):
    s = Supervisor(app, queues="a, b,,c ", concurrency=1, max_memory_per_child="1MB", threads=3)
    try:
        assert s.queues == ["a", "b", "c"]
        assert s.hostname.startswith("potatoq@")
        assert s.node_id.startswith(f"{s.hostname}:{os.getpid()}:")
        assert s.max_memory_kib == 1024
        assert s.threads == 3
    finally:
        os.close(s._wake_r)
        os.close(s._wake_w)
        s.selector.close()


def test_banner_warns_without_result_backend_on_foreign_settling_brokers(app, sup, caplog):
    app.conf.result_backend = "disabled"
    app.conf.broker_url = "memory://guest:secret@localhost"
    app._reset_connections()
    sup.consumer = FakeConsumer(can_settle_foreign=False)
    sup.threads = 4
    with caplog.at_level(logging.INFO, logger="potatoq.worker"):
        sup._banner()
    text = caplog.text
    assert "memory://guest:***@localhost" in text and "secret" not in text
    assert "results=disabled" in text
    assert "concurrency=8 (2 processes x 4 threads)" in text
    assert "requeues the other 3 task(s)" in text
    assert "No result backend" in text
    assert "Registered tasks: cov.boom, cov.errback" in text


def test_banner_says_when_results_are_off_by_default(app, sup, caplog):
    app.conf.task_ignore_result = True
    with caplog.at_level(logging.INFO, logger="potatoq.worker"):
        sup._banner()
    assert "results=off by default (task_ignore_result; " in caplog.text


def test_banner_without_tasks(sup, caplog):
    sup.app.tasks.clear()
    with caplog.at_level(logging.INFO, logger="potatoq.worker"):
        sup._banner()
    assert "Registered tasks: (none)" in caplog.text
    assert "No result backend" not in caplog.text
    assert "(prefork)" in caplog.text


# --- signals ---------------------------------------------------------------------------


def test_sigint_twice_is_a_cold_shutdown(sup, kills):
    sup.children[FAKE_PID] = child_with(sup)
    sup._on_int(signal.SIGINT, None)
    assert sup.shutting_down and not sup.cold
    assert kills == [(FAKE_PID, signal.SIGTERM)]
    assert sup.children[FAKE_PID].term_sent
    warm_deadline = sup.shutdown_deadline
    assert warm_deadline is not None and warm_deadline > time.monotonic() + sup.shutdown_timeout - 5

    sup._on_int(signal.SIGINT, None)
    assert sup.cold
    assert sup.shutdown_deadline is not None and sup.shutdown_deadline <= time.monotonic()
    assert kills == [(FAKE_PID, signal.SIGTERM)]  # SIGTERM only sent once


def test_sigquit_goes_cold_immediately_and_sigterm_is_warm(app, sup, kills):
    sup._on_term(signal.SIGTERM, None)
    assert sup.shutting_down and not sup.cold
    sup._on_term(signal.SIGTERM, None)
    assert not sup.cold

    other = Supervisor(app, concurrency=1)
    try:
        other._on_cold(signal.SIGQUIT, None)
        assert other.shutting_down and other.cold
        deadline = other.shutdown_deadline
        other._on_cold(signal.SIGQUIT, None)
        assert other.shutdown_deadline == deadline
    finally:
        os.close(other._wake_r)
        os.close(other._wake_w)
        other.selector.close()


# --- children ----------------------------------------------------------------------------


def test_spawn_in_the_forked_child_closes_supervisor_fds_and_runs_child_main(sup, monkeypatch):
    calls: list[dict] = []

    class Exited(Exception):
        pass

    def fake_child_main(app, **kwargs):
        calls.append({"app": app, **kwargs})
        raise Exited  # the real child_main never returns

    monkeypatch.setattr(os, "fork", lambda: 0)
    monkeypatch.setattr(sup_mod, "child_main", fake_child_main)
    sup.max_tasks_per_child = 7
    with pytest.raises(Exited):
        sup._spawn(1)
    try:
        (call,) = calls
        assert call["app"] is sup.app
        assert call["index"] == 1 and call["node_id"] == sup.node_id and call["queues"] == sup.queues
        assert call["max_tasks"] == 7 and call["threads"] == 1 and call["hostname"] == "cov@host"
        # The supervisor's wakeup pipe is not inherited by the child.
        for fd in (sup._wake_r, sup._wake_w):
            with pytest.raises(OSError):
                os.fstat(fd)
        assert signal.set_wakeup_fd(-1) == -1
        with pytest.raises(BrokenPipeError):  # the child closed the supervisor's (read) end
            os.write(call["write_fd"], b"x")
    finally:
        os.close(calls[0]["write_fd"])
        sup._wake_r = sup._wake_w = -1  # already closed: keep the fixture from closing reused fds
    assert sup.children == {}


def test_kill_ignores_already_gone_processes(sup, monkeypatch):
    def gone(pid, sig):
        raise ProcessLookupError(pid)

    monkeypatch.setattr(os, "kill", gone)
    sup._kill(child_with(sup), signal.SIGKILL)  # no exception


def test_read_child_parses_events_and_skips_blank_lines(app, sup):
    r, w = os.pipe()
    os.set_blocking(r, False)
    child = ChildProc(pid=FAKE_PID, index=0, read_fd=r)
    m1, m2 = message(app), message(app)
    now = time.time()
    lines = [
        {"e": "start", "message": m1.to_dict(), "count": 2, "handle": ["job", 2], "hard": now + 10, "started": now},
        {"e": "start", "message": m2.to_dict(), "count": 1, "handle": "h", "hard": None, "started": now},
        {"e": "done", "id": m2.id},
        {"e": "done", "id": "never-started"},
    ]
    data = b"\n".join(serialization.dumps(e).encode() for e in lines)
    try:
        os.write(w, b"\n\n" + data + b"\n\n" + b'{"e": "do')
        sup._read_child(child)
        assert list(child.running) == [m1.id]
        running = child.running[m1.id]
        assert running.delivery.handle == ("job", 2) and running.delivery.delivery_count == 2
        assert running.hard_deadline is not None
        assert 10 < running.hard_deadline - time.monotonic() < 11.5  # hard limit plus a little grace
        assert child.buffer == b'{"e": "do'
        os.write(w, b'ne", "id": "%s"}\n' % m1.id.encode())
        os.close(w)
        sup._read_child(child)  # EOF
        assert child.running == {} and child.buffer == b""
    finally:
        os.close(r)


def test_read_child_retries_on_eintr_and_stops_on_other_errors(app, sup, monkeypatch):
    r, w = os.pipe()
    os.set_blocking(r, False)
    child = ChildProc(pid=FAKE_PID, index=0, read_fd=r)
    real_read = os.read
    errors = [OSError(errno.EINTR, "interrupted")]

    def flaky_read(fd, n):
        if errors:
            raise errors.pop(0)
        return real_read(fd, n)

    monkeypatch.setattr(os, "read", flaky_read)
    m = message(app)
    try:
        os.write(w, serialization.dumps({"e": "start", "message": m.to_dict(), "count": 1, "handle": None,
                                         "started": time.time()}).encode() + b"\n")  # fmt: skip
        sup._read_child(child)
        assert list(child.running) == [m.id]

        errors.append(OSError(errno.EIO, "io error"))
        os.write(w, serialization.dumps({"e": "done", "id": m.id}).encode() + b"\n")
        sup._read_child(child)  # gives up on EIO without reading
        assert list(child.running) == [m.id]
        sup._read_child(child)
        assert child.running == {}
    finally:
        os.close(r)
        os.close(w)


def test_reap_ignores_processes_that_are_not_our_children(sup, monkeypatch):
    statuses = [(FAKE_PID + 1, 0), (0, 0)]
    monkeypatch.setattr(os, "waitpid", lambda pid, flags: statuses.pop(0))
    exited: list = []
    monkeypatch.setattr(sup, "_on_child_exit", lambda child, status: exited.append(child))
    sup._reap()
    assert statuses == [] and exited == []


def test_reap_handles_an_exited_child(app, sup, monkeypatch):
    r, w = os.pipe()
    os.set_blocking(r, False)
    child = ChildProc(pid=FAKE_PID, index=0, read_fd=r)
    sup.children[FAKE_PID] = child
    sup.selector.register(r, selectors.EVENT_READ, child)
    m = message(app)
    os.write(w, serialization.dumps({"e": "start", "message": m.to_dict(), "count": 1, "handle": None,
                                     "started": time.time()}).encode() + b"\n")  # fmt: skip
    os.close(w)
    statuses = [(FAKE_PID, 9), (0, 0)]  # killed by SIGKILL
    monkeypatch.setattr(os, "waitpid", lambda pid, flags: statuses.pop(0))
    exited: list = []
    monkeypatch.setattr(sup, "_on_child_exit", lambda child, status: exited.append((child, status)))
    sup._reap()
    assert exited == [(child, 9)]
    assert list(child.running) == [m.id]  # final events were read before the pipe was closed
    assert sup.children == {}
    with pytest.raises(OSError):
        os.fstat(r)


# --- dead children --------------------------------------------------------------------------


def test_child_exit_survives_broker_errors_while_looking_up_lost_tasks(app, sup, spawned, monkeypatch, caplog):
    delivery = Delivery(message(app), delivery_count=1)

    def boom(*a):
        raise ConnectionError("broker down")

    monkeypatch.setattr(app.broker, "lost_deliveries", boom)
    sup._on_child_exit(child_with(sup, delivery), 1 << 8)
    assert "Could not look up tasks of dead child" in caplog.text
    assert "exited unexpectedly (code 1)" in caplog.text
    assert sup.consumer.requeued == [(delivery.message.id, True)]
    assert spawned == [1]
    assert len(sup._recent_crashes) == 1


def test_child_exit_merges_tasks_the_broker_knows_about(app, sup, spawned, monkeypatch):
    known = Delivery(message(app), delivery_count=1)
    other = Delivery(message(app), delivery_count=2)
    monkeypatch.setattr(app.broker, "lost_deliveries", lambda node, pid: [known, other])
    sup._on_child_exit(child_with(sup, known), 1 << 8)
    assert sup.consumer.requeued == [(known.message.id, True), (other.message.id, True)]


def test_child_exit_survives_broker_errors_while_recovering(app, sup, spawned, caplog):
    sup.consumer = FakeConsumer(fail=True)
    delivery = Delivery(message(app), delivery_count=1)
    sup._on_child_exit(child_with(sup, delivery), 1 << 8)
    assert f"Could not recover task {delivery.message.id} of dead child" in caplog.text
    assert spawned == [1]


def test_children_that_keep_crashing_back_off(sup, spawned, monkeypatch, caplog):
    slept: list[float] = []
    monkeypatch.setattr(time, "sleep", slept.append)
    for _ in range(3 * sup.concurrency + 1):
        sup._on_child_exit(child_with(sup), 1 << 8)
    assert slept == [1]
    assert "Children keep crashing; backing off" in caplog.text
    assert sup._recent_crashes == []
    assert spawned == [1] * (3 * sup.concurrency + 1)


def test_no_respawn_while_shutting_down(sup, spawned, caplog):
    sup.shutting_down = True
    child = child_with(sup, abort_sent=True)
    sup._on_child_exit(child, 1 << 8)  # interrupted by us: not a crash
    assert spawned == [] and sup._recent_crashes == []
    assert "exited unexpectedly" not in caplog.text


# --- _handle_lost without foreign settling (RabbitMQ) -------------------------------------


def test_timed_out_task_is_recorded_as_failed_when_the_broker_redelivers(app, sup, monkeypatch, caplog):
    sup.consumer = FakeConsumer(can_settle_foreign=False)
    published: list[Message] = []
    monkeypatch.setattr(app, "publish_now", published.extend)
    delivery = Delivery(message(app, errback=True))
    child = child_with(sup, delivery)
    child.timed_out.add(delivery.message.id)
    child.killed_for_timeout = True
    sup._handle_lost(child, delivery, -9)
    result = app.backend.get_result(delivery.message.id)
    assert result.state == "FAILURE"
    assert "exceeded its time limit (3s) and was killed" in caplog.text
    assert [m.task for m in published] == ["cov.errback"]
    assert published[0].args == [delivery.message.id]
    assert sup.consumer.requeued == []  # RabbitMQ redelivers by itself


def test_timed_out_task_without_running_entry(app, sup, monkeypatch, caplog):
    sup.consumer = FakeConsumer(can_settle_foreign=False)
    published: list[Message] = []
    monkeypatch.setattr(app, "publish_now", published.extend)
    delivery = Delivery(message(app, ignore_result=True))
    child = child_with(sup)
    child.timed_out.add(delivery.message.id)
    sup._handle_lost(child, delivery, -9)
    assert "time limit exceeded" in caplog.text
    assert app.backend.get_result(delivery.message.id) is None  # ignore_result: nothing stored
    assert published == []


def test_dead_lettering_without_foreign_settling_and_without_backend(app, sup, caplog):
    app.conf.result_backend = "disabled"
    app._reset_connections()
    sup.consumer = FakeConsumer(can_settle_foreign=False)
    delivery = Delivery(message(app), delivery_count=5)
    sup._handle_lost(child_with(sup, delivery), delivery, 1)
    assert "Worker exited prematurely (code 1)" in caplog.text and "dead-lettering" in caplog.text
    assert sup.consumer.requeued == []


@pytest.mark.parametrize("foreign", [True, False])
def test_interrupted_tasks_are_requeued_without_penalty(app, sup, foreign, caplog):
    sup.consumer = FakeConsumer(can_settle_foreign=foreign)
    sup.shutting_down = True
    delivery = Delivery(message(app))
    child = child_with(sup, delivery, abort_sent=True)
    sup._handle_lost(child, delivery, -10)
    assert "interrupted by shutdown" in caplog.text
    assert sup.consumer.requeued == ([(delivery.message.id, False)] if foreign else [])

    sup.shutting_down = False
    bystander = child_with(sup, delivery, killed_for_timeout=True)
    sup._handle_lost(bystander, delivery, -9)
    assert "interrupted by another task's time limit" in caplog.text


def test_crashed_task_is_left_to_the_broker_when_it_cannot_settle_foreign(app, sup, caplog):
    sup.consumer = FakeConsumer(can_settle_foreign=False)
    delivery = Delivery(message(app), delivery_count=1)
    sup._handle_lost(child_with(sup, delivery), delivery, 1)
    assert "requeueing (delivery 1 of 5)" in caplog.text
    assert sup.consumer.requeued == []


# --- main loop --------------------------------------------------------------------------


def test_loop_recovers_dead_nodes_and_runs_maintenance(app, sup, monkeypatch):
    monkeypatch.setattr(sup_mod.random, "uniform", lambda a, b: 0.0)  # every timer is due right away
    dead = Delivery(message(app, errback=True), delivery_count=2)
    quiet = Delivery(message(app, ignore_result=True))
    recovered = [[dead, quiet], []]
    calls: list[str] = []
    published: list[Message] = []
    monkeypatch.setattr(app, "publish_now", published.extend)

    def recover(dead_after):
        calls.append("recover")
        assert dead_after == 60.0
        result = recovered.pop(0)
        if not result:
            sup.shutting_down = True  # second round: stop
        return result

    monkeypatch.setattr(app.broker, "recover", recover)
    monkeypatch.setattr(app.broker, "maintenance", lambda: calls.append("maintenance"))
    monkeypatch.setattr(sup, "_reap", lambda: None)
    sup._loop()
    assert calls == ["recover", "maintenance", "recover", "maintenance"]
    record = app.backend.get_result(dead.message.id)
    assert record.state == "FAILURE"
    assert "Worker node died while running cov.boom" in record.traceback
    assert app.backend.get_result(quiet.message.id) is None
    assert [m.task for m in published] == ["cov.errback"]


def test_loop_survives_errors(app, sup, monkeypatch, caplog):
    monkeypatch.setattr(sup_mod.random, "uniform", lambda a, b: 0.0)
    reaps: list[int] = []

    def reap():
        reaps.append(1)
        if len(reaps) == 1:
            raise RuntimeError("reap failed")

    def maintenance():
        if len(reaps) >= 2:
            sup.shutting_down = True
        raise RuntimeError("maintenance failed")

    monkeypatch.setattr(sup, "_reap", reap)
    monkeypatch.setattr(app.broker, "maintenance", maintenance)
    sup._loop()
    assert "Error while handling exited children" in caplog.text
    assert "Supervisor housekeeping failed (will retry)" in caplog.text
    assert len(reaps) == 2


def test_shutdown_step_aborts_then_kills(sup, kills):
    child = child_with(sup)
    sup.children[FAKE_PID] = child
    sup._shutdown_step(time.monotonic())  # no deadline yet
    sup.shutdown_deadline = time.monotonic() + 60
    sup._shutdown_step(time.monotonic())  # deadline not reached
    assert kills == []
    deadline = sup.shutdown_deadline = time.monotonic()
    sup._shutdown_step(deadline)
    sup._shutdown_step(deadline + 1)
    assert kills == [(FAKE_PID, signal.SIGUSR1)] and child.abort_sent
    sup._shutdown_step(deadline + 5)
    assert kills == [(FAKE_PID, signal.SIGUSR1), (FAKE_PID, signal.SIGKILL)]


def test_finish_kills_and_reaps_children_and_survives_unregister_errors(app, sup, kills, monkeypatch, caplog):
    sup.children[FAKE_PID] = child_with(sup)
    reaps: list[int] = []

    def reap():
        reaps.append(1)
        if len(reaps) == 2:
            sup.children.clear()

    def unregister(node_id):
        raise ConnectionError("broker down")

    monkeypatch.setattr(sup, "_reap", reap)
    monkeypatch.setattr(app.broker, "unregister", unregister)
    with caplog.at_level(logging.INFO, logger="potatoq.worker"):
        sup._finish()
    assert sup.shutting_down
    assert kills == [(FAKE_PID, signal.SIGKILL)]
    assert len(reaps) == 2
    assert "Could not unregister worker" in caplog.text
    assert "Worker cov@host stopped" in caplog.text
