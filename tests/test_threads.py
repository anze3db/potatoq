"""Threads inside worker processes (``--threads``), including time-limit semantics."""

from __future__ import annotations

import ctypes
import threading
import time

import pytest
from test_worker import Worker, events, wapp  # noqa: F401  (fixture)

from potatoq import Potatoq, states
from potatoq.exceptions import SoftTimeLimitExceeded
from potatoq.worker import executor
from potatoq.worker.child import Slot, _SetAsyncExc


def test_slot_injects_only_inside_the_task_body():
    slot = Slot(0)
    started = threading.Event()
    outcome: dict = {}

    def spin():
        started.set()
        try:
            while True:
                pass
        except SoftTimeLimitExceeded:
            outcome["raised"] = True

    def body():
        slot.thread_id = threading.get_ident()
        assert not slot.inject(SoftTimeLimitExceeded)  # not in a task yet: refused
        slot.run(spin)
        # An injection racing with the end of the body is cleared, never raised out here.
        time.sleep(0.05)
        outcome["after"] = "clean"

    t = threading.Thread(target=body)
    t.start()
    started.wait(5)
    assert slot.inject(SoftTimeLimitExceeded)
    t.join(5)
    assert outcome == {"raised": True, "after": "clean"}
    assert not slot.inject(SoftTimeLimitExceeded)


def test_injection_racing_with_the_body_returning_is_dropped():
    # inject() saw the slot in the body just as the task returned: the exception is
    # raised while the slot thread clears pending injections, right after it gets the
    # lock (3.14+) or under it. It must not fail the finished task, escape into the
    # executor, or leave the lock held (which deadlocked the slot's next task).
    app = Potatoq("threads-race", broker="memory://", set_as_current=False)
    slot = Slot(0)
    returning, go = threading.Event(), threading.Event()
    outcomes: list = []

    @app.task(name="race.finish")
    def finish():
        returning.set()
        go.wait(5)
        return "done"

    def slot_thread():
        slot.thread_id = threading.get_ident()
        executor.set_body_guard(slot)
        for _ in range(2):  # the next task in the same slot
            outcomes.append(executor.execute(app, finish.build_message([], {}), hostname="test"))

    t = threading.Thread(target=slot_thread, daemon=True)
    t.start()
    assert returning.wait(5)
    with slot.lock:  # what inject() does, having just seen in_body set
        go.set()
        time.sleep(0.2)  # the task returns; its slot thread now waits for the lock
        assert _SetAsyncExc(ctypes.c_ulong(slot.thread_id), ctypes.py_object(SoftTimeLimitExceeded)) == 1
    t.join(5)
    assert not t.is_alive(), "the slot deadlocked on its next task"
    assert [(o.state, o.retval) for o in outcomes] == [(states.SUCCESS, "done")] * 2
    assert not slot.in_body
    assert slot.lock.acquire(timeout=1)
    slot.lock.release()
    assert not slot.inject(SoftTimeLimitExceeded)
    app.close()


def test_eager_apply_inside_a_task_keeps_the_outer_task_interruptible():
    app = Potatoq("threads-nested", broker="memory://", set_as_current=False)
    slot = Slot(0)
    started = threading.Event()
    seen: list = []

    @app.task(name="nested.inner")
    def inner():
        return 1

    @app.task(name="nested.outer")
    def outer(spin):
        inner.apply()
        seen.append((executor.in_task_body(), slot.in_body))
        if spin:
            started.set()
            try:
                while True:
                    time.sleep(0.01)
            except SoftTimeLimitExceeded:
                return "interrupted"

    outer.apply((False,))  # --threads 1: the soft limit is a signal, gated by in_task_body()
    assert seen == [(True, False)] and not executor.in_task_body()

    def slot_thread():
        slot.thread_id = threading.get_ident()
        executor.set_body_guard(slot)
        seen.append(executor.execute(app, outer.build_message([True], {}), hostname="test").retval)

    t = threading.Thread(target=slot_thread, daemon=True)
    t.start()
    assert started.wait(5)
    assert slot.inject(SoftTimeLimitExceeded)
    t.join(5)
    assert seen[1:] == [(True, True), "interrupted"]
    assert not slot.in_body
    app.close()


def test_threads_run_tasks_concurrently(wapp, tmp_path):  # noqa: F811
    import workerapp

    w = Worker(wapp, tmp_path, "-c", "1", "--threads", "4")
    try:
        results = [workerapp.nap.delay(1.0, tag=i) for i in range(4)]
        assert sorted(r.get(timeout=30) for r in results) == [0, 1, 2, 3]
    finally:
        w.stop()
    starts = sorted(e["at"] for e in events(tmp_path, "nap-start"))
    ends = sorted(e["at"] for e in events(tmp_path, "nap-end"))
    assert len({e["pid"] for e in events(tmp_path, "nap-start")}) == 1  # one process
    assert starts[-1] < ends[0], "the four tasks should overlap"
    assert "4 = 1 process × 4 threads" in w.output()  # noqa: RUF001


def test_threads_soft_time_limit_interrupts_python_code(wapp, tmp_path):  # noqa: F811
    import workerapp

    w = Worker(wapp, tmp_path, "-c", "1", "--threads", "2")
    try:
        assert workerapp.spin_soft.delay().get(timeout=20) == "cleaned up"
    finally:
        w.stop()


def test_threads_hard_limit_fails_offender_and_requeues_bystander(wapp, tmp_path):  # noqa: F811
    import workerapp

    from potatoq.exceptions import TimeLimitExceeded

    w = Worker(wapp, tmp_path, "-c", "1", "--threads", "2")
    try:
        bystander = workerapp.nap.delay(4.0, tag="bystander")
        deadline = time.time() + 20
        while not events(tmp_path, "nap-start") and time.time() < deadline:
            time.sleep(0.05)
        offender = workerapp.spin_forever.delay()
        with pytest.raises(TimeLimitExceeded):
            offender.get(timeout=30)
        # The bystander was killed with the process, requeued without penalty, and
        # finishes in the replacement process.
        assert bystander.get(timeout=30) == "bystander"
    finally:
        w.stop()
    starts = [e for e in events(tmp_path, "nap-start") if e["tag"] == "bystander"]
    assert len(starts) == 2 and starts[0]["pid"] != starts[1]["pid"]
    assert "and requeueing its 1 other task(s)" in w.output()


def test_threads_shutdown_timeout_requeues(wapp, tmp_path):  # noqa: F811
    import workerapp

    w = Worker(wapp, tmp_path, "-c", "1", "--threads", "2", "--shutdown-timeout", "0.5")
    results = [workerapp.nap.delay(5.0, tag=f"t{i}") for i in range(2)]
    deadline = time.time() + 20
    while len(events(tmp_path, "nap-start")) < 2 and time.time() < deadline:
        time.sleep(0.05)
    w.stop()
    assert events(tmp_path, "nap-end") == []
    w2 = Worker(wapp, tmp_path, "-c", "1", "--threads", "2")
    try:
        assert sorted(r.get(timeout=30) for r in results) == ["t0", "t1"]
    finally:
        w2.stop()


def _free_threaded() -> bool:
    import sysconfig

    return bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


@pytest.mark.skipif(not _free_threaded(), reason="needs a free-threaded Python build (3.13t+)")
def test_free_threaded_threads_run_cpu_bound_tasks_in_parallel(wapp, tmp_path):  # noqa: F811
    import os

    import workerapp

    if wapp.kind != "sqlite":
        pytest.skip("broker-independent; run once")
    if (os.cpu_count() or 1) < 4:
        pytest.skip("needs 4 CPUs")
    w = Worker(wapp, tmp_path, "-c", "1", "--threads", "4")
    try:
        results = [r.get(timeout=60) for r in [workerapp.burn.delay(1.0) for _ in range(4)]]
    finally:
        w.stop()
    assert [r["gil"] for r in results] == [False] * 4, "the GIL was re-enabled in the worker"
    # With a GIL four CPU-bound threads would each get ~25% of a core.
    assert min(r["cpu_share"] for r in results) > 0.6, results


def test_celery_style_thread_pool_flag_maps_to_threads(monkeypatch):
    from potatoq import Potatoq, cli
    from potatoq.worker import supervisor

    captured = {}

    class FakeSupervisor:
        def __init__(self, app, **kwargs):
            captured.update(kwargs)

        def start(self):
            return 0

    monkeypatch.setattr(supervisor, "Supervisor", FakeSupervisor)
    app = Potatoq("cli-threads", broker="memory://", set_as_current=False)
    assert cli.main(["-A", app, "worker", "-P", "threads", "-c", "16"]) == 0
    assert (captured["concurrency"], captured["threads"]) == (1, 16)
    assert cli.main(["-A", app, "worker", "-c", "2", "--threads", "4"]) == 0
    assert (captured["concurrency"], captured["threads"]) == (2, 4)
