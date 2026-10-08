"""Tasks used by the worker integration tests (imported by real worker processes)."""

import json
import os
import time

from potatoq import Potatoq

app = Potatoq("workerapp", broker=os.environ.get("TEST_BROKER", "memory://"))
app.conf.update(json.loads(os.environ.get("TEST_CONF", "{}")))
LOG = os.environ.get("TEST_LOG")


def log(event: str, **data) -> None:
    if LOG:
        with open(LOG, "a") as f:
            f.write(json.dumps({"event": event, "pid": os.getpid(), **data}) + "\n")


@app.task
def add(x, y):
    return x + y


@app.task
def pid():
    return os.getpid()


@app.task(time_limit=1.5, soft_time_limit=None)
def hang():
    while True:  # ignores the soft limit's exception
        try:
            time.sleep(10)
        except Exception:
            pass


@app.task
def crash():
    log("crash")
    os._exit(13)


@app.task
def crash_with(value):
    os._exit(13)


@app.task
def sleep(seconds, tag=None):
    log("sleep-start", tag=tag)
    time.sleep(seconds)
    log("sleep-end", tag=tag)
    return tag


@app.task
def tick():
    log("tick", at=time.time())


@app.task
def nap(seconds, tag=None):
    """Sleeps in small steps, so injected exceptions land quickly."""
    log("nap-start", tag=tag, at=time.time())
    deadline = time.time() + seconds
    while time.time() < deadline:
        time.sleep(0.02)
    log("nap-end", tag=tag, at=time.time())
    return tag


@app.task(soft_time_limit=0.5, time_limit=10)
def spin_soft():
    from potatoq.exceptions import SoftTimeLimitExceeded

    try:
        while True:  # pure-Python busy loop: no signal can reach a non-main thread
            pass
    except SoftTimeLimitExceeded:
        return "cleaned up"


@app.task(time_limit=1.5)
def spin_forever():
    while True:  # swallows everything, including the soft limit: only a kill stops it
        try:
            while True:
                time.sleep(0.01)
        except BaseException:
            pass


@app.task
def burn(seconds):
    """Pure-Python CPU work. Returns how much of the wall time this thread got on a CPU
    (~1.0 when threads really run in parallel, ~1/N when they share a GIL)."""
    import sys

    wall0, cpu0 = time.perf_counter(), time.thread_time()
    x = 0
    while time.thread_time() - cpu0 < seconds:
        for i in range(10_000):
            x += i
    return {
        "cpu_share": (time.thread_time() - cpu0) / (time.perf_counter() - wall0),
        "gil": sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True,
    }
