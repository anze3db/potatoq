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
