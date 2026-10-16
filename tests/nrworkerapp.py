"""A worker app with the New Relic agent in developer mode (see test_newrelic.py)."""

import os
import time

import newrelic.agent

newrelic.agent.initialize(os.environ["TEST_NEWRELIC_INI"])

from potatoq import Potatoq  # noqa: E402
from potatoq.contrib import newrelic as potatoq_newrelic  # noqa: E402

potatoq_newrelic.instrument()

app = Potatoq("nrworkerapp", broker=os.environ["TEST_BROKER"], worker_max_tasks_per_child=1)


@app.task
def add(x, y):
    return x + y


@app.task(time_limit=1)
def hang():
    while True:
        try:
            time.sleep(10)
        except Exception:
            pass
