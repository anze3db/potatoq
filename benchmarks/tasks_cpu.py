import os
import sys

import redis

from potatoq import Potatoq

app = Potatoq("bench_cpu", broker=os.environ["BENCH_BROKER"])
app.conf.task_ignore_result = True
app.conf.worker_max_tasks_per_child = None
app.conf.task_default_queue = "bench_cpu"
app.conf.broker_transport_options = {"global_keyprefix": "potatoq-bench"}
counter = redis.Redis.from_url("redis://localhost:6379/13")


@app.task
def burn(n):
    """Pure-Python CPU work: no I/O, no C calls that release the GIL."""
    x = 0
    for i in range(n):
        x += i * i
    gil = getattr(sys, "_is_gil_enabled", lambda: True)()
    counter.set("potatoq-bench:gil", int(gil))
    counter.incr("potatoq-bench:count")
