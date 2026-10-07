import os

import redis

from potatoq import Potatoq

app = Potatoq("bench", broker=os.environ["BENCH_BROKER"])
app.conf.task_ignore_result = True
app.conf.worker_max_tasks_per_child = None
app.conf.task_default_queue = "bench_potatoq"
app.conf.broker_transport_options = {"global_keyprefix": "bench", "schema": "bench"}
counter = redis.Redis.from_url("redis://localhost:6379/13")


@app.task
def noop(i):
    counter.incr("bench:count")
