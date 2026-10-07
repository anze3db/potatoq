import os

import redis
from celery import Celery

app = Celery("bench", broker=os.environ["BENCH_BROKER"])
app.conf.task_ignore_result = True
app.conf.broker_connection_retry_on_startup = True
app.conf.task_default_queue = "bench_celery"
app.conf.broker_transport_options = {"global_keyprefix": "bench:"}
counter = redis.Redis.from_url("redis://localhost:6379/13")


@app.task
def noop(i):
    counter.incr("bench:count")


if os.environ["BENCH_BROKER"].startswith("amqp"):
    # Celery's remote-control/event queues are transient non-exclusive queues, which
    # RabbitMQ 4.3 refuses by default ("transient_nonexcl_queues is deprecated").
    app.conf.worker_enable_remote_control = False
