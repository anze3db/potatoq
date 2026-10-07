# Retries and failures

potatoq delivers each task **at least once**. A task is removed from the broker only
after it finishes, so a deploy, an OOM kill or a crashed machine never loses it. The
flip side is that tasks must be **idempotent**: occasionally a task runs twice.

## Retrying

```python
@app.task(bind=True, max_retries=5)
def charge(self, order_id):
    try:
        gateway.charge(order_id)
    except gateway.Timeout as exc:
        raise self.retry(exc=exc)                 # backoff: ~10 s, 20 s, 40 s ... max 10 min
```

Or let potatoq do it:

```python
@app.task(autoretry_for=(gateway.Timeout,), max_retries=5)
def charge(order_id):
    gateway.charge(order_id)
```

- Retries are scheduled by the broker; nothing waits in worker memory.
- The default delay is exponential backoff with jitter (`task_retry_backoff = 10`, capped
  at `task_retry_backoff_max = 600`). Setting `default_retry_delay` on a task, or passing
  `countdown=`/`eta=` to `retry()`, gives a fixed delay.
- When retries run out, `retry(exc=exc)` re-raises `exc`; a bare `retry()` raises
  `MaxRetriesExceededError`.
- A retry keeps the task id, so `AsyncResult` follows it to its final state.

## Time limits

Every task has a hard limit, 30 minutes by default:

```python
@app.task(time_limit=600, soft_time_limit=570)
def export(report_id):
    try:
        build(report_id)
    except SoftTimeLimitExceeded:
        cleanup(report_id)
        raise
```

The soft limit raises `SoftTimeLimitExceeded` inside the task, by default 30 seconds
before the hard limit. If the task is still running at the hard limit, the worker kills
the process, records a `TimeLimitExceeded` failure, and replaces the process.

## When a worker process dies

If a process dies mid-task (OOM killer, segfault, `kill -9`), the supervisor notices
straight away and requeues the task. A task that keeps killing workers is a poison
message: after `task_max_deliveries` (5) crashed deliveries it is dead-lettered as
`WorkerLostError` instead of taking down the fleet. Tasks of a whole worker that
disappeared (a lost machine) are recovered once its heartbeat is `worker_dead_after`
(60 s) old.

A graceful shutdown never counts as a crash: on `SIGTERM`, running tasks get
`worker_shutdown_timeout` (25 s) to finish and are then requeued.

## Dead letters

A task that fails for good is kept in a dead-letter store rather than discarded. That
covers a raised exception after retries, a killed time limit, too many crashes, an
unknown task name, and `Reject(requeue=False)`.

```console
$ potatoq -A proj dead list
3534bd8e-…  proj.tasks.charge  queue=default  died=2026-10-08 00:20:27  gateway.Timeout: …
$ potatoq -A proj dead retry 3534bd8e-07ad-4d58-a9e7-f3eddb5354b1
```

Dead letters are kept for `dead_letter_ttl` (30 days), up to `dead_letter_max` (10,000).
Set `task_dead_letter_failures = False` to discard failed tasks as Celery does.

## Ignore and Reject

```python
from potatoq.exceptions import Ignore, Reject


@app.task
def maybe(x):
    if not wanted(x):
        raise Ignore()                      # done: no result, no failure
    if not ready(x):
        raise Reject("not yet", requeue=True)  # give it back (counts as a delivery)
    raise Reject("malformed")               # dead-letter it
```
