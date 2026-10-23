# Retries and failures

potatoq delivers each task **at least once**. A task is removed from the broker only
after it finishes, so a deploy, an OOM kill or a crashed machine never loses it. The
flip side is that tasks must be **idempotent**: occasionally a task runs twice.

## Retrying

!!! warning "Retries are opt-in"
    A task that raises fails: it's recorded as `FAILURE` and dead-lettered, and it
    is **not retried** unless you ask for it, per task or
    [for every task](#retrying-every-task). This is Celery's behaviour. Dramatiq and
    Sidekiq retry every exception by default; to get that, set `task_autoretry_for`.

Retry from inside the task:

```python
@app.task(bind=True, max_retries=5)
def charge(self, order_id):
    try:
        gateway.charge(order_id)
    except gateway.Timeout as exc:
        raise self.retry(exc=exc)                 # backoff: ~10 s, 20 s, 40 s ... max 10 min
```

Or let potatoq do it for some exceptions:

```python
@app.task(autoretry_for=(gateway.Timeout,), dont_autoretry_for=(gateway.CardDeclined,), max_retries=5)
def charge(order_id):
    gateway.charge(order_id)
```

- Retries are scheduled by the broker; nothing waits in worker memory.
- When retries run out, the task fails and is dead-lettered: `retry(exc=exc)` and
  `autoretry_for` re-raise the exception, a bare `retry()` raises
  `MaxRetriesExceededError`.
- A retry keeps the task id, so `AsyncResult` follows it to its final state.

### Retrying every task

`task_autoretry_for` gives every task an `autoretry_for`:

=== "Django"

    ```python title="settings.py"
    POTATOQ = {
        "task_autoretry_for": (Exception,),   # retry on any error, like Dramatiq and Sidekiq
    }
    ```

=== "Python"

    ```python
    app.conf.task_autoretry_for = (Exception,)
    ```

A task's own `autoretry_for` replaces it, and `autoretry_for=()` turns retries off for
one task (one that charges a card, say). `dont_autoretry_for` still applies. The worker's
banner shows the policy:

```console
   retries   on Exception, up to 10 times, backoff from 10.0s, capped at 10m00s
```

Only retry tasks that are safe to run again: a task that sends an email and then raises
sends the email again on every retry.

### How many times

`max_retries` counts retries after the first run: with the default of 10, a task runs at
most 11 times, over about 50 minutes. Set it per task, or for every task with `task_max_retries`
(`None` retries without a limit):

```python
@app.task(autoretry_for=(Exception,), max_retries=10)
def sync_inventory(): ...

app.conf.task_max_retries = 5        # the default for tasks that don't set max_retries
```

`self.retry(max_retries=...)` overrides both for that one call.

### Backoff

The delay before a retry grows exponentially:

```text
delay = min(retry_backoff_max, retry_backoff × 2 ** retries_so_far)
```

With the defaults (`task_retry_backoff = 10`, `task_retry_backoff_max = 600`), the
delays are about 10 s, 20 s, 40 s, 80 s, 160 s, 320 s, then 10 minutes for every retry
after that: about 50 minutes for the 10 default retries, enough to ride out a
third-party outage, a database failover or a deploy. Change them per task or for every task:

```python
@app.task(autoretry_for=(Exception,), retry_backoff=60, retry_backoff_max=3600)
def import_feed(): ...      # 1 min, 2 min, 4 min ... capped at 1 hour

app.conf.task_retry_backoff = 5
app.conf.task_retry_backoff_max = 300
```

`retry_backoff=True` means a factor of 1 (1 s, 2 s, 4 s …), as in Celery. A fixed delay
replaces the backoff in three cases: `retry_backoff=False` (then `default_retry_delay`,
180 s by default via `task_default_retry_delay`, is used every time), a
`default_retry_delay` set on a task that doesn't set `retry_backoff`, and an explicit
`self.retry(countdown=...)` or `eta=`.

### Jitter

Tasks that failed together, because a dependency went down, would otherwise retry
together and hit it all at once again. Jitter spreads them out: each delay is a random
value between **half the computed delay and the full delay** ("equal jitter"). The first
retry of a task with the default backoff waits between 5 and 10 seconds, the second
between 10 and 20, and so on. A retry never happens immediately, unlike Celery's full
jitter (anywhere from 0 to the delay).

Turn it off with `retry_jitter=False` on a task, or `task_retry_jitter = False` for
every task, to get the exact delays.

### Settings at a glance

| Task option | App setting | Default | |
|---|---|---|---|
| `autoretry_for` | `task_autoretry_for` | `()` | Exceptions that retry automatically |
| `dont_autoretry_for` | | `()` | Exceptions that never do |
| `max_retries` | `task_max_retries` | `10` | Retries after the first run; `None`: no limit |
| `retry_backoff` | `task_retry_backoff` | `10` | First delay in seconds, doubled each retry; `False`: fixed delay |
| `retry_backoff_max` | `task_retry_backoff_max` | `600` | Longest delay |
| `retry_jitter` | `task_retry_jitter` | `True` | Randomize each delay between half and all of it |
| `default_retry_delay` | `task_default_retry_delay` | `180` | Fixed delay when backoff is off |

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

`time_limit=None` (or `0`) turns the limit off for one task, and `soft_time_limit=None`
(or `0`) keeps only the hard limit. Leaving them out uses `task_time_limit` and
`task_soft_time_limit`. Per call, `apply_async(time_limit=0)` turns the limit off.

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
$ potatoq -A proj dead delete 3534bd8e-07ad-4d58-a9e7-f3eddb5354b1   # never run it again
```

The [dashboard](dashboard.md) has the same Retry and Delete buttons.

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
