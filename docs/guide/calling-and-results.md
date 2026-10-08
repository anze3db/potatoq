# Calling tasks and results

```python
add.delay(2, 3)                          # shortcut for apply_async((2, 3))

add.apply_async(
    (2, 3),
    countdown=60,                        # or eta=aware_datetime
    expires=3600,                        # discard if it hasn't started within an hour
    queue="priority",
    priority=5,                          # higher runs first
    task_id="my-id",
    link=notify.s(),                     # called with the result
    link_error=alert.s(),                # called with the task id on failure
    time_limit=120,
    enqueue_on_commit=False,             # send now even inside a transaction
)

app.send_task("proj.tasks.add", (2, 3))  # by name, without importing the code
add(2, 3)                                # a plain function call, in this process
add.apply((2, 3))                        # run here, through the full machinery (EagerResult)
```

Naive datetimes are rejected for `eta`: pass a timezone-aware datetime, or use `countdown`.

From async code, use the non-blocking variants:

```python
result = await add.adelay(2, 3)
value = await result.aget(timeout=10)
```

## Results

`delay()` returns an `AsyncResult`:

| | |
|---|---|
| `result.id` | the task id |
| `result.state` | `PENDING`, `STARTED`, `RETRY`, `SUCCESS`, `FAILURE`, `REVOKED` |
| `result.get(timeout=None, propagate=True)` | wait; re-raises the task's exception |
| `result.ready()` / `successful()` / `failed()` | state checks |
| `result.result` / `result.info` | the return value or exception |
| `result.traceback` | the formatted traceback of a failure |
| `result.forget()` | delete the stored result |
| `result.revoke()` | prevent a waiting task from running |

**When are results stored?** It depends on whether storing them costs anything:

- **Postgres / SQLite brokers**: always, because the result is written in the same
  transaction that acknowledges the task.
- **Redis / RabbitMQ brokers**: only when `result_backend` is set
  (`"broker"`, `redis://…`, `postgresql://…`), as in Celery.
- A task with `ignore_result=True` never stores one, and `.get()` on it raises
  `ResultBackendDisabled` immediately instead of hanging.

Results expire after `result_expires` (1 day). On database brokers `STARTED` is reported
for free while the task runs.

## When the broker is down

`.delay()` raises `potatoq.exceptions.OperationalError` whatever the broker, like kombu's
`OperationalError` in Celery (the broker client's own exception is `__cause__`). With
Redis and RabbitMQ, potatoq first retries for about a second, which covers a restart or
failover; database brokers reconnect on their own.

```python
from potatoq.exceptions import OperationalError

try:
    send_receipt.delay(order.id)
except OperationalError:
    ...  # the task wasn't sent
```

Tasks deferred to the end of a transaction fail differently, after the data is
committed: see [transactions](transactions.md#if-sending-fails-after-commit).

!!! warning "Don't wait inside tasks"
    `result.get()` inside a task raises `RuntimeError`, because waiting on another task
    from a worker process can deadlock the pool. Use a [chain or chord](workflows.md).

## Revoking

```python
app.control.revoke(task_id)       # or result.revoke()
```

A waiting task is deleted from Postgres, SQLite and Redis before it can run. RabbitMQ
can't delete queued messages, so the revocation is recorded in the result backend and
the worker skips the task. Terminating a task that is already *running* is not
supported yet: `revoke(terminate=True)` logs a warning and only revokes it if it hasn't
started. Use [time limits](retries-and-failures.md#time-limits).
