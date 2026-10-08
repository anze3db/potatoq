# Django Tasks (`django.tasks`)

Django 6.0 added a built-in task API, [`django.tasks`](https://docs.djangoproject.com/en/stable/topics/tasks/),
but ships no production backend: tasks either run immediately in the request or not at
all. potatoq is a backend for it, so Django-native tasks run on real workers with
potatoq's guarantees.

```python title="settings.py"
INSTALLED_APPS = [..., "potatoq.contrib.django"]

TASKS = {
    "default": {
        "BACKEND": "potatoq.contrib.django.tasks.PotatoqBackend",
        "QUEUES": ["default", "emails"],   # Django rejects tasks for queues not listed here
        # "OPTIONS": {"APP": "mysite.potatoq:app"},   # default: the app potatoq.contrib.django configures
    }
}
```

```python title="shop/tasks.py"
from django.tasks import task


@task(queue_name="emails", priority=10)
def send_receipt(order_id):
    ...
```

```python
result = send_receipt.enqueue(order.id)          # or: await send_receipt.aenqueue(...)
result.status                                    # READY
...
result.refresh()
result.status, result.return_value               # SUCCESSFUL, ...
send_receipt.get_result(result.id)               # from anywhere, any process
```

Run the same workers as for any potatoq task:

```console
$ python manage.py potatoq worker -Q default,emails
```

## What's supported

| `django.tasks` feature | potatoq |
|---|---|
| `enqueue()` / `aenqueue()` | :lucide-check: |
| `priority` (−100…100, higher first) | :lucide-check: real priorities on Redis, Postgres, SQLite; RabbitMQ uses 0–31, so negative values run as 0 |
| `run_after` | :lucide-check: stored by the broker, never in worker memory |
| `queue_name` | :lucide-check: one potatoq queue per name |
| `async def` tasks | :lucide-check: |
| `takes_context` / `TaskContext` | :lucide-check: `context.attempt` counts retries |
| `get_result()` / `refresh()` | :lucide-check: `READY`, `RUNNING`, `SUCCESSFUL`, `FAILED`, with timestamps and `errors` |
| `task_enqueued` / `task_started` / `task_finished` signals | :lucide-check: |

Results need a result store: the database brokers have one built in, while on Redis set
`result_backend`. On RabbitMQ without a result backend, `get_result()` isn't available
(`supports_get_result` is `False`).

## potatoq options on Django tasks

Django 6.1 forwards extra `@task(...)` arguments to the backend, so potatoq's options
work directly:

```python
from django.tasks import task


@task(time_limit=120, autoretry_for=(ConnectionError,), max_retries=5)
def sync_inventory(sku): ...
```

On Django 6.0, use potatoq's drop-in decorator instead. It's identical, but accepts
the options:

```python
from potatoq.contrib.django.tasks import task
```

Available options: `time_limit`, `soft_time_limit`, `max_retries`, `autoretry_for`,
`dont_autoretry_for`, `retry_backoff`, `retry_backoff_max`, `retry_jitter`,
`default_retry_delay`, `ignore_result`, `enqueue_on_commit` and `expires`.

## What potatoq adds

`django.tasks` deliberately leaves delivery semantics to the backend. With potatoq:

- **At-least-once delivery**: a crashed or redeployed worker never loses a task.
- **Transactions**: `enqueue()` inside `transaction.atomic()` is sent on commit, and
  written inside the transaction when the broker is your database. Django's docs
  otherwise tell you to wrap every enqueue in `transaction.on_commit` yourself.
- **Time limits, retries with backoff and dead letters**, as for any potatoq task. A
  failed task shows up in `potatoq dead list` and can be replayed.
- **Early errors**: arguments are checked against the function signature and Django's
  JSON rules when you call `enqueue()`, not on the worker.

## Mixing with `@shared_task`

`django.tasks` tasks and potatoq/Celery-style `@shared_task` tasks run side by side on
the same workers and queues. That makes an incremental move to the Django-native API
straightforward.
