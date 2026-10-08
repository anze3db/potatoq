# Defining tasks

```python
from potatoq import Potatoq, shared_task

app = Potatoq("proj")


@app.task
def resize(image_id, width=800):
    ...


@app.task(bind=True, queue="emails", time_limit=60, max_retries=5)
def send_email(self, to, subject):
    self.request.id         # this task's id
    self.request.retries    # how many times it was retried
    ...


@shared_task                # not tied to an app: binds to the current one (reusable apps, Django)
def cleanup():
    ...
```

The task name defaults to `module.function`, as in Celery. Pass `name=` to keep names
stable across refactors; renaming a task while messages are queued leaves them
unregistered, and they get [dead-lettered](retries-and-failures.md#dead-letters).

## Options

Every option can be set per task in the decorator, and most also per call in
`apply_async`. Anything left unset falls back to the app-wide
[setting](../reference/settings.md).

| Option | Default | Meaning |
|---|---|---|
| `name` | `module.function` | Registered task name |
| `bind` | `False` | Pass the task instance as `self` |
| `queue` | `task_default_queue` (`"default"`) | Queue to send to |
| `priority` | `0` | Higher runs first, on every broker |
| `time_limit` | 1800 s | Hard limit: the process is killed |
| `soft_time_limit` | 30 s (or 10%) before `time_limit` | Raises `SoftTimeLimitExceeded` inside the task |
| `max_retries` | `3` | For `self.retry()` and `autoretry_for` |
| `autoretry_for` | `()` | Exception types that trigger an automatic retry |
| `dont_autoretry_for` | `()` | Exceptions excluded from `autoretry_for` |
| `retry_backoff` | `10` | Exponential backoff base in seconds (`False` = fixed delay) |
| `retry_backoff_max` | `600` | Cap on the backoff delay |
| `retry_jitter` | `True` | Randomise delays so retries don't stampede |
| `default_retry_delay` | — | A fixed retry delay; overrides the backoff |
| `retry_kwargs` | `{}` | Extra `retry()` arguments for `autoretry_for` |
| `ignore_result` | auto | `True` to never store the result |
| `expires` | — | Seconds, `timedelta`, or aware `datetime` after which the task is discarded |
| `enqueue_on_commit` | `True` | Send on transaction commit ([details](transactions.md)) |
| `typing` | `True` | Check arguments against the signature at `.delay()` time |
| `base` | `Task` | A custom `Task` subclass |

## Arguments must be JSON

Task arguments and results are serialized as **JSON, never pickle**. Pickle would make
broker write access equal to code execution. It also invites a classic bug: passing a
Django model to a task ships a snapshot that is stale, or broken, by the time the
worker runs it.

These types round-trip unchanged: `datetime`, `date`, `time`, `timedelta`, `UUID`,
`Decimal`, `bytes` and `set`. Enums and paths are sent as their values, and Django's
lazy translation strings as plain text. Anything else fails when you call `.delay()`,
in your web process, not later on the worker:

```pycon
>>> send_receipt.delay(order)
TypeError: Order is a Django model instance and can't be a task argument. Pass its
primary key (obj.pk) and load it inside the task with Order.objects.get(pk=...), so the
task works with current data.

>>> send_receipts.delay(Order.objects.filter(paid=True))
TypeError: QuerySet (a Django queryset) can't be a task argument. Pass a list of primary
keys (list(qs.values_list("pk", flat=True))) and query inside the task.
```

Pass ids and load the object inside the task:

```python
@shared_task
def send_receipt(order_id):
    order = Order.objects.get(pk=order_id)   # current data, at the time the task runs
    ...

send_receipt.delay(order.pk)
```

Arguments are also checked against the function signature when you enqueue, so
`resize.delay(1, 2, 3)` raises immediately. Eager mode (`task_always_eager`) still
round-trips arguments through the serializer, so tests catch these mistakes too.

## `async def` tasks

```python
@app.task
async def fetch(url):
    async with httpx.AsyncClient() as client:
        return (await client.get(url)).text
```

Each worker process keeps one event loop between tasks, so async clients and
connection pools can be reused. The soft time limit cancels the coroutine.

## Custom base classes

```python
from potatoq import Task


class Tracked(Task):
    def before_start(self, task_id, args, kwargs): ...
    def on_success(self, retval, task_id, args, kwargs): ...
    def on_failure(self, exc, task_id, args, kwargs, einfo): ...
    def on_retry(self, exc, task_id, args, kwargs, einfo): ...
    def after_return(self, status, retval, task_id, args, kwargs, einfo): ...


@app.task(base=Tracked)
def charge(order_id): ...
```

Set `app.Task = Tracked` (or `Potatoq(task_cls=Tracked)`) to make it the default.

## Logging

```python
from potatoq.utils.log import get_task_logger

logger = get_task_logger(__name__)


@app.task
def work():
    logger.info("hi")   # the worker's format adds the task name and id
```
