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

Messages are JSON, never pickle. These types round-trip unchanged: `datetime`, `date`,
`time`, `timedelta`, `UUID`, `Decimal`, `bytes` and `set`, plus enums and paths as their
values. Anything else fails at `.delay()` time, not later on the worker:

```pycon
>>> resize.delay(Image.objects.get(pk=1))
TypeError: Object of type Image is not JSON serializable. Pass primitive values (ids, strings, numbers) to tasks instead of objects.
```

Pass ids and load the object inside the task. Arguments are also checked against the
function signature when you enqueue, so `resize.delay(1, 2, 3)` raises immediately.

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
