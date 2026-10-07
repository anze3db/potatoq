# Django

```python title="settings.py"
INSTALLED_APPS = [
    ...,
    "potatoq.contrib.django",
]
```

That's the whole setup:

- **No `celery.py`.** `@shared_task` works, and `tasks.py` in every installed app is
  discovered automatically.
- **Your database is the broker** (Postgres or SQLite) unless you configure another
  one. Tasks are written inside your transactions.
- **`.delay()` in `atomic()` is sent on commit** ([details](../guide/transactions.md)).
- **Worker processes close stale database connections** around every task, and never
  reuse connections inherited from the parent process.
- `TIME_ZONE` is used for crontab schedules.

```python title="shop/tasks.py"
from potatoq import shared_task


@shared_task
def send_receipt(order_id):
    order = Order.objects.get(pk=order_id)
    ...
```

```python title="shop/views.py"
@transaction.atomic
def checkout(request):
    order = Order.objects.create(...)
    send_receipt.delay(order.id)       # committed together with the order
```

Run workers with either command:

```console
$ DJANGO_SETTINGS_MODULE=mysite.settings potatoq worker
$ python manage.py potatoq worker
```

## Settings

Use a `POTATOQ` dict, `POTATOQ_*` settings, or keep your existing `CELERY_*` ones:

```python title="settings.py"
POTATOQ = {
    "broker_url": "redis://localhost:6379/0",   # omit to use the database
    "task_time_limit": 600,
    "beat_schedule": {...},
}
# or
POTATOQ_BROKER_URL = "redis://localhost:6379/0"
# or, unchanged from Celery
CELERY_BROKER_URL = "redis://localhost:6379/0"
```

`POTATOQ_DATABASE = "queue"` uses a different database alias as the broker, for example
a separate database for the queue.

## Keeping your `celery.py`

The Celery pattern still works if you prefer an explicit app:

```python title="mysite/celery.py"
import os
from potatoq import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")
app = Celery("mysite")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
```

## Tests

`TestCase` never commits, so tasks enqueued inside it are never sent. Either set
`POTATOQ = {"task_always_eager": True}` in test settings, or use `TransactionTestCase`
together with [`drain`](../guide/testing.md).
