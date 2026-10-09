# Django

```python title="settings.py"
INSTALLED_APPS = [
    ...,
    "potatoq.contrib.django",
]
```

That's the whole setup:

- **No `celery.py`.** `@shared_task` works, and `tasks.py` in every installed app is
  discovered automatically ([tasks elsewhere](#where-tasks-live)).
- **Your database is the broker** (Postgres or SQLite) unless you configure another
  one. Tasks are written inside your transactions.
- **`.delay()` in `atomic()` is sent on commit** ([details](../guide/transactions.md)).
- **Worker processes close stale database connections** around every task, and never
  reuse connections inherited from the parent process.
- `TIME_ZONE` is used for crontab schedules. If your settings don't set it, Django's
  default is `America/Chicago`, not UTC; `potatoq schedule` shows each next run with
  its timezone.

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

!!! tip "Django 6 `django.tasks`"
    Prefer Django's built-in task API? potatoq is a backend for it, and both styles can
    share workers. See [Django Tasks](django-tasks.md).

## Pass ids, not model instances

Task arguments are [JSON](../guide/tasks.md#arguments-must-be-json), never pickle. With
pickle, passing a model instance "works", but the task gets a copy of the row as it was
when you called `.delay()`. By the time a worker runs it, the row may have changed or
been deleted, and saving that copy overwrites newer data. potatoq refuses model
instances and querysets when you call `.delay()`, in the view, and tells you what to do
instead:

```pycon
>>> send_receipt.delay(order)
TypeError: Order is a Django model instance and can't be a task argument. Pass its
primary key (obj.pk) and load it inside the task with Order.objects.get(pk=...), so the
task works with current data.

>>> send_receipts.delay(Order.objects.filter(paid=True))
TypeError: QuerySet (a Django queryset) can't be a task argument. Pass a list of primary
keys (list(qs.values_list("pk", flat=True))) and query inside the task.
```

The fix is the pattern above: pass `order.pk`, or a list of primary keys for a
queryset, and load inside the task:

```python title="shop/tasks.py"
@shared_task
def send_receipts(order_ids):
    for order in Order.objects.filter(pk__in=order_ids):
        ...
```

```python
send_receipts.delay(list(Order.objects.filter(paid=True).values_list("pk", flat=True)))
```

Decide what the task should do if the row is gone by then (`Order.DoesNotExist`).
Usually that's returning quietly. Lazy translation strings (`gettext_lazy`) are sent as
plain text in the language active at `.delay()` time. To translate inside the task,
send the language code and use `translation.override()`.

## Where tasks live

Workers import `tasks.py` from every installed app, and nothing else. A task defined in
`views.py` (or any other module) works when you call `.delay()` from the web process,
but a worker that never imported that module can't run it: the task is dead-lettered as
unregistered (`potatoq dead list` shows it).

Celery registered these by accident: its worker runs Django's system checks at startup,
and those import your URLconf and with it every view. potatoq workers don't import the
web layer by default. Instead, either:

- **Move the task to `tasks.py`** and import it from the view (recommended), or
- **List its module** in `imports`: `POTATOQ = {"imports": ["accounts.views"]}`
  (`CELERY_IMPORTS` works too), or
- **Import the URLconf** like Celery: `POTATOQ = {"worker_import_urlconf": True}`.

`potatoq -A proj inspect registered` shows what the running workers registered.

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
a separate database for the queue. `.delay()` still follows the caller's transaction (on
`default`, or the alias passed as `using=`): it waits for that COMMIT and is sent after
it, since the task row can't be written inside a transaction on another database.

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

Keep `"potatoq.contrib.django"` in `INSTALLED_APPS` too: it's what hooks potatoq into
Django (enqueue on commit, connection handling, the database broker, `TIME_ZONE`).
Without it, potatoq assumes nothing about Django, so set `CELERY_TIMEZONE` for your
crontabs: `TIME_ZONE` isn't read.

## Tests

`TestCase` never commits, so tasks enqueued inside it are never sent. Either set
`POTATOQ = {"task_always_eager": True}` in test settings, or use `TransactionTestCase`
together with [`drain`](../guide/testing.md).

When the broker is your database, it follows Django's test runner to the test database
(`test_<name>`), so tests never enqueue into the database a development worker is
reading. With SQLite's in-memory test database there is no file to share, so tasks stay
in the test process (`memory://`) until you `drain` them. A test without database access
(no `django_db` mark) can't enqueue at all: pytest-django stops it, as it would any query
([details](../guide/testing.md#django-and-pytest-django)).

## SQLite as database and broker

With SQLite, the web process, Django and the worker all write to one file. Django opens
its transactions as `DEFERRED` by default, and a deferred transaction that reads before
it writes can't wait for the lock: it fails at once with `database is locked`. Make
Django take the write lock up front and wait for it (Django 5.1+):

```python title="settings.py"
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
        "OPTIONS": {"transaction_mode": "IMMEDIATE", "timeout": 20},
    }
}
```

potatoq's own connections already do this, and switch the file to WAL mode, which lets
readers and the writer work at the same time.
