# Testing

## `drain`: run what was enqueued

Use the in-memory broker and run queued tasks synchronously, through the same code path
a worker uses: serialization, retries, chains, chords and dead letters included.

```python
from potatoq import Potatoq
from potatoq.testing import drain

app = Potatoq("tests", broker="memory://", result_backend="broker")


def test_signup():
    signup("ann@example.com")            # calls send_welcome.delay(...)
    [task] = drain(app)
    assert task.name == "proj.tasks.send_welcome"
    assert task.state == "SUCCESS"
```

`drain(app, queues=None, include_scheduled=True, raise_on_failure=False)` returns a list
of `DrainedTask(id, name, args, kwargs, state, result, exception)`. Tasks with a
countdown or a retry delay run without waiting: time is skipped ahead.

Like a starting worker, `drain` (and `tick`, below) first imports your task modules:
`tasks.py` of every Django app, and the modules in `imports` and `include`. A test
doesn't have to import a task's module for the task to run. A task defined anywhere
else (in a view, say) is dead-lettered as unregistered, as it would be on a worker,
unless the test imported it: see [where tasks live](../integrations/django.md#where-tasks-live).

It works the same on the SQLite, Postgres and Redis brokers, so a Django test can drain
the database broker it really uses. RabbitMQ is the exception: its delays live inside the
broker, so `drain` runs only what's ready and warns that countdowns and retries were
left behind. Test with `memory://` there.

## Periodic tasks

Check a schedule without waiting for the clock:

```python
from datetime import datetime
from potatoq.testing import drain, due, tick


def test_nightly_report_runs_at_three():
    # Which runs fall due in a window (naive datetimes are in the app's timezone)
    assert [name for name, _ in due(app, datetime(2026, 1, 1), datetime(2026, 1, 2))] == ["nightly-report"]


def test_nightly_report_sends_the_email():
    assert tick(app, at=datetime(2026, 1, 1, 3, 0)) == ["nightly-report"]   # what a worker sends then
    [task] = drain(app)
    assert task.state == "SUCCESS"
```

`due(app, start, end)` lists `(entry name, fire time)` pairs after `start` up to `end`,
and sends nothing. `tick(app, at=None)` sends what a worker's scheduler would send at
`at` (runs due in the minute before it), claimed through the broker like in production,
and returns the entry names; `drain` then runs them.

## Django and pytest-django

When the broker is your Django database, `.delay()` writes through Django's own
connection, so tasks follow the test database and the usual rules apply: a test without
`@pytest.mark.django_db` that enqueues a task fails with pytest-django's "Database access
not allowed", instead of queueing it into your development database where a running
worker would pick it up. (In async code, where Django refuses its connection, potatoq
uses its own.)

## Brokers that outlive a test run

The broker's state lasts as long as the broker does. That's no problem with `memory://`
or a fresh test database, but it is with a Postgres test database kept between runs
(pytest-django's `--reuse-db`, Django's `--keepdb`) or a shared Redis. There, tasks a
test left queued are still waiting in the next run, and so is the record of which
periodic runs were sent: `tick(app, at=...)` at a fixed time sends its tasks the first
time and nothing in every run after that, as a worker wouldn't send a run twice.

Start such tests from a clean broker with `reset`:

```python title="conftest.py"
import pytest
from potatoq import current_app
from potatoq.testing import reset


@pytest.fixture(autouse=True)
def clean_queues(db):          # pytest-django: after the test database is set up
    reset(current_app())
```

`reset(app, queues=None)` deletes the waiting and scheduled tasks in `queues` (default:
every queue with tasks, plus the default queue), forgets which periodic runs were sent,
and returns how many tasks it deleted. Results and dead letters are kept.

## Eager mode

```python
app.conf.task_always_eager = True    # .delay() runs the task immediately, in-process
```

This behaves as in Celery, with two improvements: arguments still round-trip through the
serializer, so an eager test fails where production would, and transactions are
respected the way a worker sees them (below).
`task_eager_propagates` (default `True`) re-raises task exceptions.

### Eager mode and transactions

Inside a transaction (Django `atomic()` or `ATOMIC_REQUESTS`, or an SQLAlchemy session
that has written something), an eager `.delay()` runs the task **when the transaction
commits**, and not at all if it rolls back, just like a worker would only see the task
after COMMIT. Celery's eager mode runs it immediately instead, inside the transaction,
which hides exactly the bugs enqueue-on-commit prevents: a task that works in
development because it can see uncommitted rows, then fails in production with
`DoesNotExist`.

```python
with transaction.atomic():
    order = Order.objects.create(...)
    result = send_receipt.delay(order.id)   # not run yet: result.state == "PENDING"
# committed: send_receipt has run, result.get() returns its value
```

Calling `result.get()` before the commit raises `RuntimeError`. Tasks with
`enqueue_on_commit=False` (or calls with `apply_async(..., enqueue_on_commit=False)`)
run immediately, inside the transaction, as they're sent immediately in production.

In a Django `TestCase`, the transaction that wraps each test doesn't count: it stands in
for autocommit, so eager tasks called outside your code's own `atomic()` blocks still
run at once. Tasks sent inside an `atomic()` (including views with `ATOMIC_REQUESTS`)
wait for a commit that never comes in a `TestCase`, like any `on_commit` callback: wrap
the code in `self.captureOnCommitCallbacks(execute=True)`, or use `TransactionTestCase`.

As in Celery:

- With `task_eager_propagates` (or `task.apply(throw=True)`), a task that retries
  raises `Retry` at its first retry, so a test can assert that it retried. Without it,
  the retries run right away, one after another, until the task succeeds or gives up.
- `delay_on_commit()` waits for the transaction to commit even in eager mode (in
  potatoq, so does `.delay()`, as described above).
