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

It works with the SQLite broker too, for tests that need real cross-process behaviour.

## Eager mode

```python
app.conf.task_always_eager = True    # .delay() runs the task immediately, in-process
```

This behaves as in Celery, with one improvement: arguments still round-trip through the
serializer, so an eager test fails where production would.
`task_eager_propagates` (default `True`) re-raises task exceptions.

As in Celery:

- With `task_eager_propagates` (or `task.apply(throw=True)`), a task that retries
  raises `Retry` at its first retry, so a test can assert that it retried. Without it,
  the retries run right away, one after another, until the task succeeds or gives up.
- `delay_on_commit()` waits for the transaction to commit even in eager mode. In a
  Django `TestCase`, wrap the code in `self.captureOnCommitCallbacks(execute=True)`.
