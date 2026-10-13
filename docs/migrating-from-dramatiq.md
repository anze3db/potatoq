# Migrating from Dramatiq

The concepts map one to one, but the defaults differ: read
[what's different](#2-things-that-behave-differently) before you deploy, especially
about retries.

!!! warning "Retries are opt-in"
    Dramatiq retries **every** exception, up to 20 times, with backoff from 15 seconds
    to 7 days. potatoq, like Celery, retries nothing unless you ask: a task that raises
    is recorded as failed and goes to the
    [dead-letter store](guide/retries-and-failures.md#dead-letters). To retry every
    task as before, set it once:

    ```python
    POTATOQ = {
        "task_autoretry_for": (Exception,),
        # Optional, Dramatiq's own schedule: 20 retries, 15 s doubling up to 7 days.
        "task_max_retries": 20,
        "task_retry_backoff": 15,
        "task_retry_backoff_max": 7 * 24 * 3600,
    }
    ```

    Tasks that must not run twice opt out with `autoretry_for=()`. See
    [retrying every task](guide/retries-and-failures.md#retrying-every-task).

## 1. Swap the code

### Actors become tasks

```python
# Dramatiq
import dramatiq

@dramatiq.actor(max_retries=5, time_limit=60_000, queue_name="emails")
def send_welcome(user_id): ...

send_welcome.send(user.id)

# potatoq
from potatoq import shared_task

@shared_task(autoretry_for=(Exception,), max_retries=5, time_limit=60, queue="emails")
def send_welcome(user_id): ...

send_welcome.delay(user.id)   # .send() works too, as an alias
```

| Dramatiq | potatoq |
|---|---|
| `@dramatiq.actor` | `@shared_task`, or `@app.task` |
| `actor.send(*args, **kwargs)` | `task.delay(*args, **kwargs)` (`.send()` is an alias) |
| `actor.send_with_options(args=..., kwargs=..., delay=60_000)` | `task.apply_async(args, kwargs, countdown=60)` |
| `queue_name="x"` | `queue="x"` |
| `priority=0` (lower runs first) | `priority=0` (**higher** runs first) |
| `max_retries=20`, `min_backoff=`, `max_backoff=` (ms) | `autoretry_for=(Exception,)`, `max_retries=10`, `retry_backoff=10`, `retry_backoff_max=600` (s): the defaults, about 50 min of retrying |
| `retry_when=`, `throws=` | `autoretry_for=(...)`, `dont_autoretry_for=(...)`, or `self.retry()` with `bind=True` |
| `time_limit=` in ms, 10 min by default | `time_limit=` in s, 30 min by default; `soft_time_limit=` raises `SoftTimeLimitExceeded` first |
| `max_age=` in ms (`AgeLimit`) | `expires=` in s, on the task or per call |
| `on_success=`, `on_failure=` (`Callbacks`) | `link=`, `link_error=`, or `.on_error()` on a signature |
| `dramatiq.pipeline([a.message(), b.message()])` | `chain(a.s(), b.s())` or `a.s() \| b.s()` |
| `dramatiq.group([...]).run()` | `group(...).apply_async()`; `chord(group, callback)` waits for all of them |
| `CurrentMessage.get_current_message()` | `bind=True` and `self.request` (`id`, `retries`, `delivery_info`…) |
| Results middleware, `message.get_result()` | `result = task.delay(...)`, `result.get(timeout=...)` |

One trap in chains: a Dramatiq pipeline passes the previous result as the **last**
argument of the next actor; a potatoq chain (like Celery's) passes it as the **first**.
Use `b.si(...)` (an immutable signature) where the next step ignores it, as with
`pipe_ignore`.

### Brokers and settings

| Dramatiq | potatoq |
|---|---|
| `RedisBroker(url=...)` | `broker="redis://..."` |
| `RabbitmqBroker(url=...)` | `broker="amqp://..."` |
| `StubBroker()` | `broker="memory://"` |
| — | `broker="postgresql://..."` or `"sqlite:///..."`: no extra service |
| `dramatiq.set_broker(...)` | `app = Potatoq("proj", broker=...)` |

With Django, replace `django_dramatiq` with `potatoq.contrib.django` in `INSTALLED_APPS`
and move the settings into a `POTATOQ` dict:

| django_dramatiq | potatoq |
|---|---|
| `DRAMATIQ_BROKER` | `POTATOQ = {"broker_url": ...}`; without it, your default database is the broker |
| `DRAMATIQ_RESULT_BACKEND` | `"result_backend"`; results are stored by default on database brokers. Set `"task_ignore_result": True` if nothing reads them. |
| `DRAMATIQ_AUTODISCOVER_MODULES` | every app's `tasks.py`, plus the `imports` setting ([where tasks live](integrations/django.md#where-tasks-live)) |
| `DRAMATIQ_TASKS_DATABASE`, `AdminMiddleware` | no task table; `potatoq status`, `potatoq queues` and `potatoq dead list` |
| `DbConnectionsMiddleware` | built in: connections are closed before and after each task |
| `transaction.on_commit(lambda: task.send(...))` | `task.delay(...)`: tasks are sent on commit by default |

After removing `django_dramatiq`, drop its leftover table:
`DROP TABLE django_dramatiq_task;` and
`DELETE FROM django_migrations WHERE app = 'django_dramatiq';`.

### The worker

| Dramatiq | potatoq |
|---|---|
| `dramatiq proj.tasks` | `potatoq -A proj worker` |
| `python manage.py rundramatiq` | `python manage.py potatoq worker` |
| `--processes 4` (default: one per CPU) | `-c 4` / `--concurrency 4` (default: the CPUs available) |
| `--threads 8` (the default) | `-t 8` / `--threads 8` (default: 1) |
| `--queues a b` | `-Q a,b` |

Dramatiq runs 8 threads in every process by default; potatoq runs one task per
process unless you ask for threads. For I/O-bound tasks, pass `--threads` to get the
same concurrency ([threads](guide/workers.md#threads)).

### Tests

```python
# Dramatiq
broker = dramatiq.get_broker()      # a StubBroker in tests
worker = Worker(broker, worker_timeout=100)
worker.start()
signup("ann@example.com")
broker.join("default", fail_fast=True)
worker.join()

# potatoq
from potatoq import current_app
from potatoq.testing import drain

signup("ann@example.com")
drain(current_app(), raise_on_failure=True)
```

`drain` runs the queued tasks in the test's process and skips ahead to countdowns and
retries instead of waiting for them, so tests that waited for Dramatiq's 15-second
backoff become instant. With Django, use `TransactionTestCase` (or
`@pytest.mark.django_db(transaction=True)`) when the code under test enqueues inside a
transaction: tasks are sent on commit. See [Testing](guide/testing.md).

## 2. Things that behave differently

| Area | Dramatiq | potatoq | What to do |
|---|---|---|---|
| Retries | every exception, 20 times, 15 s … 7 days | only `autoretry_for` or `self.retry()`; 10 times, 10 s doubling up to 10 min apart (about 50 min) | Set `task_autoretry_for = (Exception,)` |
| Units | milliseconds | seconds | Divide `delay`, `time_limit`, `max_age`, backoffs by 1000 |
| Time limit | 10 min | 30 min, soft limit 30 s earlier | Set `time_limit` where 10 min mattered |
| Priority | lower number first | higher number first | Invert priorities |
| Threads per process | 8 | 1 | Pass `--threads` for I/O-bound work |
| Enqueue in a transaction | sent immediately | sent on commit, dropped on rollback | Remove `on_commit` wrappers |
| Failed tasks | dead-letter queue, kept 7 days | [dead-letter store](guide/retries-and-failures.md#dead-letters), kept 30 days; `potatoq dead retry` replays one | Nothing |
| Results | only with the Results middleware | stored by default on database brokers | `task_ignore_result = True` if unused |
| Serialization | JSON | JSON | Nothing |
| Delivery | acked after the task, at-least-once | the same | Nothing |

## 3. Deploy

Run potatoq workers next to the Dramatiq ones, switch the producers, then stop
Dramatiq once its queues are empty: see [switching over](migrating.md#switching-over).
