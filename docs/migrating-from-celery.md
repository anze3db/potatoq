# Migrating from Celery

## 1. Swap the imports

| Celery | Potatoq |
|---|---|
| `from celery import Celery, shared_task, Task, chain, group, chord, signature` | `from potatoq import ...` (same names) |
| `from celery.schedules import crontab` | `from potatoq.schedules import crontab` |
| `from celery.exceptions import ...` | `from potatoq.exceptions import ...` |
| `from celery.result import AsyncResult` | `from potatoq.result import AsyncResult` |
| `from celery import signals` / `celery.signals` | `from potatoq import signals` |
| `from celery import states` | `from potatoq import states` |
| `from celery.utils.log import get_task_logger` | `from potatoq.utils.log import get_task_logger` |
| `celery -A proj worker` | `potatoq -A proj worker` |

`Celery("proj", broker=..., backend=..., include=[...])`, `config_from_object(...,
namespace="CELERY")`, `autodiscover_tasks()`, `app.conf.update(...)`, the
`proj/celery.py` pattern and `CELERY_*` Django settings all work unchanged.

## 2. Delete settings you no longer need

These are accepted and ignored, because the behaviour they turned on is now the default
or no longer needed: `task_acks_late`, `task_reject_on_worker_lost`,
`worker_prefetch_multiplier`, `broker_transport_options={"visibility_timeout": ...}`,
`broker_connection_retry_on_startup`, `worker_cancel_long_running_tasks_on_connection_loss`,
`task_serializer`, `accept_content`, `result_serializer`, `beat_scheduler`, `task_queues`,
and the worker flags `-B`, `-O fair`, `--without-gossip`, `--without-mingle` and
`--without-heartbeat`.

For Django, `celery.py` itself is optional: add `"potatoq.contrib.django"` to
`INSTALLED_APPS` and `@shared_task` works.

## 3. Deploy: new queues, then drain the old ones

potatoq uses its own compact, versioned message format, designed for what comes next
rather than compatibility with Celery's protocol. Celery and potatoq don't consume each
other's messages, so run them side by side and [switch over](migrating.md#switching-over):
potatoq workers first, then the producers, then stop Celery once its queues are empty.
potatoq's queues are separate (`default` instead of `celery`, its own tables or keys), so
the two never collide.

## 4. Things that behave differently, on purpose

| Area | Celery | Potatoq | What to do |
|---|---|---|---|
| Delivery | at-most-once by default | at-least-once | Make tasks idempotent. With `acks_late` you already had to. |
| Serializer | JSON, or pickle if you enabled it | JSON only | If you used pickle to pass model instances or other objects, pass primary keys and load inside the task. `.delay()` tells you which argument it can't send. |
| `.delay()` inside a DB transaction | sent immediately | sent on commit, dropped on rollback | Nothing; this fixes `DoesNotExist` races. Opt out per task with `enqueue_on_commit=False` or globally with `task_enqueue_on_commit = False`. |
| `.delay()` inside a DB transaction with `task_always_eager` | runs immediately, inside the transaction | runs on commit, never on rollback, like a worker | In a `TestCase`, wrap code that enqueues inside `atomic()` in `captureOnCommitCallbacks(execute=True)` ([details](guide/testing.md#eager-mode-and-transactions)). |
| Time limit | none | 30 min hard, soft 30 s earlier | Raise it for long tasks: `@app.task(time_limit=4 * 3600)`. |
| Automatic retries | none unless `autoretry_for` or `self.retry()` | the same | Nothing. |
| Retries before giving up | 3 | 10 | Set `max_retries=3` on a task (or `task_max_retries = 3`) to keep the old limit. |
| `self.retry()` without countdown | 180 s | exponential backoff (about 10 s, 20 s, 40 s …) | Set `default_retry_delay` on the task (or `task_retry_backoff = False`) to get the old behaviour. |
| Failed tasks | discarded | dead-lettered (`potatoq dead list`) | Nothing. Disable with `task_dead_letter_failures = False`. |
| Results on Redis/RabbitMQ | only if `result_backend` is set | the same | Nothing. On Postgres/SQLite brokers results are on by default. |
| `.get()` on an ignored result | hangs | raises `ResultBackendDisabled` | |
| Priorities | Redis: 0 = highest | higher number = higher priority everywhere | Invert priorities if you used Redis priorities. |
| Default queue | `celery` | `default` | Keep potatoq on its own queues (see below). |
| Process recycling | never | every 1000 tasks | `worker_max_tasks_per_child = None` to disable. |
| Concurrency | host CPU count | CPUs available to the container | Set `-c` explicitly if you relied on the old value. |
| `beat` | separate process | runs in every worker, deduplicated | Stop running `celery beat`. `potatoq beat` exists if you prefer a dedicated process. |
| Periodic runs | queue up while workers are down; `beat` sends a missed run once when it restarts (if its schedule file survived) | expire when the next run is due; a starting worker sends runs due in the last 60 s, older ones missed while no worker was up are skipped | Set `options={"expires": None}` on an entry to keep queued runs. Run at least two workers and restart them one at a time so no run is missed. |
| Crontab with both `day_of_month` and `day_of_week` | both must match | either matches (standard cron) | |
| Naive `eta` datetimes | treated as UTC/local | rejected | Pass aware datetimes or use `countdown`. |
| Root logger | hijacked | left alone | |
| Tasks defined outside `tasks.py` (e.g. in views) | registered, because the worker's startup checks import the URLconf | only `tasks.py` is imported | Move them to `tasks.py`, or see [where tasks live](integrations/django.md#where-tasks-live). `manage.py check --deploy` [lists the ones workers can't run](integrations/django.md#catch-it-before-you-deploy). |

## 5. Not supported (yet)

The full list of gaps and planned features is in the [wishlist](wishlist.md).

* **gevent and eventlet pools.** `-P solo` runs in-process, and `-P threads -c N` runs N
  threads in one process, as in Celery, but with time limits still enforced
  ([details](guide/workers.md#threads)). `--threads` also combines with several processes.
  `async def` tasks are supported natively, which covers most reasons for the
  green-thread pools.
* **`rate_limit`.** Accepted but not enforced yet; a global, broker-backed rate limiter is
  planned.
* **`revoke(terminate=True)`** of a running task. Waiting tasks are revoked; use time
  limits for running ones.
* `celery multi`, bootsteps, custom remote-control commands, events and Flower.
  `potatoq status` and `potatoq inspect` read worker heartbeats instead.
* Redis Cluster, `solar` schedules, `chunks` (implemented via `starmap`, lightly tested).

Coming from Dramatiq instead? See [Migrating from Dramatiq](migrating-from-dramatiq.md).
