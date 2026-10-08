# 🥔 potatoq

> [!WARNING]
> **potatoq is alpha software** in limited production use. Use it in
> production **at your own risk** for now. APIs and defaults may still change between
> releases, and bug reports are very welcome.

**A Celery-compatible task queue with production-ready defaults.**

> [!NOTE]
> potatoq was generated with **Claude Opus 5.5** (high effort): the research, design,
> code, tests and docs.

Switching from Celery is meant to be a find-and-replace: the same decorators, the same
`delay()`/`apply_async()`, the same `chain`/`group`/`chord`, the same settings names.
What changes is everything you used to have to know to run Celery safely in production.
The defaults now come from years of community post-mortems, the Ruby job-queue world
(Sidekiq, Solid Queue, GoodJob), and the best Python alternatives (Dramatiq, RQ, arq,
Procrastinate, Oban/River designs). Each backend is implemented with its own native
primitives instead of a lowest-common-denominator abstraction.

- **Free-threaded Python** (3.14t, 3.15t): tested in CI, and potatoq never turns the
  GIL back on, so `--threads` runs CPU-bound tasks in parallel in one process: 3.4×
  with 4 threads, in half the memory of 4 processes ([numbers](#free-threaded-python)).
- **Django tasks**: a backend for Django 6's built-in
  [`django.tasks`](docs/integrations/django-tasks.md), sharing workers with
  `@shared_task`. The classic setup works too: add `"potatoq.contrib.django"` to
  `INSTALLED_APPS` (no `celery.py` needed), and `.delay()` inside `atomic()` is sent on
  commit.

```python
from potatoq import Potatoq  # or: from potatoq import Celery

app = Potatoq("proj")  # broker: $POTATOQ_BROKER_URL, your Django DB, or ./potatoq.sqlite3


@app.task
def add(x, y):
    return x + y


add.delay(2, 2)
```

```console
$ potatoq -A proj worker
```

## Why

Every Celery deployment eventually learns these the hard way. In Potatoq they are the defaults:

| | Celery default | Potatoq default |
|---|---|---|
| Acknowledgement | before the task runs (crash = lost task) | **after it finishes** (at-least-once) |
| Worker process killed (OOM, segfault) | task acked and lost | **requeued**, dead-lettered after 5 crashes (poison-message guard) |
| Prefetch | 4 × concurrency (short tasks wait behind long ones) | **one task per idle process** |
| ETA / countdown | held in worker RAM; Redis `visibility_timeout` re-runs them; RabbitMQ's 30 min timeout kills them | **stored by the broker** (sorted set, `run_at` column, TTL cascade) |
| Long tasks on Redis | redelivered every hour (`visibility_timeout`) | **leases renewed** by the worker while the task runs |
| Time limits | none | **30 min hard, soft 30 s earlier** |
| Failed tasks | gone | **dead-letter store** you can list and replay (`potatoq dead list/retry`) |
| Retries | fixed 180 s | **exponential backoff with jitter** |
| Memory leaks | processes live forever | **recycled every 1000 tasks** (`max_memory_per_child="512MB"` available) |
| Concurrency | host CPU count (over-subscribes containers) | **CPUs actually available** (affinity + cgroup quota) |
| SIGTERM | waits forever, then Kubernetes SIGKILLs | **25 s grace, then requeue** unfinished tasks |
| `celery beat` | separate single process; two of them = everything twice | **every worker runs the scheduler, deduplicated** |
| Django `.delay()` inside `atomic()` | sent immediately (task runs before COMMIT) | **sent on commit**, dropped on rollback |
| RabbitMQ | classic queues, no publisher confirms | **quorum queues + publisher confirms** |
| Redis | no key prefix, eviction can eat your queue | **prefixed keys**, warns unless `noeviction` |
| Root logger | hijacked | **left alone** |
| `async def` tasks | not supported | **supported** |
| Gossip/mingle/heartbeat chatter | on | **doesn't exist** |

Why each one, with sources: [docs/design/defaults.md](docs/design/defaults.md).

## Install

```console
$ pip install potatoq                 # SQLite broker, no dependencies
$ pip install "potatoq[redis]"        # + Redis / Valkey
$ pip install "potatoq[postgres]"     # + PostgreSQL (psycopg 3)
$ pip install "potatoq[rabbitmq]"     # + RabbitMQ (pika)
```

Python 3.11+ (including free-threaded builds) on **Linux and macOS**. Windows isn't
supported: the worker relies on `fork()` and POSIX signals. Use WSL on Windows.

## Brokers

| URL | Best for | How it works |
|---|---|---|
| `postgresql://…` | Most apps: zero extra infrastructure, **transactional enqueue** | `FOR UPDATE SKIP LOCKED` claims, `LISTEN/NOTIFY` wake-ups, heartbeat-based recovery, results in the same transaction as the ack |
| `sqlite:///path.db` | Development, single-host apps | WAL, `BEGIN IMMEDIATE`, `UPDATE … RETURNING` claims, `PRAGMA data_version` wake-ups |
| `redis://…` / `valkey://…` | High throughput | Atomic Lua scripts, priority sorted sets, leases + fencing tokens, `BZPOPMIN` wake-ups |
| `amqp://…` | Existing RabbitMQ shops | Quorum queues, publisher confirms, 28-level TTL delay cascade, dead-letter queues |
| `memory://` | Unit tests | In-process, with `potatoq.testing.drain()` |

The design and research behind each backend are in [docs/design/internals.md](docs/design/internals.md).

Results are stored by default on the database brokers, where writing a result is part of
the same transaction as the acknowledgement and costs nothing. On Redis and RabbitMQ they
are opt-in, exactly like Celery: set `result_backend` (`"broker"`, `redis://…`,
`postgresql://…`). Calling `.get()` on a task whose result is ignored raises immediately
instead of hanging forever.

## Migrating from Celery

```diff
-from celery import Celery, shared_task
-from celery.schedules import crontab
+from potatoq import Celery, shared_task
+from potatoq.schedules import crontab
```

```console
-$ celery -A proj worker -l info -Q default -c 8 -B --without-gossip --without-mingle
+$ potatoq -A proj worker -l info -Q default -c 8
```

Your `CELERY_*` Django settings, `config_from_object(..., namespace="CELERY")`,
`beat_schedule`, `task_routes`, `autoretry_for`, `bind=True` + `self.retry()`, canvas,
signals, `AsyncResult` and custom `Task` base classes keep working. Then delete the
settings you only had because of Celery's defaults (`task_acks_late`,
`worker_prefetch_multiplier`, `broker_transport_options={"visibility_timeout": …}`,
`task_reject_on_worker_lost`, `broker_connection_retry_on_startup`, …).
See [docs/migrating-from-celery.md](docs/migrating-from-celery.md) for the full list,
including the few deliberate differences.

## Django

```python
INSTALLED_APPS = [..., "potatoq.contrib.django"]
```

That's it. No `celery.py`: `@shared_task` works, every app's `tasks.py` is discovered,
settings come from `POTATOQ = {...}` or your existing `CELERY_*` settings, and without a
broker setting **your default database is the broker**. Postgres and SQLite both work, so
you need no Redis to get started.

```python
from django.db import transaction
from potatoq import shared_task


@shared_task
def send_receipt(order_id): ...


with transaction.atomic():
    order = Order.objects.create(...)
    send_receipt.delay(order.id)  # runs only after COMMIT; never if the transaction rolls back
```

When the broker is your database, the task row is written inside your transaction:
the same semantics as `on_commit`, without the window where a crash between COMMIT and
publishing loses the task. `delay_on_commit()` (Celery 5.4) is supported too, and
`@shared_task(enqueue_on_commit=False)` opts a task out.

Using Django 6's built-in `django.tasks`? potatoq is a backend for it, with
priorities, `run_after`, async tasks, `get_result()` and signals. Set
`TASKS = {"default": {"BACKEND": "potatoq.contrib.django.tasks.PotatoqBackend"}}`.

Run workers with `potatoq worker` (it reads `DJANGO_SETTINGS_MODULE`) or `python manage.py potatoq worker`.

## Flask, FastAPI, SQLAlchemy

```python
# Flask: config from app.config, tasks run inside app.app_context()
from potatoq.contrib.flask import init_app

potatoq = init_app(flask_app)

# FastAPI: non-blocking enqueue and result polling
result = await send_welcome.adelay(user.id)
value = await result.aget(timeout=10)

# SQLAlchemy: .delay() inside a session transaction is sent on commit
# (or written in the transaction when the broker is the same database)
from potatoq.contrib.sqlalchemy import install

install(app)
```

## Periodic tasks

```python
app.conf.beat_schedule = {
    "nightly-report": {"task": "reports.build", "schedule": crontab(hour=3, minute=0)},
    "heartbeat": {"task": "monitoring.ping", "schedule": 30.0},
}
```

Every worker runs the scheduler. Fire times are deterministic, and each one is claimed
exactly once through the broker: a unique row on Postgres/SQLite, `SET NX` on Redis, a
single-active-consumer token on RabbitMQ. You no longer need a `beat` process, and
running two of them can't double-schedule anything. A periodic run that couldn't start
before the next one is due expires instead of piling up.

## Operations

```console
$ potatoq -A proj status                 # live workers and what they are running
$ potatoq -A proj queues                 # waiting tasks per queue
$ potatoq -A proj dead list              # dead-lettered tasks, with the reason
$ potatoq -A proj dead retry <task-id>   # replay one
$ potatoq -A proj inspect active         # Celery-style inspect
$ potatoq -A proj call proj.tasks.add -a '[2, 2]'
$ potatoq -A proj worker -P solo         # run in-process, one task at a time (pdb-friendly)
```

## Testing

```python
from potatoq.testing import drain

app = Potatoq("tests", broker="memory://", result_backend="broker")


def test_signup():
    signup("ann@example.com")  # calls send_welcome.delay(...)
    [task] = drain(app)  # runs what was enqueued: serialization, retries and chords included
    assert task.state == "SUCCESS"
```

`task_always_eager` works as in Celery, and arguments are still round-tripped through
the serializer so eager tests catch what production would.

## Performance

`benchmarks/throughput.py`: 5000 no-op tasks, 4 worker processes, all brokers on
localhost (Apple M3 Max, Python 3.13, median of 3 runs). Run it yourself with
`uv run --with 'celery[redis]' python benchmarks/throughput.py`.

| Library | Broker | Enqueue (tasks/s) | Process (tasks/s) |
|---|---|---:|---:|
| **Potatoq** | Redis | 10,200 | **11,900** |
| Celery 5.6 | Redis | 5,400 | 2,700 |
| **Potatoq** | RabbitMQ | 2,700 ¹ | 6,800 |
| Celery 5.6 | RabbitMQ | 8,300 ¹ | 8,000 ² |
| **Potatoq** | Postgres | 5,500 | 7,500 |
| **Potatoq** | SQLite | 14,700 | 5,700 |

¹ Potatoq waits for RabbitMQ publisher confirms on every publish, so an enqueued task
is replicated before `delay()` returns. Celery doesn't wait, which is how its publishes
can disappear silently ([#5410](https://github.com/celery/celery/issues/5410)).
² Celery acks before running the task (a crash loses it) and prefetches 16 messages.
Potatoq acks after the task finishes and only takes work for idle processes.

Getting Celery to run at all for this comparison took two workarounds:
`FORKED_BY_MULTIPROCESSING=1`, because the prefork pool fails on macOS with Python 3.13,
and disabling remote control, because RabbitMQ 4.3 rejects Celery's transient pidbox
queues. Potatoq needed none. Redis, Postgres and SQLite numbers vary by about ±10% between runs. RabbitMQ numbers swing up to 2× with machine load, because every publish waits for the broker to confirm it.

### Free-threaded Python

`benchmarks/cpu.py`: 400 CPU-bound tasks (a pure-Python loop, about 40 ms each) on
Redis, median of 2 runs. Speedup is over one process on the same interpreter; memory is
the RSS of the whole worker (supervisor and children).

| Python | 1 process | 4 processes (`-c 4`) | 4 threads in 1 process (`-c 1 -t 4`) |
|---|---:|---:|---:|
| 3.14, with the GIL | 25 tasks/s, 69 MB | 99 tasks/s (4.0×), 146 MB | 26 tasks/s (1.1×), 70 MB |
| **3.14t, free-threaded** | 27 tasks/s, 78 MB | 107 tasks/s (4.0×), 166 MB | **91 tasks/s (3.4×), 87 MB** |
| 3.15 rc3, with the GIL | 19 tasks/s, 70 MB | 74 tasks/s (3.9×), 151 MB | 19 tasks/s (1.0×), 72 MB |
| **3.15t rc3, free-threaded** | 20 tasks/s, 80 MB | 79 tasks/s (4.0×), 172 MB | **67 tasks/s (3.4×), 90 MB** |

With the GIL, threads only help I/O-bound tasks. On a free-threaded build, 4 threads
run CPU-bound tasks 3.4× faster than one process, close to 4 processes' 4.0×, in
about half the memory. A bare `ThreadPoolExecutor` running the same loop scales about
as well, so the remaining gap is the interpreter's, not potatoq's
([details](docs/guide/workers.md#free-threaded-python)).

There is no Rust in the hot path, and that's deliberate. The research
([docs/design/internals.md#rust](docs/design/internals.md#rust)) found that per-task overhead is
dominated by broker round trips. Potatoq minimizes those: one Lua call or one SQL
statement per state change, with the result, ack and follow-up tasks committed together.
A Rust core measured +0–10% on realistic tasks, while costing fork-safety (prefork
workers) and a much larger wheel matrix. The serialization seam is kept so a native
codec can be added later as an optional extra.

## Documentation

Full docs live in [`docs/`](docs/) and build into a site with [Zensical](https://zensical.org):

```console
$ uv run --group docs zensical serve      # http://localhost:8000
```

## Development

```console
$ uv sync
$ uv run pytest                 # needs local Redis, Postgres and RabbitMQ; skips what isn't running
```

## Roadmap

Known gaps and planned features live in the [wishlist](docs/wishlist.md). Pick one up!

## Releases

potatoq uses CalVer (`26.1`, `26.2`, …). See [CHANGELOG.md](CHANGELOG.md) for what
changed and [RELEASING.md](RELEASING.md) for how releases are cut: a release PR,
trusted publishing to PyPI, and a GitHub release with generated notes.

## License

MIT
