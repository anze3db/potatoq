# Quickstart

## 1. Define tasks

```python title="proj/tasks.py"
from potatoq import Potatoq

app = Potatoq("proj")


@app.task
def add(x, y):
    return x + y


@app.task(bind=True, autoretry_for=(ConnectionError,), max_retries=5)
def fetch_avatar(self, user_id):
    ...
```

`Potatoq("proj")` finds its broker in this order:

1. `broker=` / `app.conf.broker_url`
2. `$POTATOQ_BROKER_URL` (or `$CELERY_BROKER_URL`)
3. your Django `DATABASES["default"]`, if it's Postgres or SQLite
4. `sqlite:///potatoq.sqlite3`, with a warning. That's fine for development.

## 2. Start a worker

```console
$ potatoq -A proj.tasks worker
[INFO/MainProcess] potatoq 26.1 worker potatoq@laptop ready: broker=sqlite:///potatoq.sqlite3
  results=sqlite:///potatoq.sqlite3 queues=default concurrency=8 (prefork) time_limit=1800s ...
```

The worker forks one process per available CPU. Each process runs one task at a time,
so a slow task never holds others hostage. The same worker also runs your
[periodic tasks](../guide/periodic-tasks.md).

## 3. Send work

```pycon
>>> from proj.tasks import add
>>> result = add.delay(2, 3)
>>> result.get(timeout=10)
5
```

On the database brokers (SQLite, Postgres) results are stored automatically, because
writing them costs nothing there. On Redis and RabbitMQ you opt in, as in Celery:

```python
app = Potatoq("proj", broker="redis://localhost", backend="redis://localhost")
```

## 4. Look around

```console
$ potatoq -A proj.tasks status      # live workers and what they're running
$ potatoq -A proj.tasks queues      # waiting tasks per queue
$ potatoq -A proj.tasks dead list   # tasks that failed for good, and why
```

## Where next

<div class="grid cards" markdown>

-   :lucide-database: **[Choose a broker](choosing-a-broker.md)**: Postgres, Redis, RabbitMQ or SQLite?
-   :lucide-boxes: **[Django](../integrations/django.md)**: zero configuration, tasks committed with your transactions
-   :lucide-rotate-ccw: **[Retries and failures](../guide/retries-and-failures.md)**: backoff, time limits, dead letters
-   :lucide-arrow-right-left: **[Migrating from Celery](../migrating-from-celery.md)**: mostly a find-and-replace

</div>
