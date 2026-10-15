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

## 3. Django: django-celery-results and django-celery-beat

potatoq replaces both, so neither package is needed afterwards. Both have database
tables, so remove them in this order: **migrate to zero first, then uninstall**. Once
an app is gone from `INSTALLED_APPS`, `migrate` can't drop its tables any more, and
you'd have to drop them by hand.

### django-celery-results

`CELERY_RESULT_BACKEND = "django-db"` keeps working, but it no longer means
django-celery-results' `TaskResult` table. It means **potatoq's own result table
(`potatoq_results`) in your Django database** (`POTATOQ_DATABASE`, or `default`),
created automatically, without a migration. `"django-cache"` maps to the same table,
not to your cache. The database must be PostgreSQL or SQLite. On MySQL, set
`result_backend` to a `redis://` or `postgresql://` URL instead.

When the broker already is your Django database (the default without a broker setting),
results are stored there anyway, so you can delete the setting.

- Results stored by Celery stay in django-celery-results' tables. potatoq doesn't read
  them: `AsyncResult(old_id).state` is `PENDING`.
- Code that queries the `TaskResult` model (or its admin pages) needs to move to
  `AsyncResult(task_id)`, or to `potatoq dead list` for failures.
- `result_extended` isn't needed: potatoq always stores the task name, arguments, worker
  and start time.

Once you don't need the old results:

```console
$ python manage.py migrate django_celery_results zero   # drops its tables
```

Then remove `"django_celery_results"` from `INSTALLED_APPS` and from your requirements.

### django-celery-beat

potatoq doesn't read schedules from the database yet ([wishlist](wishlist.md)): periodic
tasks are defined in the `beat_schedule` setting and run by the workers. potatoq ignores
`CELERY_BEAT_SCHEDULER`, so without the steps below, **your database schedules silently
stop running**. While the setting still names django-celery-beat, Django's checks warn
about it (`potatoq.W002`).

1. Export the enabled schedules as a `beat_schedule` setting. Save this as
   `export_beat.py` and run `python manage.py shell < export_beat.py`:

    ```python title="export_beat.py"
    import json

    from django_celery_beat.models import PeriodicTask

    print("POTATOQ_BEAT_SCHEDULE = {")
    for t in PeriodicTask.objects.filter(enabled=True).exclude(task="celery.backend_cleanup"):
        if t.crontab:
            c = t.crontab
            schedule = (
                f"crontab(minute={c.minute!r}, hour={c.hour!r}, day_of_week={c.day_of_week!r}, "
                f"day_of_month={c.day_of_month!r}, month_of_year={c.month_of_year!r}, tz={str(c.timezone)!r})"
            )
        elif t.interval:
            schedule = f"timedelta({t.interval.period}={t.interval.every})"
        else:
            print(f"    # {t.name!r}: port its solar or clocked schedule by hand")
            continue
        options = {"queue": t.queue} if t.queue else {}
        if t.priority is not None:
            options["priority"] = t.priority
        print(f"    {t.name!r}: {{")
        print(f"        'task': {t.task!r},")
        print(f"        'schedule': {schedule},")
        print(f"        'args': {tuple(json.loads(t.args or '[]'))!r},")
        print(f"        'kwargs': {json.loads(t.kwargs or '{}')!r},")
        if options:
            print(f"        'options': {options!r},")
        print("    },")
    print("}")
    ```

    Paste the output into your settings, with `from datetime import timedelta` and
    `from potatoq.schedules import crontab` at the top. Check it with `potatoq schedule`,
    which lists each entry's next run. Look over two things by hand: potatoq's crontab
    matches a day when *either* `day_of_month` or `day_of_week` matches, as standard cron
    does, and if you used django-celery-beat's priorities on Redis, invert them (see
    the table below). `celery.backend_cleanup` is skipped because potatoq expires results
    itself (`result_expires`).

2. Deploy with the new setting, and stop `celery beat` in the same deploy
   ([step 2 below](#side-by-side-several-hosts-or-containers)). The potatoq workers run
   the schedule from then on.
3. Delete `CELERY_BEAT_SCHEDULER`, then drop the tables and uninstall:

    ```console
    $ python manage.py migrate django_celery_beat zero   # drops its tables
    ```

    and remove `"django_celery_beat"` from `INSTALLED_APPS` and your requirements.

Schedules can no longer be edited in the admin without a deploy. If you relied on
that, keep this in mind before switching.

## 4. Deploy: new queues, then drain the old ones

potatoq uses its own compact, versioned message format, designed for what comes next
rather than compatibility with Celery's protocol. Celery and potatoq don't consume each
other's messages. potatoq's queues are separate (`default` instead of `celery`, its own
tables or keys), so the two never collide, and you can run them side by side.

### Side by side (several hosts or containers)

1. **Start potatoq workers** next to the Celery ones. They find nothing to do yet.
2. **Deploy the producers** (web and app code) with potatoq. New tasks go to potatoq's
   queues; Celery's workers keep working through what's already in theirs. Stop
   `celery beat` in the same deploy: potatoq's workers already run the schedule, and
   leaving both running sends every periodic task twice.
3. **Wait for Celery's queues to drain.** A queue is empty when it has no waiting
   messages **and** no unacknowledged ones. Tasks with an `eta` or `countdown` count as
   unacknowledged: Celery workers fetch them right away and hold them until they're due.

    === "Redis"

        Each Celery queue is a Redis list named after the queue; held messages are in
        the `unacked` hash (use `-n` for your database number, and add your
        `global_keyprefix` to the key names if you set one):

        ```console
        $ redis-cli -n 0 LLEN celery          # waiting, once per queue you used
        (integer) 0
        $ redis-cli -n 0 HLEN unacked         # held by workers, mostly eta/countdown tasks
        (integer) 0
        ```

        Celery stores priorities other than the default in separate lists
        (`celery\x06\x163` and so on): `redis-cli --scan --pattern 'celery*'` lists them.

    === "RabbitMQ"

        ```console
        $ rabbitmqctl list_queues name messages_ready messages_unacknowledged
        name     messages_ready  messages_unacknowledged
        celery   0               0
        ```

    `celery -A proj inspect scheduled` lists the eta/countdown tasks workers are holding,
    with their due times. If some are days away, don't wait for them: stop Celery and
    [move them](#moving-leftover-tasks).

4. **Stop the Celery workers** with a warm shutdown (`SIGTERM`, what `systemctl stop`
   sends): they finish their running tasks and give the held ones back to the queue.
   Then remove Celery from your requirements.

### One host, one step

If a deploy replaces the code and restarts the services, Celery's workers can't keep
running: the new code no longer has Celery tasks. Deploy anyway, and move what's left:

1. Check the queues (step 3 above) right before deploying, so you know what to expect.
2. Deploy. Stopping the old Celery worker (a warm shutdown) puts the tasks it was
   holding back into its queue; potatoq's workers start on the new code.
3. [Move the leftover tasks](#moving-leftover-tasks) into potatoq's queues.

### Moving leftover tasks

These scripts read what's left in Celery's queues and send each task to potatoq, keeping
its id, arguments, `eta` and `expires`. Run them **after** Celery's workers have stopped,
with your potatoq app and Celery's broker URL. In a Django project without a
`celery.py`, use `app = current_app()` (`from potatoq import current_app`) and run the
script with `python manage.py shell < move_celery_tasks.py`. The tasks must exist under the same names
in potatoq (they do, if you kept the module paths). Only the task itself is moved: the
rest of a chain, a chord's callback, or a `link` is printed and skipped.

=== "Redis"

    ```python title="move_celery_tasks.py"
    import base64
    import json
    from datetime import UTC, datetime

    import redis

    from proj.celery import app  # your potatoq app

    OLD_BROKER_URL = "redis://localhost:6379/0"  # Celery's broker
    QUEUES = ["celery"]  # Celery's queue names

    old = redis.Redis.from_url(OLD_BROKER_URL)
    for queue in QUEUES:
        moved = 0
        # LMOVE keeps a copy in "<queue>.moved" until you've checked the result.
        while (raw := old.lmove(queue, f"{queue}.moved", "RIGHT", "LEFT")) is not None:
            envelope = json.loads(raw)
            headers, properties = envelope["headers"], envelope["properties"]
            if envelope["content-type"] != "application/json":
                print(f"Skipped {headers['task']}[{headers['id']}]: {envelope['content-type']} body")
                continue
            body = envelope["body"]
            if properties.get("body_encoding") == "base64":
                body = base64.b64decode(body)
            args, kwargs, embed = json.loads(body)
            if embed.get("chain") or embed.get("callbacks") or embed.get("chord"):
                print(f"{headers['task']}[{headers['id']}] was part of a workflow; only the task itself is moved")
            times = {}
            for name in ("eta", "expires"):
                if headers.get(name):
                    when = datetime.fromisoformat(headers[name])
                    times[name] = when if when.tzinfo else when.replace(tzinfo=UTC)
            app.send_task(
                headers["task"],
                args,
                kwargs,
                task_id=headers["id"],
                queue="default" if queue == "celery" else queue,  # or your potatoq queue names
                **times,
            )
            moved += 1
        print(f"{queue}: moved {moved} task(s) to potatoq")
    ```

    Once you've checked the result, `redis-cli DEL celery.moved`.

=== "RabbitMQ"

    ```python title="move_celery_tasks.py"
    import json
    from datetime import UTC, datetime

    import pika

    from proj.celery import app  # your potatoq app

    OLD_BROKER_URL = "amqp://guest:guest@localhost:5672/%2F"  # Celery's broker (vhost "/" is %2F)
    QUEUES = ["celery"]  # Celery's queue names

    channel = pika.BlockingConnection(pika.URLParameters(OLD_BROKER_URL)).channel()
    for queue in QUEUES:
        moved = 0
        while True:
            method, properties, body = channel.basic_get(queue)
            if method is None:
                break
            headers = properties.headers
            if properties.content_type != "application/json":
                print(f"Skipped {headers['task']}[{headers['id']}]: {properties.content_type} body")
                channel.basic_nack(method.delivery_tag, requeue=False)
                continue
            args, kwargs, embed = json.loads(body)
            if embed.get("chain") or embed.get("callbacks") or embed.get("chord"):
                print(f"{headers['task']}[{headers['id']}] was part of a workflow; only the task itself is moved")
            times = {}
            for name in ("eta", "expires"):
                if headers.get(name):
                    when = datetime.fromisoformat(headers[name])
                    times[name] = when if when.tzinfo else when.replace(tzinfo=UTC)
            app.send_task(
                headers["task"],
                args,
                kwargs,
                task_id=headers["id"],
                queue="default" if queue == "celery" else queue,  # or your potatoq queue names
                **times,
            )
            channel.basic_ack(method.delivery_tag)  # only once potatoq has it
            moved += 1
        print(f"{queue}: moved {moved} task(s) to potatoq")
    ```

    A skipped (non-JSON) message is rejected; with a dead-letter exchange on the queue,
    RabbitMQ keeps it there.

Pickled messages can't be moved this way: the scripts skip them (they'd need Celery and
your old code to unpickle). On RabbitMQ, give potatoq new queue names rather than
Celery's: potatoq uses quorum queues and refuses a name a classic queue already has.

## 5. Things that behave differently, on purpose

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

## 6. Not supported (yet)

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
