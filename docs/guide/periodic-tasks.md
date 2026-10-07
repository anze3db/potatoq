# Periodic tasks

```python
from datetime import timedelta
from potatoq.schedules import crontab

app.conf.beat_schedule = {
    "nightly-report": {
        "task": "reports.build",
        "schedule": crontab(hour=3, minute=0),
        "kwargs": {"full": True},
    },
    "ping": {"task": "monitoring.ping", "schedule": 30.0},   # seconds or timedelta
    "weekdays": {"task": "digest.send", "schedule": crontab(minute=30, hour=7, day_of_week="mon-fri")},
    "raw-cron": {"task": "cleanup.run", "schedule": "*/15 * * * *"},
}
app.conf.timezone = "Europe/Ljubljana"   # crontabs run in this timezone (default UTC; Django's TIME_ZONE is used)
```

Celery's `on_after_configure` idiom works too:

```python
@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(10.0, ping.s(), name="ping every 10s")
```

## No beat process

**Every worker runs the scheduler.** Fire times are deterministic (interval schedules
align to the clock, so "every 30 s" means :00 and :30), and each `(entry, fire time)` is
claimed exactly once through the broker:

| Broker | Deduplication |
|---|---|
| Postgres, SQLite | a unique row inserted in the same transaction as the task |
| Redis | `SET NX` in the same Lua script as the enqueue |
| RabbitMQ | a single-active-consumer token queue elects one scheduler |

So you can't end up with zero schedulers (periodic jobs silently stop) or two (everything
runs twice), the classic `celery beat` failure modes. Use `potatoq worker --no-scheduler`
on workers that shouldn't schedule, or `potatoq beat` for a dedicated process.

## Missed runs

- If no worker was up when a run was due, it is skipped once it is more than 60 s late:
  no stampede of the same job after downtime.
- Each run expires when the next one is due, so a stuck queue doesn't pile up copies.
  Set `"options": {"expires": None}` to keep them.
- A run that couldn't be enqueued because of a broker hiccup is retried on the next tick.

## crontab

`crontab(minute, hour, day_of_week, day_of_month, month_of_year)` uses Celery's
signature. Fields accept ints, iterables or cron syntax (`"*/15"`, `"1-5"`, `"mon-fri"`,
`"jan,jul"`). `crontab.from_string("0 3 * * *")` and the `@daily`/`@hourly` aliases work.
DST is handled: times that don't exist are skipped, and repeated times run once.
