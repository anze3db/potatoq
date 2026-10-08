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
app.conf.timezone = "Europe/Ljubljana"   # crontabs run in this timezone (default UTC; with potatoq.contrib.django, Django's TIME_ZONE)
```

Celery's `on_after_configure` idiom works too:

```python
@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(10.0, ping.s(), name="ping every 10s")
```

Check what will run, and when:

```console
$ potatoq -A proj schedule
nightly-report: reports.build <crontab: 0 3 * * * (m/h/dM/MY/d)> next=2026-10-09 03:00:00 Europe/Ljubljana
ping: monitoring.ping <schedule: every 30s> next=2026-10-08 16:41:30 Europe/Ljubljana
```

Workers log the same list when they start, and warn about entries whose task isn't
registered, so a typo shows up at deploy time rather than as a dead letter at fire time.

### Checking that they run

To alert on a periodic task that stopped running, ask when each entry was last sent:

```python
app.control.last_periodic_runs()
# {"nightly-report": datetime(2026, 10, 9, 3, 0, tzinfo=UTC), ...}
```

These are the fire times the scheduler sent, from the broker's claims (Postgres and
SQLite keep a week of them, Redis the latest per entry; RabbitMQ keeps none).
`potatoq schedule` shows them too. A task sent by the scheduler carries the entry's name
in `self.request.headers["periodic"]`, so a bound task can record its own runs.

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
runs twice), the classic `celery beat` failure modes. (RabbitMQ has a short exception
during leader failover, see [below](#rabbitmq-leader-failover).) Use `potatoq worker --no-scheduler`
on workers that shouldn't schedule, or `potatoq beat` for a dedicated process. `-P solo`
workers schedule too, between tasks.

## Missed runs

- **Runs are only sent while a worker is up.** A starting worker sends runs that fell due
  in the last 60 s, so a quick restart doesn't lose one (the broker's claim still sends
  it only once). A run that's due while every worker is down for longer is skipped. For
  example, with a single worker down from 11:55 to 12:05, a daily 12:00 job doesn't run
  that day. Run two or more workers and restart them one at a time, so one is always
  scheduling. Catching up after longer downtime is on the
  [wishlist](../wishlist.md#known-gaps). On RabbitMQ, where only the leader's memory
  knows what was sent, a starting worker doesn't look back.
- Each run expires when the next one is due, so a stuck queue doesn't pile up copies.
  Set `"options": {"expires": None}` to keep them.
- A run that couldn't be enqueued because of a broker hiccup is retried on the next tick,
  as long as it's less than 60 s late.

### RabbitMQ leader failover

On RabbitMQ only the leader, the worker holding the token, sends periodic runs. When it
goes away, RabbitMQ hands the token to the next worker once it notices the connection is
gone:

| How the leader stops | Failover takes about |
|---|---|
| Clean shutdown, or the process crashes while the host stays up | 1 s |
| The host dies, or the network is partitioned | 60 s (RabbitMQ's default heartbeat timeout) |

Runs due during failover are skipped: the other workers don't send them, and the new
leader doesn't know what the old one already sent. In the slow case, the old leader can
also keep sending until it notices it lost the connection, so a run due in that window
can be sent twice. Redis, Postgres and SQLite claim each fire time in the broker and
don't have these gaps.

## crontab

`crontab(minute, hour, day_of_week, day_of_month, month_of_year)` uses Celery's
signature. Fields accept ints, iterables or cron syntax (`"*/15"`, `"1-5"`, `"mon-fri"`,
`"jan,jul"`). `crontab.from_string("0 3 * * *")` and the `@daily`/`@hourly` aliases work.
DST is handled: times that don't exist are skipped, and repeated times run once.
