# Migrating from schedule

[schedule](https://schedule.readthedocs.io/) (`pip install schedule`) runs jobs inside
your own process: a `while True` loop calls `schedule.run_pending()`, and each job runs
right there, one after another. potatoq's periodic tasks run in its workers instead,
from the `beat_schedule` setting.

What you gain:

- **Run as many worker processes as you like.** Every worker runs the scheduler, and
  each run is sent once, however many workers there are. Two copies of a `schedule`
  loop run every job twice.
- **A slow job doesn't hold up the others.** Each run is a task, picked up by whichever
  worker process is free. In `schedule`, a job that takes ten minutes delays every job
  due in those ten minutes.
- **Failures are handled like any task's**: time limits, retries if you ask for them,
  and failed runs kept in the dead-letter store (`potatoq dead list`). In `schedule`, an
  exception stops your loop unless you catch it.
- **Deploys don't lose runs**: a run due while the workers restart is sent when they
  start, if it's at most 60 seconds late.

## 1. Turn the jobs into tasks

```python title="Before"
import schedule


def send_digest(kind):
    ...


schedule.every().day.at("10:30").do(send_digest, "daily")
```

```python title="After: shop/tasks.py"
from potatoq import shared_task


@shared_task
def send_digest(kind):
    ...
```

Arguments become task arguments, so they must be JSON: pass ids and plain data, not
objects.

## 2. Move the schedule to settings

```python title="settings.py (or app.conf.beat_schedule = {...})"
from datetime import timedelta

from potatoq.schedules import crontab

POTATOQ_BEAT_SCHEDULE = {
    "daily digest": {
        "task": "shop.tasks.send_digest",
        "schedule": crontab(hour=10, minute=30),
        "args": ("daily",),
    },
    "sync": {"task": "shop.tasks.sync", "schedule": timedelta(minutes=10)},
}
```

| schedule | `beat_schedule` |
|---|---|
| `every(10).minutes` | `timedelta(minutes=10)` |
| `every().hour` | `timedelta(hours=1)`, or `crontab(minute=0)` for on the hour |
| `every().hour.at(":30")` | `crontab(minute=30)` |
| `every().day.at("10:30")` | `crontab(hour=10, minute=30)` |
| `every().monday.at("13:15")` | `crontab(day_of_week="mon", hour=13, minute=15)` |
| `every().day.at("12:42", "Europe/Amsterdam")` | `crontab(hour=12, minute=42, tz="Europe/Amsterdam")` |
| `.do(job, "a", key="b")` | `"args": ("a",), "kwargs": {"key": "b"}` |
| `every().minute.at(":17")` | `timedelta(minutes=1)`: no second-level timing |
| `every(5).to(10).minutes` (random) | a fixed interval: there's no random one |
| `.until("18:30")` | check the time in the task, or remove the entry later |
| a job returning `schedule.CancelJob` (run once) | `task.apply_async(eta=...)` or `countdown=`, not a schedule |
| `.tag(...)`, `schedule.clear(tag)` | edit the setting and deploy |

Things to check:

- **Time zone.** `schedule` reads `at("10:30")` in the machine's local time. potatoq's
  crontabs use the `timezone` setting: `UTC` by default, or `TIME_ZONE` in Django. Set
  `timezone`, or pass `tz=` to a crontab, if the jobs must run at local wall-clock times.
- **Intervals are aligned to the clock**, not to when the process started:
  `timedelta(minutes=10)` runs at :00, :10, :20 and so on, every time, wherever the
  workers restarted.
- **Overlap.** A run that's still going when the next one is due doesn't stop it: the
  next run is queued and starts on the next free worker process. If a job must not
  overlap with itself, take a lock in the task.

Check the result with `potatoq schedule`, which lists every entry and its next run, or
in a test with [`due` and `tick`](guide/testing.md#periodic-tasks).

## 3. Replace the loop

Delete the `while True: schedule.run_pending(); time.sleep(1)` loop, and the process or
container that ran it, and start a worker instead:

```console
$ potatoq -A proj worker
```

Stop the old loop in the same deploy in which the workers start; running both runs
every job twice. Prefer a process that only schedules? `potatoq beat` sends the
periodic tasks without running any, and `potatoq worker --no-scheduler` runs tasks
without sending them.

See [periodic tasks](guide/periodic-tasks.md) for the rest: expiry of runs nobody
picked up in time, missed runs and catching up.

## Coming from APScheduler

The same steps apply. `CronTrigger(hour=10, minute=30)` becomes `crontab(hour=10,
minute=30)`, `IntervalTrigger(minutes=10)` becomes `timedelta(minutes=10)`, and a
`DateTrigger` (one run at a set time) becomes `task.apply_async(eta=...)`. APScheduler's
job stores (jobs added at runtime and kept in a database) have no equivalent yet:
schedules live in settings ([wishlist](wishlist.md)).
