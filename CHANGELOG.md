# Changelog

potatoq uses [calendar versioning](https://calver.org/): `YY.N` is the Nth release of
the year (`26.1`, `26.2`, … `27.1`). Alphas are pre-releases of the upcoming number
(`26.1a1`, `26.1a2`, then `26.1`). Each section lists the merged pull requests by
category, plus everyone who contributed to that release.

Notes under **Unreleased** are written by hand and become the "Highlights" of the
next release. Everything else is generated from pull request titles by
`scripts/release.py` when a release is prepared (see [RELEASING.md](https://github.com/anze3db/potatoq/blob/main/RELEASING.md)).

## Unreleased

## [26.1a9](https://github.com/anze3db/potatoq/releases/tag/26.1a9) - 2026-10-27

### Highlights

- **Dashboard: delete dead letters** that should never run again, with a **Delete**
  button next to **Retry** (both ask for confirmation first), and `potatoq dead delete`
  on the command line. Clicking a dead letter now shows its reason and traceback on
  Redis too, where results aren't stored by default.

### Changes

* Group all Dependabot updates into one monthly PR
* Dashboard: find dead letters when looking up a task, with Retry in the dialog
* Delete dead letters from the dashboard and CLI; confirm before retry and delete

<!-- Release notes generated using configuration in .github/release.yml at main -->



**Full Changelog**: https://github.com/anze3db/potatoq/compare/26.1a8...26.1a9

### Contributors

Thank you to everyone who contributed to this release: @anze3db

## [26.1a8](https://github.com/anze3db/potatoq/releases/tag/26.1a8) - 2026-10-22

### Highlights

- **🥔 A web dashboard: a Flower replacement with no extra dependencies.**
  [Docs](https://anze3db.github.io/potatoq/guide/dashboard/) ·
  [#3](https://github.com/anze3db/potatoq/issues/3)

  ![The potatoq dashboard: workers, queues, periodic tasks and dead letters](https://anze3db.github.io/potatoq/assets/images/dashboard.png)

  - **Live workers** and the tasks each one is running right now, **queue sizes**, and
    **periodic tasks** with their next run and when they were last sent.
  - **Dead letters** with their reason, and a **Retry** button to send one back.
  - **Any task by ID**: its state, arguments, timings, worker, and result or traceback.
    **Revoke** one that hasn't started yet.
  - It reads the broker and the result backend directly, so there are no events to turn
    on, nothing is lost when it restarts, and it works with every broker.
  - **Run it** with `potatoq dashboard`, on Flower's port 5555, with `--auth user:password`
    and `--read-only`. **Or mount it**: `include("potatoq.contrib.django.dashboard")`
    for Django staff users, `Dashboard(app).asgi` in FastAPI or Starlette, and
    `Dashboard(app)` as a WSGI app in Flask or gunicorn.

### Changes

* Add a bug report form and point questions to Discussions
* Docs: add link preview banners and Open Graph tags
* Wishlist: a chaos test that kills workers and checks no task is lost
* Add a web dashboard, a Flower replacement with no extra dependencies
* Changelog: a fuller highlight for the dashboard

<!-- Release notes generated using configuration in .github/release.yml at main -->



**Full Changelog**: https://github.com/anze3db/potatoq/compare/26.1a7...26.1a8

### Contributors

Thank you to everyone who contributed to this release: @anze3db

## [26.1a7](https://github.com/anze3db/potatoq/releases/tag/26.1a7) - 2026-10-17

### Highlights

- **`None` now means "no limit" for `time_limit`, `soft_time_limit` and `max_retries`**
  on a task (`@app.task(...)` and Django's `@task(...)`), as in Celery. They used to fall
  back to the app-wide settings, so `time_limit=None` still killed the task after 30
  minutes and `max_retries=None` stopped after 10 retries. Leave an option out to use
  the app-wide setting. `0` also turns the time limits off. On RabbitMQ, a task without
  a time limit raises the consumer timeout to a day.
- **`potatoq worker --time-limit 0`** (and `--soft-time-limit 0`) turn the limits off;
  `0` used to be ignored.

### Changes

* Release with REST-only gh calls; --no-dry-run pushes
* Let time_limit=None turn a task's time limit off
* Let None mean no limit for soft_time_limit and max_retries; --time-limit 0
* Stop tracking a local Redis dump.rdb
* Name every task the deploy check test finds when it fails
* Finalize the Django app before the deploy check test filters its tasks

<!-- Release notes generated using configuration in .github/release.yml at main -->



**Full Changelog**: https://github.com/anze3db/potatoq/compare/26.1a6...26.1a7

### Contributors

Thank you to everyone who contributed to this release: @anze3db

## [26.1a6](https://github.com/anze3db/potatoq/releases/tag/26.1a6) - 2026-10-16

### Changes

* New Relic: activate the agent only in worker processes, so forked ones report
* Celery guide: point celery multi users to the systemd example
* Add potatoq.testing.reset() for brokers that outlive a test run
* Tests: cover New Relic's periodic-task attribute

<!-- Release notes generated using configuration in .github/release.yml at main -->



**Full Changelog**: https://github.com/anze3db/potatoq/compare/26.1a5...26.1a6

### Contributors

Thank you to everyone who contributed to this release: @anze3db

## [26.1a5](https://github.com/anze3db/potatoq/releases/tag/26.1a5) - 2026-10-16

### Highlights

- **Tracing, APM and error tracking**: integrations for OpenTelemetry (also Logfire and
  Datadog), Sentry (errors, tracing, opt-in Crons) and New Relic, with trace context carried
  from `.delay()` into the task, and a `potatoq.instrumentation` API for your own.
- **Eager mode respects transactions**: with `task_always_eager`, `.delay()` inside a
  transaction runs the task when it commits, as a worker would see it.
- **Django deploy check** (`manage.py check --deploy`) for tasks workers can't import,
  and a warning when `CELERY_BEAT_SCHEDULER` still points at django-celery-beat.

### Changes

* Tests: connect the consumer before timing a broker-held ETA
* Eager mode: run .delay() inside a transaction when it commits, like a worker
* Django: deploy check for tasks that workers can't import
* Celery guide: django-celery-results/beat, concrete cutover steps and scripts to move leftover tasks
* Test the deploy check with workers that import the URLconf
* Add OpenTelemetry, Sentry and New Relic integrations with distributed tracing
* Django: use the memory broker for an in-memory SQLite database, not ./potatoq.sqlite3
* Django: close the async ORM's connections around async def tasks
* drain() and tick() import task modules like a starting worker
* Add a guide for migrating from schedule (and APScheduler)
* Sentry docs: Crons is opt-in; show how to monitor only some entries

<!-- Release notes generated using configuration in .github/release.yml at main -->



**Full Changelog**: https://github.com/anze3db/potatoq/compare/26.1a4...26.1a5

### Contributors

Thank you to everyone who contributed to this release: @anze3db

## [26.1a4](https://github.com/anze3db/potatoq/releases/tag/26.1a4) - 2026-10-13

### Changes

* Dramatiq guide: max_retries defaults to 10 now
* Supervisor: don't spin a core when the schedule is off or while shutting down

<!-- Release notes generated using configuration in .github/release.yml at main -->



**Full Changelog**: https://github.com/anze3db/potatoq/compare/26.1a3...26.1a4

### Contributors

Thank you to everyone who contributed to this release: @anze3db

## [26.1a3](https://github.com/anze3db/potatoq/releases/tag/26.1a3) - 2026-10-13

### Changes

* release --push waits for CI and the Release workflow, then reports the outcome
* SQLite broker: refuse SQLite older than 3.35 with a clear error
* Document the minimum PostgreSQL, Redis/Valkey, RabbitMQ and SQLite versions; test them in CI
* Tests: restore the shared worker app's configuration after each test
* drain() skips ahead to countdowns and retries on Postgres and Redis too
* Keep the banner's labels when a project has its own logging setup
* Docs: retries are opt-in, said plainly; a Migrating section with a Dramatiq guide
* Docs: the same Requirements notice on every broker page
* Docs home: the install command is a code block, not a pill
* Docs home: crashes are requeued, exceptions are retried only on request
* Defaults tables: what happens on an exception sits right below what happens on a crash
* task_autoretry_for: retry every task on these exceptions, in one setting
* Docs: retrying every task, how many times, backoff and jitter
* Retry up to 10 times by default, about 50 minutes, instead of 3
* Defaults tables: a task that raises is dead-lettered, said in the same row
* Django tasks: refresh() of a task that's just finishing no longer raises

<!-- Release notes generated using configuration in .github/release.yml at main -->



**Full Changelog**: https://github.com/anze3db/potatoq/compare/26.1a2...26.1a3

### Contributors

Thank you to everyone who contributed to this release: @anze3db

## [26.1a2](https://github.com/anze3db/potatoq/releases/tag/26.1a2) - 2026-10-09

### Highlights

- `potatoq --version` (also `-V`) prints the potatoq and Python versions.
- Tests can no longer queue tasks into the development database: with the Django
  database as broker, `.delay()` writes through Django's connection, so it follows the
  test database and a test without `django_db` fails with pytest-django's error.
- `potatoq.testing.due()` and `tick()` test periodic task schedules.
- `potatoq/worker` is a regular package (it had no `__init__.py`).
- `SIGHUP` reloads the worker in place, like gunicorn: running tasks finish, then the same
  process starts again with the new code (`systemctl reload` with `ExecReload`). Before,
  it killed the worker on the spot.
- Signal handling: the worker's signal handlers only record the signal and its main
  loop acts on it, so two SIGTERMs at once (systemd and `uv run` each send one) no
  longer log "Shutting down" and send `worker_shutting_down` twice. `potatoq beat` stops
  cleanly on SIGTERM, and a child process whose supervisor died exits without a
  `BrokenPipeError` traceback.
- A SIGTERM that reached a just-forked worker process before it had installed its own
  signal handlers was lost, so stopping the worker shortly after a crash or a process
  recycle waited out the whole 25 s shutdown timeout. Signals are now blocked across
  `fork()` until the child is ready.

### Changes

* Release on the push to main that merging a release PR creates: a merged PR's run has ref refs/pull/N/merge, which the main-only pypi environment rejects
* potatoq --version
* Tests can't queue tasks into the development database; potatoq.testing.due/tick
* Docs home: benchmark table shows enqueue and process throughput for both potatoq and Celery
* SIGHUP reloads the worker in place
* Signal handlers only record the signal; the supervisor's main loop acts on it
* Block signals across fork() so a new child can't lose a SIGTERM; run tests in parallel
* CI: keep RabbitMQ scheduler-leader tests on one xdist worker; run CPU-timing tests alone
* Release from a local commit, published only after CI passes
* Threads: don't leave Python 3.11's async-exception flag raised after every task
* CI: measure coverage on every Python version and combine it
* SQLAlchemy: track every sessionmaker passed to install()
* Tests: wait for RabbitMQ scheduler leadership before checking periodic dedup
* Add a Justfile for tests, coverage, lint, docs, benchmarks and releases
* just release previews the next alpha; --no-dry-run commits it

<!-- Release notes generated using configuration in .github/release.yml at main -->



**Full Changelog**: https://github.com/anze3db/potatoq/compare/26.1a1...26.1a2

### Contributors

Thank you to everyone who contributed to this release: @anze3db

## [26.1a1](https://github.com/anze3db/potatoq/releases/tag/26.1a1) - 2026-10-09

### Highlights

The first release of potatoq: a Celery-compatible task queue with production-ready defaults.
**This is an alpha release** in limited production use: use it in production at your own
risk, and expect APIs and defaults to change while potatoq matures.

- **Celery-compatible API**: `Potatoq` (also importable as `Celery`), `@app.task`,
  `@shared_task`, `delay`/`apply_async`, `AsyncResult`, `chain`/`group`/`chord`,
  signals, `beat_schedule` and `crontab`, plus `CELERY_*` settings.
- **Four native brokers**: PostgreSQL (`SKIP LOCKED`, `LISTEN/NOTIFY`, transactional
  enqueue), Redis/Valkey (Lua scripts, leases), RabbitMQ (quorum queues, publisher
  confirms, TTL delay cascade) and SQLite (WAL, `data_version` wake-ups), plus `memory://`
  for tests.
- **Safe defaults**: ack after completion, requeue on worker crash with a poison-message
  limit, one task per idle process, broker-side ETAs, 30-minute time limits, exponential
  retry backoff, a dead-letter store, process recycling and a 25-second graceful shutdown.
- **Prefork workers** with optional `--threads`, enforcing time limits even with threads,
  and support for free-threaded Python.
- **Scheduler in every worker**, deduplicated through the broker: no separate `beat`.
- **Integrations**: Django (zero config, the database as broker, enqueue on commit,
  `django.tasks` backend), Flask, FastAPI and SQLAlchemy.
- `async def` tasks, `potatoq.testing.drain()`, and a `potatoq` CLI with `status`,
  `queues`, `dead` and `inspect`.
- Python 3.11–3.15, including free-threaded builds, on Linux and macOS (Windows isn't supported).
- Fully typed (`mypy` clean, ships `py.typed`), 100% test coverage, MIT licensed.

<!-- Release notes generated using configuration in .github/release.yml at main -->



**Full Changelog**: https://github.com/anze3db/potatoq/commits/26.1a1

### Contributors

Thank you to everyone who contributed to this release: @anze3db
