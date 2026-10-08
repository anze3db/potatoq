# Changelog

potatoq uses [calendar versioning](https://calver.org/): `YY.N` is the Nth release of
the year (`26.1`, `26.2`, … `27.1`). Each section lists the merged pull requests by
category, plus everyone who contributed to that release.

Notes under **Unreleased** are written by hand and become the "Highlights" of the
next release. Everything else is generated from pull request titles by
`scripts/release.py` when a release is prepared (see [RELEASING.md](https://github.com/anze3db/potatoq/blob/main/RELEASING.md)).

## Unreleased

The first release of potatoq: a Celery-compatible task queue with production-ready defaults.
**This is an alpha release**: use it in production at your own risk, and expect APIs and
defaults to change while potatoq matures.

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
