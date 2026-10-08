# Roadmap and wishlist

What potatoq doesn't do yet. It covers known gaps, features people expect from the
Celery ecosystem, and ideas from the [research](design/research/celery-pain-points.md).
Contributions are very welcome: open an issue to discuss the design before starting
on anything large ([contributing guide](https://github.com/anze3db/potatoq/blob/main/CONTRIBUTING.md)).

Each item says *why* it matters, so the priority is easy to judge.

## Known gaps

Behaviour that's accepted for compatibility but not implemented yet.

- [ ] **Enforce `rate_limit`.** It's accepted (with a warning) but ignored. Plan: a
  **global**, broker-backed token bucket (Redis script, SQL row, result-backend counter
  for RabbitMQ). Celery's limit is per worker, which is rarely what you want.
- [ ] **`revoke(terminate=True)` for running tasks.** Waiting tasks are revoked today.
  For running ones, the supervisor knows which process runs which task, so it could
  inject `TaskRevokedError` (threads) or signal the process (prefork).
- [ ] **Redis Cluster.** Scripts touch keys of several queues, so they'd need per-queue
  hash tags and per-queue scripts.
- [ ] **RabbitMQ hard time limits without a result backend.** A killed task gets
  redelivered until the delivery limit, because only the dead process's channel could
  have acked it. Fix: have the supervisor dead-letter it on a channel of its own.
- [ ] **Windows.** Workers need `fork()` and POSIX signals. A spawn-based worker would
  be needed; WSL works today.
- [ ] **`solar` schedules** (sunrise/sunset), from Celery.

## Monitoring and operations

What people get from Flower and friends today.

- [ ] **Web dashboard.** Queues, running tasks, workers, dead letters (with replay),
  results and schedules. A Flower replacement, ideally mountable in Django, FastAPI
  and Starlette, or runnable standalone (`potatoq dashboard`).
- [ ] **Prometheus metrics.** Queue depth, **age of the oldest waiting task** (the
  metric to alert on, per Solid Queue and GoodJob), throughput, runtime histograms,
  failures, retries, dead letters and worker counts.
- [ ] **Health endpoint** for Kubernetes liveness and readiness probes (`--health-port`),
  plus a stack-dump signal for stuck workers.
- [ ] **OpenTelemetry**: carry trace context from `delay()` into the task, with spans
  for enqueue and execution.
- [ ] **Sentry**: Sentry's SDK instruments Celery automatically, but not potatoq. Build
  an integration on top of potatoq's signals.
- [ ] **Structured logging**: an optional JSON log format for the worker.

## Ecosystem

- [ ] **Editable periodic tasks**, like django-celery-beat: schedules stored in the
  database and changeable at runtime without a deploy.
- [ ] **Django admin** pages for dead letters, results and schedules.
- [ ] **pytest plugin**: fixtures for an isolated app, `drain`, and asserting what was
  enqueued.

## Features from the research

- [ ] **Unique tasks / deduplication keys**: at most one waiting copy of a task per
  key, with a TTL (Sidekiq Enterprise, Oban, River).
- [ ] **Concurrency limits**: at most N running tasks per key, queue or task type
  (Solid Queue semaphores, Oban queue limits).
- [ ] **Resumable long tasks**: tasks that checkpoint a cursor and continue after a
  deploy instead of restarting (Sidekiq iterable jobs, Rails continuations).
- [ ] **Batches with callbacks** for large fan-outs, with progress (Sidekiq batches).
  Chords cover the basic case.
- [ ] **Pause and resume queues** at runtime.
- [ ] **Catch-up window for missed periodic runs**: run missed fire times once a worker
  is back, opt-in per entry.
- [ ] **Weighted queue consumption** (`-Q critical:3,default:1`) in addition to fair
  rotation.

## Performance

- [ ] **Batched claims and acks**: claim several tasks per round trip and ack in batches
  (Postgres `unnest`, Redis multi-claim). Oban measured about 3× from async acks.
- [ ] **Faster RabbitMQ bulk enqueue**: pipeline publisher confirms instead of waiting
  on each one.
- [ ] **AMQP 1.0 transport for RabbitMQ 4.4**: native per-message delays instead of the
  TTL cascade.
- [ ] **Redis Streams backend** for Redis ≥ 8.8 (`XREADGROUP … CLAIM`, `XNACK`), as an
  alternative for queues without delays or priorities.
- [ ] **Partitioned results table on Postgres**: drop old partitions instead of deleting
  expired results.
- [ ] **Optional native codec** (Rust), only if measurements ever show serialization or
  AMQP frame parsing as the bottleneck ([why not yet](design/internals.md#rust)).

## Project

- [ ] **First release** (`26.1`) and the PyPI trusted publisher
  ([RELEASING.md](https://github.com/anze3db/potatoq/blob/main/RELEASING.md)).
- [ ] **Run the CI, docs and release workflows on GitHub for the first time.** They've
  only been checked locally (actionlint, zizmor), and the release-notes API call hasn't
  been exercised yet.
- [ ] **Benchmarks in CI**, to catch performance regressions.
- [ ] **Versioned docs**, one per release.

## Deliberately not planned

- **Celery's wire protocol.** potatoq has its own compact, versioned message format.
  Switching means new queues, while old Celery queues drain
  ([migration guide](migrating-from-celery.md)).
- **Pickle serialization.** JSON only: broker write access must not mean code execution.
- **gevent and eventlet pools.** `async def` tasks and `--threads` cover the same needs
  without monkey-patching.
