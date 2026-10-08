# Roadmap and wishlist

What potatoq doesn't do yet. It covers known issues and gaps, features people expect
from the Celery ecosystem, and ideas from the [research](design/research/celery-pain-points.md).
Contributions are very welcome: open an issue to discuss the design before starting
on anything large ([contributing guide](https://github.com/anze3db/potatoq/blob/main/CONTRIBUTING.md)).

Each item says *why* it matters, so the priority is easy to judge.

## Known issues

Bugs found in review before the first release, to be fixed after it. Each has a
reproducer; most need an unusual setup or a crash at the wrong moment.

### Canvas

- [ ] **A chord body can run twice.** A header task that counted itself and then lost
  its worker before acking is redelivered after the chord finished, and the brokers'
  `chord_part_done` hands back the results again. Fix: remember in the chord row that
  the callback was sent, and only return results while it wasn't.
- [ ] **Chords wait forever on header tasks that never finish.** A header task that
  expires, is revoked, rejected (`Reject`) or unregistered is never counted. Fix: count
  these terminal outcomes as failures, so the chord fails with `ChordError` like Celery.
- [ ] **Signatures by name only lose options as later chain steps or callbacks.**
  `Signature("remote.x").set(countdown=60, priority=7)` is sent without its ETA,
  priority, expiry or chord membership. Fix: build the message through the same stub
  task `send_task` uses.
- [ ] **An errback can fire twice** for a chain with an errback whose group step is
  followed by more steps, when one header task fails and a different one finishes last.
- [ ] **Eager mode skips some errbacks.** With `.apply()`, an errback attached to one
  later step doesn't fire when an earlier step fails (errbacks on the chain itself do).
- [ ] **Small API gaps:** chain results have no `.parent` (`res.parent.get()`), and
  `group.clone(**opts)` / `chord.clone(**opts)` ignore `opts`.

### Workers with `--threads`

- [ ] **A crashing task costs its neighbours a delivery.** When a process dies
  (segfault, OOM, `os._exit`), every task running in its other threads is requeued as a
  failed delivery, so a poison task can get healthy tasks dead-lettered with it. Fix:
  requeue them without counting, flagged to run alone next time so the culprit shows.
- [ ] **On RabbitMQ, a hard time limit costs its neighbours a delivery.** Tasks killed
  because another task in their process overran count toward the quorum queue's
  delivery limit and are eventually dead-lettered by RabbitMQ. Other brokers requeue
  them without counting. Fix: have the child nack them before the supervisor kills it.
- [ ] **Recycling idles the whole process.** At `max_tasks_per_child` or
  `max_memory_per_child` all threads stop taking tasks until the slowest one finishes,
  and the replacement process only starts then. Fix: report "draining" to the
  supervisor so it can start the replacement right away.
- [ ] **`-P threads` ignores `worker_threads` and `worker_concurrency`** from the
  config and defaults to 10 threads; pass `-c` for now.

### `django.tasks` backend

- [ ] **`get_result()` can raise `TaskResultDoesNotExist` for a valid id** while the
  task is finishing: it reads the result store and the queue separately. Fix: read the
  queue first, then the result.
- [ ] **Results on RabbitMQ and Redis.** With RabbitMQ and a result backend, a waiting
  task isn't found until it finishes. With Redis and no result backend,
  `supports_get_result` is true but results aren't stored. Fix: store results for
  Django tasks, and treat a missing record as waiting when the broker can't peek.
- [ ] **`USE_TZ = False`**: result timestamps are aware while Django's are naive, so
  subtracting them raises.
- [ ] **Arguments aren't normalized like Django's.** The raw arguments are sent instead
  of Django's JSON-normalized ones (bytes, integer dict keys, `range` and `deque` behave
  differently than with `ImmediateBackend`).
- [ ] **`task_always_eager`**: the result says READY although the task ran, signals
  fire in the wrong order, and `refresh()` raises.
- [ ] **Result details**: RUNNING results have no `started_at` or worker id;
  `get_result().task` forgets `.using()` overrides and the backend alias; autoretried
  attempts send `task_finished` with FAILED, and earlier attempts' errors aren't kept.

### Transactions and serialization

- [ ] **`group` and `chord` ignore `using=` and `enqueue_on_commit`**, from the call and
  from their tasks, so a group inside a SQLAlchemy session or a non-default Django
  database is sent before COMMIT. Fix: pass them to `app.publish` like `apply_async`.
- [ ] **A plain dict can be decoded as a tagged value.** A task argument like
  `{"__type__": "datetime", "__value__": "…"}` arrives as a `datetime`. Fix: escape
  dicts that use the tag keys when encoding.
- [ ] **Postgres wake-ups are delayed after an enqueue inside a transaction.** Its
  `NOTIFY` only goes out at COMMIT but still starts the 50 ms debounce, so other
  enqueues in that window wait for the next poll (up to `poll_interval`). Nothing is lost.

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
- [ ] **Catch up missed periodic runs.** A run due while every worker is down is skipped
  once it's 60 s late, because the scheduler starts fresh on restart. A daily job is
  lost if the only worker restarts around its fire time, where Celery's `beat` would
  send it once on restart. On RabbitMQ, leader failover (about 1 s, or about 60 s when a
  host is lost) also skips runs due in that window, and can send one twice.
  Plan: on start and on each tick, send the latest fire time if the next one isn't due
  yet. The broker already claims each fire time once, so this is safe to repeat. That
  needs claim records kept for at least one schedule interval (Redis keys expire after
  7 days today, so monthly jobs would re-run), and RabbitMQ's record of sent runs moved
  from the leader's memory into the result backend. Also set a shorter heartbeat
  (about 10 s) on the RabbitMQ leader connection to cut host-loss failover.

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
