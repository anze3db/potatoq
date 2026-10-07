# Python Celery alternatives (and prior art from other ecosystems): research for Potatoq

Research date: 2026-10-07. Versions checked: Dramatiq 2.2.1 (2026-09), RQ 2.12 (2026-08), Huey 3.4.0 (2026-09), arq 0.28.0 (2026-04, maintenance-only), Taskiq 0.13.0 (2026-09), Procrastinate 3.10.0 (2026-09), SAQ (master), Chancy 0.25.1, Hatchet v0.110.x, django-q2 1.11.1, django-tasks 0.12.0, Django 6.0/6.1, River (Go), Oban 2.24.

Defaults were checked against source code wherever possible (raw GitHub files). Where a number comes from memory or a secondary source, the text says so.

---

## 0. Why people leave Celery (the problems Potatoq has to fix)

Most-reacted Celery issues ([search](https://github.com/celery/celery/issues?q=is%3Aissue+sort%3Areactions-%2B1-desc)):

| Issue | Reactions | Theme |
|---|---|---|
| [#6552 Support async function](https://github.com/celery/celery/issues/6552) / [#7874](https://github.com/celery/celery/issues/7874) / [#3884](https://github.com/celery/celery/issues/3884) | 127/61/36 | No asyncio support |
| [#5149 PostgreSQL as a broker](https://github.com/celery/celery/issues/5149) | 96 | People want a Postgres broker |
| [#4843 Continuous memory leak](https://github.com/celery/celery/issues/4843) | 82 | Worker memory growth |
| [#1599 Concurrency per queue](https://github.com/celery/celery/issues/1599) | 59 | Missing per-queue concurrency |
| [#3759 Tasks received but not executing](https://github.com/celery/celery/issues/3759), [#4185 workers hang on IPC](https://github.com/celery/celery/issues/4185) | 53/28 | Prefork/IPC hangs |
| [#4400 Same task runs multiple times at once](https://github.com/celery/celery/issues/4400) | 48 | Redis visibility_timeout combined with ETA/countdown causes duplicates |
| [#3773 Couldn't ack, BrokenPipe](https://github.com/celery/celery/issues/3773) | 47 | AMQP connection/heartbeat fragility |
| [#5410 confirm_publish defaults to False](https://github.com/celery/celery/issues/5410) | 41 | Unsafe default: publishes are fire-and-forget |
| [#4079 liveness/readiness probes](https://github.com/celery/celery/issues/4079) | 43 | No k8s health checks |
| [#9092 Valkey support](https://github.com/celery/celery/issues/9092) | 36 | Backend lag |

HN comments on Celery's defaults:
- "long standing bugs / poor defaults, like prefetching tasks so they can get stuck behind long running tasks" ([HN 30127847](https://news.ycombinator.com/item?id=30127847)); "weird performance gotchas (like the workers prefetch jobs which is terrible if they aren't all the same size)" ([HN 34339618](https://news.ycombinator.com/item?id=34339618)); one user found prefetch "a deal breaker" for long tasks and switched to RQ ([HN 30129111](https://news.ycombinator.com/item?id=30129111)).
- "It was difficult to debug, going through Celery's layers of code that try to make various backends present the same interface" ([Dramatiq Show HN](https://news.ycombinator.com/item?id=15681066)).
- Celery's own author (asksol) acknowledged that the defaults are not tuned for small jobs ([HN 1710446](https://news.ycombinator.com/item?id=1710446)).
- From the Hatchet launch thread: "the observability is pretty bad. Even if you use Celery Flower, it still just doesn't give me enough insight" ([HN 39643136](https://news.ycombinator.com/item?id=39643136)).

Dramatiq's [motivation page](https://dramatiq.io/motivation.html) lists these Celery problems: tasks are acked as soon as a worker pulls them; delayed tasks sit on the normal queue and are held in worker memory (which breaks queue-size-based autoscaling); the code is "spread across 3 different projects (celery, billiard and kombu) and it's impenetrable", with runtime stack-frame manipulation; there is no global prioritization.

Celery defaults for reference (from memory of the Celery docs; worth re-verifying in the Celery-specific research): `worker_prefetch_multiplier=4`, `task_acks_late=False`, `task_reject_on_worker_lost=False`, no time limit, Redis `visibility_timeout=3600`, `result_expires=1 day`, `max_retries=3` / `default_retry_delay=180s` (used only when `self.retry()` is called explicitly), `confirm_publish=False`.

---

## 1. Dramatiq

Links: [docs](https://dramatiq.io/), [motivation](https://dramatiq.io/motivation.html), [cookbook](https://dramatiq.io/cookbook.html), [GitHub](https://github.com/Bogdanp/dramatiq). LGPL (it started as AGPL plus a paid commercial license, and the AGPL drew heavy pushback in the [2017 Show HN](https://news.ycombinator.com/item?id=15681066)).

### Defaults (from source)
| Setting | Default | Source |
|---|---|---|
| Ack mode | Ack after processing (acks late) | motivation page |
| `max_retries` | **20** | [retries.py](https://github.com/Bogdanp/dramatiq/blob/master/dramatiq/middleware/retries.py) |
| `min_backoff` | **15 s** | same |
| `max_backoff` | **7 days** | same |
| Backoff formula | `factor * 2**attempts`, jitter ×U(1,2), and when over the cap, U(0.5,1)×max | [common.py `compute_backoff`](https://github.com/Bogdanp/dramatiq/blob/master/dramatiq/common.py) (2.0 fixed it to respect min_backoff) |
| `retry_when` / `throws` / `on_retry_exhausted` | predicate / exception allowlist that skips retries / actor called when retries run out | retries.py |
| `time_limit` | **10 min** (600,000 ms), checked every 1 s | [time_limit.py](https://github.com/Bogdanp/dramatiq/blob/master/dramatiq/middleware/time_limit.py) |
| `max_age` (AgeLimit) | None | reference |
| Results | Opt-in middleware, and since 2.0 `backend` is required. Result TTL **10 min** | cookbook, [2.0 notes](https://github.com/Bogdanp/dramatiq/releases/tag/v2.0.0) |
| Worker | processes = CPU count, **threads = 8** | [worker.py](https://github.com/Bogdanp/dramatiq/blob/master/dramatiq/worker.py) |
| Queue prefetch | `min(threads*2, 65535)`, so 16 | worker.py |
| Delay-queue prefetch | `min(threads*1000, 65535)`, so 8000 | worker.py |
| Redis `heartbeat_timeout` | 60 s | [redis broker](https://github.com/Bogdanp/dramatiq/blob/master/dramatiq/brokers/redis.py) |
| Dead-message TTL (Redis & RabbitMQ) | **7 days** | broker sources |
| Redis maintenance | Probabilistic: `maintenance_chance=1000`, roughly 1-in-1M commands | redis.py |
| RabbitMQ `confirm_delivery` | **False** | [rabbitmq broker](https://github.com/Bogdanp/dramatiq/blob/master/dramatiq/brokers/rabbitmq.py) |
| RabbitMQ `max_priority` | None | same |
| Default middleware | AgeLimit, TimeLimit, ShutdownNotifications, Callbacks, Pipelines, Retries (Prometheus became optional in 2.0) | [middleware/__init__.py](https://github.com/Bogdanp/dramatiq/blob/master/dramatiq/middleware/__init__.py) |

Note on retry span: 20 retries from a 15 s base, doubling, jittered and capped at 7 days, add up to roughly **3–6 weeks** before a message is dead-lettered. That is a long time for most web-app tasks.

### Architecture worth copying
- **Redis Lua dispatch script** ([dispatch.lua](https://github.com/Bogdanp/dramatiq/blob/master/dramatiq/brokers/redis/dispatch.lua)). Each queue is a list of message IDs (`ns:queue`) plus a hash of payloads (`ns:queue.msgs`). Each worker has an ack set (`ns:__acks__.<worker_id>.<queue>`). A heartbeats ZSET (`ns:__heartbeats__`) holds worker liveness, and the dead-letter queue is `queue.XQ` (a ZSET by timestamp plus a msgs hash). The fetch command pops up to `prefetch` IDs into the worker's ack set atomically. Maintenance moves unacked messages owned by dead workers (heartbeat older than timeout) back to the queue and purges expired DLQ entries.
- **Delay queues** (`queue.DQ`) are separate queues, so the main queue length stays an honest autoscaling signal.
- **Middleware architecture** with hooks (`before_enqueue`, `before_process_message`, `after_process_message`, `after_skip_message`, worker boot/shutdown, and so on). Retries, time limits, age limits, callbacks, pipelines, results and Prometheus are all middleware.
- **Rate limiters** are separate from tasks: `ConcurrentRateLimiter` (a distributed mutex when limit=1), `BucketRateLimiter` and `WindowRateLimiter`, on Redis or Memcached backends. When the limit is hit, the default raises and the task is retried with backoff.
- **TimeLimit** injects `TimeLimitExceeded` into the worker thread with `PyThreadState_SetAsyncExc`. The source admits that it "can't cancel system calls" and only fires when the thread next acquires the GIL.
- Composition: `group`, `pipeline` (`a.message() | b.message()`), `pipe_ignore`.
- `StubBroker` for tests; `join(fail_fast=True)` became the default in 2.0.

### What they got wrong / community complaints
- **Delayed messages are still held in worker memory.** On both brokers the worker consumes the `.DQ` queue with `delay_prefetch` (8000 by default) and keeps messages in an in-process `PriorityQueue` until their ETA, then re-enqueues them to the canonical queue and acks the DQ copy. That is better than Celery (separate queue) but has the same failure modes:
  - [#431 "Delayed messages are being duplicated on work queue"](https://github.com/Bogdanp/dramatiq/issues/431): copy-then-ack is not atomic, so a crash between the two steps duplicates the message.
  - [PR #434](https://github.com/Bogdanp/dramatiq/pull/434): with many delayed messages, the Lua stack overflows in the prefetch.
  - An external guide recommends lowering the default delay prefetch to avoid cold-start memory pressure ([markaicode](https://markaicode.com/architecture/scalable-dramatiq-architecture-production/)).
- `confirm_delivery=False` on RabbitMQ is the same unsafe default as Celery's.
- Probabilistic maintenance (1 in a million) makes dead-worker recovery latency unpredictable on quiet queues.
- No built-in cron (the docs point to APScheduler or periodiq). No Redis priorities ([#412](https://github.com/Bogdanp/dramatiq/issues/412)). No pipelines of groups ([#103](https://github.com/Bogdanp/dramatiq/issues/103)). Actor registry is coupled to the broker ([#324](https://github.com/Bogdanp/dramatiq/issues/324)).
- Configuring logging inside the library was a top complaint ([#48](https://github.com/Bogdanp/dramatiq/issues/48)). Lesson: never call `logging.basicConfig`.
- Bus factor: Bogdan posted ["Maintainer Wanted" #680](https://github.com/Bogdanp/dramatiq/issues/680), and a team now maintains it (LincolnPuzey and others). 2.0 shipped in Nov 2025.
- Asyncio support was added late, through the `AsyncIO` middleware.

---

## 2. RQ (python-rq)

Links: [docs](https://python-rq.org/docs/), [workers](https://python-rq.org/docs/workers/), [defaults.py](https://github.com/rq/rq/blob/master/rq/defaults.py), [CHANGES](https://github.com/rq/rq/blob/master/CHANGES.md).

### Defaults
| Setting | Default |
|---|---|
| `job_timeout` | **180 s** (`Queue.DEFAULT_TIMEOUT`; enforced with SIGALRM via `UnixSignalDeathPenalty`) |
| `result_ttl` | **500 s** |
| `failure_ttl` | **1 year** |
| `ttl` (time a job may sit in the queue) | None (infinite) |
| Retries | **None by default**. `Retry(max=3, interval=[10, 30, 60])` is opt-in |
| `DEFAULT_WORKER_TTL` | 420 s |
| `DEFAULT_JOB_MONITORING_INTERVAL` | 30 s |
| `DEFAULT_MAINTENANCE_TASK_INTERVAL` | 600 s |
| Callback timeout | 60 s |
| Scheduler fallback period | 120 s |

### Design
- **Fork per job.** The worker forks a "work horse" for every job, which gives isolation and no memory leaks across jobs. `SimpleWorker` runs in-process. `SpawnWorker` (2.2, 2025) covers Windows and macOS. `rq worker-pool` (2.5) runs N workers from one CLI.
- **Registries:** Started, Finished, Failed, Deferred (dependencies), Scheduled, Canceled ([registries](https://python-rq.org/docs/job_registries/)). `depends_on` provides job dependencies.
- 2.x: Redis Streams for results, multiple executions per job, a built-in scheduler (`--with-scheduler`), `Repeat` (2.3), `rq cron` / CronScheduler (2.4/2.5), and Valkey support. rq-scheduler is now mostly superseded.
- Dequeue strategies: `default` (strict queue order), `round_robin`, `random`.

### Complaints / what they got wrong
- **No acks.** Abandoned jobs (worker killed mid-job) are moved to the **FailedJobRegistry** with `AbandonedJobError`, not requeued ([exceptions docs](https://python-rq.org/docs/exceptions/)). That is effectively at-most-once unless you retry from the failed registry.
- **No in-process concurrency.** [#45 "Worker concurrency?"](https://github.com/rq/rq/issues/45) has been open since 2012. Fork per job costs CPU: Redash reported RQ workers needing much more CPU than Celery for the same load ([dev.to/redash](https://dev.to/redash/how-we-spotted-and-fixed-a-performance-degradation-in-our-python-code-4g5l)).
- macOS fork-safety crash ([#1418](https://github.com/rq/rq/issues/1418), 101 reactions). Lesson: prefork on macOS needs `OBJC_DISABLE_INITIALIZE_FORK_SAFETY` or spawn.
- Zombie workers ([#787](https://github.com/rq/rq/issues/787)), no per-queue rate limit ([#725](https://github.com/rq/rq/issues/725)), cancel running jobs ([#684](https://github.com/rq/rq/issues/684)), key prefix ([#1855](https://github.com/rq/rq/issues/1855)).
- Redis only.
- Community: praised as "rock solid for years", "5 minutes setup", with an API that is "pretty small and comprehensible" ([HN 35533590](https://news.ycombinator.com/item?id=35533590), [HN 20433061](https://news.ycombinator.com/item?id=20433061)). One user noted the circular dependency between app, Redis and Postgres ([HN 21944154](https://news.ycombinator.com/item?id=21944154)).

---

## 3. Huey (Charles Leifer)

Links: [API](https://huey.readthedocs.io/en/latest/api.html), [consumer](https://huey.readthedocs.io/en/latest/consumer.html), [guide](https://huey.readthedocs.io/en/latest/guide.html), [releases](https://github.com/coleifer/huey/releases).

### Defaults
| Setting | Default |
|---|---|
| Storage | `RedisHuey`, `PriorityRedisHuey`, `RedisExpireHuey`, `SqliteHuey`, `FileHuey`, `MemoryHuey`, peewee `SqlHuey` |
| `results` | True; `store_none=False`; **reads are destructive** (a result disappears after `get()` unless `preserve=True`) |
| `utc` | True |
| Serializer | **pickle** |
| `retries` / `retry_delay` | **0 / 0**. `retry_backoff` multiplier added in 3.3 (Jul 2026) |
| Consumer `workers` | **1** (the docs say "most applications will want at least 2") |
| Worker type | **thread** (process and greenlet also available) |
| Poll | initial 0.1 s, ×1.15 backoff, max 10 s |
| Scheduler interval | 1 s |
| Health check | every 10 s |
| Shutdown | SIGINT is graceful, SIGTERM is immediate (3.4 adds `--graceful-signal` and `--shutdown-timeout`) |
| `store_intermediate_errors` | True (3.2) |

### Strengths
- Periodic tasks built in (`@huey.periodic_task(crontab(...))`). Only one consumer should run the scheduler (`-n` disables it).
- **Immediate mode** (`huey.immediate = True`) runs tasks synchronously with in-memory storage, which suits tests.
- `lock_task` (TTL added in 3.4), `expires`, `revoke`, `RetryTask`, `CancelExecution`, signals (`SIGNAL_ERROR`, `SIGNAL_INTERRUPTED`, `SIGNAL_TIMEOUT`, and others), and pipelines with `.then()`.
- 3.0 (Apr 2026) added `group()` / `chord()`, `timeout` (SIGALRM for processes, gevent.Timeout for greenlets, cooperative only for threads), a fixed-window `rate_limit()`, and low-latency results via `notify_result=True` ([3.0.0](https://github.com/coleifer/huey/releases/tag/3.0.0)). 3.2 added a Django admin stats dashboard.
- Very small; no broker needed with SQLite ([HN 28568473](https://news.ycombinator.com/item?id=28568473), [HN 32731032](https://news.ycombinator.com/item?id=32731032)).

### What they got wrong
- **At-most-once by design.** "Huey does not guarantee at-least-once delivery" and in-flight tasks "will be lost" if the consumer dies. The suggested workaround is a `SIGNAL_INTERRUPTED` handler that re-enqueues, which only helps on SIGTERM, not SIGKILL or OOM ([consumer docs](https://huey.readthedocs.io/en/latest/consumer.html), [recipes](https://huey.readthedocs.io/en/latest/recipes.html)).
- pickle by default; destructive result reads surprise people; one default worker; no retries by default.
- Maintainer tone was criticized as "dismissive/borderline rude" ([HN 21950594](https://news.ycombinator.com/item?id=21950594)).

---

## 4. arq (Samuel Colvin / Pydantic)

Links: [docs](https://arq-docs.helpmanual.io/), [worker.py](https://github.com/python-arq/arq/blob/main/arq/worker.py), [#437 Future plan](https://github.com/python-arq/arq/issues/437), [#510 Maintenance only](https://github.com/python-arq/arq/issues/510).

### Defaults
| Setting | Default |
|---|---|
| `max_jobs` (concurrency per worker) | **10** |
| `job_timeout` | **300 s** |
| `keep_result` | **3600 s** |
| `poll_delay` | **0.5 s** (polling, not blocking) |
| `queue_read_limit` | `max(max_jobs*5, 100)` |
| `max_tries` | **5**. Retries happen only on `Retry` exceptions, cancellation/timeout, or worker shutdown, not on ordinary exceptions |
| `health_check_interval` | 3600 s (writes a health-check key) |
| `allow_abort_jobs` | False |
| Retry counter key TTL | 88,400 s |

### Design
- The queue is a **ZSET scored by the time the job should run**, so deferred jobs (`_defer_until`, `_defer_by`) and immediate jobs share one structure. `_job_id` gives uniqueness: a job with the same ID cannot be re-enqueued until its result expires.
- **"Pessimistic execution."** A job stays in the queue until it completes. A worker claims it with `WATCH`/`MULTI` and `PSETEX in_progress_key (timeout+10s)`. If the worker dies, the in-progress key expires and another worker re-runs the job.
- `raise Retry(defer=ctx['job_try'] * 5)` for custom backoff; cron jobs built in; asyncio only.

### What they got wrong
- Polling every 0.5 s plus optimistic WATCH/MULTI claiming means every worker contends on the head of one ZSET. SAQ claims to be up to 8× faster because it uses BLMOVE ([SAQ README](https://github.com/tobymao/saq)), and streaq claims 14× ([#437 comment](https://github.com/python-arq/arq/issues/437)).
- **Abandoned.** Colvin wrote "no one is more disappointed that we haven't found a way to work more on arq than me", and since 2025 it is in maintenance-only mode ([#510](https://github.com/python-arq/arq/issues/510)). Long-standing asks include cancel/abort ([#246](https://github.com/python-arq/arq/issues/246), [#290](https://github.com/python-arq/arq/issues/290)), monitoring ([#297](https://github.com/python-arq/arq/issues/297)), and newer Redis versions ([#454](https://github.com/python-arq/arq/issues/454)). Downloads are still about 3.5M/month, so an async Redis user base is waiting for a home.
- The planned redesign in #437 is a useful spec for Potatoq: ParamSpec type safety, **enqueue by name when worker code isn't importable** (tiangolo: "That's the main reason I had gone for Celery over RQ"), and **running sync `def` functions on threads alongside async ones** (also tiangolo).

---

## 5. Taskiq

Links: [architecture](https://taskiq-python.github.io/guide/architecture-overview.html), [CLI](https://taskiq-python.github.io/guide/cli.html), [middlewares](https://taskiq-python.github.io/available-components/middlewares.html), [FastAPI](https://taskiq-python.github.io/framework_integrations/taskiq-with-fastapi.html), [taskiq-redis](https://github.com/taskiq-python/taskiq-redis).

### Defaults
| Setting | Default |
|---|---|
| `--workers` (processes) | **2** |
| `--ack-type` | `when_saved` (options: `when_received`, `when_executed`, `when_saved`, `manual`) |
| `--max-prefetch` | **0** extra deliveries beyond async capacity |
| `--shutdown-timeout` | 5 s |
| Sync functions | Run in a ThreadPoolExecutor (`--max-threadpool-threads`) |
| Result backend | `DummyResultBackend` (stores nothing; [#427](https://github.com/taskiq-python/taskiq/issues/427) asks to warn users about this) |
| Retries | **Off**. `SimpleRetryMiddleware(default_retry_count=3)` or `SmartRetryMiddleware(default_retry_count=5, default_delay=10, use_jitter=True, use_delay_exponent=True, max_delay_exponent=120)`, and each task must also opt in with `retry_on_error=True` |
| Task discovery | `--tasks-pattern **/tasks.py` with `-fsd` |

### Strengths
- A clean broker / result-backend / middleware / schedule-source abstraction. Middleware hooks are `pre_send`, `post_send`, `pre_execute`, `on_error`, `post_execute` and `post_save`.
- **Dependency injection** (`TaskiqDepends`, FastAPI-style). `taskiq-fastapi` `init(broker, "app:app")` lets tasks reuse FastAPI dependencies such as `Request` and `app.state`.
- `InMemoryBroker` for tests. Async-native, and kicks with `.kiq()`.

### What they got wrong
- **Data-loss footguns in broker choice.** taskiq-redis `PubSubBroker` and `ListQueueBroker` "don't support acknowledgements… the message is going to be lost". Only `RedisStreamBroker` acks ([taskiq-redis](https://github.com/taskiq-python/taskiq-redis)).
- Results and retries are off unless you wire them up. Missing liveness/readiness commands ([#240](https://github.com/taskiq-python/taskiq/issues/240), [#303](https://github.com/taskiq-python/taskiq/issues/303)), unique jobs ([#271](https://github.com/taskiq-python/taskiq/issues/271)), and per-process Prometheus metrics ([#590](https://github.com/taskiq-python/taskiq/issues/590)).
- Many knobs and small separate packages; quality varies between broker packages.

---

## 6. Procrastinate (Postgres)

Links: [discussions](https://procrastinate.readthedocs.io/en/stable/discussions.html), [reference](https://procrastinate.readthedocs.io/en/stable/reference.html), [retry](https://procrastinate.readthedocs.io/en/stable/howto/advanced/retry.html), [stalled jobs](https://procrastinate.readthedocs.io/en/stable/howto/production/retry_stalled_jobs.html), [Django](https://procrastinate.readthedocs.io/en/stable/howto/django.html), [HN 2022](https://news.ycombinator.com/item?id=30126152).

### Defaults
| Setting | Default |
|---|---|
| Worker `concurrency` | **1** |
| `fetch_job_polling_interval` | **5 s** (fallback when LISTEN/NOTIFY misses something) |
| `abort_job_polling_interval` | 5 s |
| `listen_notify` | True |
| `delete_jobs` | **"never"** (also `"successful"`, `"always"`) |
| `update_heartbeat_interval` | 10 s |
| `stalled_worker_timeout` | 30 s |
| `shutdown_graceful_timeout` | None |
| Task `retry` | **False**. `retry=5` or `True` (infinite), or `RetryStrategy(max_attempts, wait / linear_wait / exponential_wait, retry_exceptions)` |
| `priority` | 0 (higher runs first) |
| Job timeout | none |

### Design
- Postgres with `LISTEN/NOTIFY` and `SELECT … FOR UPDATE SKIP LOCKED`. Job states: todo, doing, succeeded, failed, cancelled, aborted. Async at the core; sync tasks run through `asgiref.sync_to_async`, and there are separate sync and async defer paths.
- **`lock`**: jobs that share a lock string run sequentially (serialized execution). **`queueing_lock`**: at most one job with a given lock can be in `todo` (enqueue-time dedup through a unique index).
- Periodic tasks via `@app.periodic(cron=...)`, deduplicated in the DB (`procrastinate_periodic_defers`).
- Django: uses the Django DB settings, ships Django migrations, read-only admin models, and the management command worker.

### What they got wrong
- **Stalled-job recovery is DIY.** You write a periodic task that calls `get_stalled_jobs()` and then `retry_job()` ([docs](https://procrastinate.readthedocs.io/en/stable/howto/production/retry_stalled_jobs.html)).
- No retries, no timeout and no pruning by default (jobs are kept forever). Concurrency 1. Polling fallback of 5 s.
- The docs say they'd "like to develop real monitoring tools before we call this really ready for production".
- Community sentiment is very positive: "moved all our celery tasks to procrastinate… it has been great" ([HN 46086198](https://news.ycombinator.com/item?id=46086198)); "codebase is like one-tenth that of Celery" ([HN 46084615](https://news.ycombinator.com/item?id=46084615)); "Django with Procrastinate… works like a dream" ([HN 47330075](https://news.ycombinator.com/item?id=47330075)).

---

## 7. SAQ (Simple Async Queue, Toby Mao)

Links: [GitHub](https://github.com/tobymao/saq), [job.py](https://github.com/tobymao/saq/blob/master/saq/job.py).

- Backends are Redis and Postgres (SKIP LOCKED plus LISTEN/NOTIFY). The Redis backend uses **BLMOVE/RPOPLPUSH instead of polling**, claiming under 5 ms latency against arq's 0.5 s and "up to 8x faster than arq". Includes a web UI, heartbeats and a sweeper for abandoned jobs, and cron.
- **Job defaults:** `timeout=10` s (aggressive), `heartbeat=0` (disabled), `retries=1` (that is, one attempt and no retry), `ttl=600` s (result and info retention), `retry_delay=0.0`, `retry_backoff=False`, `priority=0` (Postgres only).
- Lesson: a 10 s default timeout is too aggressive for a general-purpose replacement, but low-latency blocking dequeue is the right call.

---

## 8. Chancy (TkTech, Postgres)

Links: [GitHub](https://github.com/TkTech/chancy), [docs](https://tkte.ch/chancy/), [HN 46084714](https://news.ycombinator.com/item?id=46084714).

- Postgres-only and async-first, with only psycopg3 as a dependency. Features: "rate limiting, global uniqueness, timeouts, memory limits, mix asyncio/processes/threads/sub-interpreters in the same worker, workflows, cron jobs, dashboard, metrics, django integrations, reprioritization, triggers, pruning, Windows support, queue tagging".
- **Queues are DB rows** with concurrency, executor, rate limit and polling interval, and can be created, paused or changed at runtime. Workers can be tagged so that queues route to particular machines (GPU, OS). Plugins: Pruner, Recovery, Leadership, Cron, Workflow, Metrics, Reprioritize.
- Planned django-tasks integration. Small adoption (about 290 stars) and pre-1.0 (0.25), but the feature list is close to "Oban for Python".

---

## 9. Hatchet

Links: [docs](https://docs.hatchet.run/home), [timeouts](https://docs.hatchet.run/home/timeouts), [retries](https://docs.hatchet.run/home/retry-policies), [HN v1](https://news.ycombinator.com/item?id=43572733), [HN launch](https://news.ycombinator.com/item?id=39643136).

- A separate Go engine plus Postgres (RabbitMQ was optional and removed as a requirement in v1). Workers connect over gRPC, so the engine polls Postgres on behalf of workers and individual workers don't hammer the DB. MIT licensed, with a cloud offering.
- **Defaults:** schedule timeout **5 min**, execution timeout **60 s** (refreshable and additive), retries **0**. Backoff via `backoff_factor` and `backoff_max_seconds`. `NonRetryableException`.
- Python API: `@hatchet.task(name=..., input_validator=PydanticModel)`, `def fn(input, ctx: Context)`. DAG workflows, durable tasks, concurrency keys (fairness per tenant), rate limits, priority.
- **Scaling notes from the authors:** "a simple Postgres queue utilizing FOR UPDATE SKIP LOCKED doesn't cut it at this scale" (5k+ tasks/s, 25k TPS). They fixed it with range partitioning of time-series tables, hash partitioning for event updates, separate monitoring and queue tables, **buffered reads/writes flushed every 10 ms**, and identity columns instead of UUIDs ("UUIDs caused some headaches… index bloat").
- Downsides: heavy (a separate service), and it is an orchestration platform rather than a drop-in for Celery.

---

## 10. django-q2

Links: [configure](https://django-q2.readthedocs.io/en/master/configure.html).

- **Defaults:** `workers` = CPU count; `recycle=500` tasks; **`timeout=None`**; `retry=60` s (re-delivery for unacked tasks); `max_attempts=0` (**infinite**); `ack_failures=False`; `save_limit=250` successful results; `queue_limit=workers**2`; ORM broker `poll=0.2` s; `bulk=1`. Brokers: ORM, Redis, SQS, MongoDB.
- **Main footgun:** `timeout` must be less than `retry`, otherwise a long task is re-delivered while still running and executes twice ([docs](https://django-q2.readthedocs.io/en/master/configure.html), [django-q #183](https://github.com/Koed00/django-q/issues/183)). The scheduler also duplicates tasks across clusters. Lesson: the visibility timeout (lease) must derive from the task time limit automatically and never be set independently.

---

## 11. Django 6.0/6.1 Tasks framework (DEP 0014): the compatibility target

Links: [topic guide 6.0](https://docs.djangoproject.com/en/6.0/topics/tasks/), [reference 6.1](https://docs.djangoproject.com/en/6.1/ref/tasks/), [6.1 release notes](https://docs.djangoproject.com/en/6.1/releases/6.1/), [DEP 14 announcement](https://www.djangoproject.com/weblog/2024/may/29/django-enhancement-proposal-14-background-workers/), [SC vote thread](https://forum.djangoproject.com/t/steering-council-vote-on-background-tasks-dep-14/31131), [django-tasks backport](https://github.com/RealOrangeOne/django-tasks), [django-tasks-db](https://github.com/RealOrangeOne/django-tasks-db), [critique](https://www.loopwerk.io/articles/2026/django-tasks-review/).

### API (verbatim signatures)
```python
from django.tasks import task, task_backends, default_task_backend

task(*, priority=0, queue_name="default", backend="default", takes_context=False, **kwargs)  # **kwargs new in 6.1 -> backend.task_class
Task.using(*, priority=None, backend=None, queue_name=None, run_after=None)  # returns a new Task
Task.enqueue(*args, **kwargs) -> TaskResult        # args must be JSON-serializable
Task.aenqueue(*args, **kwargs)
Task.get_result(result_id) / Task.aget_result(result_id)
Task.call(...) / Task.acall(...)                   # run inline

TaskResult: task, id (str, < 64 chars), status, enqueued_at, started_at, last_attempted_at,
            finished_at, backend, errors (list[TaskError]), worker_ids, return_value
            (ValueError unless SUCCESSFUL), attempts, is_finished, refresh()/arefresh()
TaskResultStatus: READY, RUNNING, SUCCESSFUL, FAILED
TaskError: exception_class, traceback
TaskContext (takes_context=True, first arg): task_result, attempt (starts at 1)

BaseTaskBackend: options, task_class (6.1), supports_defer, supports_async_task,
                 supports_get_result, supports_priority;
                 enqueue(task, args, kwargs) / aenqueue / get_result / aget_result / validate_task(task)
Exceptions: InvalidTask, InvalidTaskBackend, TaskResultDoesNotExist, TaskResultMismatch
Priority range: -100..100 (higher = sooner), default 0. Queue default "default".
Settings: TASKS = {"default": {"BACKEND": "...", "QUEUES": [...], "OPTIONS": {...}}}
Built-ins: ImmediateBackend (default; runs inline), DummyBackend (stores, never runs; .results, .clear())
Signals: task_enqueued, task_started, task_finished
```

### Key facts for Potatoq
- **There is no worker and no production backend in core.** There are also no retries, scheduling or periodic tasks. DEP 14 deliberately defines "a shared API contract between worker libraries and developers". Potatoq should ship a first-class `TASKS` backend.
- **`ENQUEUE_ON_COMMIT` was removed** from core before 6.0 because a global setting couldn't support multi-database setups. The docs now recommend `transaction.on_commit(partial(my_task.enqueue, ...))` explicitly. Adam Johnson stressed during the DEP vote that on_commit is essential, and he also warned about workers holding many future-dated tasks in memory ([forum](https://forum.djangoproject.com/t/steering-council-vote-on-background-tasks-dep-14/31131)).
- **Django 6.1 (Aug 2026)** forwards `@task(**kwargs)` to `backend.task_class`, so Potatoq can define `PotatoqTask(Task)` with `max_retries`, `time_limit`, `unique_key`, `max_age` and so on, and users keep writing `@task(...)`. 6.1 also makes Task and TaskResult picklable.
- JSON round-trip semantics are strict: a tuple comes back as a list and the result is `FAILED` in the docs example. Potatoq's Django backend must use the same JSON rules so that switching backends doesn't change behaviour.
- django-tasks 0.12 split DB and RQ backends into `django-tasks-db` and `django-tasks-rq`.
- Other django.tasks backends in the wild or planned: Chancy (planned), Steady Queue (a Solid Queue port for Django, [internals](https://steady-queue.readthedocs.io/en/latest/internals.html)).

---

## 12. Oban (Elixir, Postgres/SQLite/MySQL): design insights

Links: [Oban](https://oban.hexdocs.pm/Oban.html), [Worker](https://oban.hexdocs.pm/Oban.Worker.html), [unique jobs](https://oban.hexdocs.pm/unique_jobs.html), [Pruner](https://oban.hexdocs.pm/Oban.Pruner.html), [Lifeline](https://oban.hexdocs.pm/Oban.Lifeline.html), [1M jobs/min](https://oban.pro/articles/one-million-jobs-a-minute-with-oban), [Postgres notifier](https://oban.hexdocs.pm/Oban.Notifiers.Postgres.html).

- **States:** available, scheduled, executing, retryable, completed, cancelled, discarded.
- **Worker defaults:** `max_attempts=20`; backoff is exponential with a 15 s minimum plus jitter; `timeout=:infinity`; `priority=0` (0–9, 0 highest); `queue=:default`.
- **Return values as control flow:** `:ok`, `{:error, reason}` (retry), `{:cancel, reason}` (no retry), `{:snooze, seconds}` (reschedule **without consuming an attempt**). Potatoq should expose `raise Snooze(60)`, `raise Cancel(...)` and `raise Retry(in_=...)`.
- **Pruner:** interval 30 s, **max_age 60 s**, limit 10,000 per run. Completed, cancelled and discarded jobs are deleted almost immediately, which keeps the table small.
- **Lifeline:** rescues `executing` jobs after **1 h** purely by time. The docs admit it can duplicate genuinely running jobs; Pro uses producer heartbeats instead.
- **Stager:** every 1 s, promotes scheduled/retryable jobs to available (limit 5,000) and notifies queues.
- **Unique jobs:** defaults are period 60 s, fields `[:worker, :queue, :args]`, states `:successful`. OSS Oban enforces this with **transactional locks plus queries, not a unique constraint**, so it can race; Pro's Smart engine uses real constraints. Duplicates return `{:ok, job}` with `conflict?: true`, and `replace:` can update fields on conflict.
- **Notifier:** Postgres LISTEN/NOTIFY by default, or the Erlang `PG` notifier. Insert notifications **moved from DB triggers into application code**, are debounced per queue, and can be disabled (`insert_trigger: false`) because triggers added overhead on bulk inserts and conflicted with poolers.
- **Leadership:** a DB peer (an `oban_peers` row with expiry) elects one node for cron, pruning and staging.
- **Engines:** Basic (Postgres), Lite (SQLite ≥ 3.37), Dolphin (MySQL 8.4). This shows that one abstraction can cover PG, SQLite and MySQL.
- **Performance:** index-only scans, compound indexes and debounced fetching (cooldown). **Async (batched) acking gave +200–210% throughput**. Peak 32k jobs/s on one queue and node.
- `shutdown_grace_period` 15 s.

---

## 13. River (Go, Postgres): design insights

Links: [brandur.org/river](https://brandur.org/river), [HN](https://news.ycombinator.com/item?id=38349716), [unique jobs](https://riverqueue.com/docs/unique-jobs), [maintenance services](https://riverqueue.com/docs/maintenance-services), [retries](https://riverqueue.com/docs/job-retries), [client.go](https://github.com/riverqueue/river/blob/master/client.go), [notifier pattern](https://brandur.org/notifier).

- **Transactional enqueue is the main selling point.** "When used well, transactions and background jobs are a match made in heaven and completely sidestep a whole host of distributed systems problems." A job inserted in the app's transaction becomes visible exactly when the data commits, and a rollback discards it.
- **Defaults (client.go):** `JobTimeout=1 min`, `JobStuckThreshold=10 s`, `FetchCooldown=100 ms`, `FetchPollInterval=1 s`, `MaxAttempts=25`. Retry backoff is `attempt^4 + ±10% jitter` (1 s, 16 s, … about 3 weeks in total). Retention is completed 24 h, cancelled 24 h, discarded **7 days**. `SoftStopTimeout` is unset (wait indefinitely).
- **Maintenance services run only on the elected leader:** Cleaner (retention above); Rescuer (stuck `running` jobs after **1 h**, or JobTimeout + 1 h); Scheduler (every 5 s, promotes scheduled to available); Periodic Enqueuer; Queue Cleaner (24 h); **Reindexer (daily `REINDEX INDEX CONCURRENTLY`, one index at a time)**.
- **Unique jobs:** `ByArgs`, `ByPeriod`, `ByQueue`, `ByState` (default: all states except cancelled/discarded). Enforced by a `unique_key` column with a **partial unique index** filtered by a state bitmask, so uniqueness is real and race-free. Caveat: uniqueness covers insertion only; execution is still at-least-once.
- **Snooze:** `JobSnooze(d)` doesn't consume attempts.
- **Why Postgres is fine now (vs. 2017):** SKIP LOCKED (9.5), `REINDEX CONCURRENTLY` (12), B-tree dedup (13), **bottom-up B-tree deletion on insert (14)**, which removes most of the index bloat from high-churn queue tables. Also batch fetch and update, `COPY FROM` for bulk insert, and the binary protocol.
- **Notifier pattern:** one LISTEN connection per process, with fan-out to subscribers over buffered non-blocking channels. That connection needs session pooling; everything else can use PgBouncer in transaction mode.

---

## 14. pgmq

Link: [GitHub](https://github.com/pgmq/pgmq).

- An SQS-like extension: `send` (with delay), `read(vt)` (visibility timeout), `pop`, `archive`, `delete`, `set_vt`, `read_with_poll`. **One table per queue** (`pgmq.q_<name>`) plus an archive table (`a_<name>`). Optional unlogged queues and partitioned queues via pg_partman. FIFO/grouping and topic routing. PG 14–18; used by Supabase and Tembo.
- Lesson: the **visibility-timeout lease** (`vt`) is simpler than "running plus heartbeat plus rescuer". It needs an extension, though, which managed providers may not offer, so Potatoq should use plain SQL.

---

## 15. Postgres-as-queue literature

- **Brandur 2015/2017, "Postgres Job Queues & Failure By MVCC"** ([post](https://brandur.org/postgres-queues), [HN](https://news.ycombinator.com/item?id=14730685)): a long-running transaction anywhere in the DB pins the MVCC horizon. Dead tuples pile up and every lock attempt walks thousands of invisible index entries. Lock time went from under 10 ms to over 100 ms, and the queue grew to 60k jobs in an hour. Mitigations: more specific predicates, terminating long-lived transactions, and not sharing the DB across teams.
- **Brandur, "Transactionally Staged Job Drains"** ([post](https://brandur.org/job-drain)): insert into a `staged_jobs` table in the app transaction, then an enqueuer process (repeatable read) moves jobs to Redis or Sidekiq and deletes them. You get transactional enqueue semantics with a non-DB broker, and never "job runs before commit". **This is the outbox pattern; Potatoq can offer it for Redis/RabbitMQ when the app DB is Postgres or SQLite.**
- **PlanetScale 2026, "Keeping a Postgres queue healthy"** ([post](https://planetscale.com/blog/keeping-a-postgres-queue-healthy)): reproduced the death spiral on PG 18. Concurrent analytics pinned the horizon and produced 383k dead tuples and over 300 ms lock time at 800 jobs/s. Batching (10 per transaction) helped the baseline (1.3–3 ms) but not the horizon problem. `statement_timeout`, `idle_in_transaction_session_timeout` and `transaction_timeout` (PG17) are "blunt instruments". The fix is to protect VACUUM by limiting competing long queries.
- **Rich Yen 2026** ([post](https://richyen.com/postgres/2026/05/04/postgres_job_queue.html)): **MultiXact SLRU contention** under many workers, queue tables "tens of gigabytes when the actual 'live' data was only a few megabytes", and "every lock/unlock is a full WAL-logged transaction". Fine below about 100 concurrent workers.
- **DBOS 2026** ([post](https://www.dbos.dev/blog/making-postgres-queues-scale)): 30k+/s with SKIP LOCKED, **READ COMMITTED** (REPEATABLE READ caused serialization failures), and **partial indexes on the enqueued state that include the sort columns**, so the dequeue needs no sort step.
- **Recall.ai 2025, "LISTEN/NOTIFY does not scale"** ([post](https://www.recall.ai/blog/postgres-listen-notify-does-not-scale), [Simon Willison](https://simonwillison.net/2025/Jul/11/postgres-listen-notify/)): a transaction that issued `NOTIFY` takes a **global lock during commit** (`AccessExclusiveLock on object 0 of class 1262`) that is held through fsync, which serializes all NOTIFY-ing commits. Under many writers this caused outages. DBOS published a rebuttal, ["LISTEN/NOTIFY actually scales"](https://dbos.dev/blog/postgres-listen-notify-scalability), arguing it works if used carefully.
- **PgQ/pgque** ([thebuild.com](https://thebuild.com/blog/pgque-two-snapshots-and-a-diff/)): snapshot-diff batches, consumers that only read, and 3 rotating tables with `TRUNCATE`, giving zero dead tuples at the cost of about 1–2 s latency. A good fit for event fan-out, not for job dispatch.
- **Adriano, "Choose Postgres queue technology"** ([post](https://adriano.fyi/posts/2023-09-24-choose-postgres-queue-technology/)): LISTEN/NOTIFY plus `FOR UPDATE SKIP LOCKED` is enough for most apps; against the "cargo cult of scale"; build queue-agnostic abstractions (his Neoq).
- **SKIP LOCKED** (Craig Ringer, 2ndQuadrant, "What is SELECT … SKIP LOCKED for in PostgreSQL 9.5?"; the original URL now redirects to EDB's homepage. See also [depesz](https://www.depesz.com/2014/10/10/waiting-for-9-5-implement-skip-locked-for-row-level-locks/) and [Paquier](https://paquier.xyz/postgresql-2/postgres-9-5-feature-highlight-skip-locked-row-level/)). The canonical pattern is `DELETE FROM queue WHERE id = (SELECT id FROM queue ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *`, done inside the transaction that does the work, so a rollback automatically returns the job. Caveats: it gives an inconsistent view (fine for queues, not for general use), and holding a transaction open while a long job runs is exactly what pins the MVCC horizon. **For long jobs, claim (UPDATE state=running with a lease), commit, then work.**
- **Rails Solid Queue** ([GitHub](https://github.com/rails/solid_queue)) and its Django port **Steady Queue** ([internals](https://steady-queue.readthedocs.io/en/latest/internals.html)) **split the hot "ready" table from the cold "jobs" table** (ready, scheduled, claimed, blocked, failed executions) so the polled table stays tiny. Defaults: worker poll 0.1 s, dispatcher 1 s / batch 500, process heartbeat 60 s, alive threshold 5 min, `clear_finished_jobs_after` 1 day. Concurrency limits use a semaphores table. They recommend a separate DB, which gives up transactional enqueue.

---

## 16. Redis queue reliability

- **Lists:** `RPOP`/`BRPOP` loses messages if the consumer crashes after receiving one. The documented **reliable queue** pattern is `LMOVE`/`BLMOVE` into a processing list, `LREM` on completion, and a monitor that re-pushes stale items ([LMOVE docs](https://redis.io/docs/latest/commands/lmove/)). Used by SAQ, Sidekiq Pro super_fetch and RQ (in spirit). The weaknesses are that there is no per-item claim timestamp (you need a side ZSET or hash), `LREM` is O(N), and blocking commands can't be called inside Lua.
- **Streams plus consumer groups:** `XREADGROUP >` gives at-least-once delivery through the PEL, with `XACK`, `XPENDING` delivery counts and `XAUTOCLAIM key group consumer min-idle start COUNT 100` (since 6.2; scans up to COUNT×10 PEL entries; since 7.0 it removes trimmed or deleted IDs from the PEL; it increments the delivery count unless `JUSTID` is used) ([XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/), [streams](https://redis.io/docs/latest/develop/data-types/streams/)). Redis 8.2 adds `XACKDEL`/`XDELEX` (ack plus delete); 8.8 adds `XNACK` (release a message for redelivery). Weaknesses:
  - No delayed delivery and no priorities; you still need a ZSET for ETA/countdown.
  - Idle-time-based reclaim conflates "slow job" with "dead consumer". Long jobs must periodically `XCLAIM … JUSTID` themselves to reset idle time (a lease extension).
  - Valkey lacks the 8.x additions.
  - Taskiq's `RedisStreamBroker` is the main Python user.
- **Lua-script designs:** Dramatiq (lists, an ack set per worker, a heartbeat ZSET, a DLQ ZSET), arq (a ZSET queue with optimistic WATCH, which contends at scale), and BullMQ (wait list, active list, delayed ZSET, a priority ZSET, a lock key with TTL per active job, a stalled-job checker, and a "marker" key that workers block on with BZPOPMIN, which gets around the "no blocking in Lua" limit; [BullMQ docs](https://docs.bullmq.io/), [BullMQ Python vs RQ benchmark](https://bullmq.io/articles/benchmarks/bullmq-python-vs-rq/)).
- **Durability:** Redis with AOF `everysec` can lose about 1 s of acknowledged writes. `WAIT`/`WAITAOF` (7.2) can make critical enqueues synchronous. Redis Cluster needs a hash tag such as `{queue}` so that all of a queue's keys live in one slot for Lua.

---

## 17. RabbitMQ best practices (2025–2026)

- **Quorum queues are the default choice** for durability and replication (Raft) ([quorum queues](https://www.rabbitmq.com/docs/quorum-queues)). Classic mirrored queues were removed in 4.0.
  - **Delivery limit defaults to 20 in 4.0+**. When it is exceeded the message is dropped or dead-lettered. Explicit nack/modify returns don't count.
  - At-least-once dead-lettering needs `x-dead-letter-strategy=at-least-once` and `x-overflow=reject-publish`.
  - Priorities: strict priority with **32 levels as of 4.3** (earlier 4.x: 2 levels).
  - Not suited to backlogs over 5M messages or to temporary queues. Each message costs about 32 B of metadata in memory.
- **`consumer_timeout` defaults to 30 min.** If a delivery isn't acked in time, the channel is closed with PRECONDITION_FAILED and **all deliveries on that channel are requeued**. It can be overridden per queue with `x-consumer-timeout` or a policy ([consumers](https://www.rabbitmq.com/docs/consumers)). Potatoq must set it to at least the queue's time limit, or ack long tasks through a different strategy.
- **Publisher confirms:** ack arrives after the message is persisted for durable queues, with possibly hundreds of ms latency under load. Batch or pipeline confirms. **Prefetch:** "100 through 300 range usually offer optimal throughput", which suits short messages, but for tasks prefetch should be about concurrency. **Requeue loops:** track redeliveries ([confirms](https://www.rabbitmq.com/docs/confirms)).
- **Delayed messages without the plugin:** `rabbitmq-delayed-message-exchange` is **no longer maintained**. It relied on Mnesia, which was removed in 4.3; it kept a single non-replicated copy, lost timers on restart, and doesn't scale to hundreds of thousands of delayed messages ([plugin](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)).
  - Per-message TTL only expires at the **head of the queue**, so a single "delay" queue with per-message TTL blocks shorter delays behind longer ones ([TTL](https://www.rabbitmq.com/docs/ttl)).
  - The fix is the **NServiceBus / Kombu 28-level binary TTL+DLX topology**: exchanges `celery_delayed_{0..27}`, each with a quorum queue whose `x-message-ttl=2^level s` dead-letters to level−1. The routing key is the delay's 28-bit binary with dots plus the destination routing key, giving a maximum of about 149 days ([kombu native_delayed_delivery](https://docs.celeryq.dev/projects/kombu/en/main/_modules/kombu/transport/native_delayed_delivery.html), [NServiceBus](https://docs.particular.net/transports/rabbitmq/delayed-delivery)). Celery 5.5 adopted this for quorum queues.
- **Single active consumer** (`x-single-active-consumer`) gives ordered or serialized queues with failover.
- AMQP heartbeats plus long-running tasks on the same thread cause "Couldn't ack, BrokenPipe" (Celery #3773). Keep the AMQP I/O loop on a dedicated thread or in the Rust core, never on the task thread.

---

## 18. SQLite as a queue

- **Required settings:** `PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA synchronous=NORMAL;` ([dev.to](https://dev.to/arthurpro/you-probably-dont-need-redis-put-the-job-queue-in-your-sqlite-file-624), [goqite](https://github.com/maragudk/goqite)).
- **Use `BEGIN IMMEDIATE` for anything that writes.** A deferred transaction that reads first and then tries to write gets `SQLITE_BUSY` immediately, **ignoring busy_timeout**, because SQLite can't upgrade the snapshot ([Bert Hubert](https://berthub.eu/articles/posts/a-brief-post-on-sqlite3-database-locked-despite-timeout/), [Simon Willison](https://simonwillison.net/2025/Feb/17/sqlite-busy)). In Python's `sqlite3`, legacy transaction control issues `BEGIN` (deferred) implicitly. Use `isolation_level=None` (or `autocommit=True` on 3.12+) and issue `BEGIN IMMEDIATE` yourself. Django 5.1+ supports `OPTIONS={"transaction_mode": "IMMEDIATE", "init_command": "PRAGMA ..."}` for SQLite (from memory of the Django 5.1 release notes; verify).
- **Claim query** (no SKIP LOCKED in SQLite; single-writer serialization makes it unnecessary):
  ```sql
  UPDATE jobs SET state='running', attempts=attempts+1, lease_until=:now+:lease, worker_id=:w
  WHERE id IN (SELECT id FROM jobs WHERE queue=:q AND state='ready' AND run_at<=:now
               ORDER BY priority DESC, run_at, id LIMIT :n)
  RETURNING id, payload;   -- RETURNING needs SQLite >= 3.35
  ```
  Back it with a partial index `ON jobs(queue, priority DESC, run_at, id) WHERE state='ready'`.
- **Throughput:** goqite reports about 18.5k msg/s with 1 producer and 1 consumer, and about 12.5k with 16 parallel participants (M3 Max). Fine for single-host deployments.
- **litequeue** ([GitHub](https://github.com/litements/litequeue)): READY/LOCKED/DONE/FAILED states, one write connection plus a pool of query-only connections, and a `claim_id` fencing token so stale workers can't ack newer deliveries (a good idea for every backend). Includes `retry_expired()` and `prune()`.
- **No NOTIFY.** Wake-ups come from in-process signalling, polling with backoff (Huey: 0.1 s rising to 10 s), or a cheap `PRAGMA data_version` check (it changes when another connection commits). Oban Lite uses the Erlang PG notifier.
- **Caveats:**
  - Single host only, never over NFS.
  - Long readers starve WAL checkpoints and the `-wal` file grows; run `wal_checkpoint(TRUNCATE)` periodically.
  - Keep claim transactions to microseconds and never run a task inside the write transaction.
  - Solid Queue notes that SQLite lacks SKIP LOCKED, so workers queue up on the write lock. Batch claims (claim N, process, batch-ack) to amortize.

---

## 19. Comparison matrix (defaults)

| Lib | Ack semantics | Default retries | Backoff | Default time limit | Results default | Prefetch / concurrency | Delayed jobs |
|---|---|---|---|---|---|---|---|
| Celery | **early ack** | 0 (manual `retry`, max 3) | fixed 180 s | none | backend-dependent, 1 day expiry | multiplier 4 × procs | ETA held in worker RAM |
| Dramatiq | late ack | **20** | 15 s × 2^n, jitter, cap 7 d | **10 min** | off (10 min TTL) | threads×2; 8 threads × CPU procs | DQ, but held in worker RAM |
| RQ | no ack (abandoned → failed) | 0 | intervals list | **180 s** | 500 s | 1 job per worker (fork) | scheduled ZSET + scheduler |
| Huey | **at-most-once** | 0 | delay × backoff (3.3+) | none (3.0+ opt-in) | on, read-once | 1 thread | schedule ZSET |
| arq | lease via in-progress key | 5 (Retry only) | user-defined | **300 s** | 1 h | 10 async | ZSET by score |
| Taskiq | `when_saved` | 0 (middleware) | Smart: 10 s exp + jitter | none | Dummy (none) | 2 procs, prefetch 0 | via scheduler |
| Procrastinate | DB state | 0 | configurable | none | DB rows forever | concurrency 1 | `scheduled_at` |
| SAQ | heartbeat/sweep | 0 (`retries=1`) | off | **10 s** | 600 s | configurable | scheduled |
| Hatchet | engine | 0 | factor/max | **60 s** (+5 min schedule) | stored | slots | native |
| django-q2 | retry timer | ∞ (`max_attempts=0`) | fixed `retry=60` | none | 250 saved | CPU procs | schedule model |
| Oban | DB state | **20** | 15 s + 2^n + jitter | ∞ | n/a (pruned after 60 s) | per-queue limit | scheduled state |
| River | DB state | **25** | attempt^4 ± 10% | **1 min** | n/a (24 h) | per-queue max workers | scheduled state |

---

## 20. Synthesis: recommended Potatoq defaults

The principle is to be safe by default (at-least-once delivery, no hoarding, bounded everything) while staying compatible with Celery's call-site API (`@app.task`, `.delay()`, `.apply_async(countdown=, eta=)`) and the Django Tasks API.

| Setting | Potatoq default | Rationale |
|---|---|---|
| Delivery | **At-least-once: ack after the task finishes**, requeue when a worker is lost, fencing token per delivery | Every serious system does this (Dramatiq, River, Oban, BullMQ). Fixes Celery's early ack and Huey/RQ losing jobs. Document idempotency prominently. |
| Prefetch | **= concurrency** (each process fetches only as many jobs as it has idle slots), optional `prefetch_extra=0` | Celery's ×4 and Dramatiq's threads×2 cause head-of-line blocking behind long tasks (top HN complaint). Taskiq's default of 0 extra is right. |
| Time limit | **10 min** hard (`time_limit=600`) plus an optional soft limit, `None` allowed explicitly | Dramatiq's 10 min is the most battle-tested middle ground. RQ 180 s and SAQ 10 s break on report-style tasks; Oban/Celery "infinite" leads to stuck workers. Prefork lets the hard kill be real (SIGKILL the child), unlike thread exception injection. |
| Lease / visibility | **Derived automatically** as `time_limit + grace (30 s)`, extended by heartbeats every 10 s | Removes the django-q2 "retry < timeout" duplicate bug and Celery's Redis `visibility_timeout` vs ETA issue (#4400). |
| Retries | **On for unexpected exceptions, `max_retries=10`**, polynomial backoff `15 s + attempt^4 s` with ±10–20% jitter, capped at 1 h per step (about 4–5 h total) | Between Dramatiq/Oban/River (20–25 attempts over weeks, too long for web apps) and Celery/RQ/Huey/Taskiq (0, which surprises users). Per-task `retries=0` opt-out; `throws=(...)` / `NonRetryable`; `retry_when`. **Alternative:** Celery-compatible "0 retries" with a one-line global switch. This is a product decision. |
| Snooze / Cancel | `raise Snooze(seconds)` (no attempt consumed), `raise Cancel()` | Oban and River. |
| max_age | None, configurable per task (`expires=` in Celery terms) | Dramatiq AgeLimit / Huey `expires`. |
| Results | **Status and metadata always stored; return values opt-in per task** (`store_result=True`), TTL **24 h**; failures and DLQ kept **7 days** | Django `TaskResult` needs status, attempts, errors and worker_ids. Dramatiq/River/Oban use 7 days for dead jobs; Celery uses 1 day for results. Avoid Huey's destructive reads. |
| Retention of finished jobs (DB backends) | completed 24 h, cancelled 24 h, discarded/failed 7 days, pruned in batches of 10k by the leader | River. Oban's 60 s is great for throughput but bad for debugging. |
| Concurrency model | `processes = CPU count` (prefork), `threads = 1` per process by default, plus an asyncio loop per process for `async def` tasks; per-queue concurrency limits | Celery-compatible prefork (the drop-in requirement). Dramatiq's 8 threads surprises CPU-bound users. Celery #1599 asks for per-queue concurrency. Run sync and async tasks side by side (the arq #437 ask). |
| Worker recycling | `max_tasks_per_child=None`, `max_memory_per_child=None`, both easy to set | Celery #4843 (memory leaks). |
| Shutdown | graceful SIGTERM; **25 s** grace (under k8s' 30 s default), then requeue unacked and in-flight jobs; second signal is immediate | Oban 15 s, Huey 3.4's `--shutdown-timeout`. |
| Heartbeats / dead-worker detection | 10 s interval, dead after 60 s, recovery by the leader every 10–30 s (deterministic, not probabilistic) | Procrastinate 10/30; avoid Dramatiq's 1-in-1M maintenance. Avoid Oban Lifeline's time-only rescue. |
| Delayed / ETA jobs | **Never held in worker memory.** Stored broker-side (ZSET, DB `run_at`, or a RabbitMQ TTL-ladder) and promoted by a scheduler | Celery and Dramatiq's worst flaw (#431 duplicates). |
| Publisher confirms (RabbitMQ) / durable enqueue | **On** | Celery #5410, Dramatiq's `confirm_delivery=False`. |
| Serialization | **JSON** by default (Django Tasks compatible); pickle opt-in | Huey's pickle default is a security and compatibility risk; Django requires JSON. |
| Unique jobs | `unique_key=` / `unique_for=` with a period and states; real constraint where possible | River (partial unique index), Procrastinate's `queueing_lock`, Oban (but avoid its racy OSS implementation). |
| Locks / rate limits | `concurrency_key` + limit (serialized per key), token-bucket / window rate limits per task and queue | Dramatiq limiters, Procrastinate `lock`, Solid Queue semaphores, Hatchet concurrency keys. |
| Periodic tasks | Built in, leader-elected, deduplicated by `(task, scheduled_time)` unique key | Huey/arq/Procrastinate/Oban have it built in; Dramatiq doesn't (a complaint). Avoid celery-beat as a single point of failure. |
| Transactions | **Postgres/SQLite backend in the same DB: transactional enqueue.** Redis/RabbitMQ: **enqueue on commit when inside `atomic()`** by default (`enqueue_on_commit=True` per task, overridable), plus an optional outbox (staged-job drain) | River/Brandur; Django docs; Celery 5.4 `delay_on_commit`. Must be per-DB-alias aware (the reason Django dropped the setting). |
| Logging | Never configure the root logger | Dramatiq #48. |
| Health | `potatoq ping` / liveness and readiness endpoints, Prometheus metrics aggregated across processes | Celery #4079, Taskiq #240/#590. |
| Testing | `immediate`/eager mode and a stub broker with `join(fail_fast=True)` | Huey immediate mode, Dramatiq StubBroker, Django ImmediateBackend/DummyBackend. |

---

## 21. Per-backend implementation strategy

### Postgres (the flagship backend; Oban/River-class design)
1. **Schema:** `potatoq_job(id bigint identity, queue text, task text, args jsonb, state smallint/enum, priority smallint, run_at timestamptz, attempt int, max_attempts int, lease_until timestamptz, worker_id, unique_key bytea NULL, unique_states bit(8), errors jsonb[], result jsonb, created/started/finished_at, meta jsonb)`. Use identity columns, not UUIDs, for the PK (Hatchet's bloat lesson); expose a string ID for Django.
2. **Indexes:** a partial fetch index `(queue, priority DESC, run_at, id) WHERE state='available'` so the dequeue needs no sort (DBOS); `WHERE state='scheduled'` on `run_at` for the stager; a partial unique index on `unique_key` filtered by a state bitmask (River).
3. **Fetch:** `UPDATE … SET state='running', attempt=attempt+1, lease_until=now()+lease, worker_id=$w WHERE id IN (SELECT id … FOR UPDATE SKIP LOCKED LIMIT $free_slots) RETURNING *`, under READ COMMITTED. **Commit immediately and never hold a transaction during task execution.**
4. **Ack:** **batched async completion** (flush every ~10–50 ms, or N rows) in a single `UPDATE … FROM unnest(...)`. Oban saw +200% throughput from this, and Hatchet buffers at 10 ms.
5. **Wake-ups:** one LISTEN connection per worker *process*. Send NOTIFY from **application code after commit** (on_commit), not from triggers, and debounce it per queue per transaction. Fall back to polling every **1 s** (River). Allow `listen_notify=False` for PgBouncer in transaction mode and for high-write databases (Recall.ai's global commit lock).
6. **Leader election:** a `potatoq_leader` row with expiry (or `pg_try_advisory_lock` on a session connection). The leader runs the stager (scheduled/retryable to available, every 1 s), the rescuer (expired leases to retryable or discarded), the pruner (batch delete), the cron enqueuer and an optional daily `REINDEX INDEX CONCURRENTLY`.
7. **Bloat hygiene:** per-table `autovacuum_vacuum_scale_factor=0.01` (or a fixed threshold) and `autovacuum_vacuum_cost_delay=0`. Document the MVCC-horizon risk (analytics or long transactions on the same DB) and offer the `transaction_timeout` (PG17) / `idle_in_transaction_session_timeout` guidance. Support a separate DB or schema for the queue, which costs transactional enqueue. For very high throughput, consider a Solid-Queue-style split of a small hot "ready" table from the cold job/history table, or time-range partitions for history (Hatchet).
8. Bulk insert via `COPY`/`executemany` for `group()`/`chunks()`.

### SQLite
- One DB file (it can be the Django DB, which gives transactional enqueue; or a separate `potatoq.db`). Set `WAL`, `synchronous=NORMAL`, `busy_timeout=5000` and `foreign_keys=ON` on every connection, and use **`BEGIN IMMEDIATE`** for every write path. Keep the same schema as Postgres (minus SKIP LOCKED and bitmask tricks), with lease, attempt and fencing token.
- Claim N rows per `UPDATE … RETURNING` (≥ 3.35), commit, run tasks outside the transaction, and batch acks.
- Wake-ups: in-process events when producer and worker share a process; otherwise poll `PRAGMA data_version` every 50–100 ms, backing off to 1 s when idle.
- Maintenance: periodic `wal_checkpoint(TRUNCATE)`, batch pruning, and incremental vacuum.
- Document clearly: single host, no network FS, roughly 10k+/s ceiling. Refuse multi-host setups loudly.

### Redis / Valkey
- Keep all keys for a queue under a hash tag `{potatoq:<queue>}` for Cluster.
- **Data model** (BullMQ/Dramatiq-style lists, *not* Streams, because priorities and delays matter more for tasks):
  - `ready:<prio>` lists (or one ZSET with a priority+seq score);
  - a `delayed` ZSET scored by ETA;
  - an `inflight` ZSET scored by `lease_until` (member = job ID);
  - a `jobs` hash (payload plus metadata), or one hash per job;
  - a `dead` ZSET with 7-day TTL;
  - a `workers` heartbeat ZSET;
  - a `marker` key/list for blocking wake-up.
- **All state transitions are Lua scripts** (EVALSHA, compatible with Valkey):
  - `enqueue`
  - `fetch(n)`: promote due delayed jobs, pop up to n, add to inflight with the lease
  - `ack`
  - `nack/retry(delay)`
  - `extend_lease`
  - `reap`: expired leases go back to ready with an attempt increment, or to dead
- Workers block with `BZPOPMIN`/`BLPOP` on the marker key with a short timeout (blocking can't happen inside Lua), then call `fetch`. This gives sub-ms latency without polling (SAQ/BullMQ), unlike arq's 0.5 s poll and WATCH contention.
- Lease-based reclaim (not "worker is dead") correctly handles slow jobs, SIGKILL and network partitions. The reaper runs on every worker every few seconds and is idempotent, rather than Dramatiq's 1-in-1M dice roll.
- Optional `WAITAOF` for durable enqueue. Results go in separate keys with TTL (or Streams for multi-result, as RQ 2.0 does).
- Streams (`XREADGROUP` / `XAUTOCLAIM` / `XACKDEL`) can be offered later as an alternative mode for very high-throughput FIFO queues without priorities.

### RabbitMQ
- **Quorum queues by default** (`x-queue-type=quorum`) with `x-delivery-limit` aligned to max_retries + safety margin (or rely on the 4.0 default of 20), DLX to `<queue>.dead` with `x-dead-letter-strategy=at-least-once` and `x-overflow=reject-publish`. Set a **per-queue `x-consumer-timeout` ≥ the time limit plus grace** (the default of 30 min would kill long tasks and requeue the whole channel).
- **Publisher confirms always on** (pipelined, async). Manual acks. **`basic.qos(prefetch_count=free slots)` per consumer**; one channel per worker process, with the AMQP I/O on a dedicated thread or the Rust core (heartbeats must not depend on task execution).
- **Delays / countdown / retry backoff:** the 28-level binary TTL+DLX quorum ladder (`potatoq.delay.{0..27}`, compatible in spirit with Kombu's), which supports up to about 149 days. Never consume-and-hold in worker memory (Dramatiq's .DQ approach). Long-dated ETAs (> N days) could alternatively be stored in a DB or Redis scheduler if one is configured.
- Priorities: quorum queues in 4.3+ have 32 levels, so map Django's -100..100 range into buckets. On older brokers, use classic queues with `x-max-priority` only on request.
- Ordered or serialized queues via `x-single-active-consumer`.
- Results/status: RabbitMQ is a bad results store (avoid the Celery `rpc://` backend). Require or default to a Redis or SQL result backend, or a DB-backed status table when used with Django.
- Retries: re-publish to the delay ladder with an attempt header and ack the original, so retries aren't counted by the delivery limit. The delivery limit then only catches crash loops (poison messages).

### Django integration
- `TASKS = {"default": {"BACKEND": "potatoq.django.Backend", "QUEUES": [...], "OPTIONS": {"broker": "...", ...}}}` with `supports_defer = supports_priority = supports_get_result = supports_async_task = True`.
- `task_class = PotatoqTask` (Django 6.1 `**kwargs`) to accept `max_retries`, `time_limit`, `unique_key`, `rate_limit`, `enqueue_on_commit` and so on. Map `TaskResultStatus` READY/RUNNING/SUCCESSFUL/FAILED to the internal states (scheduled/available/retryable map to READY). Fill `attempts`, `errors` (exception class plus traceback string), `worker_ids` and `last_attempted_at`. Pass `TaskContext.attempt`.
- Also support the backport `django-tasks` for Django < 6.
- The Postgres/SQLite backends using the Django connection give true transactional enqueue (the job row commits with the data). Redis and RabbitMQ use `transaction.on_commit(using=<alias>)`.
- Ship migrations, read-only admin models, `manage.py potatoq worker`, and Huey-3.2-style admin stats.

---

## 22. Key takeaways (short list)
1. The biggest differentiators are **ack-late, no prefetch hoarding, and leases derived from time limits**. Those three fix most of the Celery horror stories.
2. **Delayed jobs must live in the broker, not in worker RAM.** Dramatiq's RabbitMQ and Redis implementations still get this wrong (#431).
3. **Postgres backend = River/Oban design:** partial indexes, SKIP LOCKED claim-and-commit, batched acks, leader-run maintenance, optional debounced NOTIFY plus a 1 s poll, transactional enqueue, real unique constraints. Watch out for the MVCC horizon, the NOTIFY commit lock and MultiXact under heavy concurrency.
4. **Redis = Lua plus leases plus marker-key blocking** (BullMQ-style), not BRPOP (Huey/Taskiq List lose jobs) and not WATCH/MULTI polling (arq).
5. **RabbitMQ = quorum queues, confirms, prefetch equal to slots, a TTL+DLX delay ladder, and per-queue consumer_timeout.**
6. **SQLite = WAL plus BEGIN IMMEDIATE plus UPDATE…RETURNING plus short transactions plus data_version polling.**
7. **Django 6.x Tasks is an API contract with no worker.** Potatoq can be the de-facto production backend. Use 6.1's `task_class` kwargs for Potatoq-specific options, and handle on-commit per DB alias.
8. arq is in maintenance mode while 3.5M downloads/month wait for a home, and Dramatiq has a bus-factor history. There is room for a well-maintained, async-and-sync, typed, Celery-compatible library.
