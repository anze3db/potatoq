# Defaults, and why

Each default below was chosen by cross-checking:

* **Celery's actual behaviour.** Verified against the source of Celery 5.6.3, Kombu
  5.6.2 and Billiard 4.3.1.
* **Community post-mortems.** Blog posts, HN threads and the most-upvoted Celery issues.
* **The Ruby job-queue ecosystem.** Sidekiq, Solid Queue, GoodJob, Que and ActiveJob.
* **Python alternatives.** Dramatiq, RQ, Huey, arq, Taskiq, Procrastinate, SAQ and Django 6
  Tasks, plus the Postgres-native designs of Oban and River.

The raw research, with every source: [Celery pain points](research/celery-pain-points.md), [Ruby job queues](research/ruby-queues.md), [Python alternatives](research/python-alternatives.md).

All settings use Celery's names (`app.conf.task_time_limit = ...`,
`CELERY_TASK_TIME_LIMIT`, or `POTATOQ_TASK_TIME_LIMIT` in Django).

## Delivery guarantees

| Setting | Celery | Potatoq | Why |
|---|---|---|---|
| `task_acks_late` | `False` | `True` | Celery acks *before* running, so a deploy, OOM kill or crash silently loses the task ([Celery FAQ](https://docs.celeryq.dev/en/stable/faq.html), [Adam Johnson](https://adamj.eu/tech/2020/02/03/common-celery-issues-on-django-projects/), [Hatchet](https://hatchet.run/blog/problems-with-celery)). Every mature queue is at-least-once: Sidekiq, Solid Queue, Dramatiq, Oban, River. Tasks must be idempotent, which they already had to be. |
| `task_reject_on_worker_lost` | `False` | `True` | Even with `acks_late`, Celery acks the message when the child process dies. Potatoq requeues it. |
| `task_max_deliveries` | n/a | `5` | Requeue-on-crash needs a cap, or a task that OOMs the worker crashes it forever. Sidekiq Pro and super_fetch use 3 recoveries; RabbitMQ quorum queues use 20. After 5 crashed deliveries the task is dead-lettered with `WorkerLostError`. Graceful shutdowns don't count. |
| Redelivery of stuck tasks | Redis `visibility_timeout = 3600` | leases renewed every 5 s; a dead worker's tasks return within `worker_dead_after = 60` s | Celery's fixed visibility timeout is the most-reported Redis problem: tasks running longer than an hour, or with an ETA further out, run again ([#4400](https://github.com/celery/celery/issues/4400), [Instawork](https://engineering.instawork.com/celery-eta-tasks-demystified-424b836e4e94)). A renewed lease never redelivers a healthy task and recovers a dead one quickly. |
| Fencing | none | every claim carries a token (delivery count or random token) | A worker that lost its claim, for example after a network partition, can't ack a task that was handed to someone else. |
| Publisher confirms (RabbitMQ) | off | on, with `mandatory` | Publishes can be lost silently without them ([#5410](https://github.com/celery/celery/issues/5410)). |
| Queue type (RabbitMQ) | classic | quorum | Replicated, with a delivery limit and dead-lettering built in. Classic mirroring was removed in RabbitMQ 4.0. |

## Scheduling work onto workers

| Setting | Celery | Potatoq | Why |
|---|---|---|---|
| `worker_prefetch_multiplier` | `4` | effectively `1`: each idle process claims one task | With prefetch, short tasks wait behind long ones that are already reserved by a busy process. This is the most common "Celery is slow/stuck" complaint. The old "`-O fair`" advice has been obsolete since 4.0; the hoarding is at the broker level. |
| ETA / countdown storage | worker RAM, unacked | broker: Redis sorted set, `run_at` column, RabbitMQ TTL cascade | Celery's ETA tasks consume worker memory without bound, are redelivered by `visibility_timeout`, and trip RabbitMQ's 30-minute `consumer_timeout` (`PRECONDITION_FAILED`). Dramatiq still holds delayed messages in memory, and its non-atomic hand-off causes duplicates. |
| `worker_concurrency` | `os.cpu_count()` (host) | CPUs available to the process: affinity and cgroup `cpu.max` | A container limited to 2 CPUs on a 64-core host gets 64 Celery processes and is OOM-killed. |
| Queue consumption | Celery consumes all queues concurrently | rotates fairly across `-Q` queues | |
| Priority | Redis: 0 = highest (emulated); RabbitMQ: higher = higher | **higher number runs first, on every broker** | One meaning everywhere, as in Django Tasks. Redis and the SQL brokers have real priorities, FIFO within a priority. |
| Default queue name | `celery` | `default` | Matches Django 6 Tasks. Set `task_default_queue="celery"` if you need to share queues with Celery. |

## Time limits and resource hygiene

| Setting | Celery | Potatoq | Why |
|---|---|---|---|
| `task_time_limit` | none | 30 min | A hung HTTP call should not hold a worker process forever. Dramatiq defaults to 10 min, RQ to 3 min, arq to 5 min. 30 min is long enough not to break typical Celery workloads when migrating, and is easy to raise per task. |
| `task_soft_time_limit` | none | 30 s before the hard limit | `SoftTimeLimitExceeded` gives the task a chance to clean up before `SIGKILL`. |
| RabbitMQ `x-consumer-timeout` | broker default, 30 min | time limit + 5 min | A healthy long task never gets its channel closed. |
| `worker_max_tasks_per_child` | unlimited | 1000 | Memory leaks and fragmentation in long-running Python processes are the most-upvoted Celery issue after asyncio ([#4843](https://github.com/celery/celery/issues/4843)). Recycling a forked child is cheap. |
| `worker_max_memory_per_child` | unlimited | opt-in, accepts `"512MB"` | Same reason. It is opt-in because the right number is workload specific. Recycles are always logged. |
| `worker_shutdown_timeout` | Celery waits forever (warm), then Kubernetes SIGKILLs it | 25 s, then running tasks are interrupted and **requeued without counting a delivery** | Fits Kubernetes' 30 s and Heroku's 30 s grace periods, as Sidekiq does. |

## Failures and retries

| Setting | Celery | Potatoq | Why |
|---|---|---|---|
| Terminal failures | acked and gone | **dead-lettered**: listed with `potatoq dead list`, replayed with `potatoq dead retry` | Sidekiq's dead set and RabbitMQ DLQs are what make on-call bearable. Capped at `dead_letter_max = 10_000` and `dead_letter_ttl = 30 days`. |
| `task_retry_backoff` | `False` (fixed 180 s) | `10`: exponential, about 10, 20, 40 … s, capped at `task_retry_backoff_max = 600` | Fixed delays hammer a struggling dependency in lock-step. Sidekiq, Dramatiq and River all back off exponentially. An explicit `default_retry_delay` on a task still wins, so Celery code keeps its behaviour. |
| `task_retry_jitter` | `True` (full jitter: 0…delay) | `True` (equal jitter: delay/2…delay) | Spreads retries out without ever retrying immediately. |
| `task_max_retries` | 3 | 3 | Kept for compatibility. Automatic retries stay opt-in (`autoretry_for`), as in Celery. Retrying every exception by default, as Sidekiq does, would re-run non-idempotent side effects in migrated code. |
| Unregistered task name | error log, message dropped | dead-lettered | Visible and replayable after you deploy the missing code. |
| Redelivery of an already finished task | runs again | skipped if a final result was already stored | Narrows the at-least-once window after a crash between storing the result and acking. |

## Results

| Setting | Celery | Potatoq | Why |
|---|---|---|---|
| `task_ignore_result` | `False`, but no backend unless configured | **auto**: stored on database brokers and whenever `result_backend` is set; otherwise off | On Postgres and SQLite the result is written in the same transaction as the ack, so it is free. On Redis, results cost memory, so they stay opt-in like in Celery. |
| `.get()` on an ignored result | hangs in `PENDING` forever | raises `ResultBackendDisabled` immediately | |
| `STARTED` state | needs `task_track_started=True`, which costs an extra write | free on database brokers (the task row says it is running) | |
| `result_expires` | 1 day; DB backends need beat to clean up | 1 day, enforced by the broker or the worker's maintenance loop | No beat needed. |
| `.get()` inside a task | `RuntimeError` | `RuntimeError` | Deadlock prevention, kept. |

## Transactions (Django, SQLAlchemy)

| Setting | Celery | Potatoq | Why |
|---|---|---|---|
| `task_enqueue_on_commit` | n/a (`delay_on_commit` is opt-in since 5.4) | `True` | "The task ran before the row was committed" (`DoesNotExist`) is the classic Celery/Django bug. Rails 7.2 made enqueue-after-commit the default; 8.0 rolled that back because the adapter-dependent behaviour was a "mystery box"; 8.2 makes it the default again for new apps. Potatoq applies it uniformly to every broker. When the broker *is* the database, the task row is written inside the transaction, which has the same semantics and no crash window. |

## Operations

| Setting | Celery | Potatoq | Why |
|---|---|---|---|
| Scheduler | separate `celery beat`, single instance, local shelve file | runs in every worker, fire times claimed once through the broker | Running two beats double-schedules everything; running zero silently stops periodic jobs. Solid Queue and GoodJob deduplicate on a unique `(task, run_at)` key the same way. |
| Missed periodic runs | — | skipped if more than 60 s late; each run expires when the next one is due | No stampede of the same job after downtime. |
| Gossip / mingle / heartbeats | on (n² broadcast traffic) | don't exist | Liveness comes from a heartbeat row or key per node. `potatoq status` and `inspect` read it. |
| Root logger | hijacked | left alone if already configured | |
| Serializer | JSON (pickle before 4.0) | JSON only, with `datetime`, `date`, `time`, `timedelta`, `UUID`, `Decimal`, `bytes` and `set` round-tripping | Pickle turns broker write access into remote code execution. Eager mode still round-trips arguments through the serializer so tests see what workers see. |
| Redis keys | no prefix | `potatoq:` prefix (`global_keyprefix`) | |
| Redis `maxmemory-policy` | not checked | warns unless `noeviction` | Eviction silently deletes queued tasks. |
| Connection timeout | `.delay()` can block forever when the broker is down ([#4296](https://github.com/celery/celery/issues/4296)) | 10 s (`broker_connection_timeout`) | |
| `async def` tasks | unsupported ([#6552](https://github.com/celery/celery/issues/6552), most-upvoted issue) | supported, on a per-process event loop kept between tasks; `adelay()` / `aget()` for callers | |

## Settings you can delete when migrating

These only existed to work around Celery defaults and are accepted but ignored:
`task_acks_late`, `task_reject_on_worker_lost`, `worker_prefetch_multiplier`,
`broker_transport_options["visibility_timeout"]`, `broker_connection_retry_on_startup`,
`broker_connection_retry`, `broker_connection_max_retries`, `broker_pool_limit`,
`broker_heartbeat`, `worker_cancel_long_running_tasks_on_connection_loss`,
`task_acks_on_failure_or_timeout`, `accept_content`, `task_serializer`,
`result_serializer`, `result_accept_content`, `result_extended`, `worker_send_task_events`,
`task_send_sent_event`, `beat_scheduler`, `beat_schedule_filename`, `task_queues`,
`task_default_exchange*`, `task_default_routing_key`, `task_queue_max_priority`,
`worker_pool`, plus the `--without-gossip --without-mingle --without-heartbeat -O fair -B`
worker flags.
