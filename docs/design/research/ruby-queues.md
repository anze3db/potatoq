# Ruby Job Systems: Lessons for Potatoq

Research date: 2026-10-07. Sources are linked inline. Numbers marked "(source code)" were read directly from the current `main` branch of each project.

Contents:
1. Sidekiq
2. Solid Queue (Rails / 37signals)
3. GoodJob
4. Que
5. Delayed::Job
6. Resque
7. ActiveJob API conventions, plus the `enqueue_after_transaction_commit` debate
8. Comparison tables (retry formulas, defaults, shutdown, polling vs notify)
9. Recommendations for Potatoq

---

## 1. Sidekiq (Mike Perham)

Redis-backed and multithreaded. It's the most widely used Ruby job system. The OSS core ships with Pro and Enterprise add-ons.

Key references:
- How it works (architecture walkthrough): https://mikeperham.com/how-sidekiq-works
- Best Practices: https://github.com/sidekiq/sidekiq/wiki/Best-Practices
- Error Handling: https://github.com/sidekiq/sidekiq/wiki/Error-Handling
- Signals: https://github.com/sidekiq/sidekiq/wiki/Signals
- Using Redis: https://github.com/sidekiq/sidekiq/wiki/Using-Redis
- Reliability: https://github.com/sidekiq/sidekiq/wiki/Reliability
- Scheduled jobs: https://github.com/sidekiq/sidekiq/wiki/Scheduled-Jobs
- Advanced options: https://github.com/sidekiq/sidekiq/wiki/Advanced-Options
- 7.0 upgrade notes: https://github.com/sidekiq/sidekiq/blob/main/docs/7.0-Upgrade.md
- Changelog: https://github.com/sidekiq/sidekiq/blob/main/Changes.md
- Config defaults (source code): https://github.com/sidekiq/sidekiq/blob/main/lib/sidekiq/config.rb

### 1.1 Defaults (from `Sidekiq::Config::DEFAULTS`, source code)

| Setting | Default | Notes |
|---|---|---|
| `concurrency` | **5** threads | Was 25 originally, then **25 to 10 in 5.2.0**, then **10 to 5 in 7.0**. Rationale for 7.0: it "matches the Rails default concurrency and database pool size". Docs advise against going above 50. |
| `timeout` (shutdown) | **25 s** | Chosen to fit Heroku's 30 s SIGTERM-to-SIGKILL window. |
| `average_scheduled_poll_interval` | **5 s** | Was 15 s before 5.1. |
| `max_retries` | **25** | About 20 days 10 hours of total retrying. |
| `dead_max_jobs` | **10,000** | Dead set cap. |
| `dead_timeout_in_seconds` | **180 days** (6 months) | Dead set retention. |
| heartbeat | every **10 s** | Process info is stored in Redis with a TTL. |
| `on_complex_arguments` | `:raise` | This is strict args. It raises by default since 7.0. |
| BasicFetch `TIMEOUT` | 2 s | BRPOP timeout, so threads can notice shutdown. |

### 1.2 Retry and backoff (source code: `lib/sidekiq/job_retry.rb`)

```ruby
DEFAULT_MAX_RETRY_ATTEMPTS = 25
delay  = (count**4) + 15                 # base delay, count = retry_count (0-based)
jitter = rand(10 * (count + 1))          # jitter grows with attempt number
retry_at = Time.now.to_f + delay + jitter
conn.zadd("retry", retry_at.to_s, payload)   # retry set is a ZSET scored by time
```
- The wiki shows the formula as `(retry_count ** 4) + 15 + (rand(10) * (retry_count + 1))`. Current code uses `rand(10 * (count + 1))`, which has the same intent.
- Resulting delays are about 15 s, 16 s, 31 s, 96 s, 271 s, and so on. The 25th retry lands about 20.4 days after the first failure. This is long enough to survive a weekend or a multi-day outage of a third party, so a human can fix the bug and the retry then succeeds.
- Failure metadata is stored on the job payload itself: `retry_count`, `error_message` (truncated to 10k chars), `error_class`, `failed_at`, `retried_at`, and `error_backtrace` (zlib-compressed, base64). Backtraces are opt-in (`backtrace: true|N`) because they consume Redis memory.
- `sidekiq_retry_in { |count, exc, job| ... }` can return seconds, `:kill` (go to Dead now), or `:discard` (drop silently). If the block raises, Sidekiq falls back to the default formula.
- `sidekiq_retries_exhausted { |job, exc| }` runs per job class. `death_handlers` are global and run "right before Sidekiq moves the job to the Dead set".
- `retry: false` means no retry and no dead set (the job vanishes). `retry: 0` sends it straight to Dead. `dead: false` skips the dead set.
- `retry_for: 48.hours` (since 7.1.3) does time-based retries. It is mutually exclusive with `retry` since 8.1.
- **Shutdown is not a failure.** `Sidekiq::Shutdown` exceptions are re-raised and the job is pushed back to the queue without incrementing `retry_count`. This holds even if the shutdown exception is wrapped as a `cause`.
- The dead set ("morgue") is the `dead` ZSET. On every insert it is trimmed by age (`zremrangebyscore`) and by count (`zremrangebyrank`) inside one `MULTI`.

### 1.3 Reliability guarantees

- Sidekiq states the guarantee as **at-least-once**: "Even a job which has completed can be re-run" if Redis goes away between completion and acknowledgement (Best Practices).
- **OSS BasicFetch uses BRPOP.** The job is removed from Redis when fetched. "If Sidekiq crashes while processing that job, it is lost forever." On graceful shutdown, in-progress jobs are re-pushed (`bulk_requeue`). Jobs are lost only on SIGKILL, OOM, or power loss. So OSS Sidekiq is at-most-once on hard crash and at-least-once otherwise.
- **Pro `super_fetch`** uses `LMOVE` (formerly `BRPOPLPUSH`) into a **private per-process queue**. The job stays in Redis until it is acknowledged. Another process detects orphaned private queues via expired heartbeats and pushes their jobs back. Recovery timing is not guaranteed ("5 minutes or 3 hours").
- **Poison pill detection.** A job recovered 3 times within 72 hours is moved to Dead instead of being re-queued, so a job that crashes the process can't take down the whole fleet. Solid Queue made the same decision more conservatively (section 2.6).
- **Reliable scheduler (Pro)** moves due jobs from the schedule ZSET to the queue atomically in Lua. OSS is two steps: an atomic Lua pop from the ZSET, then a push, so a crash in between can lose a job.
- **Reliable push (Pro, client side):** `Sidekiq::Client.reliable_push!` buffers jobs locally while Redis is unreachable.
- GoodJob's comparison table summarizes it bluntly: "Sidekiq: crashes lose jobs; Sidekiq Pro: RPOPLPUSH."

### 1.4 Scheduled jobs and the poller (source code: `lib/sidekiq/scheduled.rb`)

- `perform_in(seconds)` and `perform_at(ts)` put jobs into the `schedule` ZSET scored by Unix time (`.to_f`, which avoids timezone confusion). Retries use the `retry` ZSET. One poller drains both.
- Jobs scheduled in the past run immediately. The docs say plainly that the scheduler is "**not** meant to be second-precise".
- Atomic pop is done in Lua: `zrange key -inf now byscore limit 0 1`, then `zrem`, then return. It moves one job at a time.
- **Random poll interval, with no coordination.** The comment in the source:
  > "We want one Sidekiq process to schedule jobs every N seconds. We have M processes and **don't** want to coordinate. So in N*M second timespan, we want each process to schedule once."
  - The base interval is `process_count * average_scheduled_poll_interval`. Process count comes from the heartbeat set.
  - If `count < 10`: `interval * rand + interval / 2` (±50% around the mean, which avoids gaps with small clusters).
  - Otherwise: `interval * rand * 2`.
  - The initial sleep is 5 to 15 s so heartbeats register first and restarted fleets don't poll in lockstep (thundering herd).
- Lesson: you can avoid leader election for the scheduler if the move operation is atomic and idempotent. Randomized polling scaled by cluster size keeps load on the backend constant as the fleet grows.

### 1.5 Signals and graceful shutdown

- **TSTP** = quiet: stop fetching, finish current work. (It was USR1 before 5.0.)
- **TERM**: stop fetching, wait up to `-t` (25 s), then hard shutdown. Remaining jobs are pushed back to Redis and `Sidekiq::Shutdown` is raised in worker threads.
- **TTIN** dumps all thread backtraces to the log, which helps debug stuck processes.
- Deploy recipe: send TSTP at the start of the deploy and TERM at the end, which gives in-flight jobs as long as possible.
- Signal handlers write to a self-pipe and the real logic runs outside trap context, to avoid mutex reentrancy issues.
- **Iterable jobs (7.3):** `build_enumerator(*args, cursor:)` plus `each_iteration(item)`. The cursor is persisted in Redis at `it-<jid>`. On shutdown the job checks `interrupted?` after each iteration, saves its cursor, and raises `Sidekiq::Job::Interrupted`. The retry subsystem ignores that exception and the job resumes later. Each iteration must finish within the 25 s timeout. `max_iteration_runtime` re-enqueues long-running iterations at the back of the queue. This idea came from Shopify's `job-iteration`. Rails 8.1 added the same concept as `ActiveJob::Continuable` (section 7.4). https://github.com/sidekiq/sidekiq/wiki/Iteration

### 1.6 Best practices (wiki)

1. **Small, simple parameters.** Pass only JSON-native types. "Don't save state to Sidekiq, save simple identifiers." Pass IDs and look up the record in `perform`. Symbols, Date/Time, and objects don't round-trip.
2. **Make jobs idempotent and transactional.** Execution is at-least-once. Wrap DB changes in a transaction, or make side effects safe to repeat.
3. **Embrace concurrency.** Use connection pools to protect limited resources rather than single-threaded special queues.
4. **Precise terminology:** "job" (enqueued unit), "job class" (code), "thread", "process". Sidekiq renamed `Worker` to `Job` in 6.3.

**Strict args:** added in 6.4 as a warning. Since 7.0 it raises by default on non-JSON-native args. Since 8.1 `perform_inline` enforces it too, so tests can't hide serialization bugs. A celery-replacement equivalent is to reject pickle and validate that args are JSON-serializable at enqueue time.

### 1.7 Transaction-aware client

- Blog post: https://mikeperham.com/2022/06/17/coming-soon-in-sidekiq-2022-edition
- The problem: "Sidekiq has long had a 'problem' with executing jobs very fast, before any associated transaction has committed, leading to 'Cannot find Model id=1234' errors."
- `Sidekiq.transactional_push!` (6.5 beta) defers the Redis push to `after_commit`. It needs `after_commit_everywhere` on Rails < 7.2. The JID is **pre-allocated** so `perform_async` can return it immediately and the caller can store it in the same transaction.
- It is not applied to `push_bulk`, because holding potentially hundreds of thousands of payloads in memory until commit is risky.
- Remaining gap (Brandur, https://brandur.org/job-drain): after-commit enqueue can still **lose** the job if the process dies between COMMIT and the push. Only a transactional outbox (staged-jobs table drained by a separate process) or a same-DB queue closes that gap.

### 1.8 Redis advice (wiki "Using Redis")

- `maxmemory-policy noeviction` is mandatory. Otherwise Redis silently evicts jobs.
- Use a **separate Redis for cache vs jobs.** The job Redis must be "a persistent store", not a cache. `redis-namespace` was dropped in 7.0; use separate DBs or instances instead.
- Raise `network_timeout` and `pool_timeout` from 1 s to 5 s in cloud environments.
- Use Sentinel for HA. **Redis Cluster is not appropriate**: there are hot keys and Sidekiq needs multi-key transactions.
- Requirements over time: Redis 6.2+ for 7.0, Redis 7.0+ for 8.0. 8.0 also supports Valkey 7.2+ and Dragonfly 1.27+.
- Capacity: about 20k jobs/s on a single Redis.

### 1.9 Sidekiq 7 capsules, embedding, queues

- **Capsules (7.0)** are multiple thread pools in one process, each with its own queues and concurrency. Typical use is a single-threaded capsule for thread-unsafe or serial jobs. The default capsule has 5 threads on `default`.
- **Embedding (7.0):** run inside Puma. Keep concurrency at 1 to 2 because "your puma threads + sidekiq concurrency should never be greater than 5" (GVL contention). Graceful restarts aren't supported when embedded. https://github.com/sidekiq/sidekiq/wiki/Embedding
- **Queue selection:** strict ordering (`[critical, default, low]`) or weighted (`[[critical, 2], [default, 1]]`, implemented by repeating queue names and shuffling before each BRPOP). You can't mix the two modes.

### 1.10 Unique jobs, batches, cron, rate limits (Pro and Enterprise)

- **Unique jobs (Ent)** ([wiki](https://github.com/sidekiq/sidekiq/wiki/Ent-Unique-Jobs)): the key is (class, queue, args) and `unique_for` is a TTL. `unique_until: :success` (default) holds the lock through retries. `:start` unlocks right before execution. Perham's advice: treat uniqueness as "best effort, not a 100% guarantee" and "A lock period of more than a few minutes should be considered a code smell." Uniqueness is mainly an optimization to avoid wasted work, not a correctness guarantee. Rosa Gutiérrez says the same for Solid Queue (https://github.com/rails/solid_queue/issues/105).
- **Batches (Pro)** ([wiki](https://github.com/sidekiq/sidekiq/wiki/Batches)): callbacks are `success` (all succeeded), `complete` (all ran once, regardless of outcome), and `death` (the first job to die). Callbacks are themselves jobs. A job can reopen its own batch to add children. Successful batches expire after 24 h. Pending batches expire after 30 days.
- **Periodic jobs (Ent)** ([wiki](https://github.com/sidekiq/sidekiq/wiki/Ent-Periodic-Jobs)): the **leader** process enqueues cron jobs. There is no backfill of missed runs; the advice is to make cron jobs process "the last N hours" rather than "the last hour". Minimum granularity is 1 minute. Per-job timezone is supported. The Web UI can pause, unpause, and enqueue now.
- **Leader election (Ent)** ([wiki](https://github.com/sidekiq/sidekiq/wiki/Ent-Leader-Election)): a Redis key with TTL. The leader renews every **15 s** and followers check every **60 s**. The leader steps down on clean exit so failover during deploys is fast.
- **Rate limiters (Ent)** ([wiki](https://github.com/sidekiq/sidekiq/wiki/Ent-Rate-Limiting)) come in five kinds: concurrent, bucket, window, leaky, and points. On `OverLimit` the job is rescheduled with `(300 * overrated) + rand(300) + 1` seconds of backoff, up to 20 times (about 1 day), and then treated as a normal failure. `wait_timeout` defaults to 5 s.

### 1.11 Web UI and monitoring

([wiki](https://github.com/sidekiq/sidekiq/wiki/Monitoring))
- Pages: Dashboard (processed/failed graphs, Redis info), Busy (processes and threads with current jobs, quiet/stop buttons), Queues (size, **latency**, delete, pause in Pro), Retries, Scheduled, Dead (retry, delete, kill), Metrics (per-class execution time histograms with deploy markers, added in 7.0, kept for 72 h since 8.0), Cron and Batches (Pro/Ent).
- JSON endpoints: `/stats` and `/stats/queues`. There's also the `sidekiqmon` CLI.
- **Monitor queue latency, not queue size.** Large bursts make backlog alerts give false positives. Latency is the age of the oldest job in the queue.
- Auth is the host app's responsibility. It needs a valid session for CSRF protection.
- 8.0 added Vernier profiling of individual jobs.

---

## 2. Solid Queue (Rails default since Rails 8; 37signals)

References:
- README: https://github.com/rails/solid_queue
- Intro post: https://dev.37signals.com/introducing-solid-queue/
- Schema: https://github.com/rails/solid_queue/blob/main/lib/generators/solid_queue/install/templates/db/queue_schema.rb

### 2.1 Why DB-backed

- 37signals moved off Resque plus "seven different gems". The goals were simplicity and operability: "Having everything stored in a relational DB, interfaced by Active Record, has made debugging job-related issues significantly easier compared to troubleshooting issues with Resque."
- It must support **MySQL, PostgreSQL, and SQLite** (unlike GoodJob, which is Postgres only), because it's the Rails default.
- Scale at HEY: about **5.6M jobs/day**, about **1,300 polling queries/s**, an **average query time of 110 µs**, and **0.02 rows examined per query**.

### 2.2 Schema design (one row per job plus one "execution" table per state)

`solid_queue_jobs` is the immutable job record: queue_name, class_name, arguments, priority, active_job_id, scheduled_at, finished_at, concurrency_key, and batch_id. Its **current state** is shown by the presence of a row in exactly one execution table. Each execution table has a unique index on `job_id` and an FK with `ON DELETE CASCADE`.

| Table | Purpose | Key index |
|---|---|---|
| `ready_executions` | polled by workers | `(priority, job_id)` = poll all; `(queue_name, priority, job_id)` = poll one queue |
| `scheduled_executions` | future jobs, polled by dispatcher | `(scheduled_at, priority, job_id)` |
| `claimed_executions` | in-flight, with `process_id` | `(process_id, job_id)` |
| `blocked_executions` | waiting on a concurrency semaphore | `(concurrency_key, priority, job_id)`, `(expires_at, concurrency_key)` |
| `failed_executions` | error text | `job_id` |
| `recurring_executions` | cron dedup | **unique `(task_key, run_at)`** |
| `semaphores` | concurrency limits | unique `key`, plus `expires_at` |
| `processes` | registry and heartbeats | `last_heartbeat_at`; `kind`, `pid`, `hostname`, `supervisor_id`, `name` |
| `pauses` | paused queues | unique `queue_name` |
| `recurring_tasks` | static and dynamic cron definitions | unique `key` |
| `batches`, `batch_executions` | batch tracking | |

Rationale: jobs are "isolated from jobs ready to be executed" by state. This "keep[s] the table that workers poll as small as possible". The hot polled table stays tiny and has a **covering index that also serves the sort**, so `SELECT ... ORDER BY priority, job_id LIMIT n FOR UPDATE SKIP LOCKED` reads almost no dead or irrelevant rows.

### 2.3 Polling queries

Only two polling query shapes are allowed, so a covering index is always used:
```sql
SELECT job_id FROM solid_queue_ready_executions
ORDER BY priority ASC, job_id ASC LIMIT ? FOR UPDATE SKIP LOCKED;

SELECT job_id FROM solid_queue_ready_executions
WHERE queue_name = ? ORDER BY priority ASC, job_id ASC LIMIT ? FOR UPDATE SKIP LOCKED;
```
- A list of queues means **one query per queue, in order** (strict queue priority). Queue order beats job priority. The README advises using one or the other, not both.
- Wildcards (`beta*`) and paused queues need a `SELECT DISTINCT queue_name` first. MySQL handles this with a loose index scan. On Postgres, Solid Queue emulates it with a recursive CTE. "If you want to ensure optimal performance on polling... always specify exact names... and not have any queues paused."
- Claiming happens in one transaction: select ready rows with SKIP LOCKED, insert claimed_executions, delete ready_executions.
- `FOR UPDATE SKIP LOCKED` requires MySQL 8+, MariaDB 10.6+, or PostgreSQL 9.5+. SQLite writes are serialized, so SKIP LOCKED isn't needed.
- On MySQL, run the queue DB at `READ COMMITTED`. Under REPEATABLE READ, gap locks can deadlock enqueue against claim. 37signals runs it this way.
- **The polling interval only applies when idle.** A worker that found work polls again immediately. Rosa: "The polling interval is intended for when there aren't any more jobs to execute" (https://github.com/rails/solid_queue/issues/647). So the polling interval isn't a throttle; use concurrency controls for throttling.

### 2.4 Why not LISTEN/NOTIFY

- Rosa Gutiérrez in https://github.com/rails/solid_queue/issues/772 (closed): "it's very unlikely we'll add that... It's too specific for PostgreSQL. This aims to be quite generic so that it works mostly in the same way with the 3 RDBMS... That would be too much of a divergence." She recommends GoodJob for Postgres-specific needs. A third-party gem `solid_queue-listen_notify` exists. It wires in through lifecycle hooks and treats polling as the "correctness backstop".
- The practical arguments against LISTEN/NOTIFY: it needs a **dedicated, persistent connection**, so it breaks with PgBouncer in transaction mode. Network blips silently drop the subscription. Notifications are lost if nobody is listening. It still needs polling for scheduled jobs and as a safety net. And with a covering index, 0.1 s polling costs only microseconds per query.

### 2.5 Process model

- **Supervisor** forks **workers**, **dispatchers**, and a **scheduler**. Fork is the default and recommended mode. `async` mode runs them as threads in one process. The **Puma plugin** runs the supervisor alongside the web server.
- **Workers** have 3 threads by default, or `fibers: N`. The `processes` default is 1. Size the DB pool to threads plus 2 (polling and heartbeat).
- **Dispatchers** move due scheduled jobs to ready in batches of 500, polling every 1 s. They also run concurrency maintenance, unblocking expired semaphores every **600 s**.
- The **scheduler** enqueues recurring tasks. Its dynamic-task polling interval is 5 s.

| Setting | Default |
|---|---|
| worker `polling_interval` | **0.1 s** |
| worker `threads` | **3** |
| worker `processes` | 1 |
| dispatcher `polling_interval` | **1 s** |
| dispatcher `batch_size` | **500** |
| `concurrency_maintenance_interval` | 600 s |
| `process_heartbeat_interval` | **60 s** |
| `process_alive_threshold` | **5 min** |
| `shutdown_timeout` | **5 s** |
| `preserve_finished_jobs` | true |
| `clear_finished_jobs_after` | **1 day** (a recurring task clears hourly at :12) |
| `silence_polling` | true |
| `default_concurrency_control_period` | 3 min |
| `queues` | `*` |

### 2.6 Shutdown and crash semantics

- `TERM`/`INT`: the supervisor sends TERM to its children and waits `shutdown_timeout` (5 s). Then it sends `QUIT`, which means immediate exit. With QUIT, in-flight jobs "will be returned to the queue when the processes are deregistered".
- **Hard crash** (SIGKILL, OOM): the heartbeat expires and the supervisor prunes the process. Its claimed jobs are marked **failed with `ProcessPrunedError`** and **not retried automatically**. The rationale: "the job itself might be what's killing the process (for example, a job that exhausts the container's memory), and retrying it blindly would just kill the next worker too." `retry_on` can't catch it because nothing raised inside `perform`. You can opt into retrying via the `fail_many_claimed.solid_queue` event, with your own loop guard.
- Liveness is tracked per process, not per thread. A stuck job in a live process stays claimed forever, so stuck-job detection needs an explicit timeout or watchdog. Issue #808 is open: "A heartbeat that blocks forever is never detected".
- Solid Queue has **no retries of its own**. It relies on ActiveJob `retry_on`/`discard_on`. Unhandled failures go to `failed_executions` for manual retry or discard via Mission Control.

### 2.7 Concurrency controls (`limits_concurrency`)

```ruby
limits_concurrency to: 2, key: ->(contact) { contact.account }, duration: 5.minutes,
                   group: "ContactActions", on_conflict: :block   # or :discard
```
- These are semaphores (`key` plus a `value` counter plus `expires_at`). At enqueue, if the semaphore is open, the job is decremented into `ready`. Otherwise it goes to `blocked_executions`.
- When a job finishes (success or failure), it signals the semaphore and moves the next blocked job (by priority) to ready.
- `duration` is a **failsafe** for jobs that never released the semaphore (crash). After it expires, blocked jobs are candidates to unblock during maintenance. The guarantee covers overlap only, not ordering.
- Concurrency for scheduled jobs is checked when they become due, not at enqueue.
- Unknown job classes (renamed or deleted in a deploy) fail with `ClassMissingError` and can be retried later.

### 2.8 Recurring tasks

- Defined in `config/recurring.yml` with `class` + `args` or `command`, a Fugit `schedule` (cron or natural language like "every day at 9am America/New_York"), and optional `queue` and `priority`. Timezone defaults to the app TZ.
- **Dedup without leader election:** many schedulers can run with the same config. Each enqueue inserts into `recurring_executions` **in the same transaction**, and a unique index on `(task_key, run_at)` lets only one insert win. This depends on `preserve_finished_jobs = true`. "Each task schedules the next one... inspired by what GoodJob does."
- Dynamic tasks are supported via `SolidQueue.schedule_recurring_task`. They're stored in the DB and need `dynamic_tasks_enabled`.
- `SOLID_QUEUE_SKIP_RECURRING` and `--only-recurring` flags turn cron off in staging or run it in isolation.

### 2.9 Separate DB by default

- Rails 8 configures Solid Queue in a **separate database** (SQLite file or a separate connection). The README warns: transactional integrity with app data "can also backfire if you base some of your logic on this behaviour, and in the future, you move to another active job backend, or if you simply move Solid Queue to its own database." See section 7.3 for the debate.
- Dashboard: **Mission Control — Jobs** (https://github.com/rails/mission_control-jobs) is a separate engine that works with Solid Queue and Resque.

---

## 3. GoodJob (Ben Sheldon)

References:
- README: https://github.com/bensheldon/good_job
- Intro post: https://island94.org/2020/07/introducing-goodjob-1-0
- Migration schema: https://github.com/bensheldon/good_job/blob/main/lib/generators/good_job/templates/install/migrations/create_good_jobs.rb.erb

### 3.1 Design

- Postgres-only. Multithreaded via concurrent-ruby. Leans fully on ActiveJob for retries, so the core was about 600 LOC at 1.0 versus 1,200 for Que and 2,300 for DJ. "Second-generation: predecessors had to maintain functionality ActiveJob now provides."
- **Session-level advisory locks** (default) give run-once safety. The lock is released automatically if the connection dies, and there's no long-lived transaction during the job. The dequeue query is a materialized CTE of candidates (LIMIT `queue_select_limit`, default 1000), followed by `pg_try_advisory_lock(hash(active_job_id))` and `LIMIT 1`:
  ```sql
  SELECT * FROM good_jobs WHERE id IN (
    WITH rows AS MATERIALIZED (
      SELECT id, active_job_id FROM good_jobs
      WHERE (scheduled_at <= NOW() OR scheduled_at IS NULL) AND finished_at IS NULL
      ORDER BY priority DESC NULLS LAST, created_at ASC LIMIT 1000)
    SELECT id FROM rows WHERE pg_try_advisory_lock(...) LIMIT 1)
  ```
- Advisory lock lessons: session locks only release on the same connection. Combining SKIP LOCKED with advisory locks in one subselect leaked locks (PR #1113). Advisory locks are **incompatible with PgBouncer transaction mode**. Newer GoodJob offers `lock_strategy = :skiplocked` (plain `FOR UPDATE SKIP LOCKED`, with lock state written into `locked_by_id` and `locked_at` columns) and `:hybrid` for rolling migrations. Running behind PgBouncer needs four settings: `:skiplocked`, LISTEN/NOTIFY off, the advisory-lock heartbeat off, and polling on.
- **Schema:** a single `good_jobs` table (with `finished_at`, `scheduled_at`, `error`, `error_event`, `executions_count`, `concurrency_key`, `cron_key`, `cron_at`, `batch_id`, `labels[]`, `locked_by_id`, `locked_at`) plus `good_job_executions` (one row per attempt, with error, backtrace, duration, and process_id), `good_job_processes`, `good_job_settings` (pauses, cron enable/disable), `good_job_batches`, and `good_job_concurrency_claims`. It relies on **partial indexes `WHERE finished_at IS NULL`** to keep the hot index small, which is the single-table alternative to Solid Queue's split tables.

### 3.2 LISTEN/NOTIFY plus polling

- `enable_listen_notify` defaults to true. Enqueue sends NOTIFY, so latency is near zero.
- `poll_interval` is **10 s in production** "in case of a LISTEN/NOTIFY blip". It's -1 (disabled) in development.
- Scheduled jobs are cached in memory (`max_cache` 10,000, about 20 MB) and woken exactly at their time. Polling covers cache overflow.
- Connections needed: one per thread plus **2 extra** (LISTEN/NOTIFY and cron).

### 3.3 Execution modes (in-process "async")

- `:external` is the production default (a separate `good_job start` CLI). `:async` runs threads inside the Rails web server only, not in console or migrations, which is a useful guard. `:inline` is for tests.
- Async mode needs the DB pool sized to web threads plus job threads, and Puma `before_fork`/`before_worker_boot` hooks to stop and restart the scheduler.

### 3.4 Defaults

| Setting | Default |
|---|---|
| `max_threads` | **5** |
| `poll_interval` | 10 s (prod), -1 (dev) |
| `shutdown_timeout` | **-1 (wait forever)**; examples use 25 |
| `queues` | `*` |
| `enable_cron` | false (opt-in per process) |
| `preserve_job_records` | **true**; cleaned after **14 days**, every 1,000 jobs or 600 s |
| `retry_on_unhandled_error` | **false** (no retry unless ActiveJob `retry_on`) |
| `queue_select_limit` | 1000 |
| `dequeue_query_sort` | `:created_at` (moving to `:scheduled_at`) |

### 3.5 Reliability and shutdown

- "GoodJob guarantees that a completely-performed job will run once and only once." A crashed or interrupted job is **retried automatically**: the advisory lock drops with the connection. On retry, `GoodJob::InterruptError` is raised inside the job if you include the `InterruptErrors` extension, so jobs can `discard_on` or `retry_on` an interrupt. Compare Solid Queue's "fail, don't retry" choice for crashes.
- Jobs can check `GoodJob.current_thread_shutting_down?` to cooperatively exit loops.
- **Timeouts:** avoid Ruby's `Timeout` (Perham: "Ruby's most dangerous API"). Use library-level timeouts (`open_timeout`, `read_timeout`) together with `discard_on` or `retry_on`.

### 3.6 Cron, concurrency, batches, dashboard

- **Cron:** any process with `enable_cron` can enqueue, and duplicates are prevented by a **unique index on `(cron_key, cron_at)`**. Fugit supports **seconds-level** resolution. `cron_graceful_restart_period` backfills runs missed during a deploy window. Cron can also be a proc `(last_ran) -> next_time`. Tasks can be enabled or disabled from the dashboard via `good_job_settings`.
- **Concurrency:** `total_limit`, `enqueue_limit`, `perform_limit`, and throttles (N per period). Perform-limit is "optimistic retry with incremental backoff": raise `ConcurrencyExceededError` and `retry_on` it. With `concurrency_claims`, the longest waiter is woken when the key frees up. FIFO isn't strictly preserved. For heavy contention, use dedicated thread pools instead.
- **Batches:** `on_finish`, `on_success`, and `on_discard` callback jobs. They fire even for empty batches. Callbacks may run concurrently.
- **Dashboard:** a mountable Rails engine with jobs, cron, batches, processes, and performance views, plus live polling. Auth is up to the host. There's also an HTTP **health probe** on `--probe-port` for Kubernetes liveness and readiness.
- **Bulk enqueue:** a single multi-row INSERT.

### 3.7 Queue design advice ("Doing your best job")

- **Name queues by latency target** (`latency_30s`, `latency_5m`), not by function (`mailers`).
- "**Priority can't fix a lack of capacity**." Once an elephant (long job) is in the doorway, priority can't preempt it (head-of-line blocking). The fix is **isolated pools**: `"mice:2; badgers,mice:2; *:2"`, so short jobs always have a door the elephants can't block.
- More than 3 to 5 threads per process causes GVL contention in Ruby. (Python's GIL has the same issue; free-threaded 3.13+ changes this.)
- **Scale on queue latency.**

---

## 4. Que (Chris Hanks)

References:
- README: https://github.com/que-rb/que
- Docs: https://github.com/que-rb/que/blob/master/docs/README.md

- Postgres with **advisory locks**: "Workers don't block each other... Locks are held in memory, so locking a job doesn't incur a disk write... Under heavy load, Que's bottleneck is CPU, not I/O." "If a Ruby process dies, the jobs it's working won't be lost... they immediately become available."
- It sells **transactional enqueue**: the job commits or rolls back with your data. Backups are atomic. There are fewer moving parts.
- Que 1.x and 2.x use a **locker thread** per process with LISTEN/NOTIFY (an insert trigger NOTIFYs) and a small **in-memory buffer** (`maximum-buffer-size` 8). It polls only for scheduled or retried jobs (`poll-interval` **5 s**) and checks its buffer every 50 ms (`wait-period`). Registered lockers live in a `que_lockers` table.
- **Worker priorities:** the default pool `10,30,50,any,any,any` (6 workers) reserves workers for higher-priority jobs, so a backlog of low-priority work can't starve urgent jobs. This is a neat alternative to separate queues.
- **Retry formula:** `error_count**4 + 3` seconds (4 s, 19 s, 84 s, 259 s, ...). `maximum_retry_count` defaults to **15** (about 2 days). Exhausted jobs are **expired**: they stay in the table with `expired_at` set, and you revive them with `UPDATE ... SET error_count=0, expired_at=NULL`.
- **Writing reliable jobs:** do the DB writes **and `destroy`/`finish` the job in the same transaction**. That gives exactly-once for DB-only jobs. For external side effects, use an idempotency check (for example, "check for previous charge"). Emails may occasionally double-send, and that's accepted.
- **Shutdown:** Que **refuses to Thread#kill** workers, because killing a thread mid-transaction in Ruby could commit a partial transaction. It waits for jobs to finish and expects the platform to SIGKILL. SIGKILL is safe because Postgres rolls back and releases the advisory lock.
- **Bloat and vacuum warning:** "Que's job table undergoes a lot of churn... susceptible to bloat... The most common cause of this is long-running transactions." Brandur's analysis (https://brandur.org/postgres-queues): a long-running transaction anywhere in the DB pins the MVCC horizon. Dead tuples accumulate in the job index, so each lock attempt scans more of them. Lock time grew 15x and the queue ran away to 60k jobs. Mitigations: keep transactions short, set `statement_timeout`/`idle_in_transaction_session_timeout`, **don't share the queue DB across teams**, use more specific predicates, consider a manual `VACUUM ANALYZE` job, and lock multiple jobs per query.
- Bulk enqueue skips the NOTIFY trigger by default (one NOTIFY per row is a bottleneck).

---

## 5. Delayed::Job (historic)

References: https://github.com/collectiveidea/delayed_job, https://github.blog/2009-11-03-introducing-resque/

- Single `delayed_jobs` table with `priority`, `attempts`, `handler` (**YAML-serialized object**), `last_error`, `run_at`, `locked_at`, `locked_by`, `failed_at`, and `queue`.
- Locking is optimistic: `UPDATE ... SET locked_at=now, locked_by=me WHERE id=? AND (locked_at IS NULL OR locked_at < now - max_run_time) AND run_at <= now`. There is no SKIP LOCKED, so there's contention, and a crashed job stays locked until `max_run_time` (**4 h**) passes.
- Defaults: `max_attempts` **25**, retry `5 + attempts**4` seconds, `sleep_delay` **5 s**, `read_ahead` 5, priority 0 (lower runs first), `destroy_failed_jobs` true (failed jobs are deleted by default, which is a bad default). Single-threaded workers.
- **Lessons:**
  - **YAML/object serialization** caused security issues (YAML deserialization RCE) and broke whenever classes changed. This is why Sidekiq and ActiveJob insist on JSON-safe args. Celery's pickle has the same problem.
  - GitHub, 2009: "DJ works great with small datasets, but once your site starts overloading and the queue backs up (to, say, 30,000 pending jobs) its queries become expensive". Enqueue took 2+ s. "All the workers were babysitting each other." They had to bolt on god or monit. This led to Resque. Modern DB queues (SKIP LOCKED, split or partial indexes) fixed the query-cost problem DJ had.
  - Lock expiry tied to max runtime means slow failure detection. Heartbeats are better.

---

## 6. Resque (GitHub, 2009)

Reference: https://github.com/resque/resque

- Redis lists, with a **fork per job**: "Resque assumes chaos. Resque assumes your background workers will lock up, run too long, or have unwanted memory growth." The child process exits after each job, so memory leaks can't accumulate and a crashed job can't corrupt the parent. The costs are fork overhead per job (around ms on CoW Linux, much worse with large heaps and no CoW-friendly GC) and one job at a time per process.
- Signals: `QUIT` waits for the child then exits. `TERM`/`INT` kill the child immediately. `USR1` kills the child without exiting. `USR2` pauses and `CONT` resumes. On Heroku the `TERM_CHILD`, `RESQUE_PRE_SHUTDOWN_TIMEOUT`, and `RESQUE_TERM_TIMEOUT` knobs were needed to avoid `Resque::TermException`. Lesson: the default should be graceful, and it should match platform grace periods.
- Queue priority is strict left-to-right queue order. Polling `INTERVAL` defaults to **5 s**. `INTERVAL=0` means drain then exit.
- No built-in retries (only plugins such as resque-retry and resque-scheduler) and no scheduled jobs in core. The job is popped (`LPOP`), so a crash loses it. Failure backends are pluggable and chainable. The original web UI (resque-web) was a big part of its appeal.
- The lesson from its decline: shipping retries, scheduling, and reliability as plugins fragmented the ecosystem. 37signals' Basecamp needed about 7 gems. Batteries-included wins.
- Takeaway for Python: a fork-per-job or max-tasks-per-child option is valuable for leaky native-extension workloads (Celery has `worker_max_tasks_per_child` and `worker_max_memory_per_child`), but it should not be the default.

---

## 7. ActiveJob API conventions

References:
- Rails guide: https://guides.rubyonrails.org/active_job_basics.html
- Source for exceptions and retries: https://github.com/rails/rails/blob/main/activejob/lib/active_job/exceptions.rb

### 7.1 Enqueue API

- `MyJob.perform_later(args)`; `MyJob.set(wait: 5.minutes)`, `set(wait_until: t)`, `set(queue: :x, priority: n)`; `perform_now` (inline); `ActiveJob.perform_all_later(jobs)` (bulk). Class-level `queue_as`, `queue_with_priority`.
- Arguments are serialized to JSON, with GlobalID for AR records (records are passed by reference and fetched at perform). `DeserializationError` occurs if the record has since been deleted, which is often handled with `discard_on ActiveJob::DeserializationError`.
- Callbacks: `before/around/after_enqueue`, `before/around/after_perform`, `after_discard`.
- `job.provider_job_id` and `successfully_enqueued?`. An `enqueue_error` is set instead of raising, which Solid Queue considered a footgun for jobs enqueued by framework code.

### 7.2 Retries (`retry_on` / `discard_on`)

```ruby
retry_on(*exceptions, wait: 3.seconds, attempts: 5, queue: nil, priority: nil, jitter: 0.15, report: false)
# :polynomially_longer => executions**4 + rand*executions**4*jitter + 2   (~3s, ~18s, ~83s, ...)
# attempts: :unlimited supported; block called when attempts exhausted; otherwise re-raise to backend
discard_on ActiveJob::DeserializationError
rescue_from(SomeError) { |e| ... }
```
- Default `retry_on` is **5 attempts with a fixed 3 s wait plus 15% jitter**. `:exponentially_longer` was renamed to `:polynomially_longer` because it was never exponential.
- Retry counts are tracked **per exception class group** (`exception_executions` hash in the payload), so different errors have independent budgets.
- `retry_job` re-enqueues a fresh job with incremented `executions`. Retries are new enqueues, which is why backend-level retries (Sidekiq) and AJ-level retries layer awkwardly.
- **By default ActiveJob does not retry.** Without `retry_on`, the exception goes to the backend. The backend's behavior differs: Sidekiq retries 25 times, Solid Queue marks failed, GoodJob marks failed. This inconsistency is a well-known foot-gun. GoodJob's README recommends `retry_on StandardError, wait: :polynomially_longer, attempts: Float::INFINITY` on `ApplicationJob`. `ActionMailer::MailDeliveryJob` doesn't inherit `ApplicationJob`, so the config has to be duplicated there, which is another foot-gun.

### 7.3 `enqueue_after_transaction_commit` (the debate)

Timeline:
1. **Rails 7.2 (PR #51426, Jean Boussier, merged 2024-04-03)** made Active Job transaction-aware. https://github.com/rails/rails/pull/51426
   > "A fairly common mistake with Rails is to enqueue a job from inside a transaction, with a record as argument, which then lead to a `RecordNotFound` error when picked up by the queue. This is even one of the arguments advanced for job runners backed by the database such as solid_queue, delayed_job or good_job. But relying on this is undesirable in my opinion as it makes the Active Job abstraction leaky, and if in the future you need to migrate to another backend or even just move the queue to a separate database, you may experience a lot of race conditions of the sort."

   The setting took `:never`, `:always`, or `:default`. `:default` meant "ask the adapter". Sidekiq and Resque adapters said yes, so in practice it was on for most apps. Jobs were deferred to after commit and **dropped on rollback**.
2. **Solid Queue's position flip-flopped** (https://github.com/rails/solid_queue/issues/212). DHH at first leaned toward `false` (Solid Queue in the same DB gives real transactional enqueue), then reversed:
   > "we're going to change this back to true... The window for false is simply too small. By default, Rails 8.0 will use sqlite and sq out of the box, and sq will have a separate sqlite file for that use case. So transactional guarantees can't be offered in that case. But worse, if you do start with mysql/pgsql, you might end up building app logic dependent on the transactional guarantees that lead to a lot of subtle bugs when scale necessitates that you move sq to its own db. That seems like a ticking bomb..."
3. DHH on the API (https://github.com/rails/rails/pull/52659): "I don't love the :default option being this mystery box that's dependent on the adapter... this setting should just either be true or false."
4. **Rails 8.0 (PR #53375, Rafael França)** deprecated the global config, made it a boolean, and **defaulted to false**. https://github.com/rails/rails/pull/53375
   > "The value of this setting isn't delegated to the queue adapter anymore, since the behavior that this setting controls is too dangerous to leave this change of behavior as something that users don't control."

   Issue #52675: "changing its value globally to all jobs can be too much of a sharp knife." It stayed per-job opt-in.
5. **Rails 8.1** made the global setting non-functional (per-job only).
6. **Rails 8.2 (main, current)** un-deprecated the global toggle (Jeremy Daer) and **made `enqueue_after_transaction_commit = true` the default for new apps** (`load_defaults "8.2"`). Changelog: "Jobs are now enqueued after transaction commit. This fixes that jobs would surprisingly run against uncommitted and rolled-back records." https://github.com/rails/rails/blob/main/activejob/CHANGELOG.md

The implementation uses `ActiveRecord.after_all_transactions_commit { super }`. `successfully_enqueued` is set optimistically. `perform_all_later` partitions jobs into deferred and immediate.

Arguments on each side:
- **For defer-by-default:** it eliminates the #1 job bug (`RecordNotFound` or stale reads because the worker beat the commit). Behavior is identical across backends and across same-DB vs separate-DB setups. Jobs enqueued in rolled-back transactions don't run.
- **Against or caveats:**
  - (a) Code that reads the job ID or expects the enqueue to happen immediately behaves differently: `perform_later` returns before the job exists and `provider_job_id` is nil.
  - (b) A crash between COMMIT and the push loses the job (Brandur's job-drain argument). Only an outbox or same-DB queue truly fixes this.
  - (c) Some jobs legitimately must be enqueued even if the transaction rolls back, for example audit or alert jobs.
  - (d) Per-adapter magic defaults are confusing. The behavior should be explicit and owned by the user.
  - (e) Same-DB queues can offer real atomic enqueue (Que's selling point), but building on it is a "ticking bomb" if you ever move the queue.

### 7.4 Continuations (Rails 8.1)

`include ActiveJob::Continuable`. Then `step :name do |step| ... step.advance! from: record.id end`. The cursor is saved when the queue is shutting down and the job resumes from the last step and cursor. This standardizes Sidekiq Iteration and Shopify's job-iteration across backends.

---

## 8. Cross-system comparison

### 8.1 Retry and backoff

| System | Default attempts | Formula (seconds) | Total span | After exhaustion |
|---|---|---|---|---|
| Sidekiq | 25 retries | `count**4 + 15 + rand(10*(count+1))` | ~20.4 days | Dead set (10k cap, 6 months) |
| ActiveJob `retry_on` | 5 attempts | fixed 3 s (±15%), or `n**4 + jitter + 2` | seconds to minutes | re-raise to backend, or block |
| Que | 15 retries | `count**4 + 3` | ~2 days | `expired_at` set, row kept |
| Delayed::Job | 25 attempts | `attempts**4 + 5` | ~20 days | `failed_at` set (deleted by default) |
| Sidekiq Ent rate limit | 20 reschedules | `300*n + rand(300) + 1` | ~1 day | normal failure |
| Solid Queue / GoodJob | 0 (delegate to AJ) | n/a | n/a | failed record kept for manual retry |
| Resque | 0 | n/a | n/a | failure backend |

The `n**4` polynomial is the Ruby consensus (DJ, then Sidekiq, Que, and AJ). It's gentle at first and reaches about 4.5 days at n=24. Every system adds jitter to avoid synchronized retry storms.

### 8.2 Shutdown

| System | Signal for graceful | Default grace | After timeout |
|---|---|---|---|
| Sidekiq | TSTP (quiet), TERM | 25 s | re-push to queue, no retry count increment |
| Solid Queue | TERM/INT, QUIT=immediate | 5 s | QUIT; in-flight released back to queue on deregistration |
| GoodJob | TERM/INT | forever (-1) | n/a (relies on platform SIGKILL; advisory lock drops, job retried) |
| Que | TERM | waits for jobs | expects SIGKILL; PG rolls back and unlocks |
| Resque | QUIT (graceful), TERM (kill child) | n/a | child killed |

### 8.3 Crash recovery (SIGKILL / OOM / power loss)

| System | Behavior |
|---|---|
| Sidekiq OSS | **job lost** |
| Sidekiq Pro super_fetch | re-queued after heartbeat expiry; poison pill (3 times in 72 h) goes to Dead |
| Solid Queue | after 5 min heartbeat threshold: **marked failed, not retried** (`ProcessPrunedError`) |
| GoodJob | advisory lock released when the connection drops; **retried**, with `InterruptError` raised in the job if opted in |
| Que | advisory lock released; retried immediately |
| Delayed::Job | stays locked until `max_run_time` (4 h), then retried |

### 8.4 Polling vs LISTEN/NOTIFY

| System | Mechanism | Idle poll |
|---|---|---|
| Sidekiq | Redis BRPOP (blocking, push-like) | 2 s BRPOP timeout; schedule poller ~5 s × process count |
| Solid Queue | polling only, all DBs | 0.1 s worker, 1 s dispatcher |
| GoodJob | LISTEN/NOTIFY plus poll fallback plus in-memory scheduled cache | 10 s |
| Que | LISTEN/NOTIFY plus poll for scheduled jobs | 5 s |
| DJ / Resque | polling | 5 s |

The consensus: **notification is a latency optimization, polling is the correctness backstop.** Solid Queue shows that cheap polling against a tiny covering-indexed table is fine at 5.6M jobs/day. LISTEN/NOTIFY needs a dedicated, non-pooled connection, so it is incompatible with PgBouncer in transaction mode.

---

## 9. Recommendations for Potatoq

### 9.1 Reliability model
1. **At-least-once by default on every backend, and say so loudly** in docs. Lead with "make tasks idempotent and transactional". Use Que's recipe of finishing the job in the same transaction as the DB writes when the queue shares the DB.
2. **Never lose jobs on crash.** Every broker should use a reliable fetch:
   - Redis: `LMOVE`/`BLMOVE` into a per-worker processing list, or Redis Streams with consumer groups plus `XAUTOCLAIM`.
   - RabbitMQ: manual ack with `acks_late` semantics.
   - DB backends: claim rows with heartbeat and lease.

   This is the biggest gap in OSS Sidekiq and is paid-only there.
3. **Poison pill protection.** Count crash recoveries per job (`recovered_count`). After 3 recoveries in a window (Sidekiq Pro: 3 in 72 h), move the job to dead instead of re-queueing. This gives the benefit of Solid Queue's "don't blindly retry OOM killers" without its cost of never auto-recovering.
4. **Shutdown is not a failure.** Re-queue interrupted jobs without incrementing the retry count.

### 9.2 Defaults
| Setting | Proposed default | Rationale |
|---|---|---|
| concurrency | 5 threads per process (or `cpu_count` for a process pool) | Sidekiq 7 and GoodJob consensus; matches typical DB pool sizes |
| retries | 25 with `count**4 + 15 + rand(10*(count+1))` s (~20 days) | Sidekiq's battle-tested curve; survives weekends |
| retry policy | **retry by default** (unlike Celery, where autoretry is opt-in) | Celery's opt-in retry and AJ's no-retry default both cause silent loss |
| dead letter | keep the last 10,000 or 180 days, whichever is first; trim on insert | Sidekiq morgue; visible and retryable from the UI |
| shutdown grace | 25 s, configurable; also a "quiet" signal (SIGTSTP) | Fits Heroku and k8s 30 s windows |
| hard-kill path | re-queue in-flight work before exiting; mark as interrupted | |
| heartbeat | 10 s interval, dead after 60 s (Redis); DB: 10 to 30 s and 2 to 5 min | Solid Queue's 60 s / 5 min is slow for recovery |
| scheduled poll | ~5 s average, randomized, scaled by process count | Sidekiq's no-coordination poller |
| DB worker idle poll | 0.1 to 1 s, poll again immediately when work is found | Solid Queue |
| NOTIFY | on for Postgres (opt-out for PgBouncer), with polling always on as a fallback | GoodJob / Que |
| finished-job retention (DB) | keep 1 day (Solid Queue) to 14 days (GoodJob); auto-prune via built-in recurring task | |
| args | JSON-only, **strict by default** (raise on non-JSON). No pickle. | Sidekiq 7 strict_args; DJ YAML lesson |
| enqueue inside a DB transaction | **defer to after commit by default** when the integration knows about the transaction (Django `transaction.on_commit`, SQLAlchemy session events); per-task opt-out | Rails 8.2 landed here after two years of debate |

### 9.3 DB backend schema (Postgres, SQLite, MySQL)
- Adopt Solid Queue's **split-state design**: a `jobs` table (immutable payload plus `finished_at`) and small per-state tables `ready`, `scheduled`, `claimed`, `blocked`, `failed`, each with a unique `job_id` FK and `ON DELETE CASCADE`. The polled table stays tiny with a covering index `(queue, priority, id)` and `(priority, id)`. Alternative: GoodJob's single table with partial indexes `WHERE finished_at IS NULL` (Postgres only; SQLite also supports partial indexes, MySQL does not).
- Claim: `SELECT ... ORDER BY priority, id LIMIT n FOR UPDATE SKIP LOCKED`, then insert into claimed and delete from ready in one short transaction. **Never hold a transaction open while running user code.** Que, GoodJob, and Brandur all point at long transactions as the cause of MVCC bloat and runaway queues.
- SQLite: writes are serialized, so no SKIP LOCKED is needed. Use `BEGIN IMMEDIATE`, keep transactions tiny, and turn on WAL.
- MySQL: recommend `READ COMMITTED` for the queue connection.
- Restrict polling query shapes (exact queue names). Treat wildcard and paused-queue lookups as slower paths.
- **Recommend a separate DB or connection for the queue by default** (Solid Queue). Document the bloat and long-transaction risks. If the queue shares the DB, offer real transactional enqueue as an explicit "sharp knife" mode.
- Bulk enqueue with a single multi-row INSERT. Send one NOTIFY per batch, not per row.

### 9.4 Process model
- A supervisor with forked children (Solid Queue) for isolation, with thread or async pools inside. Separate roles: **worker**, **scheduler/dispatcher** (promotes due scheduled jobs, unblocks semaphores, prunes dead processes), and **cron**. All of them can run in one command for small deployments, or be split.
- Isolated named pools in one process (Sidekiq capsules, GoodJob's `"mice:2; elephants,mice:1"`). Encourage queues **named by latency target**, and teach that "priority can't fix a lack of capacity".
- An optional embedded or in-web-process mode (GoodJob async, Sidekiq embed) for small apps, guarded so it doesn't start in shells or migrations.
- Optional `max_tasks_per_child` or memory recycling (the Resque fork-per-job lesson), off by default.
- An HTTP health and liveness probe port (GoodJob) plus a "dump stacks" signal (Sidekiq TTIN, which maps to Python `faulthandler`).

### 9.5 Scheduling, cron, and leader election
- Delayed jobs: Redis ZSET with an atomic Lua move (pop by score, then push in the **same** script, unlike the OSS Sidekiq two-step). DB: a `scheduled` table plus a dispatcher batch move (500 per tick).
- Cron: **dedupe via a unique key `(task_key, scheduled_run_at)`**, inserted atomically with the enqueue (Solid Queue and GoodJob). For Redis, `SET key NX EX` per tick. This makes **leader election unnecessary**: run cron in any number of processes. If a leader is used anyway, model it on Sidekiq Ent (TTL key, renew every 15 s, step down on clean exit).
- Support an optional **missed-run catch-up window** (GoodJob `cron_graceful_restart_period`). The default is no backfill (Sidekiq Ent), with docs advising cron tasks to process "the last N hours".
- Cron spec: standard 5-field with optional seconds, timezone per task (default app TZ), and natural language optional. Support dynamic tasks stored in the backend, pause and resume and run-now from the UI, and a flag to disable cron in staging.

### 9.6 Concurrency, uniqueness, and rate limits
- Built-in `limits_concurrency(key=..., to=1, duration=3min, on_conflict="block"|"discard")` with semaphore semantics. `duration` acts as a crash failsafe (Solid Queue). Blocked jobs are released by priority.
- Unique jobs: TTL-based, "best effort" (Sidekiq Ent / SQS dedup window). Unlock on success by default. Warn that locks longer than a few minutes are a smell.
- Rate limits (bucket, window, concurrent) with reschedule-on-overlimit backoff (`300*n + rand(300)`), capped.

### 9.7 Long jobs
- Cooperative interruption API (`is_shutting_down()`), plus an iteration or continuation helper with a persisted cursor (Sidekiq IterableJob, Rails Continuable). Interruption must not count as a failure.
- Per-task timeouts implemented by the worker supervisor (kill and re-queue), not by in-thread exceptions. Ruby's `Timeout` is "the most dangerous API"; Python thread-interruption has the same problem.

### 9.8 Dashboard and observability
- Ship a built-in web UI (Sidekiq Web and Resque-web drove adoption; DJ's lack of one hurt it). Pages: overview (processed and failed graphs), queues (size **and latency**, pause), busy (processes and threads, current job, quiet/stop), scheduled, retries, dead (retry, delete, bulk), cron (pause, run now), batches, and per-task metrics (p50/p95 runtime, deploy markers). It should be mountable in Django, FastAPI, or Flask with host-provided auth, and offer JSON `/stats` endpoints.
- Alert on **queue latency** (oldest job age), not queue length.
- Lifecycle hooks (startup, quiet, shutdown, heartbeat) and structured events for every state transition (enqueued, started, succeeded, retried, dead, discarded, recovered).
