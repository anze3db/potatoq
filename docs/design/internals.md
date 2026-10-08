# Backend designs

Every broker implements the same contract (`potatoq/brokers/base.py`): enqueue (now or
later), claim one task, then complete, retry, requeue or dead-letter it, with the result
and follow-up tasks (chain links, chord callbacks) settled together. Each also provides
liveness, recovery, chord counters and deduplicated periodic fire times. How each one
does it is chosen to fit that backend. `tests/test_brokers.py` runs the same contract
suite against all of them.

Shared guarantees:

* **At-least-once.** A task is removed only after it finishes. A process that dies mid-task
  (OOM, segfault, `SIGKILL`) gets its task requeued; after `task_max_deliveries` (5)
  crashed deliveries it is dead-lettered as `WorkerLostError`.
* **Claims are fenced.** A worker that lost its claim (recovered as dead, then came back)
  can't ack a newer delivery of the same task.
* **Future tasks live in the broker.** ETA, countdown and retry backoff are never held
  in worker memory.
* **No polling storms.** Idle workers block on a broker-native wake-up and poll
  only as a safety net.

## PostgreSQL

Prior art studied: River, Oban, Solid Queue, graphile-worker, Procrastinate, Que, PGMQ.
Also Brandur's ["Postgres Job Queues & Failure By MVCC"](https://brandur.org/postgres-queues)
and [notifier pattern](https://brandur.org/notifier), Recall.ai's [LISTEN/NOTIFY global
lock post-mortem](https://www.recall.ai/blog/postgres-listen-notify-does-not-scale), and
DBOS's NOTIFY batching numbers.

* **Tables**:
  * `potatoq_jobs`, the hot table. It only holds scheduled, ready and running tasks;
    finished tasks are deleted.
  * `potatoq_results`, with an expiry index.
  * `potatoq_dead`.
  * `potatoq_workers`, the heartbeat table: low fillfactor and no index on the heartbeat
    column, so heartbeat updates are HOT.
  * Chord tables and a `potatoq_periodic` dedup table.

  Tables are created on first use under a transaction-level advisory lock
  (`potatoq migrate` does it explicitly). Use `broker_transport_options={"schema": "..."}`
  to put them in their own schema.
* **Indexes**: partial indexes per state, so each hot query is a short index scan:
  `(queue, priority DESC, seq) WHERE state = 1`, `(run_at) WHERE state = 0`,
  `(worker, pid) WHERE state = 2`. The state literal is in the SQL text, not a
  parameter, so generic plans still match the partial index.
* **Claim**: one statement.
  ```sql
  WITH picked AS MATERIALIZED (
    SELECT seq FROM potatoq_jobs WHERE state = 1 AND queue = $1
    ORDER BY priority DESC, seq LIMIT 1 FOR UPDATE SKIP LOCKED)
  UPDATE potatoq_jobs j SET state = 2, deliveries = j.deliveries + 1, worker = $2, pid = $3, claimed_at = now()
  FROM picked WHERE j.seq = picked.seq RETURNING j.id, j.deliveries, j.payload
  ```
  `MATERIALIZED` matters. Since PG 12 a plain CTE can be inlined, and a nested-loop plan
  can re-run the locking sub-select and claim more rows than `LIMIT`. Oban documents
  this, and it has bitten db-scheduler.
* **No transaction is held while a task runs.** A long transaction pins the xmin
  horizon, the queue table bloats with dead tuples, and claims slow down by orders of
  magnitude. Ownership is `(worker node, pid)` plus a heartbeat row per node. The worker
  supervisor recovers tasks of nodes whose heartbeat is older than `worker_dead_after`,
  and requeues a crashed child's task immediately.
* **Settle in one transaction**: delete the job row (fenced on `deliveries`), upsert the
  result, insert follow-up tasks.
* **Scheduled tasks** sit in `state = 0` with their own partial index. Every worker
  promotes due ones about once a second using `FOR UPDATE SKIP LOCKED`, which is safe to
  run concurrently, then NOTIFYs the affected queues once. Far-future ETAs never touch the
  ready-queue scan; graphile-worker measured 11.8k → 843 jobs/s when they did.
* **Wake-ups**: `LISTEN potatoq:<queue>` on the consumer's connection, with a 1 s poll as
  fallback. NOTIFY takes a cluster-wide lock at commit, so producers debounce it per queue
  (50 ms per process), and busy workers never need it. A single `delay()` is one
  statement: `WITH ins AS (INSERT …) SELECT pg_notify(…) FROM ins`. Behind PgBouncer in
  transaction mode, set `broker_transport_options={"notify": False}`.
* **Transactional enqueue**: `enqueue(messages, connection=...)` writes through the
  caller's DB-API connection (psycopg 2 or 3). The Django and SQLAlchemy integrations do
  this automatically when the broker is the same database.
* **Autovacuum** is tuned on the jobs table: scale factor 0, fixed thresholds, no cost
  delay.

## SQLite

Prior art: huey, litequeue, Litestack, Solid Queue on SQLite, django-tasks-db, plus the
SQLite docs on WAL, busy handling and `data_version`.

* **WAL mode** (persistent), `synchronous=NORMAL`. That is durable across process crashes;
  only an OS crash or power loss can drop the last commits. Pass
  `broker_transport_options={"durable": True}` for `FULL`. Also `busy_timeout=30000`,
  `secure_delete=OFF` (Debian builds turn it on) and `journal_size_limit`.
* **Every write transaction is `BEGIN IMMEDIATE`.** A deferred transaction that reads,
  then writes, fails with `SQLITE_BUSY_SNAPSHOT` immediately; the busy timeout doesn't
  apply to lock upgrades. Connections are in autocommit mode (`isolation_level=None`),
  so an idle connection never holds a snapshot that would starve checkpoints.
* **Claim**: `UPDATE … WHERE seq = (SELECT … ORDER BY priority DESC, seq LIMIT 1)
  RETURNING …`. It is atomic without `SKIP LOCKED` because SQLite has one writer. It needs
  SQLite 3.35+, which every supported Python ships. A cheap read checks for work before the
  write lock is taken.
* **Wake-ups**: `PRAGMA data_version` (about 2 µs) changes whenever another connection
  commits. Idle workers check it with a backoff from 2 ms to 50 ms. That's millisecond
  latency at negligible cost, with no extra processes.
* **Fork safety**: connections opened before `fork()` are never used, closed or even
  garbage-collected in the child. Closing one can release the parent's POSIX locks and
  corrupt the database.
* Measured locally: about 14k enqueues/s and about 5–6k processed/s with 4 worker processes.
  Use a local filesystem; WAL doesn't work over NFS.

## Redis / Valkey

Prior art: BullMQ (the most sophisticated Redis queue), Sidekiq and super_fetch, arq,
Dramatiq, kombu.

* **Everything is one Lua script call**: enqueue (batched), claim, extend, complete,
  retry, requeue, dead-letter, reap, revoke, chord, periodic. Complete, retry and
  dead-letter include the result write and the follow-up enqueues, so they are atomic.
* **Keys** (prefix `potatoq`, configurable with `global_keyprefix`):
  * `q:<queue>:ready`, a ZSET scored `-priority·2⁴⁰ + seq`. That gives real priorities
    with FIFO order inside each priority.
  * `q:<queue>:delayed`, a ZSET scored by due time in ms.
  * `q:<queue>:active`, a ZSET scored by lease deadline.
  * `q:<queue>:marker`, the wake-up key.
  * `job:<id>`, a hash holding the payload, priority, delivery count, fencing token and
    state.
  * `result:<id>`, written with `PX` expiry.
  * `dead`, a ZSET capped by count and age.
  * `workers`, `chord:<group>`, `periodic:<name>:<ts>`.
* **Leases, not visibility timeouts.** A claim adds the task to `active` with
  deadline = now + `worker_dead_after` (60 s), plus a random fencing token. The worker
  supervisor extends the leases of all running tasks every 5 s, so a long task is never
  redelivered. The supervisor reaps expired leases: requeued, or dead-lettered after
  `task_max_deliveries`.
* **Wake-ups**: idle workers `BZPOPMIN` their queues' marker keys. Enqueue sets the marker;
  a claim re-arms it while work remains, so wake-ups chain from worker to worker
  (BullMQ's trick). Unlike pub/sub, the marker is persistent, so a wake-up can't fall
  into the gap between "nothing to claim" and "start blocking".
* **Delayed tasks** are promoted atomically inside `claim`. There is no separate poller,
  and no pop-then-push gap like the one Sidekiq OSS accepts.
* **Waiting on a result** with `.get()` costs nothing for tasks nobody waits on. The
  waiter sets `waiting:<id>`; only then does completion `RPUSH notify:<id>`, and the
  waiter `BLPOP`s it.
* Revoked waiting tasks are deleted; `claim` skips IDs whose hash is gone.
* Startup warns unless `maxmemory-policy` is `noeviction`. Use AOF (`appendfsync everysec`).
  Redis Cluster is not supported yet, because scripts touch keys of several queues.

## RabbitMQ

Prior art: RabbitMQ 4.x docs and release notes, NServiceBus's delay infrastructure,
kombu 5.5's native delayed delivery, Dramatiq's broker. Requires RabbitMQ 4.0+;
optimized for 4.3.

* **Quorum queues** with `x-delivery-limit` (`task_max_deliveries`), an at-least-once
  dead-letter exchange into `<queue>.dlq`, and `x-overflow=reject-publish`, which at-least-once
  dead-lettering requires.
* **Publisher confirms and `mandatory`** on every publish. An unroutable message means
  the queue doesn't exist yet: declare it and retry.
* **Delays**: the 28-level binary TTL cascade. Exchanges and queues `potatoq.delay.L27` …
  `L00` have TTLs of 2^level seconds and dead-letter into each other; the routing key is
  the delay in binary followed by the queue name. It is replicated, needs no plugin (the
  delayed-message plugin is archived and doesn't work on 4.3), and supports delays of up to
  about 8.5 years at whole-second precision. Queue names can't contain `.`.
* **Consuming**: each worker process consumes with prefetch 1 on an I/O thread that
  owns the connection, so heartbeats keep flowing while a task runs. pika isn't
  thread-safe, so every channel operation is marshalled onto that thread. With several
  `-Q` queues, the other consumers are paused while a task runs, so nothing waits in a
  local buffer.
* `x-consumer-timeout` is set on each consumer to `task_time_limit` + 5 min, so the 30 min
  default never kills a healthy long task.
* **Acks**: success is publish follow-ups (confirmed), then ack. Retry is publish the new
  message through the delay cascade, then ack. Shutdown is `nack(requeue)`, which on 4.3
  doesn't count towards the delivery limit. A failure is published to the DLX with the
  reason in a header, then acked.
* **Results and revocation** need a `result_backend` (Redis, Postgres or SQLite). RabbitMQ
  can't delete queued messages, so revoked IDs are recorded in the result backend and
  workers skip them. Worker heartbeats also go to the result backend when there is one.
* **Hard time limits without a result backend**: the supervisor can't ack a message
  owned by a killed child's channel. With a result backend, the `TimeLimitExceeded`
  failure is recorded and the redelivered copy is skipped. Without one, RabbitMQ
  redelivers it until the delivery limit, and the worker warns about this at startup.
* **Periodic tasks**: a single-active-consumer quorum queue holds a "token" message that
  is never acked. Whoever holds it runs the scheduler, and if that process dies the token
  moves to the next consumer. No external lock is needed. Each worker polls its token
  consumer about once a second. RabbitMQ requeues the token as soon as the leader's
  connection closes, so failover takes about 1 s after a clean shutdown or a process
  crash, and about 60 s after a lost host (the default heartbeat timeout, which potatoq
  doesn't override). Which fire times were sent is kept in the leader's memory, so
  runs due during failover are skipped, and a leader that hasn't yet noticed it lost its
  connection can send a run the new leader also sends.

## Rust

The original plan allowed for a Rust core. We measured and researched before
writing any, and decided against it for now:

* **Per-task cost is dominated by broker round trips** (about 100 µs or more each on
  localhost, more across hosts) and by Python executing the task. Serialization (JSON)
  and protocol parsing (hiredis, psycopg's C layer) are already native or negligible.
* **The comparable Rust-core Python queue** (ArdiQ, PyO3 + tokio) measured +41% on no-op
  dispatch, +10% on CPU-bound tasks and about 0% on I/O-bound tasks compared with a
  pure-Python peer. valkey-glide's Rust-backed async client was 2.3× slower than redis-py
  because of the async bridge. Its embedded tokio runtime is also not fork-safe, which is
  fatal for prefork workers.
* **Packaging cost**: abi3 wheels don't load on free-threaded 3.13t/3.14t, sdists need a
  Rust toolchain, and every contributor needs two languages.
* **What moves throughput is protocol design**: batching, fewer round trips, push instead
  of poll. Potatoq already does that. One Lua call or one SQL statement per state
  change; result, ack and follow-ups in one transaction; blocking wake-ups.

The serialization layer is a single module (`potatoq/serialization.py`), so an optional
native codec (or an AMQP frame codec, the one place pure-Python parsing was measured as
a bottleneck at about 6.5k msg/s) can be added later as an extra with a pure-Python
fallback. That way it stays invisible to users.
