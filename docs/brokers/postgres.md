# PostgreSQL

```python
app = Potatoq("proj", broker="postgresql://user:pass@db/app")
```

Requires `potatoq[postgres]` (psycopg 3) and PostgreSQL 13 or newer (tested on 18).
Tables are created automatically on first use (run `potatoq migrate` to do it
explicitly), under a lock so concurrent workers don't race.

## Why it's good

- **Transactional enqueue**: tasks commit and roll back with your data.
- **Results for free**: written in the same transaction as the acknowledgement.
- **Fast claims**: `FOR UPDATE SKIP LOCKED` on a partial index, a single statement per
  claim, and no transaction held while a task runs.
- **Instant wake-ups**: `LISTEN/NOTIFY`, rate-limited per queue so it never becomes a
  bottleneck, with a 1-second poll as a safety net.

## Options

```python
app.conf.broker_transport_options = {
    "schema": "queue",        # put the tables in their own schema (created if missing)
    "table_prefix": "potatoq_",
    "notify": True,           # False behind PgBouncer in transaction-pooling mode
    "poll_interval": 1.0,     # seconds, safety-net poll while idle
}
```

## Operations

- **Tables**: `potatoq_jobs` (only waiting and running tasks; finished ones are deleted
  right away), `potatoq_results`, `potatoq_dead`, `potatoq_workers`, plus small chord and
  periodic-dedup tables.
- **Connections**: one per worker process, plus one for the supervisor. With PgBouncer
  in transaction mode, set `notify=False`; LISTEN needs session pooling or a direct
  connection.
- **Vacuum**: the jobs table has aggressive autovacuum settings built in. Avoid
  long-running transactions anywhere in the database (`idle_in_transaction_session_timeout`):
  they stop vacuum from reclaiming queue rows, and every queue slows down.
- **Maintenance**: expired results, old dead letters and chord bookkeeping are pruned by
  the workers, one node at a time (advisory lock). No cron needed.

```sql
-- what's waiting, per queue
SELECT queue, count(*) FROM potatoq_jobs WHERE state IN (0, 1) GROUP BY queue;
-- what's running, and where
SELECT task, worker, claimed_at FROM potatoq_jobs WHERE state = 2;
```

[How it works in detail :octicons-arrow-right-24:](../design/internals.md#postgresql)
