# SQLite

```python
app = Potatoq("proj", broker="sqlite:///var/lib/myapp/queue.db")   # sqlite:////absolute/path
```

No dependencies. This is the default broker (`./potatoq.sqlite3`) when nothing is
configured.

## Why it's good

- **Zero infrastructure**, yet durable and multi-process: WAL mode, every write in a
  `BEGIN IMMEDIATE` transaction, atomic `UPDATE … RETURNING` claims.
- **Millisecond wake-ups** without polling the table: idle workers watch
  `PRAGMA data_version`, a ~2 µs check.
- **Transactional enqueue and free results**, as with Postgres.
- About 14k enqueues/s and 5k processed tasks/s on a laptop with 4 worker processes.

## Options

```python
app.conf.broker_transport_options = {
    "durable": False,   # True: synchronous=FULL (survives power loss, slower writes)
}
```

With the default `synchronous=NORMAL`, nothing is lost when a process crashes. Only an
OS crash or power loss can drop the last few commits.

## Limits

- **One machine**: all workers must use the same local filesystem. WAL doesn't work
  over NFS or SMB.
- Writes are serialized; beyond a few thousand tasks per second, use Postgres or Redis.

[How it works in detail :octicons-arrow-right-24:](../design/internals.md#sqlite)
