# SQLite

```python
app = Potatoq("proj", broker="sqlite:///var/lib/myapp/queue.db")   # sqlite:////absolute/path
```

No dependencies. This is the default broker (`./potatoq.sqlite3`) when nothing is
configured.

## Requirements

SQLite 3.35 or newer: claims use `UPDATE … RETURNING`, new in 3.35. Python uses the
SQLite it was built with, so check yours:

```console
$ python -c "import sqlite3; print(sqlite3.sqlite_version)"
3.53.1
```

Pythons from uv and python.org bundle a recent SQLite. A Linux distribution's Python
uses the system library, which is new enough on current Debian, Ubuntu and Amazon Linux
but is 3.34 on RHEL 9 and its rebuilds (Rocky, Alma): there, use a uv-managed Python.
An older SQLite is refused when the broker starts. With Django, Django 6.1 needs SQLite
3.37 itself.

## Why it's good

- **Zero infrastructure**, yet durable and multi-process: WAL mode, every write in a
  `BEGIN IMMEDIATE` transaction, atomic `UPDATE … RETURNING` claims.
- **Millisecond wake-ups** without polling the table: idle workers watch
  `PRAGMA data_version`, a ~2 µs check.
- **Transactional enqueue and free results**, as with Postgres.
- About 15k enqueues/s and 6k processed tasks/s on a laptop with 4 worker processes.

## Options

```python
app.conf.broker_transport_options = {
    "durable": False,   # True: synchronous=FULL (survives power loss, slower writes)
}
```

With the default `synchronous=NORMAL`, nothing is lost when a process crashes. Only an
OS crash or power loss can drop the last few commits.

## As your Django database

When the broker is Django's SQLite database (the default with
`potatoq.contrib.django`), set `"OPTIONS": {"transaction_mode": "IMMEDIATE", "timeout": 20}`
on the database, or your web requests can fail with `database is locked` while workers
write ([details](../integrations/django.md#sqlite-as-database-and-broker)).

## Limits

- **One machine**: all workers must use the same local filesystem. WAL doesn't work
  over NFS or SMB.
- Writes are serialized; beyond a few thousand tasks per second, use Postgres or Redis.

[How it works in detail :octicons-arrow-right-24:](../design/internals.md#sqlite)
