# SQLAlchemy

```python
from potatoq.contrib.sqlalchemy import install

install(app)                  # every Session; or install(app, my_sessionmaker)
```

From then on, `.delay()` follows the session's transaction:

```python
with Session(engine) as session, session.begin():
    session.add(User(email=email))
    send_welcome.delay(email)    # sent after COMMIT, dropped on ROLLBACK
```

- **Only real writes defer tasks.** SQLAlchemy 2.0 starts a transaction on *any*
  query, so a task is deferred only once the transaction has written something or has
  objects waiting to be flushed (`session.add()`). Writes are anything but a SELECT run
  through the session or its connection: flushes, bulk operations, Core statements on
  `session.connection()`, `text()` and `exec_driver_sql()` (where SQL starting with
  `WITH` counts as a write, as it may be `WITH ... INSERT`). A read-only request that
  never commits doesn't swallow its tasks. Statements run on the raw DB-API connection
  (`session.connection().connection.cursor()`) aren't seen; pass `using=session` there.
- **Same database, same transaction.** When the broker is the session's database
  (Postgres or SQLite), the task row is written through the session's connection, so it
  commits atomically with your data.
- **Explicit session:** `task.apply_async(args, using=session)` defers to that session's
  transaction regardless.
- **Rollback, `close()` or `reset()`** without a commit drops the deferred tasks. If
  sending after COMMIT fails (say, the broker is down), the error is logged on the
  `potatoq.sqlalchemy` logger; `commit()` doesn't raise for data that did commit, and
  the other deferred tasks are still sent.
- Works with `sessionmaker`, `scoped_session`, Flask-SQLAlchemy and `AsyncSession`. With
  `AsyncSession` and the same database as the broker, tasks are sent right after COMMIT
  rather than written inside the transaction: the broker can't write through an async
  driver's connection.
