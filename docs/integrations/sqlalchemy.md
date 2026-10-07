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
  query, so a task is deferred only once the transaction has written something
  (a flush or a non-SELECT statement). A read-only request that never commits doesn't
  swallow its tasks.
- **Same database, same transaction.** When the broker is the session's database
  (Postgres or SQLite), the task row is written through the session's connection, so it
  commits atomically with your data.
- **Explicit session:** `task.apply_async(args, using=session)` defers to that session's
  transaction regardless.
- Works with `sessionmaker`, `scoped_session`, Flask-SQLAlchemy and `AsyncSession`.
