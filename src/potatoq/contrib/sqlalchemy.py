"""SQLAlchemy integration: enqueue on commit, or inside the transaction.

    from potatoq.contrib.sqlalchemy import install
    install(app)                      # all sessions (or pass a sessionmaker)

    with Session() as session, session.begin():
        session.add(user)
        send_welcome.delay(user.id)   # sent after COMMIT, dropped on ROLLBACK

Savepoints work like Django's: what a rolled-back ``begin_nested()`` deferred is
dropped, and releasing a savepoint sends nothing until the outer COMMIT. Ending the
transaction any other way (``rollback()``, ``close()``, ``reset()``) drops it all.

Tasks are only deferred once the transaction has written something (any statement but
a SELECT, through the session or its connections) or has pending objects to flush:
SQLAlchemy 2.0 begins a transaction on any query, and a read-only request that never
commits must not drop its tasks. When the broker is the same database as the session
(Postgres or SQLite), the task rows are written through the session's own connection
instead, so they commit atomically with your data. Works for ``Session``,
``scoped_session``, Flask-SQLAlchemy and ``AsyncSession`` (its sync session fires the
same events; the broker's code is synchronous, so with the same database the tasks are
sent after COMMIT instead of inside the transaction).

If sending after COMMIT fails (the broker is down), the error is logged; the commit
itself still succeeds and the other deferred tasks are still sent.

To pick a session explicitly: ``task.apply_async(args, using=session)``.
"""

from __future__ import annotations

import logging
import weakref
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from sqlalchemy import event
from sqlalchemy.orm import Session

from ..exceptions import EnqueueAfterCommitError

if TYPE_CHECKING:
    from ..app import Potatoq
    from ..message import Message

logger = logging.getLogger("potatoq.sqlalchemy")

_KEY = "potatoq_on_commit"
_WROTE = "potatoq_wrote"
_COMMITTED = "potatoq_committed"
_WATCHED = "potatoq_watched"
_sessions: ContextVar[tuple[weakref.ref[Session], ...]] = ContextVar("potatoq_sqla_sessions", default=())
# Weak, not ids (nor event.contains(), which is keyed by id too): a new sessionmaker can
# get the id of a collected one, and would silently not be tracked.
_installed: weakref.WeakSet[Any] = weakref.WeakSet()


def _after_transaction_create(session: Session, transaction: Any) -> None:
    """Track the session from ``begin()`` or autobegin (``add()``, a query), not only
    once it has a connection: ``s.add(user); task.delay()`` must already defer."""
    if transaction.parent is not None:
        return
    current = _sessions.get()
    if not any(ref() is session for ref in current):
        _sessions.set((*tuple(r for r in current if r() is not None), weakref.ref(session)))


_READ_ONLY = ("select", "show", "explain", "pragma", "values")  # not "with": WITH ... INSERT


def _writes(statement: str, context: Any) -> bool:
    if context is None or context.is_text:  # text() and exec_driver_sql(): look at the SQL
        return not statement.lstrip().lower().startswith(_READ_ONLY)
    return bool(context.is_crud or context.isddl)  # not SELECTs or SQLAlchemy's SAVEPOINTs


def _after_begin(session: Session, transaction: Any, connection: Any) -> None:
    """Watch what the transaction runs on this connection: ORM flushes, bulk
    operations and Core statements on ``session.connection()`` all end up here."""
    if transaction.parent is not None:
        return  # a savepoint on a connection that is already watched
    ref = weakref.ref(session)

    def watch(conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool) -> None:
        if _writes(statement, context) and (owner := ref()) is not None:
            owner.info[_WROTE] = True

    event.listen(connection, "before_cursor_execute", watch)
    session.info.setdefault(_WATCHED, []).append((connection, watch))


def _defer(session: Session, fn: Any) -> None:
    """Run ``fn`` when ``session`` commits; remember the savepoint it belongs to."""
    session.info.setdefault(_KEY, []).append((session.get_nested_transaction(), fn))


def _after_commit(session: Session) -> None:
    if not session.in_nested_transaction():  # releasing a savepoint is not the COMMIT
        session.info[_COMMITTED] = True


def _after_transaction_end(session: Session, transaction: Any) -> None:
    """The outermost transaction is over: run what was deferred if it committed, drop
    it otherwise. Unlike ``after_commit``, the session has already left the transaction
    here, so a failing callback can't make ``commit()`` raise for committed data or
    leave the session stuck; it's logged and the rest still run."""
    if transaction.parent is not None:
        return
    info = session.info
    for connection, watch in info.pop(_WATCHED, ()):
        event.remove(connection, "before_cursor_execute", watch)
    info.pop(_WROTE, None)
    callbacks = info.pop(_KEY, None) or ()
    if info.pop(_COMMITTED, False):
        for _, fn in callbacks:
            try:
                fn()
            except EnqueueAfterCommitError:
                pass  # already logged, one line per lost task
            except Exception:
                logger.exception("Callback deferred to COMMIT failed: %r", fn)


def _within(transaction: Any, savepoint: Any) -> bool:
    while transaction is not None:
        if transaction is savepoint:
            return True
        transaction = transaction.parent
    return False


def _after_soft_rollback(session: Session, previous: Any) -> None:
    if previous.nested:
        # A savepoint rolled back: drop only what was deferred inside it.
        callbacks = session.info.get(_KEY)
        if callbacks:
            session.info[_KEY] = [(tx, fn) for tx, fn in callbacks if not _within(tx, previous)]


def _writing(session: Session) -> bool:
    """SQLAlchemy 2.0 "autobegins" a transaction on any query, so being in a
    transaction doesn't mean the task depends on uncommitted data. Only defer when
    this transaction wrote something (or is about to flush something); otherwise a
    read-only request that never commits would silently drop its tasks."""
    return bool(session.info.get(_WROTE) or session.new or session.dirty or session.deleted)


def _current_session(using: Any) -> Session | None:
    if isinstance(using, Session):  # explicit: the caller knows best
        return using if using.in_transaction() else None
    if using is not None and hasattr(using, "registry") and callable(using):  # scoped_session
        session = using()
        return session if session.in_transaction() else None
    for ref in reversed(_sessions.get()):
        session = ref()
        if session is not None and session.in_transaction() and _writing(session):
            return session
    return None


def _broker_matches(broker: Any, session: Session) -> bool:
    if not getattr(broker, "transactional", False):
        return False
    try:
        bind = session.get_bind()
        url = bind.engine.url
    except Exception:
        return False
    if bind.dialect.is_async:
        return False  # AsyncSession: the broker can't write through an async driver; send after COMMIT
    from .django import _canonical

    try:
        if url.get_backend_name() == "sqlite":
            return _canonical("sqlite:///" + str(url.database)) == _canonical(broker.url)
        if url.get_backend_name() == "postgresql":
            plain = url.set(drivername="postgresql").render_as_string(hide_password=False)
            return _canonical(plain) == _canonical(broker.url)
    except Exception:
        return False
    return False


class SQLAlchemyTransactionHook:
    def publish(self, app: Potatoq, messages: list[Message], using: Any, on_commit: bool = True) -> bool:
        session = _current_session(using)
        if session is None or not on_commit:
            return False
        if _broker_matches(app.broker, session):
            # The session's connection (acquired now if the transaction has none yet),
            # so the task rows commit or roll back with the data.
            dbapi = session.connection().connection.driver_connection
            app.publish_now(messages, connection=dbapi)
            return True
        _defer(session, lambda: app.publish_after_commit(messages))
        return True

    def on_commit(self, fn: Any, using: Any) -> bool:
        session = _current_session(using)
        if session is None:
            return False
        _defer(session, fn)
        return True


def install(app: Potatoq, target: Any = Session) -> None:
    """Track transactions of ``target`` (a Session class, sessionmaker or scoped_session)."""
    if hasattr(target, "session_factory"):  # scoped_session
        target = target.session_factory
    if target not in _installed:
        event.listen(target, "after_transaction_create", _after_transaction_create)
        event.listen(target, "after_begin", _after_begin)
        event.listen(target, "after_commit", _after_commit)
        event.listen(target, "after_transaction_end", _after_transaction_end)
        event.listen(target, "after_soft_rollback", _after_soft_rollback)
        _installed.add(target)
    app.add_transaction_hook(SQLAlchemyTransactionHook())
