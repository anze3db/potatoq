"""SQLAlchemy integration: enqueue on commit, or inside the transaction.

    from potatoq.contrib.sqlalchemy import install
    install(app)                      # all sessions (or pass a sessionmaker)

    with Session() as session, session.begin():
        session.add(user)
        send_welcome.delay(user.id)   # sent after COMMIT, dropped on ROLLBACK

Tasks are only deferred once the transaction has written something: SQLAlchemy 2.0
begins a transaction on any query, and a read-only request that never commits must
not drop its tasks. When the broker is the same database as the session (Postgres or SQLite), the task
rows are written through the session's own connection instead, so they commit
atomically with your data. Works for ``Session``, ``scoped_session``, Flask-SQLAlchemy
and ``AsyncSession`` (its sync session fires the same events).

To pick a session explicitly: ``task.apply_async(args, using=session)``.
"""

from __future__ import annotations

import logging
import weakref
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from sqlalchemy import event
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from ..app import Potatoq
    from ..message import Message

logger = logging.getLogger("potatoq.sqlalchemy")

_KEY = "potatoq_on_commit"
_sessions: ContextVar[tuple[weakref.ref[Session], ...]] = ContextVar("potatoq_sqla_sessions", default=())
_installed: set[int] = set()


def _after_begin(session: Session, transaction: Any, connection: Any) -> None:
    current = _sessions.get()
    if not any(ref() is session for ref in current):
        _sessions.set((*tuple(r for r in current if r() is not None), weakref.ref(session)))


_WROTE = "potatoq_wrote"


def _after_commit(session: Session) -> None:
    session.info.pop(_WROTE, None)
    callbacks = session.info.pop(_KEY, None)
    for fn in callbacks or ():
        fn()


def _after_rollback(session: Session) -> None:
    session.info.pop(_WROTE, None)
    session.info.pop(_KEY, None)


def _after_flush(session: Session, flush_context: Any) -> None:
    session.info[_WROTE] = True


_READ_ONLY = ("select", "with", "show", "explain", "pragma", "values")


def _do_orm_execute(state: Any) -> None:
    if state.is_select:
        return
    sql = getattr(state.statement, "text", None)  # text("...") isn't flagged as a select
    if isinstance(sql, str) and sql.lstrip().lower().startswith(_READ_ONLY):
        return
    state.session.info[_WROTE] = True


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
        url = session.get_bind().engine.url
    except Exception:
        return False
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
    def publish(self, app: Potatoq, messages: list[Message], using: Any) -> bool:
        session = _current_session(using)
        if session is None:
            return False
        if _broker_matches(app.broker, session):
            dbapi = session.connection().connection.driver_connection
            app.publish_now(messages, connection=dbapi)
            return True
        session.info.setdefault(_KEY, []).append(lambda: app.publish_now(messages))
        return True

    def on_commit(self, fn: Any, using: Any) -> bool:
        session = _current_session(using)
        if session is None:
            return False
        session.info.setdefault(_KEY, []).append(fn)
        return True


def install(app: Potatoq, target: Any = Session) -> None:
    """Track transactions of ``target`` (a Session class, sessionmaker or scoped_session)."""
    if hasattr(target, "session_factory"):  # scoped_session
        target = target.session_factory
    if id(target) not in _installed:
        event.listen(target, "after_begin", _after_begin)
        event.listen(target, "after_commit", _after_commit)
        event.listen(target, "after_rollback", _after_rollback)
        event.listen(target, "after_flush", _after_flush)
        event.listen(target, "do_orm_execute", _do_orm_execute)
        event.listen(
            target,
            "after_soft_rollback",
            lambda session, previous: _after_rollback(session) if not session.in_transaction() else None,
        )
        _installed.add(id(target))
    app.add_transaction_hook(SQLAlchemyTransactionHook())
