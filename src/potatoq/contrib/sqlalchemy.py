"""SQLAlchemy integration: enqueue on commit, or inside the transaction.

    from potatoq.contrib.sqlalchemy import install
    install(app)                      # all sessions (or pass a sessionmaker)

    with Session() as session, session.begin():
        session.add(user)
        send_welcome.delay(user.id)   # sent after COMMIT, dropped on ROLLBACK

When the broker is the same database as the session (Postgres or SQLite), the task
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
        _sessions.set(tuple(r for r in current if r() is not None) + (weakref.ref(session),))


def _after_commit(session: Session) -> None:
    callbacks = session.info.pop(_KEY, None)
    for fn in callbacks or ():
        fn()


def _after_rollback(session: Session) -> None:
    session.info.pop(_KEY, None)


def _current_session(using: Any) -> Session | None:
    if isinstance(using, Session):
        return using if using.in_transaction() else None
    if using is not None and hasattr(using, "registry") and hasattr(using, "__call__"):  # scoped_session
        session = using()
        return session if session.in_transaction() else None
    for ref in reversed(_sessions.get()):
        session = ref()
        if session is not None and session.in_transaction():
            return session
    return None


def _broker_matches(broker: Any, session: Session) -> bool:
    if not getattr(broker, "transactional", False):
        return False
    try:
        url = session.get_bind().url
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
        event.listen(
            target,
            "after_soft_rollback",
            lambda session, previous: _after_rollback(session) if not session.in_transaction() else None,
        )
        _installed.add(id(target))
    app.add_transaction_hook(SQLAlchemyTransactionHook())
