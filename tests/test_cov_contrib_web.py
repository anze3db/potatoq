"""Flask, FastAPI and SQLAlchemy integration edge cases."""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace

import pytest
from sqlalchemy import Integer, create_engine, literal, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, scoped_session, sessionmaker

from potatoq import Potatoq
from potatoq.contrib.sqlalchemy import SQLAlchemyTransactionHook, _broker_matches, install
from potatoq.testing import drain


def size(app):
    return app.broker.queue_sizes().get(app.conf.task_default_queue, 0)


# --- Flask ------------------------------------------------------------------------------


def test_flask_creates_app_and_installs_sqlalchemy_hook(monkeypatch):
    from flask import Flask, current_app

    from potatoq.contrib.flask import init_app

    # Pretend Flask-SQLAlchemy is installed: init_app then tracks SQLAlchemy sessions.
    monkeypatch.setitem(sys.modules, "flask_sqlalchemy", types.SimpleNamespace(SQLAlchemy=object))
    flask_app = Flask("shopfront")
    flask_app.config.update(POTATOQ={"broker_url": "memory://", "result_backend": "broker"}, NAME="shopfront")
    app = init_app(flask_app)
    try:
        assert app.main == "shopfront"
        assert any(isinstance(h, SQLAlchemyTransactionHook) for h in app._transaction_hooks)

        @app.task
        def name():
            return current_app.config["NAME"]

        result = name.delay()
        drain(app)
        assert result.get() == "shopfront"
    finally:
        app.close()


# --- FastAPI ----------------------------------------------------------------------------


def test_fastapi_task_status_reports_failures():
    from potatoq.contrib.fastapi import task_status

    app = Potatoq("fastapi-fail", broker="memory://", result_backend="broker", set_as_current=False)
    try:

        @app.task
        def fail():
            raise ValueError("nope")

        task_id = fail.delay().id
        drain(app)
        assert task_status(task_id, app) == {
            "id": task_id,
            "state": "FAILURE",
            "ready": True,
            "error": "ValueError('nope')",
        }
    finally:
        app.close()


# --- SQLAlchemy -------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


class Row(Base):
    __tablename__ = "rows"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)


@pytest.fixture
def mem_app():
    app = Potatoq("sqla-cov", broker="memory://", set_as_current=False)
    install(app)
    yield app
    app.close()


@pytest.fixture
def engine():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


def test_orm_flush_and_selects(mem_app, engine):
    @mem_app.task
    def notify(x):
        return x

    with Session(engine) as session:
        session.execute(select(literal(1)))  # an ORM select doesn't count as a write
        notify.delay(1)
        assert size(mem_app) == 1
        session.add(Row(id=1))
        session.flush()  # flushed rows do
        assert session.info.get("potatoq_wrote") is True
        notify.delay(2)
        assert size(mem_app) == 1
        session.commit()
        assert size(mem_app) == 2


def test_explicit_session_and_on_commit(mem_app, engine):
    @mem_app.task
    def notify(x):
        return x

    calls = []
    with Session(engine) as session:
        # Explicit session: deferred even though nothing was written yet.
        session.execute(select(literal(1)))
        notify.apply_async((1,), using=session, enqueue_on_commit=True)
        mem_app.on_commit(lambda: calls.append("read-only"))  # nothing written: runs now
        assert calls == ["read-only"]
        session.add(Row(id=1))
        mem_app.on_commit(lambda: calls.append("committed"))
        assert size(mem_app) == 0 and calls == ["read-only"]
        session.commit()
        assert size(mem_app) == 1 and calls == ["read-only", "committed"]
        # Not in a transaction any more: the explicit session doesn't defer.
        notify.apply_async((2,), using=session, enqueue_on_commit=True)
        assert size(mem_app) == 2


def test_scoped_session(engine):
    app = Potatoq("sqla-scoped", broker="memory://", set_as_current=False)
    Scoped = scoped_session(sessionmaker(engine))
    install(app, Scoped)
    try:

        @app.task
        def notify(x):
            return x

        notify.apply_async((1,), using=Scoped, enqueue_on_commit=True)  # no transaction: sent now
        assert size(app) == 1
        Scoped.execute(text("SELECT 1"))
        notify.apply_async((2,), using=Scoped, enqueue_on_commit=True)
        assert size(app) == 1
        Scoped.commit()
        assert size(app) == 2
    finally:
        Scoped.remove()
        app.close()


def test_savepoint_rollback_keeps_outer_tasks(mem_app, engine):
    @mem_app.task
    def notify(x):
        return x

    sent = []
    with Session(engine) as session:
        session.begin()
        session.add(Row(id=1))
        session.flush()
        notify.delay("outer")
        with pytest.raises(RuntimeError), session.begin_nested():
            notify.delay("rolled-back savepoint")
            raise RuntimeError
        outer_sp = session.begin_nested()
        inner_sp = session.begin_nested()
        mem_app.on_commit(lambda: sent.append("released into a rolled-back savepoint"))
        inner_sp.commit()
        outer_sp.rollback()
        with session.begin_nested():
            mem_app.on_commit(lambda: sent.append("released savepoint"))
        assert session.in_transaction()
        assert size(mem_app) == 0 and sent == []  # releasing a savepoint is not a commit
        session.commit()
    assert sent == ["released savepoint"]
    assert [d.args for d in drain(mem_app)] == [["outer"]]

    with Session(engine) as session:
        session.add(Row(id=2))
        session.flush()
        notify.delay("rolled back")
        session.rollback()
        session.commit()
    assert size(mem_app) == 0


def test_broker_match_detection(tmp_path):
    def session_for(url):
        return SimpleNamespace(get_bind=lambda: SimpleNamespace(engine=SimpleNamespace(url=make_url(url))))

    db = tmp_path / "x.db"
    sqlite_broker = SimpleNamespace(transactional=True, url=f"sqlite:///{db}")
    pg_broker = SimpleNamespace(transactional=True, url="postgresql://localhost/app")
    assert _broker_matches(sqlite_broker, session_for(f"sqlite:///{db}")) is True
    assert _broker_matches(sqlite_broker, session_for(f"sqlite:///{tmp_path}/y.db")) is False
    assert _broker_matches(pg_broker, session_for("postgresql+psycopg://u:p@127.0.0.1:5432/app")) is True
    assert _broker_matches(pg_broker, session_for("postgresql+psycopg://localhost/other")) is False
    assert _broker_matches(pg_broker, session_for("mysql+pymysql://localhost/app")) is False
    bad_port = SimpleNamespace(transactional=True, url="postgresql://localhost:notaport/app")
    assert _broker_matches(bad_port, session_for("postgresql://localhost/app")) is False
    assert _broker_matches(SimpleNamespace(transactional=False, url=pg_broker.url), session_for(pg_broker.url)) is False
    assert _broker_matches(pg_broker, Session()) is False  # unbound session
