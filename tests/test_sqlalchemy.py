"""SQLAlchemy integration: tasks follow the session's transaction."""

from __future__ import annotations

import logging
import uuid

import pytest
from conftest import POSTGRES_URL, make_app, postgres_available
from sqlalchemy import Integer, Uuid, create_engine, insert, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from potatoq import Potatoq
from potatoq.contrib.sqlalchemy import install


def size(app):
    return app.broker.queue_sizes().get(app.conf.task_default_queue, 0)


def test_deferred_until_commit_with_other_broker():
    app = Potatoq("sqla-mem", broker="memory://")
    install(app)

    @app.task
    def notify(x):
        return x

    engine = create_engine("sqlite://")
    with Session(engine) as session:
        session.execute(text("CREATE TABLE t (x int)"))
        session.commit()
        with session.begin():
            session.execute(text("INSERT INTO t VALUES (1)"))
            notify.delay(1)
            assert size(app) == 0
        assert size(app) == 1
        with pytest.raises(RuntimeError), session.begin():
            session.execute(text("INSERT INTO t VALUES (2)"))
            notify.delay(2)
            raise RuntimeError
    assert size(app) == 1
    notify.delay(3)  # no transaction: immediate
    assert size(app) == 2


def test_read_only_transaction_does_not_swallow_tasks():
    """Autobegin after a SELECT, then the session is closed without commit."""
    app = Potatoq("sqla-ro", broker="memory://")
    install(app)

    @app.task
    def notify(x):
        return x

    engine = create_engine("sqlite://")
    with Session(engine) as session:
        session.execute(text("select 1"))
        assert session.in_transaction()
        notify.delay(1)
        assert size(app) == 1  # sent right away, not deferred to a commit that never comes


@pytest.mark.parametrize("kind", ["sqlite", "postgres"])
def test_same_database_enqueues_inside_transaction(kind, tmp_path):
    if kind == "postgres" and not postgres_available():
        pytest.skip("Postgres not available")
    app, cleanup = make_app(kind, tmp_path)
    try:
        if kind == "sqlite":
            engine = create_engine("sqlite:///" + app.conf.broker_url.split(":///", 1)[1])
        else:
            engine = create_engine(POSTGRES_URL.replace("postgresql://", "postgresql+psycopg://"))
            app.broker.setup()
        install(app, sessionmaker(engine))
        factory = sessionmaker(engine)

        @app.task
        def notify(x):
            return x

        with factory() as session:
            session.execute(text("CREATE TEMP TABLE t (x int)"))
            session.commit()
            with session.begin():
                session.execute(text("INSERT INTO t VALUES (1)"))
                notify.delay(1)
                assert size(app) == 0  # in the transaction, not visible yet
            assert size(app) == 1
            with pytest.raises(RuntimeError), session.begin():
                session.execute(text("INSERT INTO t VALUES (2)"))
                notify.delay(2)
                raise RuntimeError
        assert size(app) == 1
        engine.dispose()
    finally:
        app.close()
        cleanup()


def test_delay_on_commit_without_session_runs_now():
    app = Potatoq("sqla-now", broker="memory://")
    install(app)

    @app.task
    def notify(x):
        return x

    notify.delay_on_commit(1)
    assert size(app) == 1


def models(schema=None):
    class Base(DeclarativeBase):
        pass

    class User(Base):
        __tablename__ = "sqla_users"
        __table_args__ = {"schema": schema}
        id: Mapped[int] = mapped_column(Integer, primary_key=True)

    class Token(Base):
        __tablename__ = "sqla_tokens"
        __table_args__ = {"schema": schema}
        id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)

    return Base, User, Token


@pytest.fixture
def mem_app():
    app = Potatoq("sqla-fresh", broker="memory://", set_as_current=False)
    install(app)
    yield app
    app.close()


@pytest.fixture
def engine():
    engine = create_engine("sqlite://")
    yield engine
    engine.dispose()


@pytest.mark.parametrize("style", ["begin", "autobegin", "uuid"])
def test_fresh_session_defers_before_any_sql(mem_app, engine, style):
    """The documented example: delay() right after add(), before the session has run
    any SQL or even acquired a connection."""
    Base, User, Token = models()
    Base.metadata.create_all(engine)

    @mem_app.task
    def notify(x):
        return x

    def write(session, n):
        session.add(Token() if style == "uuid" else User(id=n))
        notify.delay(n)
        assert size(mem_app) == 0

    if style == "begin":
        with pytest.raises(RuntimeError), Session(engine) as session, session.begin():
            write(session, 1)
            raise RuntimeError
        assert size(mem_app) == 0
        with Session(engine) as session, session.begin():
            write(session, 2)
    else:  # Flask-SQLAlchemy style: add(), delay(), then rollback() or commit()
        with Session(engine) as session:
            write(session, 1)
            session.rollback()
            assert size(mem_app) == 0
            write(session, 2)
            session.commit()
    assert size(mem_app) == 1


@pytest.mark.parametrize("kind", ["sqlite", "postgres"])
def test_fresh_session_same_database_is_atomic(kind, tmp_path):
    app, cleanup = make_app(kind, tmp_path)
    try:
        if kind == "sqlite":
            engine = create_engine("sqlite:///" + app.conf.broker_url.split(":///", 1)[1])
            schema = None
        else:
            engine = create_engine(POSTGRES_URL.replace("postgresql://", "postgresql+psycopg://"))
            schema = app.conf.broker_transport_options["schema"]  # unique per test, dropped by cleanup
        app.broker.setup()
        Base, User, _ = models(schema)
        Base.metadata.create_all(engine)
        install(app)

        @app.task
        def notify(x):
            return x

        with pytest.raises(RuntimeError), Session(engine) as session, session.begin():
            session.add(User(id=1))
            notify.delay(1)  # written through the session's connection, in its transaction
            raise RuntimeError
        assert size(app) == 0
        with Session(engine) as session, session.begin():
            session.add(User(id=2))
            notify.delay(2)
            assert size(app) == 0
        assert size(app) == 1
        with Session(engine) as session:
            assert session.scalars(select(User.id)).all() == [2]
        engine.dispose()
    finally:
        app.close()
        cleanup()


WRITES = {
    "bulk_insert_mappings": lambda s, User: s.bulk_insert_mappings(User, [{"id": 1}]),
    "bulk_save_objects": lambda s, User: s.bulk_save_objects([User(id=1)]),
    "connection_insert": lambda s, User: s.connection().execute(insert(User.__table__).values(id=1)),
    "text_with_insert": lambda s, User: s.execute(
        text("WITH v(x) AS (VALUES (1)) INSERT INTO sqla_users SELECT x FROM v")
    ),
    "exec_driver_sql": lambda s, User: s.connection().exec_driver_sql("INSERT INTO sqla_users VALUES (1)"),
    "ddl": lambda s, User: s.execute(text("CREATE TABLE other (x int)")),
}


@pytest.mark.parametrize("write", WRITES)
def test_writes_outside_the_orm_flush_defer(mem_app, engine, write):
    Base, User, _ = models()
    Base.metadata.create_all(engine)

    @mem_app.task
    def notify(x):
        return x

    with Session(engine) as session:
        session.execute(select(User.id)).all()  # read first: the connection is already in use
        WRITES[write](session, User)
        notify.delay(1)
        assert size(mem_app) == 0
        session.rollback()
    assert size(mem_app) == 0


def test_reads_do_not_defer(mem_app, engine):
    Base, User, _ = models()
    Base.metadata.create_all(engine)

    @mem_app.task
    def notify(x):
        return x

    with Session(engine) as session:
        session.execute(select(select(User.id).cte())).all()  # a compiled SELECT starting with WITH
        session.execute(text("VALUES (1)")).all()
        session.connection().exec_driver_sql("SELECT 1").all()
        with session.begin_nested():  # SAVEPOINT and RELEASE, emitted by SQLAlchemy itself
            session.execute(text("SELECT 1"))
        notify.delay(1)
        assert size(mem_app) == 1


def test_commit_sends_even_if_one_callback_fails(mem_app, engine, caplog):
    Base, User, _ = models()
    Base.metadata.create_all(engine)

    @mem_app.task
    def notify(x):
        return x

    def broker_down():
        raise ConnectionError("broker down")

    calls = []
    with Session(engine) as session:
        session.add(User(id=1))
        mem_app.on_commit(broker_down)
        notify.delay(1)
        mem_app.on_commit(lambda: calls.append("after"))
        with caplog.at_level(logging.ERROR, logger="potatoq.sqlalchemy"):
            session.commit()  # the data committed: commit() must not raise
        assert "Callback deferred to COMMIT failed" in caplog.text
        assert size(mem_app) == 1 and calls == ["after"]
        session.add(User(id=2))  # and the session is still usable
        session.commit()
        assert session.scalars(select(User.id)).all() == [1, 2]


def test_tasks_lost_after_commit_are_logged_once_each(mem_app, engine, caplog, monkeypatch):
    from potatoq import app as app_module

    Base, User, _ = models()
    Base.metadata.create_all(engine)

    @mem_app.task(name="sqla.notify")
    def notify(x):
        return x

    def down(messages, connection=None):
        raise ConnectionRefusedError("broker down")

    monkeypatch.setattr(app_module, "_PUBLISH_RETRY_DELAYS", ())
    monkeypatch.setattr(mem_app.broker, "enqueue", down)
    with Session(engine) as session, caplog.at_level(logging.ERROR):
        session.add(User(id=1))
        lost = notify.delay(1)
        session.commit()  # the data committed: commit() must not raise
    assert [r.getMessage() for r in caplog.records] == [
        f"Lost task sqla.notify[{lost.id}] (queue default): the transaction committed but the task couldn't be "
        "sent: broker down"
    ]


@pytest.mark.parametrize("end", ["close", "reset"])
def test_close_without_commit_drops_deferred_tasks(mem_app, engine, end):
    Base, User, _ = models()
    Base.metadata.create_all(engine)

    @mem_app.task
    def notify(x):
        return x

    session = Session(engine)
    session.add(User(id=1))
    session.flush()
    notify.delay("ghost")
    getattr(session, end)()  # discards the transaction without a rollback() call
    assert session.info == {}
    session.add(User(id=2))
    session.commit()  # an unrelated transaction later: the ghost task stays dropped
    assert size(mem_app) == 0
    session.execute(text("SELECT 1"))
    notify.delay("read-only")  # and no stale "wrote" flag: sent right away
    assert size(mem_app) == 1
    session.close()


def test_commit_from_inside_a_savepoint_sends_once(mem_app, engine):
    Base, User, _ = models()
    Base.metadata.create_all(engine)

    @mem_app.task
    def notify(x):
        return x

    with Session(engine) as session:
        session.add(User(id=1))
        notify.delay("outer")
        session.begin_nested()
        session.add(User(id=2))
        session.flush()
        notify.delay("inner")
        session.commit()  # releases the savepoint and commits the transaction
        assert size(mem_app) == 2
        session.commit()
    assert size(mem_app) == 2


def test_external_connection_is_unwatched_when_the_session_ends(mem_app, engine):
    """A session bound to a connection it doesn't own: once its transaction ends, later
    statements on that connection aren't the session's writes."""
    Base, User, _ = models()
    Base.metadata.create_all(engine)
    with engine.connect() as conn:
        session = Session(bind=conn)
        session.execute(select(User.id)).all()
        assert conn.dispatch.before_cursor_execute
        session.close()
        assert not conn.dispatch.before_cursor_execute
        conn.execute(insert(User.__table__).values(id=1))
        assert session.info == {}
