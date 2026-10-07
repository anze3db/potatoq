"""SQLAlchemy integration: tasks follow the session's transaction."""

from __future__ import annotations

import pytest
from conftest import POSTGRES_URL, make_app, postgres_available
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

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
