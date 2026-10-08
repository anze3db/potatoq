"""Postgres broker edge cases: connection loss, LISTEN/NOTIFY, polling, maintenance."""

from __future__ import annotations

import threading
import time

import psycopg
import pytest
from conftest import POSTGRES_URL, make_app, postgres_available

from potatoq import Potatoq, states
from potatoq.brokers import postgres as pg_mod
from potatoq.brokers.base import ResultRecord
from potatoq.brokers.postgres import PostgresBroker, _channel
from potatoq.message import Message

pytestmark = pytest.mark.skipif(not postgres_available(), reason="Postgres not available")


@pytest.fixture
def app(tmp_path):
    app, cleanup = make_app("postgres", tmp_path)
    try:
        yield app
    finally:
        app.close()
        cleanup()


def with_options(app, **options):
    app.conf.broker_transport_options = {**app.conf.broker_transport_options, **options}
    return app


def q(app):
    return app.conf.task_default_queue


def kill(conn):
    """Terminate ``conn``'s server session, as a failover or idle timeout would."""
    with psycopg.connect(POSTGRES_URL, autocommit=True) as admin:
        admin.execute("SELECT pg_terminate_backend(%s)", (conn.info.backend_pid,))
    time.sleep(0.1)


def test_url_normalization_and_unqualified_tables():
    app = Potatoq("pgcov", set_as_current=False)
    b = PostgresBroker("postgres+psycopg://u@h:5/db", app)
    assert b.dsn == "postgresql://u@h:5/db"
    assert b.t["jobs"] == pg_mod.pgsql.Identifier("potatoq_jobs")
    b = PostgresBroker("postgresql://h/db", app, schema="s", table_prefix="x_")
    assert b.t["jobs"] == pg_mod.pgsql.Identifier("s", "x_jobs")


def test_channel_names_fit_postgres_identifiers():
    assert _channel("emails") == "potatoq:emails"
    long = _channel("q" * 80)
    assert len(long) <= 63 and long.startswith("potatoq:")
    assert long == _channel("q" * 80) != _channel("q" * 81)


def test_notify_wakes_a_waiting_worker_on_a_long_queue(app):
    with_options(app, poll_interval=30)
    queue = "long-" + "x" * 70
    c = app.broker.consumer([queue], "w")
    m = Message("t", queue=queue)
    producer = PostgresBroker(app.broker.url, app, **app.conf.broker_transport_options)

    def produce():
        producer.enqueue([m])
        producer.close()

    timer = threading.Timer(0.3, produce)
    timer.start()
    start = time.monotonic()
    d = c.fetch(10)
    elapsed = time.monotonic() - start
    timer.join()
    assert d is not None and d.message.id == m.id
    assert elapsed < 5  # woken by NOTIFY, not the 30s poll
    assert not c._listening  # stopped listening while it runs the task
    c.complete(d, None, [])
    c.close()


def test_task_enqueued_while_starting_to_listen_is_claimed(app, monkeypatch):
    m = Message("t", queue=q(app))
    app.broker.enqueue([m])
    c = app.broker.consumer([q(app)], "w")
    real = c._claim
    calls = []

    def claim():
        calls.append(1)
        return None if len(calls) == 1 else real()

    monkeypatch.setattr(c, "_claim", claim)
    d = c.fetch(5)
    assert d is not None and d.message.id == m.id
    assert len(calls) == 2 and not c._listening
    c.complete(d, None, [])
    c.close()


def test_poll_mode_without_notify(app):
    with_options(app, notify=False, poll_interval=0.05)
    b = app.broker
    assert b.notify is False
    c = b.consumer([q(app)], "w")
    start = time.monotonic()
    assert c.fetch(0.3) is None
    assert time.monotonic() - start >= 0.25
    m = Message("t", queue=q(app))
    b.enqueue([m])
    d = c.fetch(1)
    assert d.message.id == m.id
    c.retry(d, Message.from_dict(m.to_dict()), None)  # no NOTIFY
    d = c.fetch(1)
    c.complete(d, None, [])
    assert not c._listening
    c.close()


def test_broker_reconnects_after_connection_loss(app):
    b = app.broker
    b.store_result(ResultRecord("r", states.SUCCESS, result=1), 60)
    old = b.conn
    kill(old)
    assert b.get_result("r").result == 1
    assert b.conn is not old and old.closed


def test_broker_reraises_errors_on_a_healthy_connection(app):
    b = app.broker

    def slow(conn):
        conn.execute("SET statement_timeout = 10")
        try:
            conn.execute("SELECT pg_sleep(1)")
        finally:
            conn.execute("RESET statement_timeout")

    conn = b.conn
    with pytest.raises(psycopg.errors.QueryCanceled):
        b._run(slow)
    assert b.conn is conn  # not replaced


def test_new_connection_after_fork(app):
    b = app.broker
    inherited = b.conn
    b._pid = -1
    conn = b.conn
    assert conn is not inherited and not inherited.closed
    assert pg_mod._INHERITED.pop().conn is inherited
    b._pid = -1
    b.close()  # never closes a connection opened in another process
    assert not conn.closed
    conn.close()
    inherited.close()


def test_fetch_survives_connection_loss_while_claiming(app):
    c = app.broker.consumer([q(app)], "w")
    conn = c.conn
    kill(conn)
    start = time.monotonic()
    assert c.fetch(0.2) is None
    assert time.monotonic() - start < 2
    assert c._conn is None
    m = Message("t", queue=q(app))
    app.broker.enqueue([m])
    d = c.fetch(2)
    assert d.message.id == m.id
    c.complete(d, None, [])
    c.close()


def test_fetch_survives_connection_loss_while_waiting(app):
    with_options(app, poll_interval=30)
    c = app.broker.consumer([q(app)], "w")
    timer = threading.Timer(0.3, kill, [c.conn])
    timer.start()
    start = time.monotonic()
    assert c.fetch(10) is None
    timer.join()
    assert time.monotonic() - start < 5
    assert c._conn is None and not c._listening


def test_reset_ignores_errors_closing_a_broken_connection(app):
    c = app.broker.consumer([q(app)], "w")

    class Broken:
        closed = broken = True

        def close(self):
            raise OSError("socket already gone")

    c._conn = Broken()  # type: ignore[assignment]
    c._listening = True
    c.close()
    assert c._conn is None and not c._listening


def test_settling_retries_on_a_new_connection(app):
    b = app.broker
    m = Message("t", queue=q(app))
    b.enqueue([m])
    c = b.consumer([q(app)], "w")
    d = c.fetch(2)
    kill(c.conn)
    c.complete(d, ResultRecord(m.id, states.SUCCESS, result=3), [])
    assert b.get_result(m.id).result == 3
    assert b.peek(m.id) is None


def test_settling_gives_up_after_three_attempts(app, monkeypatch):
    c = app.broker.consumer([q(app)], "w")
    attempts = []

    def fail(cur):
        attempts.append(1)
        raise psycopg.OperationalError("server closed the connection")

    monkeypatch.setattr(pg_mod.time, "sleep", lambda s: None)
    with pytest.raises(psycopg.OperationalError):
        c._write(fail)
    assert len(attempts) == 3


def test_retry_stores_record_and_ignores_stale_claims(app):
    b = app.broker
    m = Message("t", queue=q(app))
    b.enqueue([m])
    c = b.consumer([q(app)], "w")
    stale = c.fetch(2)
    c.requeue(stale, count=True)
    d = c.fetch(2)
    c.retry(stale, Message.from_dict(m.to_dict()), ResultRecord(m.id, states.RETRY))  # ignored
    assert b.get_result(m.id).state == states.STARTED
    retried = Message.from_dict(m.to_dict())
    retried.retries = 1
    retried.eta = time.time() + 60
    c.retry(d, retried, ResultRecord(m.id, states.RETRY))
    assert b.get_result(m.id).state == states.RETRY
    message, state = b.peek(m.id)
    assert (message.retries, state) == (1, "scheduled")
    c.close()


def test_peek_states(app):
    b = app.broker
    ready = Message("t", queue=q(app))
    later = Message("t", queue=q(app), eta=time.time() + 60)
    b.enqueue([ready, later])
    assert b.peek("missing") is None
    assert b.peek(later.id)[1] == "scheduled"
    assert b.peek(ready.id)[1] == "ready"
    c = b.consumer([q(app)], "w")
    d = c.fetch(2)
    assert b.peek(ready.id) == (d.message, "running")
    assert b.queue_sizes() == {q(app): 1}
    assert b.purge(q(app)) == 1
    c.close()


def test_enqueue_nothing(app):
    app.broker.enqueue([])
    assert app.broker.queue_sizes() == {}


def test_workers(app):
    b = app.broker
    b.heartbeat("w2", {"pid": 2})
    b.heartbeat("w1", {"pid": 1})
    b.conn.execute(b._sql("INSERT INTO {workers} (id, info) VALUES ('w0', NULL)"))
    workers = b.workers()
    assert [(w["id"], w.get("pid")) for w in workers] == [("w0", None), ("w1", 1), ("w2", 2)]
    assert all(abs(w["heartbeat"] - time.time()) < 60 for w in workers)
    b.unregister("w1")
    assert [w["id"] for w in b.workers()] == ["w0", "w2"]


def test_dead_letter_with_followups_and_requeue_unknown(app):
    b = app.broker
    m = Message("t", queue=q(app))
    follow = Message("t", queue=q(app))
    b.enqueue([m])
    c = b.consumer([q(app)], "w")
    d = c.fetch(2)
    c.dead_letter(d, "boom", ResultRecord(m.id, states.FAILURE), [follow])
    assert [e["id"] for e in b.dead_letters()] == [m.id]
    assert b.get_result(m.id).state == states.FAILURE
    assert c.fetch(2).message.id == follow.id
    assert b.requeue_dead("nope") is False
    c.close()


def _seed_maintenance(b):
    b.store_result(ResultRecord("fresh", states.SUCCESS), 60)
    b.store_result(ResultRecord("forever", states.SUCCESS), None)
    b.store_result(ResultRecord("old", states.SUCCESS), 60)
    run = b.conn.execute
    run(b._sql("UPDATE {results} SET expires_at = now() - interval '1 second' WHERE id = 'old'"))
    run(b._sql("INSERT INTO {periodic} VALUES ('p', extract(epoch FROM now()) - 8 * 86400), ('p', 1e12)"))
    run(
        b._sql(
            "INSERT INTO {chord_parts} (group_id, idx, result, created_at) "
            "VALUES ('old', 0, '1', now() - interval '8 days'), ('new', 0, '1', now())"
        )
    )
    run(
        b._sql(
            "INSERT INTO {chords} (group_id, remaining, created_at) "
            "VALUES ('old', 1, now() - interval '8 days'), ('new', 1, now())"
        )
    )
    for i, age in enumerate([7200, 30, 20, 10]):
        run(
            b._sql(
                "INSERT INTO {dead} (id, queue, task, reason, died_at, payload) "
                "VALUES (%s, 'q', 't', 'r', now() - make_interval(secs => %s), '{{}}')"
            ),
            (f"d{i}", age),
        )


def _maintenance_state(b):
    def ids(sql):
        return sorted(r[0] for r in b.conn.execute(b._sql(sql)))

    return (
        ids("SELECT id FROM {results}"),
        ids("SELECT count(*) FROM {periodic}"),
        ids("SELECT group_id FROM {chord_parts}"),
        ids("SELECT group_id FROM {chords}"),
        [e["id"] for e in b.dead_letters()],
    )


def test_maintenance(app):
    app.conf.dead_letter_max = 2
    app.conf.dead_letter_ttl = 3600
    b = app.broker
    _seed_maintenance(b)
    before = _maintenance_state(b)
    # Another node holds the maintenance lock: nothing to do here.
    with psycopg.connect(POSTGRES_URL, autocommit=True) as other:
        other.execute("SELECT pg_advisory_lock(%s)", (pg_mod._LOCK_MAINTENANCE,))
        b.maintenance()
        other.execute("SELECT pg_advisory_unlock(%s)", (pg_mod._LOCK_MAINTENANCE,))
    assert _maintenance_state(b) == before
    expected = (["forever", "fresh"], [1], ["new"], ["new"], ["d3", "d2"])
    # The lock is shared by every broker on this database; retry if another test has it.
    for _ in range(50):
        b.maintenance()
        if _maintenance_state(b) == expected:
            break
        time.sleep(0.1)
    assert _maintenance_state(b) == expected


def test_interrupt_wakes_polling_fetch(app):
    with_options(app, notify=False, poll_interval=0.05)
    c = app.broker.consumer([q(app)], "w")
    timer = threading.Timer(0.2, c.interrupt)
    start = time.monotonic()
    timer.start()
    assert c.fetch(10) is None
    timer.join()
    assert time.monotonic() - start < 2
    c.close()


def test_lost_deliveries(app):
    b = app.broker
    m = Message("t", queue=q(app))
    b.enqueue([m])
    c = b.consumer([q(app)], "node", pid=4321)
    d = c.fetch(2)
    lost = b.lost_deliveries("node", 4321)
    assert [(x.message.id, x.delivery_count, x.handle) for x in lost] == [(m.id, 1, d.handle)]
    assert b.lost_deliveries("node", 1) == []
    c.complete(lost[0], None, [])  # the supervisor settles it on the child's behalf
    assert b.peek(m.id) is None
    c.close()


@pytest.mark.parametrize("commit", [True, False])
def test_enqueue_joins_the_callers_transaction(app, commit):
    b = app.broker
    b.setup()
    m = Message("t", queue=q(app))
    later = Message("t", queue=q(app), eta=time.time() + 60)
    with psycopg.connect(POSTGRES_URL) as conn:  # not autocommit: a transaction is open
        b.enqueue([m, later], connection=conn)
        assert b.peek(m.id) is None  # not visible before COMMIT
        if commit:
            conn.commit()
        else:
            conn.rollback()
    assert (b.peek(m.id) is not None) is commit
    assert (b.peek(later.id) is not None) is commit
