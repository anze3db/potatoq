"""SQLite broker edge cases: URLs, migrations, fork safety, maintenance, recovery."""

from __future__ import annotations

import sqlite3
import threading
import time

import pytest

from potatoq import Potatoq, states
from potatoq.brokers import sqlite as sqlite_mod
from potatoq.brokers.base import ResultRecord
from potatoq.brokers.sqlite import SQLiteBroker, _path_from_url
from potatoq.exceptions import ImproperlyConfigured
from potatoq.message import Message


@pytest.fixture
def app(tmp_path):
    app = Potatoq("sqlitecov", broker=f"sqlite:///{tmp_path}/b.db", set_as_current=False)
    app.conf.result_backend = "broker"
    yield app
    app.close()


def test_path_from_url():
    assert _path_from_url("plain.db") == "plain.db"
    assert _path_from_url("sqlite:///rel.db") == "rel.db"
    assert _path_from_url("sqlite:////abs/x.db?mode=rw") == "/abs/x.db"
    assert _path_from_url("sqlite://rel.db") == "rel.db"
    with pytest.raises(ImproperlyConfigured, match="No database path"):
        _path_from_url("sqlite:///")
    with pytest.raises(ImproperlyConfigured, match=":memory: can't be shared"):
        _path_from_url("sqlite:///:memory:")


def test_durable_uses_synchronous_full(tmp_path, app):
    b = SQLiteBroker(f"sqlite:///{tmp_path}/d.db", app, durable=True)
    try:
        assert b.conn.execute("PRAGMA synchronous").fetchone()[0] == 2  # FULL
    finally:
        b.close()
    plain = app.broker
    assert plain.conn.execute("PRAGMA synchronous").fetchone()[0] == 1  # NORMAL


def test_new_connection_after_fork(app):
    b = app.broker
    inherited = b.conn
    b._pid = -1  # as if we were the child of a fork()
    conn = b.conn
    assert conn is not inherited
    assert b._pid > 0
    assert sqlite_mod._INHERITED[-1].conn is inherited  # kept alive, never closed
    inherited.execute("SELECT 1")
    b._pid = -1
    b.close()  # a connection opened in another process is never closed
    conn.execute("SELECT 1")
    conn.close()
    assert sqlite_mod._INHERITED.pop().conn is inherited
    inherited.close()


def test_setup_migrates_tables_without_token(tmp_path, app):
    path = tmp_path / "old.db"
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE potatoq_jobs (seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, "
            "queue TEXT NOT NULL, task TEXT NOT NULL, state INTEGER NOT NULL, priority INTEGER NOT NULL DEFAULT 0, "
            "run_at REAL NOT NULL, deliveries INTEGER NOT NULL DEFAULT 0, worker TEXT, pid INTEGER, "
            "claimed_at REAL, created_at REAL NOT NULL, payload TEXT NOT NULL)"
        )
    conn.close()
    b = SQLiteBroker(str(path), app)
    try:
        b.setup()
        columns = {row[1] for row in b.conn.execute("PRAGMA table_info(potatoq_jobs)")}
        assert "token" in columns
        b.setup()  # idempotent
        b.enqueue([Message("t", queue="q")])
        c = b.consumer(["q"], "w")
        d = c.fetch(1)
        assert d is not None
        c.complete(d, None, [])
    finally:
        b.close()


def test_enqueue_nothing_and_rollback_on_error(app):
    b = app.broker
    b.enqueue([])
    assert b.queue_sizes() == {}
    with pytest.raises(sqlite3.IntegrityError):
        with b._write() as conn:
            conn.execute(
                "INSERT INTO potatoq_workers (id, heartbeat) VALUES ('w', 1)",
            )
            conn.execute("INSERT INTO potatoq_workers (id, heartbeat) VALUES ('w', 1)")
    assert b.workers() == []  # rolled back
    assert not b.conn.in_transaction


def test_enqueue_on_external_connection_with_format_paramstyle(app):
    """Django's SQLite cursor uses %s placeholders; it marks its connection."""
    b = app.broker
    raw = sqlite3.connect(b.path, isolation_level=None)

    class FormatCursor:
        def __init__(self, cur):
            self.cur = cur

        def executemany(self, sql, rows):
            assert "%s" in sql and "?" not in sql
            return self.cur.executemany(sql.replace("%s", "?"), rows)

    class FormatConnection:
        _potatoq_paramstyle = "format"

        def cursor(self):
            return FormatCursor(raw.cursor())

    m = Message("t", queue="q")
    raw.execute("BEGIN")
    b.enqueue([m], connection=FormatConnection())
    assert b.peek(m.id) is None  # not committed yet
    raw.execute("COMMIT")
    assert b.peek(m.id)[1] == "ready"
    # qmark connections (sqlite3 itself) work as-is.
    m2 = Message("t", queue="q")
    b.enqueue([m2], connection=raw)
    assert b.queue_sizes() == {"q": 2}
    raw.close()


def test_workers_heartbeat_unregister(app):
    b = app.broker
    b.heartbeat("w2", {"pid": 2})
    b.heartbeat("w1", {"pid": 1})
    b.conn.execute("INSERT INTO potatoq_workers (id, heartbeat, info) VALUES ('w0', 1, NULL)")
    workers = b.workers()
    assert [(w["id"], w.get("pid")) for w in workers] == [("w0", None), ("w1", 1), ("w2", 2)]
    b.unregister("w1")
    assert [w["id"] for w in b.workers()] == ["w0", "w2"]
    # Workers that stopped heartbeating over an hour before the cutoff are forgotten.
    assert b.recover(60) == []
    assert [w["id"] for w in b.workers()] == ["w2"]


def test_recover_dead_letters_exhausted_tasks(app):
    app.conf.task_max_deliveries = 1
    b = app.broker
    m = Message("t.add", queue="q")
    b.enqueue([m])
    c = b.consumer(["q"], "dead-node", pid=1234)
    d = c.fetch(1)
    assert d is not None
    lost = b.lost_deliveries("dead-node", 1234)
    assert [x.message.id for x in lost] == [m.id] and lost[0].handle == d.handle
    assert b.lost_deliveries("dead-node", 999) == []
    recovered = b.recover(60)  # "dead-node" never heartbeated
    assert [(x.message.id, x.delivery_count) for x in recovered] == [(m.id, 1)]
    assert [(e["id"], e["reason"], e["task"]) for e in b.dead_letters()] == [
        (m.id, "worker lost too many times", "t.add")
    ]
    assert b.peek(m.id) is None


def test_requeue_dead_unknown(app):
    assert app.broker.requeue_dead("nope") is False


def test_dead_letter_with_followups_and_record(app):
    b = app.broker
    m = Message("t", queue="q")
    follow = Message("t", queue="q")
    b.enqueue([m])
    c = b.consumer(["q"], "w")
    d = c.fetch(1)
    c.dead_letter(d, "boom", ResultRecord(m.id, states.FAILURE, result="x"), [follow])
    assert b.get_result(m.id).state == states.FAILURE
    assert c.fetch(1).message.id == follow.id


def test_maintenance(app):
    b = app.broker
    app.conf.dead_letter_max = 2
    app.conf.dead_letter_ttl = 3600
    now = time.time()
    b.store_result(ResultRecord("fresh", states.SUCCESS), 60)
    b.store_result(ResultRecord("forever", states.SUCCESS), None)
    b.store_result(ResultRecord("old", states.SUCCESS), 60)
    conn = b.conn
    conn.execute("UPDATE potatoq_results SET expires_at = ? WHERE id = 'old'", (now - 1,))
    week = 7 * 86400
    conn.execute("INSERT INTO potatoq_periodic VALUES ('p', ?), ('p', ?)", (now - week - 10, now))
    conn.execute(
        "INSERT INTO potatoq_chord_parts VALUES ('old', 0, '1', ?), ('new', 0, '1', ?)", (now - week - 10, now)
    )
    conn.execute("INSERT INTO potatoq_chords VALUES ('old', 1, ?), ('new', 1, ?)", (now - week - 10, now))
    for i, died in enumerate([now - 7200, now - 30, now - 20, now - 10]):
        conn.execute("INSERT INTO potatoq_dead VALUES (?, 'q', 't', 'r', ?, '{}')", (f"d{i}", died))
    b.maintenance()
    assert {r[0] for r in conn.execute("SELECT id FROM potatoq_results")} == {"fresh", "forever"}
    assert conn.execute("SELECT count(*) FROM potatoq_periodic").fetchone()[0] == 1
    assert [r[0] for r in conn.execute("SELECT group_id FROM potatoq_chord_parts")] == ["new"]
    assert [r[0] for r in conn.execute("SELECT group_id FROM potatoq_chords")] == ["new"]
    # d0 is past its TTL; of the rest only the newest dead_letter_max are kept.
    assert [e["id"] for e in b.dead_letters()] == ["d3", "d2"]


def test_fetch_promotes_due_tasks_of_other_queues(app):
    b = app.broker
    other = Message("t", queue="other", eta=time.time() + 0.05)
    b.enqueue([other])
    assert b.peek(other.id)[1] == "scheduled"
    time.sleep(0.1)
    c = b.consumer(["q"], "w")
    assert c.fetch(0.05) is None
    assert b.peek(other.id)[1] == "ready"


def test_fetch_rechecks_when_a_scheduled_task_became_due(app, monkeypatch):
    """A task can become due between the claim and the next-due check; fetch must
    claim it straight away instead of sleeping until another commit."""
    b = app.broker
    m = Message("t", queue="q", eta=time.time() + 0.05)
    b.enqueue([m])
    time.sleep(0.1)
    c = b.consumer(["q"], "w")
    real = c._has_work
    calls = []

    def has_work(conn, now):
        calls.append(now)
        return False if len(calls) == 1 else real(conn, now)

    monkeypatch.setattr(c, "_has_work", has_work)
    d = c.fetch(0.5)
    assert d is not None and d.message.id == m.id
    assert len(calls) == 2


def test_interrupt_wakes_fetch(app):
    c = app.broker.consumer(["q"], "w")
    timer = threading.Timer(0.2, c.interrupt)
    start = time.monotonic()
    timer.start()
    assert c.fetch(10) is None
    timer.join()
    assert time.monotonic() - start < 2


def test_retry_stores_record(app):
    b = app.broker
    m = Message("t", queue="q")
    b.enqueue([m])
    c = b.consumer(["q"], "w")
    d = c.fetch(1)
    retried = Message.from_dict(m.to_dict())
    retried.eta = time.time() + 60
    c.retry(d, retried, ResultRecord(m.id, states.RETRY))
    assert b.get_result(m.id).state == states.RETRY
    assert b.peek(m.id)[1] == "scheduled"
