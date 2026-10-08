"""SQLite broker: zero-dependency, great for development and single-host deployments.

Design (see docs/backends.md for the research behind it):

* WAL mode, ``synchronous=NORMAL`` (durable across process crashes; only an OS crash
  can lose the last commits), explicit ``BEGIN IMMEDIATE`` for every write so busy
  handling works, autocommit connections so no snapshot is held while idle.
* Claims are ``UPDATE ... WHERE seq = (SELECT ... LIMIT 1) RETURNING``: SQLite has
  a single writer, so this is atomic without ``SKIP LOCKED``.
* Workers sleep on ``PRAGMA data_version`` (a ~2µs check that changes whenever any
  other connection commits) and only take the write lock when a cheap read says
  there is work. Idle workers cost almost nothing and wake within milliseconds.
* Running tasks are owned by ``(worker node, pid)``; nodes heartbeat; tasks of dead
  nodes are recovered. Every claim gets a random ``token`` that fences acks, so a worker that
  lost its claim can't ack a task that was handed to someone else.
"""

from __future__ import annotations

import json
import os
import secrets
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from .. import serialization, states
from ..exceptions import ImproperlyConfigured
from ..message import Message
from .base import Broker, Consumer, Delivery, ResultRecord

SCHEDULED, READY, RUNNING = 0, 1, 2
_INHERITED: list[Any] = []

SCHEMA = """
CREATE TABLE IF NOT EXISTS potatoq_jobs (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    id          TEXT    NOT NULL UNIQUE,
    queue       TEXT    NOT NULL,
    task        TEXT    NOT NULL,
    state       INTEGER NOT NULL,
    priority    INTEGER NOT NULL DEFAULT 0,
    run_at      REAL    NOT NULL,
    deliveries  INTEGER NOT NULL DEFAULT 0,
    token       INTEGER,
    worker      TEXT,
    pid         INTEGER,
    claimed_at  REAL,
    created_at  REAL    NOT NULL,
    payload     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS potatoq_jobs_ready ON potatoq_jobs (queue, priority DESC, seq) WHERE state = 1;
CREATE INDEX IF NOT EXISTS potatoq_jobs_scheduled ON potatoq_jobs (run_at) WHERE state = 0;
CREATE INDEX IF NOT EXISTS potatoq_jobs_running ON potatoq_jobs (worker, pid) WHERE state = 2;
CREATE TABLE IF NOT EXISTS potatoq_results (
    id          TEXT PRIMARY KEY,
    state       TEXT NOT NULL,
    payload     TEXT NOT NULL,
    expires_at  REAL
);
CREATE INDEX IF NOT EXISTS potatoq_results_expires ON potatoq_results (expires_at);
CREATE TABLE IF NOT EXISTS potatoq_dead (
    id          TEXT PRIMARY KEY,
    queue       TEXT NOT NULL,
    task        TEXT NOT NULL,
    reason      TEXT,
    died_at     REAL NOT NULL,
    payload     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS potatoq_dead_died ON potatoq_dead (died_at);
CREATE TABLE IF NOT EXISTS potatoq_workers (
    id          TEXT PRIMARY KEY,
    heartbeat   REAL NOT NULL,
    info        TEXT
);
CREATE TABLE IF NOT EXISTS potatoq_chord_parts (
    group_id    TEXT    NOT NULL,
    idx         INTEGER NOT NULL,
    result      TEXT    NOT NULL,
    created_at  REAL    NOT NULL,
    PRIMARY KEY (group_id, idx)
);
CREATE TABLE IF NOT EXISTS potatoq_chords (
    group_id    TEXT PRIMARY KEY,
    remaining   INTEGER NOT NULL,
    created_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS potatoq_periodic (
    name        TEXT NOT NULL,
    fire_at     REAL NOT NULL,
    PRIMARY KEY (name, fire_at)
);
"""

PRAGMAS = (
    "busy_timeout=30000",
    "synchronous=NORMAL",
    "cache_size=-32000",
    "temp_store=MEMORY",
    "journal_size_limit=67108864",
    "secure_delete=OFF",
)


def _path_from_url(url: str) -> str:
    # sqlite:///relative.db, sqlite:////absolute.db, sqlite:///:memory: (not shared!)
    if "://" not in url:
        return url
    rest = url.split("://", 1)[1]
    path = rest[1:] if rest.startswith("/") else rest
    path = path.split("?", 1)[0]
    if not path:
        raise ImproperlyConfigured(f"No database path in {url!r}")
    if path == ":memory:":
        raise ImproperlyConfigured("sqlite:///:memory: can't be shared between processes; use a file")
    return path


class SQLiteBroker(Broker):
    schemes = ("sqlite",)
    transactional = True

    def __init__(self, url: str, app: Any, **options: Any):
        super().__init__(url, app, **options)
        self.path = str(Path(_path_from_url(url)).expanduser())
        self._local = threading.local()
        self._pid = os.getpid()
        self.durable = bool(options.get("durable", False))

    # --- connections -------------------------------------------------------------

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=30.0, check_same_thread=False)
        for pragma in PRAGMAS:
            conn.execute(f"PRAGMA {pragma}")
        if self.durable:
            conn.execute("PRAGMA synchronous=FULL")
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        if self._pid != os.getpid():
            self.after_fork()
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._local.conn = self.connect()
        return conn

    def after_fork(self) -> None:
        # Never use (or even close) a connection opened before fork().
        _INHERITED.append(self._local)
        self._local = threading.local()
        self._pid = os.getpid()

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None and self._pid == os.getpid():
            conn.close()
        self._local = threading.local()

    def setup(self) -> None:
        conn = self.conn
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        if mode.lower() != "wal":
            conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(SCHEMA)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(potatoq_jobs)")}
        if "token" not in columns:  # tables created by potatoq < 0.1.0 final
            conn.execute("ALTER TABLE potatoq_jobs ADD COLUMN token INTEGER")

    def _write(self, conn: sqlite3.Connection | None = None) -> _WriteTxn:
        return _WriteTxn(conn or self.conn)

    # --- producing ---------------------------------------------------------------

    @staticmethod
    def _job_row(message: Message, now: float) -> tuple[Any, ...]:
        eta = message.eta
        state = SCHEDULED if eta is not None and eta > now else READY
        return (message.id, message.queue, message.task, state, message.priority, eta or now, now, message.encode())

    _INSERT = (
        "INSERT INTO potatoq_jobs (id, queue, task, state, priority, run_at, created_at, payload) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING"
    )

    def _insert(self, conn: sqlite3.Connection, messages: list[Message]) -> None:
        now = time.time()
        conn.executemany(self._INSERT, [self._job_row(m, now) for m in messages])

    def enqueue(self, messages: list[Message], connection: Any = None) -> None:
        if not messages:
            return
        if connection is not None:
            # The caller's connection (e.g. Django's): join its transaction.
            cursor = connection.cursor()
            now = time.time()
            rows = [self._job_row(m, now) for m in messages]
            if getattr(connection, "_potatoq_paramstyle", "qmark") == "format":
                cursor.executemany(self._INSERT.replace("?", "%s"), rows)
            else:
                cursor.executemany(self._INSERT, rows)
            return
        with self._write() as conn:
            self._insert(conn, messages)

    def enqueue_periodic(self, name: str, fire_at: float, message: Message) -> bool:
        with self._write() as conn:
            cur = conn.execute(
                "INSERT INTO potatoq_periodic (name, fire_at) VALUES (?, ?) ON CONFLICT DO NOTHING", (name, fire_at)
            )
            if cur.rowcount == 0:
                return False
            self._insert(conn, [message])
            return True

    def consumer(self, queues: list[str], worker_id: str, pid: int | None = None) -> SQLiteConsumer:
        return SQLiteConsumer(self, queues, worker_id, pid)

    # --- results -----------------------------------------------------------------

    def _store(self, conn: sqlite3.Connection, record: ResultRecord, expires: float | None) -> None:
        conn.execute(
            "INSERT INTO potatoq_results (id, state, payload, expires_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT (id) DO UPDATE SET state = excluded.state, payload = excluded.payload, expires_at = excluded.expires_at",
            (
                record.task_id,
                record.state,
                serialization.dumps(record.to_dict()),
                time.time() + expires if expires else None,
            ),
        )

    def store_result(self, record: ResultRecord, expires: float | None) -> None:
        with self._write() as conn:
            self._store(conn, record, expires)

    def get_result(self, task_id: str) -> ResultRecord | None:
        conn = self.conn
        row = conn.execute("SELECT payload FROM potatoq_results WHERE id = ?", (task_id,)).fetchone()
        if row is not None:
            return ResultRecord.from_dict(serialization.loads(row[0]))
        row = conn.execute("SELECT state FROM potatoq_jobs WHERE id = ?", (task_id,)).fetchone()
        if row is not None and row[0] == RUNNING:
            return ResultRecord(task_id=task_id, state=states.STARTED)
        return None

    def peek(self, task_id: str) -> tuple[Message, str] | None:
        row = self.conn.execute("SELECT state, payload FROM potatoq_jobs WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            return None
        return Message.decode(row[1]), {SCHEDULED: "scheduled", READY: "ready", RUNNING: "running"}[row[0]]

    def forget(self, task_id: str) -> None:
        with self._write() as conn:
            conn.execute("DELETE FROM potatoq_results WHERE id = ?", (task_id,))

    # --- coordination --------------------------------------------------------------

    def heartbeat(self, worker_id: str, info: dict[str, Any]) -> None:
        with self._write() as conn:
            conn.execute(
                "INSERT INTO potatoq_workers (id, heartbeat, info) VALUES (?, ?, ?) "
                "ON CONFLICT (id) DO UPDATE SET heartbeat = excluded.heartbeat, info = excluded.info",
                (worker_id, time.time(), json.dumps(info)),
            )

    def unregister(self, worker_id: str) -> None:
        with self._write() as conn:
            conn.execute("DELETE FROM potatoq_workers WHERE id = ?", (worker_id,))

    def workers(self) -> list[dict[str, Any]]:
        rows = self.conn.execute("SELECT id, heartbeat, info FROM potatoq_workers ORDER BY id").fetchall()
        return [{"id": r[0], "heartbeat": r[1], **json.loads(r[2] or "{}")} for r in rows]

    def _deliveries(self, rows: list[tuple[Any, ...]]) -> list[Delivery]:
        out = []
        for job_id, deliveries, token, payload in rows:
            out.append(Delivery(Message.decode(payload), delivery_count=deliveries, handle=(job_id, token)))
        return out

    def recover(self, worker_dead_after: float) -> list[Delivery]:  # type: ignore[override]
        """Requeue tasks of dead nodes; return those that exhausted their deliveries."""
        cutoff = time.time() - worker_dead_after
        limit = int(self.app.conf.task_max_deliveries)
        with self._write() as conn:
            dead_workers = "(SELECT id FROM potatoq_workers WHERE heartbeat >= ?)"
            exhausted = conn.execute(
                f"DELETE FROM potatoq_jobs WHERE state = 2 AND worker NOT IN {dead_workers} AND deliveries >= ? "
                "RETURNING id, deliveries, token, payload",
                (cutoff, limit),
            ).fetchall()
            for job_id, _, _, payload in exhausted:
                message = Message.decode(payload)
                conn.execute(
                    "INSERT INTO potatoq_dead (id, queue, task, reason, died_at, payload) VALUES (?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT (id) DO UPDATE SET reason = excluded.reason, died_at = excluded.died_at",
                    (job_id, message.queue, message.task, "worker lost too many times", time.time(), payload),
                )
            conn.execute(
                f"UPDATE potatoq_jobs SET state = 1, worker = NULL, pid = NULL, token = NULL WHERE state = 2 AND worker NOT IN {dead_workers} AND deliveries < ?",
                (cutoff, limit),
            )
            conn.execute("DELETE FROM potatoq_workers WHERE heartbeat < ?", (cutoff - 3600,))
        return self._deliveries(exhausted)

    def lost_deliveries(self, worker_id: str, pid: int) -> list[Delivery]:
        rows = self.conn.execute(
            "SELECT id, deliveries, token, payload FROM potatoq_jobs WHERE state = 2 AND worker = ? AND pid = ?",
            (worker_id, pid),
        ).fetchall()
        return self._deliveries(rows)

    def tick(self) -> None:
        with self._write() as conn:
            conn.execute("UPDATE potatoq_jobs SET state = 1 WHERE state = 0 AND run_at <= ?", (time.time(),))

    def maintenance(self) -> None:
        now = time.time()
        with self._write() as conn:
            conn.execute("DELETE FROM potatoq_results WHERE expires_at IS NOT NULL AND expires_at < ?", (now,))
            conn.execute("DELETE FROM potatoq_periodic WHERE fire_at < ?", (now - 7 * 86400,))
            conn.execute("DELETE FROM potatoq_chord_parts WHERE created_at < ?", (now - 7 * 86400,))
            conn.execute("DELETE FROM potatoq_chords WHERE created_at < ?", (now - 7 * 86400,))
            conn.execute(
                "DELETE FROM potatoq_dead WHERE died_at < ? OR id IN "
                "(SELECT id FROM potatoq_dead ORDER BY died_at DESC LIMIT -1 OFFSET ?)",
                (now - self.app.conf.get("dead_letter_ttl", 180 * 86400), self.app.conf.get("dead_letter_max", 10_000)),
            )
        self.conn.execute("PRAGMA optimize")

    def chord_part_done(self, group_id: str, index: int, size: int, result: Any) -> list[Any] | None:
        """See PostgresBroker.chord_part_done: complete exactly once, but return the
        results again if a part is redelivered after completion."""
        now = time.time()
        with self._write() as conn:
            inserted = conn.execute(
                "INSERT INTO potatoq_chord_parts (group_id, idx, result, created_at) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (group_id, index, serialization.dumps(result), now),
            ).rowcount
            conn.execute(
                "INSERT INTO potatoq_chords (group_id, remaining, created_at) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                (group_id, size, now),
            )
            if inserted:
                remaining = conn.execute(
                    "UPDATE potatoq_chords SET remaining = remaining - 1 WHERE group_id = ? RETURNING remaining",
                    (group_id,),
                ).fetchone()[0]
            else:
                remaining = conn.execute(
                    "SELECT remaining FROM potatoq_chords WHERE group_id = ?", (group_id,)
                ).fetchone()[0]
            if remaining > 0:
                return None
            rows = conn.execute(
                "SELECT result FROM potatoq_chord_parts WHERE group_id = ? ORDER BY idx", (group_id,)
            ).fetchall()
        return [serialization.loads(r[0]) for r in rows]

    def revoke(self, task_ids: list[str], expires: float) -> None:
        """Delete waiting tasks outright; the worker never sees them."""
        now = time.time()
        with self._write() as conn:
            for task_id in task_ids:
                row = conn.execute(
                    "DELETE FROM potatoq_jobs WHERE id = ? AND state != 2 RETURNING task", (task_id,)
                ).fetchone()
                record = ResultRecord(
                    task_id=task_id, state=states.REVOKED, date_done=now, task_name=row[0] if row else None
                )
                self._store(conn, record, self.app.conf.result_expires)

    # --- inspection --------------------------------------------------------------

    def queue_sizes(self) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT queue, count(*) FROM potatoq_jobs WHERE state IN (0, 1) GROUP BY queue"
        ).fetchall()
        return dict(rows)

    def purge(self, queue: str) -> int:
        with self._write() as conn:
            return conn.execute("DELETE FROM potatoq_jobs WHERE queue = ? AND state IN (0, 1)", (queue,)).rowcount

    def dead_letters(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT id, queue, task, reason, died_at, payload FROM potatoq_dead ORDER BY died_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [
            {
                "id": r[0],
                "queue": r[1],
                "task": r[2],
                "reason": r[3],
                "died_at": r[4],
                "message": serialization.loads(r[5]),
            }
            for r in rows
        ]

    def requeue_dead(self, task_id: str) -> bool:
        with self._write() as conn:
            row = conn.execute("DELETE FROM potatoq_dead WHERE id = ? RETURNING payload", (task_id,)).fetchone()
            if row is None:
                return False
            message = Message.decode(row[0])
            message.eta = None
            conn.execute("DELETE FROM potatoq_results WHERE id = ?", (task_id,))
            self._insert(conn, [message])
            return True


class _WriteTxn:
    """``BEGIN IMMEDIATE`` ... ``COMMIT`` (taking the write lock up front avoids
    SQLITE_BUSY on lock upgrade, which the busy timeout can't retry)."""

    __slots__ = ("conn",)

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def __enter__(self) -> sqlite3.Connection:
        self.conn.execute("BEGIN IMMEDIATE")
        return self.conn

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is None:
            self.conn.execute("COMMIT")
        else:
            self.conn.execute("ROLLBACK")


class SQLiteConsumer(Consumer):
    broker: SQLiteBroker

    def __init__(self, broker: SQLiteBroker, queues: list[str], worker_id: str, pid: int | None = None):
        super().__init__(broker, queues, worker_id, pid)
        self._rotation = 0
        self._data_version: int | None = None
        self._interrupted = False

    def _has_work(self, conn: sqlite3.Connection, now: float) -> bool:
        for queue in self.queues:
            if conn.execute("SELECT 1 FROM potatoq_jobs WHERE state = 1 AND queue = ? LIMIT 1", (queue,)).fetchone():
                return True
        return (
            conn.execute("SELECT 1 FROM potatoq_jobs WHERE state = 0 AND run_at <= ? LIMIT 1", (now,)).fetchone()
            is not None
        )

    def _next_due(self, conn: sqlite3.Connection) -> float | None:
        row = conn.execute("SELECT min(run_at) FROM potatoq_jobs WHERE state = 0").fetchone()
        return row[0] if row else None

    def _claim(self, conn: sqlite3.Connection) -> Delivery | None:
        now = time.time()
        if not self._has_work(conn, now):
            return None
        n = len(self.queues)
        order = [self.queues[(self._rotation + i) % n] for i in range(n)]
        self._rotation = (self._rotation + 1) % max(n, 1)
        with self.broker._write(conn):
            conn.execute("UPDATE potatoq_jobs SET state = 1 WHERE state = 0 AND run_at <= ?", (now,))
            for queue in order:
                token = secrets.randbits(62)
                row = conn.execute(
                    "UPDATE potatoq_jobs SET state = 2, deliveries = deliveries + 1, worker = ?, pid = ?, token = ?, claimed_at = ? "
                    "WHERE seq = (SELECT seq FROM potatoq_jobs WHERE state = 1 AND queue = ? ORDER BY priority DESC, seq LIMIT 1) "
                    "RETURNING id, deliveries, payload",
                    (self.worker_id, self.pid, token, now, queue),
                ).fetchall()
                if row:
                    job_id, deliveries, payload = row[0]
                    return Delivery(Message.decode(payload), delivery_count=deliveries, handle=(job_id, token))
        return None

    def fetch(self, timeout: float) -> Delivery | None:
        conn = self.broker.conn
        deadline = time.monotonic() + timeout
        sleep = 0.002
        self._interrupted = False
        # data_version ignores this connection's own commits (our last ack), so always
        # look once before sleeping on it.
        self._data_version = None
        while True:
            version = conn.execute("PRAGMA data_version").fetchone()[0]
            if version != self._data_version:
                self._data_version = version
                delivery = self._claim(conn)
                if delivery is not None:
                    return delivery
                sleep = 0.002
                next_due = self._next_due(conn)
                if next_due is not None and next_due <= time.time():
                    continue
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self._interrupted:
                # Periodically re-check even without commits, for scheduled tasks.
                self._data_version = None
                return None
            time.sleep(min(sleep, remaining))
            # Back off gently while idle: 2ms -> 50ms.
            sleep = min(sleep * 1.5, 0.05)

    def interrupt(self) -> None:
        self._interrupted = True

    def _delete_fenced(self, conn: sqlite3.Connection, delivery: Delivery) -> bool:
        """False if we no longer own the claim; then nothing else may be written."""
        job_id, token = delivery.handle
        return (
            conn.execute("DELETE FROM potatoq_jobs WHERE id = ? AND state = 2 AND token = ?", (job_id, token)).rowcount
            > 0
        )

    def complete(self, delivery: Delivery, record: ResultRecord | None, followups: list[Message]) -> None:
        with self.broker._write() as conn:
            if not self._delete_fenced(conn, delivery):
                return
            if record is not None:
                self.broker._store(conn, record, self.broker.app.conf.result_expires)
            if followups:
                self.broker._insert(conn, followups)

    def retry(self, delivery: Delivery, message: Message, record: ResultRecord | None) -> None:
        job_id, token = delivery.handle
        now = time.time()
        state = SCHEDULED if message.eta and message.eta > now else READY
        with self.broker._write() as conn:
            updated = conn.execute(
                "UPDATE potatoq_jobs SET state = ?, run_at = ?, queue = ?, priority = ?, payload = ?, deliveries = 0, "
                "worker = NULL, pid = NULL, token = NULL WHERE id = ? AND state = 2 AND token = ?",
                (state, message.eta or now, message.queue, message.priority, message.encode(), job_id, token),
            ).rowcount
            if updated and record is not None:
                self.broker._store(conn, record, self.broker.app.conf.result_expires)

    def requeue(self, delivery: Delivery, count: bool = False) -> None:
        job_id, token = delivery.handle
        with self.broker._write() as conn:
            conn.execute(
                "UPDATE potatoq_jobs SET state = 1, worker = NULL, pid = NULL, token = NULL, deliveries = deliveries - ? "
                "WHERE id = ? AND state = 2 AND token = ?",
                (0 if count else 1, job_id, token),
            )

    def dead_letter(
        self, delivery: Delivery, reason: str, record: ResultRecord | None, followups: list[Message] | None = None
    ) -> None:
        message = delivery.message
        with self.broker._write() as conn:
            if not self._delete_fenced(conn, delivery):
                return
            if followups:
                self.broker._insert(conn, followups)
            conn.execute(
                "INSERT INTO potatoq_dead (id, queue, task, reason, died_at, payload) VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (id) DO UPDATE SET reason = excluded.reason, died_at = excluded.died_at, payload = excluded.payload",
                (message.id, message.queue, message.task, reason, time.time(), message.encode()),
            )
            if record is not None:
                self.broker._store(conn, record, self.broker.app.conf.result_expires)
