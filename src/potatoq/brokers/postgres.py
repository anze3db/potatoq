"""PostgreSQL broker.

Design (research in docs/backends.md; borrows from River, Oban, Solid Queue):

* **Transactional enqueue.** Tasks are rows. Enqueued on the application's own
  connection (Django does this automatically), a task commits or rolls back with the
  data it refers to: no "task ran before the row existed", no "task for data that was
  rolled back", and no lost tasks if the process dies right after COMMIT.
* **Claiming** is one statement: a ``MATERIALIZED`` CTE picks the next row with
  ``FOR UPDATE SKIP LOCKED`` (the explicit fence stops the planner from re-running the
  sub-select and over-claiming) and the outer ``UPDATE ... RETURNING`` marks it
  running. The ORDER BY matches a partial index, so the scan reads one index entry.
* **No transaction is held while a task runs.** Running tasks are owned by
  ``(worker node, pid)``; nodes heartbeat into a small HOT-update-friendly table; tasks
  of nodes that stop heartbeating are recovered. Every claim gets a random ``token``
  that fences acks: a worker that lost its claim can't settle someone else's.
* **Scheduled tasks** live in their own state/partial index (so far-future ETAs never
  slow down the ready-queue scan) and are promoted once a second.
* **Wake-ups** use LISTEN/NOTIFY, debounced per queue and per process because a
  transaction that NOTIFYs takes a cluster-wide lock at commit. A 1s poll is always the
  fallback; set ``broker_transport_options={"notify": False}`` behind PgBouncer in
  transaction pooling mode.
* Finished tasks leave the hot table; results live in their own table and expire.
"""

from __future__ import annotations

import json
import os
import random
import secrets
import threading
import time
from typing import Any

from .. import serialization, states
from ..message import Message
from .base import Broker, Consumer, Delivery, ResultRecord

try:
    import psycopg
    from psycopg import sql as pgsql
except ImportError as exc:  # pragma: no cover
    raise ImportError("PostgreSQL support requires psycopg 3: pip install 'potatoq[postgres]'") from exc

SCHEDULED, READY, RUNNING = 0, 1, 2
_INHERITED: list[Any] = []

SCHEMA = """
CREATE TABLE IF NOT EXISTS {jobs} (
    seq         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id          text        NOT NULL UNIQUE,
    queue       text        NOT NULL,
    task        text        NOT NULL,
    state       smallint    NOT NULL,
    priority    smallint    NOT NULL DEFAULT 0,
    deliveries  integer     NOT NULL DEFAULT 0,
    pid         integer,
    token       bigint,
    run_at      timestamptz NOT NULL DEFAULT now(),
    created_at  timestamptz NOT NULL DEFAULT now(),
    claimed_at  timestamptz,
    worker      text,
    payload     text        NOT NULL
) WITH (
    fillfactor = 90,
    autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000,
    autovacuum_vacuum_insert_scale_factor = 0, autovacuum_vacuum_insert_threshold = 1000,
    autovacuum_analyze_scale_factor = 0, autovacuum_analyze_threshold = 1000,
    autovacuum_vacuum_cost_delay = 0
);
ALTER TABLE {jobs} ADD COLUMN IF NOT EXISTS token bigint;
CREATE INDEX IF NOT EXISTS {jobs_ready} ON {jobs} (queue, priority DESC, seq) WHERE state = 1;
CREATE INDEX IF NOT EXISTS {jobs_scheduled} ON {jobs} (run_at) WHERE state = 0;
CREATE INDEX IF NOT EXISTS {jobs_running} ON {jobs} (worker, pid) WHERE state = 2;
CREATE TABLE IF NOT EXISTS {results} (
    id          text PRIMARY KEY,
    state       text NOT NULL,
    payload     text NOT NULL,
    expires_at  timestamptz
) WITH (autovacuum_vacuum_scale_factor = 0.02);
CREATE INDEX IF NOT EXISTS {results_expires} ON {results} (expires_at);
CREATE TABLE IF NOT EXISTS {dead} (
    id          text PRIMARY KEY,
    queue       text NOT NULL,
    task        text NOT NULL,
    reason      text,
    died_at     timestamptz NOT NULL DEFAULT now(),
    payload     text NOT NULL
);
CREATE INDEX IF NOT EXISTS {dead_died} ON {dead} (died_at);
CREATE TABLE IF NOT EXISTS {workers} (
    id          text PRIMARY KEY,
    heartbeat   timestamptz NOT NULL DEFAULT now(),
    info        text
) WITH (fillfactor = 50);
CREATE TABLE IF NOT EXISTS {chord_parts} (
    group_id    text NOT NULL,
    idx         integer NOT NULL,
    result      text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, idx)
);
CREATE TABLE IF NOT EXISTS {chords} (
    group_id    text PRIMARY KEY,
    remaining   integer NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS {periodic} (
    name        text NOT NULL,
    fire_at     double precision NOT NULL,
    PRIMARY KEY (name, fire_at)
);
"""

#: Advisory lock key namespace for schema setup and maintenance.
_LOCK_SETUP = 0x706F7461  # "pota"
_LOCK_MAINTENANCE = 0x706F7462


def _normalize_url(url: str) -> str:
    return "postgresql://" + url.split("://", 1)[1]


def _channel(queue: str) -> str:
    name = f"potatoq:{queue}"
    if len(name) > 63:
        import hashlib

        name = "potatoq:" + hashlib.sha1(queue.encode()).hexdigest()
    return name


class PostgresBroker(Broker):
    schemes = ("postgresql", "postgres")
    transactional = True

    def __init__(self, url: str, app: Any, **options: Any):
        super().__init__(url, app, **options)
        self.dsn = _normalize_url(url)
        self.notify = bool(options.get("notify", True))
        self.poll_interval = float(options.get("poll_interval", 1.0))
        self.table_prefix = str(options.get("table_prefix", "potatoq_"))
        self.schema = options.get("schema")
        self._local = threading.local()
        self._pid = os.getpid()
        self._last_notify: dict[str, float] = {}
        self._notify_debounce = float(options.get("notify_debounce", 0.05))
        self.t = {
            name: self._ident(name)
            for name in ("jobs", "results", "dead", "workers", "chord_parts", "chords", "periodic")
        }

    def _ident(self, name: str) -> Any:
        if self.schema:
            return pgsql.Identifier(self.schema, self.table_prefix + name)
        return pgsql.Identifier(self.table_prefix + name)

    def _sql(self, template: str) -> pgsql.Composed:
        names = {k: v for k, v in self.t.items()}
        for index in ("jobs_ready", "jobs_scheduled", "jobs_running", "results_expires", "dead_died"):
            names[index] = pgsql.Identifier(self.table_prefix + index)
        return pgsql.SQL(template).format(**names)

    # --- connections -------------------------------------------------------------

    def connect(self, autocommit: bool = True) -> psycopg.Connection:
        timeout = self.app.conf.broker_connection_timeout
        return psycopg.connect(self.dsn, autocommit=autocommit, connect_timeout=int(timeout), prepare_threshold=None)

    @property
    def conn(self) -> psycopg.Connection:
        if self._pid != os.getpid():
            self.after_fork()
        conn = getattr(self._local, "conn", None)
        if conn is None or conn.closed or conn.broken:
            conn = self._local.conn = self.connect()
        return conn

    def _run(self, fn: Any) -> Any:
        """Run ``fn(conn)``; reconnect and retry once if the connection was dropped."""
        try:
            return fn(self.conn)
        except psycopg.OperationalError:
            conn = getattr(self._local, "conn", None)
            if conn is not None and not conn.closed and not conn.broken:
                raise
            self._local.conn = None
            return fn(self.conn)

    def after_fork(self) -> None:
        # Closing an inherited connection would send Terminate and kill the parent's
        # session. Keep it referenced, never touch it.
        _INHERITED.append(self._local)
        self._local = threading.local()
        self._pid = os.getpid()

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None and self._pid == os.getpid():
            conn.close()
        self._local = threading.local()

    def setup(self) -> None:
        def _setup(conn: psycopg.Connection) -> None:
            with conn.transaction():
                conn.execute("SELECT pg_advisory_xact_lock(%s)", (_LOCK_SETUP,))
                if self.schema:
                    conn.execute(pgsql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(pgsql.Identifier(self.schema)))
                conn.execute(self._sql(SCHEMA))

        self._run(_setup)

    # --- producing ---------------------------------------------------------------

    def _rows(self, messages: list[Message]) -> list[tuple[Any, ...]]:
        now = time.time()
        rows = []
        for m in messages:
            eta = m.eta if m.eta is not None and m.eta > now else None
            rows.append(
                (
                    m.id,
                    m.queue,
                    m.task,
                    SCHEDULED if eta else READY,
                    max(-32768, min(32767, m.priority)),
                    eta,
                    m.encode(),
                )
            )
        return rows

    @property
    def _insert_sql(self) -> pgsql.Composed:
        return self._sql(
            "INSERT INTO {jobs} (id, queue, task, state, priority, run_at, payload) "
            "VALUES (%s, %s, %s, %s, %s, coalesce(to_timestamp(%s), now()), %s) ON CONFLICT (id) DO NOTHING"
        )

    def _insert(self, cursor: Any, messages: list[Message], notify: bool = True) -> None:
        rows = self._rows(messages)
        if len(rows) == 1:
            cursor.execute(self._insert_sql, rows[0])
        else:
            cursor.executemany(self._insert_sql, rows)
        if notify and self.notify:
            queues = self._queues_to_notify(m for m in messages if m.eta is None or m.eta <= time.time())
            for queue in queues:
                cursor.execute("SELECT pg_notify(%s, '')", (_channel(queue),))

    def _queues_to_notify(self, messages: Any) -> list[str]:
        now = time.monotonic()
        out = []
        for queue in {m.queue for m in messages}:
            if now - self._last_notify.get(queue, 0.0) >= self._notify_debounce:
                self._last_notify[queue] = now
                out.append(queue)
        return out

    def enqueue(self, messages: list[Message], connection: Any = None) -> None:
        if not messages:
            return
        if connection is not None:
            # Join the caller's transaction (psycopg 3 or psycopg2 DB-API connection).
            rows = self._rows(messages)
            sql = self._insert_sql.as_string(None)
            with connection.cursor() as cur:
                cur.executemany(sql, rows)
                if self.notify:
                    for queue in self._queues_to_notify(m for m in messages if m.eta is None or m.eta <= time.time()):
                        cur.execute("SELECT pg_notify(%s, '')", (_channel(queue),))
            return

        if len(messages) == 1:
            # The common case (``delay()``): one statement, one round trip. The NOTIFY
            # rides along in the same (implicit) transaction.
            message = messages[0]
            row = self._rows(messages)[0]
            notify = self.notify and message.eta is None and self._queues_to_notify(messages)
            if notify:
                sql = self._sql(
                    "WITH ins AS (INSERT INTO {jobs} (id, queue, task, state, priority, run_at, payload) "
                    "VALUES (%s, %s, %s, %s, %s, coalesce(to_timestamp(%s), now()), %s) ON CONFLICT (id) DO NOTHING "
                    "RETURNING queue) SELECT pg_notify(%s, '') FROM ins"
                )
                self._run(lambda conn: conn.execute(sql, (*row, _channel(message.queue))))
            else:
                self._run(lambda conn: conn.execute(self._insert_sql, row))
            return

        def _enqueue(conn: psycopg.Connection) -> None:
            with conn.transaction(), conn.cursor() as cur:
                self._insert(cur, messages)

        self._run(_enqueue)

    def enqueue_periodic(self, name: str, fire_at: float, message: Message) -> bool:
        def _enqueue(conn: psycopg.Connection) -> bool:
            with conn.transaction(), conn.cursor() as cur:
                cur.execute(
                    self._sql("INSERT INTO {periodic} (name, fire_at) VALUES (%s, %s) ON CONFLICT DO NOTHING"),
                    (name, fire_at),
                )
                if cur.rowcount == 0:
                    return False
                self._insert(cur, [message])
                return True

        return self._run(_enqueue)

    def consumer(self, queues: list[str], worker_id: str, pid: int | None = None) -> PostgresConsumer:
        return PostgresConsumer(self, queues, worker_id, pid)

    # --- results -----------------------------------------------------------------

    def _store(self, cur: Any, record: ResultRecord, expires: float | None) -> None:
        cur.execute(
            self._sql(
                "INSERT INTO {results} (id, state, payload, expires_at) "
                "VALUES (%s, %s, %s, CASE WHEN %s::float8 IS NULL THEN NULL ELSE now() + make_interval(secs => %s::float8) END) "
                "ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, payload = EXCLUDED.payload, expires_at = EXCLUDED.expires_at"
            ),
            (record.task_id, record.state, serialization.dumps(record.to_dict()), expires, expires),
        )

    def store_result(self, record: ResultRecord, expires: float | None) -> None:
        def _store(conn: psycopg.Connection) -> None:
            with conn.cursor() as cur:
                self._store(cur, record, expires)

        self._run(_store)

    def get_result(self, task_id: str) -> ResultRecord | None:
        def _get(conn: psycopg.Connection) -> ResultRecord | None:
            row = conn.execute(self._sql("SELECT payload FROM {results} WHERE id = %s"), (task_id,)).fetchone()
            if row is not None:
                return ResultRecord.from_dict(serialization.loads(row[0]))
            row = conn.execute(self._sql("SELECT state FROM {jobs} WHERE id = %s"), (task_id,)).fetchone()
            if row is not None and row[0] == RUNNING:
                return ResultRecord(task_id=task_id, state=states.STARTED)
            return None

        return self._run(_get)

    def peek(self, task_id: str) -> tuple[Message, str] | None:
        row = self._run(
            lambda conn: conn.execute(
                self._sql("SELECT state, payload FROM {jobs} WHERE id = %s"), (task_id,)
            ).fetchone()
        )
        if row is None:
            return None
        return Message.decode(row[1]), {SCHEDULED: "scheduled", READY: "ready", RUNNING: "running"}[row[0]]

    def forget(self, task_id: str) -> None:
        self._run(lambda conn: conn.execute(self._sql("DELETE FROM {results} WHERE id = %s"), (task_id,)))

    # --- coordination ------------------------------------------------------------

    def heartbeat(self, worker_id: str, info: dict[str, Any]) -> None:
        self._run(
            lambda conn: conn.execute(
                self._sql(
                    "INSERT INTO {workers} (id, heartbeat, info) VALUES (%s, now(), %s) "
                    "ON CONFLICT (id) DO UPDATE SET heartbeat = now(), info = EXCLUDED.info"
                ),
                (worker_id, json.dumps(info)),
            )
        )

    def unregister(self, worker_id: str) -> None:
        self._run(lambda conn: conn.execute(self._sql("DELETE FROM {workers} WHERE id = %s"), (worker_id,)))

    def workers(self) -> list[dict[str, Any]]:
        rows = self._run(
            lambda conn: conn.execute(
                self._sql("SELECT id, extract(epoch FROM heartbeat), info FROM {workers} ORDER BY id")
            ).fetchall()
        )
        return [{"id": r[0], "heartbeat": float(r[1]), **json.loads(r[2] or "{}")} for r in rows]

    @staticmethod
    def _deliveries(rows: list[tuple[Any, ...]]) -> list[Delivery]:
        return [
            Delivery(Message.decode(payload), delivery_count=n, handle=(job_id, token))
            for job_id, n, token, payload in rows
        ]

    def recover(self, worker_dead_after: float) -> list[Delivery]:  # type: ignore[override]
        limit = int(self.app.conf.task_max_deliveries)
        live = "(SELECT id FROM {workers} WHERE heartbeat >= now() - make_interval(secs => %(after)s))"

        def _recover(conn: psycopg.Connection) -> list[Delivery]:
            with conn.transaction():
                conn.execute(
                    self._sql(
                        "WITH stuck AS MATERIALIZED (SELECT seq FROM {jobs} WHERE state = 2 AND deliveries < %(limit)s "
                        f"AND (worker IS NULL OR worker NOT IN {live}) FOR UPDATE SKIP LOCKED) "
                        "UPDATE {jobs} j SET state = 1, worker = NULL, pid = NULL, token = NULL FROM stuck WHERE j.seq = stuck.seq"
                    ),
                    {"after": worker_dead_after, "limit": limit},
                )
                rows = conn.execute(
                    self._sql(
                        "WITH stuck AS MATERIALIZED (SELECT seq FROM {jobs} WHERE state = 2 AND deliveries >= %(limit)s "
                        f"AND (worker IS NULL OR worker NOT IN {live}) FOR UPDATE SKIP LOCKED), "
                        "gone AS (DELETE FROM {jobs} j USING stuck WHERE j.seq = stuck.seq RETURNING j.id, j.queue, j.task, j.deliveries, j.token, j.payload), "
                        "ins AS (INSERT INTO {dead} (id, queue, task, reason, payload) "
                        "SELECT id, queue, task, 'worker lost too many times', payload FROM gone ON CONFLICT (id) DO NOTHING) "
                        "SELECT id, deliveries, token, payload FROM gone"
                    ),
                    {"after": worker_dead_after, "limit": limit},
                ).fetchall()
                conn.execute(
                    self._sql("DELETE FROM {workers} WHERE heartbeat < now() - make_interval(secs => %s)"),
                    (worker_dead_after + 3600,),
                )
            return self._deliveries(rows)

        return self._run(_recover)

    def lost_deliveries(self, worker_id: str, pid: int) -> list[Delivery]:
        rows = self._run(
            lambda conn: conn.execute(
                self._sql(
                    "SELECT id, deliveries, token, payload FROM {jobs} WHERE state = 2 AND worker = %s AND pid = %s"
                ),
                (worker_id, pid),
            ).fetchall()
        )
        return self._deliveries(rows)

    def tick(self) -> None:
        """Promote scheduled tasks that are due and wake the workers of their queues."""

        def _tick(conn: psycopg.Connection) -> None:
            with conn.transaction():
                rows = conn.execute(
                    self._sql(
                        "WITH due AS MATERIALIZED (SELECT seq FROM {jobs} WHERE state = 0 AND run_at <= now() "
                        "ORDER BY run_at LIMIT 5000 FOR UPDATE SKIP LOCKED) "
                        "UPDATE {jobs} j SET state = 1 FROM due WHERE j.seq = due.seq RETURNING j.queue"
                    )
                ).fetchall()
                if self.notify:
                    for queue in {r[0] for r in rows}:
                        conn.execute("SELECT pg_notify(%s, '')", (_channel(queue),))

        self._run(_tick)

    def maintenance(self) -> None:
        conf = self.app.conf

        def _maintain(conn: psycopg.Connection) -> None:
            with conn.transaction():
                if not conn.execute("SELECT pg_try_advisory_xact_lock(%s)", (_LOCK_MAINTENANCE,)).fetchone()[0]:
                    return  # another node is doing it
                conn.execute(
                    self._sql(
                        "DELETE FROM {results} WHERE id IN (SELECT id FROM {results} WHERE expires_at < now() LIMIT 10000)"
                    )
                )
                conn.execute(self._sql("DELETE FROM {periodic} WHERE fire_at < extract(epoch FROM now()) - 7 * 86400"))
                conn.execute(self._sql("DELETE FROM {chord_parts} WHERE created_at < now() - interval '7 days'"))
                conn.execute(self._sql("DELETE FROM {chords} WHERE created_at < now() - interval '7 days'"))
                conn.execute(
                    self._sql(
                        "DELETE FROM {dead} WHERE died_at < now() - make_interval(secs => %s) OR id IN "
                        "(SELECT id FROM {dead} ORDER BY died_at DESC OFFSET %s)"
                    ),
                    (float(conf.dead_letter_ttl), int(conf.dead_letter_max)),
                )

        self._run(_maintain)

    def chord_part_done(self, group_id: str, index: int, size: int, result: Any) -> list[Any] | None:
        """Exactly one caller sees the chord complete, unless a part is redelivered
        after the chord completed (the finisher crashed before acking): then the
        results are returned again so the callback isn't lost. The callback has a
        fixed id, so enqueueing it twice is deduplicated while it is still queued."""

        def _done(conn: psycopg.Connection) -> list[Any] | None:
            with conn.transaction():
                inserted = conn.execute(
                    self._sql(
                        "INSERT INTO {chord_parts} (group_id, idx, result) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
                    ),
                    (group_id, index, serialization.dumps(result)),
                ).rowcount
                conn.execute(
                    self._sql("INSERT INTO {chords} (group_id, remaining) VALUES (%s, %s) ON CONFLICT DO NOTHING"),
                    (group_id, size),
                )
                if inserted:
                    # The row lock serializes concurrent finishers; exactly one sees 0.
                    remaining = conn.execute(
                        self._sql(
                            "UPDATE {chords} SET remaining = remaining - 1 WHERE group_id = %s RETURNING remaining"
                        ),
                        (group_id,),
                    ).fetchone()[0]
                else:
                    remaining = conn.execute(
                        self._sql("SELECT remaining FROM {chords} WHERE group_id = %s FOR UPDATE"), (group_id,)
                    ).fetchone()[0]
                if remaining > 0:
                    return None
                rows = conn.execute(
                    self._sql("SELECT idx, result FROM {chord_parts} WHERE group_id = %s ORDER BY idx"), (group_id,)
                ).fetchall()
            return [serialization.loads(r[1]) for r in rows]

        return self._run(_done)

    def revoke(self, task_ids: list[str], expires: float) -> None:
        def _revoke(conn: psycopg.Connection) -> None:
            with conn.transaction(), conn.cursor() as cur:
                for task_id in task_ids:
                    row = cur.execute(
                        self._sql("DELETE FROM {jobs} WHERE id = %s AND state != 2 RETURNING task"), (task_id,)
                    ).fetchone()
                    record = ResultRecord(
                        task_id=task_id, state=states.REVOKED, date_done=time.time(), task_name=row[0] if row else None
                    )
                    self._store(cur, record, self.app.conf.result_expires)

        self._run(_revoke)

    # --- inspection --------------------------------------------------------------

    def queue_sizes(self) -> dict[str, int]:
        rows = self._run(
            lambda conn: conn.execute(
                self._sql("SELECT queue, count(*) FROM {jobs} WHERE state IN (0, 1) GROUP BY queue")
            ).fetchall()
        )
        return {r[0]: r[1] for r in rows}

    def purge(self, queue: str) -> int:
        return self._run(
            lambda conn: (
                conn.execute(self._sql("DELETE FROM {jobs} WHERE queue = %s AND state IN (0, 1)"), (queue,)).rowcount
            )
        )

    def dead_letters(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._run(
            lambda conn: conn.execute(
                self._sql(
                    "SELECT id, queue, task, reason, extract(epoch FROM died_at), payload FROM {dead} ORDER BY died_at DESC LIMIT %s"
                ),
                (limit,),
            ).fetchall()
        )
        return [
            {
                "id": r[0],
                "queue": r[1],
                "task": r[2],
                "reason": r[3],
                "died_at": float(r[4]),
                "message": serialization.loads(r[5]),
            }
            for r in rows
        ]

    def requeue_dead(self, task_id: str) -> bool:
        def _requeue(conn: psycopg.Connection) -> bool:
            with conn.transaction(), conn.cursor() as cur:
                row = cur.execute(
                    self._sql("DELETE FROM {dead} WHERE id = %s RETURNING payload"), (task_id,)
                ).fetchone()
                if row is None:
                    return False
                message = Message.decode(row[0])
                message.eta = None
                cur.execute(self._sql("DELETE FROM {results} WHERE id = %s"), (task_id,))
                self._insert(cur, [message])
                return True

        return self._run(_requeue)


class PostgresConsumer(Consumer):
    broker: PostgresBroker

    def __init__(self, broker: PostgresBroker, queues: list[str], worker_id: str, pid: int | None = None):
        super().__init__(broker, queues, worker_id, pid)
        self._conn: psycopg.Connection | None = None
        self._rotation = 0
        self._interrupted = False
        self._listening = False

    @property
    def conn(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed or self._conn.broken:
            self._conn = self.broker.connect()
            self._listening = False
        return self._conn

    def _listen(self, on: bool) -> None:
        """LISTEN only while idle. A connection that LISTENs but doesn't read (busy
        running a task, or the supervisor's settling connection) holds back the
        cluster-wide notification queue until NOTIFY starts failing everywhere."""
        if on == self._listening or not self.broker.notify:
            return
        if on:
            for queue in self.queues:
                self.conn.execute(pgsql.SQL("LISTEN {}").format(pgsql.Identifier(_channel(queue))))
        else:
            self.conn.execute("UNLISTEN *")
            for _ in self.conn.notifies(timeout=0):  # drop what was already delivered
                pass
        self._listening = on

    def _claim(self) -> Delivery | None:
        b = self.broker
        n = len(self.queues)
        order = [self.queues[(self._rotation + i) % n] for i in range(n)]
        self._rotation = (self._rotation + 1) % max(n, 1)
        sql = b._sql(
            "WITH picked AS MATERIALIZED (SELECT seq FROM {jobs} WHERE state = 1 AND queue = %s "
            "ORDER BY priority DESC, seq LIMIT 1 FOR UPDATE SKIP LOCKED) "
            "UPDATE {jobs} j SET state = 2, deliveries = j.deliveries + 1, worker = %s, pid = %s, token = %s, "
            "claimed_at = now() FROM picked WHERE j.seq = picked.seq RETURNING j.id, j.deliveries, j.payload"
        )
        conn = self.conn
        for queue in order:
            token = secrets.randbits(62)
            row = conn.execute(sql, (queue, self.worker_id, self.pid, token)).fetchone()
            if row is not None:
                job_id, deliveries, payload = row
                return Delivery(Message.decode(payload), delivery_count=deliveries, handle=(job_id, token))
        return None

    def fetch(self, timeout: float) -> Delivery | None:
        deadline = time.monotonic() + timeout
        self._interrupted = False
        while True:
            try:
                delivery = self._claim()
                if delivery is not None:
                    self._listen(False)
                    return delivery
                if self.broker.notify and not self._listening:
                    # Start listening, then look once more so a task enqueued in
                    # between can't be missed.
                    self._listen(True)
                    delivery = self._claim()
                    if delivery is not None:
                        self._listen(False)
                        return delivery
            except psycopg.OperationalError:
                self._reset()
                time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self._interrupted:
                return None
            wait = min(remaining, self.broker.poll_interval * random.uniform(0.9, 1.1))
            try:
                if self.broker.notify:
                    notified = False
                    for _ in self.conn.notifies(timeout=wait, stop_after=1):
                        notified = True
                    if not notified:
                        return None
                else:
                    time.sleep(wait)
            except psycopg.OperationalError:
                self._reset()
                return None

    def _reset(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = None
        self._listening = False

    def interrupt(self) -> None:
        self._interrupted = True

    def _write(self, fn: Any) -> Any:
        """Settling must not be lost to a dropped connection: retry on a new one."""
        for attempt in range(3):
            try:
                with self.conn.transaction(), self.conn.cursor() as cur:
                    return fn(cur)
            except psycopg.OperationalError:
                self._reset()
                if attempt == 2:
                    raise
                time.sleep(0.2 * (attempt + 1))

    def _delete_fenced(self, cur: Any, delivery: Delivery) -> bool:
        """Delete our claimed row. False if we no longer own it (it was recovered and
        maybe handed to another worker): then nothing else may be written either."""
        job_id, token = delivery.handle
        cur.execute(self.broker._sql("DELETE FROM {jobs} WHERE id = %s AND state = 2 AND token = %s"), (job_id, token))
        return cur.rowcount > 0

    def complete(self, delivery: Delivery, record: ResultRecord | None, followups: list[Message]) -> None:
        b = self.broker

        def _complete(cur: Any) -> None:
            if not self._delete_fenced(cur, delivery):
                return
            if record is not None:
                b._store(cur, record, b.app.conf.result_expires)
            if followups:
                b._insert(cur, followups)

        self._write(_complete)

    def retry(self, delivery: Delivery, message: Message, record: ResultRecord | None) -> None:
        b = self.broker
        job_id, token = delivery.handle
        now = time.time()
        scheduled = message.eta is not None and message.eta > now

        def _retry(cur: Any) -> None:
            cur.execute(
                b._sql(
                    "UPDATE {jobs} SET state = %s, run_at = coalesce(to_timestamp(%s), now()), queue = %s, priority = %s, "
                    "payload = %s, deliveries = 0, worker = NULL, pid = NULL, token = NULL WHERE id = %s AND state = 2 AND token = %s"
                ),
                (SCHEDULED if scheduled else READY, message.eta if scheduled else None, message.queue,
                 max(-32768, min(32767, message.priority)), message.encode(), job_id, token),
            )  # fmt: skip
            if cur.rowcount == 0:
                return
            if record is not None:
                b._store(cur, record, b.app.conf.result_expires)
            if not scheduled and b.notify:
                cur.execute("SELECT pg_notify(%s, '')", (_channel(message.queue),))

        self._write(_retry)

    def requeue(self, delivery: Delivery, count: bool = False) -> None:
        b = self.broker
        job_id, token = delivery.handle

        def _requeue(cur: Any) -> None:
            cur.execute(
                b._sql(
                    "UPDATE {jobs} SET state = 1, worker = NULL, pid = NULL, token = NULL, deliveries = deliveries - %s "
                    "WHERE id = %s AND state = 2 AND token = %s RETURNING queue"
                ),
                (0 if count else 1, job_id, token),
            )
            row = cur.fetchone()
            if row and b.notify:
                cur.execute("SELECT pg_notify(%s, '')", (_channel(row[0]),))

        self._write(_requeue)

    def dead_letter(
        self, delivery: Delivery, reason: str, record: ResultRecord | None, followups: list[Message] | None = None
    ) -> None:
        b = self.broker
        message = delivery.message

        def _dead(cur: Any) -> None:
            if not self._delete_fenced(cur, delivery):
                return
            cur.execute(
                b._sql(
                    "INSERT INTO {dead} (id, queue, task, reason, payload) VALUES (%s, %s, %s, %s, %s) "
                    "ON CONFLICT (id) DO UPDATE SET reason = EXCLUDED.reason, died_at = now(), payload = EXCLUDED.payload"
                ),
                (message.id, message.queue, message.task, reason, message.encode()),
            )
            if record is not None:
                b._store(cur, record, b.app.conf.result_expires)
            if followups:
                b._insert(cur, followups)

        self._write(_dead)

    def close(self) -> None:
        self._reset()
