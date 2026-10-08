"""Redis broker edge cases: URLs, setup checks, waiting, recovery, maintenance, errors."""

from __future__ import annotations

import logging
import threading
import time

import pytest
import redis
from conftest import make_app, redis_available

from potatoq import Potatoq, states
from potatoq.brokers import redis as redis_broker
from potatoq.brokers.base import Delivery, ResultRecord
from potatoq.brokers.redis import RedisBroker, _client_from_url, _s
from potatoq.message import Message

needs_redis = pytest.mark.skipif(not redis_available(), reason="Redis not available")


@pytest.fixture
def app(tmp_path):
    app, cleanup = make_app("redis", tmp_path)
    try:
        yield app
    finally:
        app.close()
        cleanup()


def q(app):
    return app.conf.task_default_queue


# --- no server needed -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "connection_class", "kwargs"),
    [
        ("valkey://h:1/2", redis.Connection, {"host": "h", "port": 1, "db": 2}),
        ("valkeys://h:1/0", redis.SSLConnection, {"host": "h", "port": 1}),
        ("rediss://h/0", redis.SSLConnection, {"host": "h"}),
        ("redis+socket:///tmp/r.sock", redis.UnixDomainSocketConnection, {"path": "/tmp/r.sock"}),
        ("unix:///tmp/r.sock?db=3", redis.UnixDomainSocketConnection, {"path": "/tmp/r.sock", "db": 3}),
    ],
)
def test_client_from_url(url, connection_class, kwargs):
    client = _client_from_url(url, 2.0)
    pool = client.connection_pool
    assert pool.connection_class is connection_class
    assert kwargs.items() <= pool.connection_kwargs.items()
    assert pool.connection_kwargs["socket_connect_timeout"] == 2.0
    assert pool.connection_kwargs["socket_timeout"] == 10.0


def test_client_options_are_passed_through():
    client = _client_from_url(
        "redis://h/0", 30.0, socket_timeout=5, max_connections=7, username="u", password="p", ignored=1
    )
    kwargs = client.connection_pool.connection_kwargs
    assert (kwargs["socket_timeout"], kwargs["username"], kwargs["password"]) == (5, "u", "p")
    assert kwargs["socket_connect_timeout"] == 30.0
    assert "ignored" not in kwargs
    assert client.connection_pool.max_connections == 7


def test_s():
    assert _s(b"x") == "x"
    assert _s("x") == "x"
    assert _s(1) == "1"


def test_prefix_option():
    app = Potatoq("rediscov", set_as_current=False)
    assert RedisBroker("redis://h", app).prefix == "potatoq"
    assert RedisBroker("redis://h", app, prefix="a:").prefix == "a"
    assert RedisBroker("redis://h", app, global_keyprefix="b", prefix="a").prefix == "b"


def test_fetch_and_settle_when_redis_is_unreachable(caplog, monkeypatch):
    app = Potatoq("rediscov", set_as_current=False)
    app.conf.broker_connection_timeout = 0.5
    b = RedisBroker("redis://127.0.0.1:1/0", app)
    c = b.consumer(["q"], "w")
    with caplog.at_level(logging.WARNING, "potatoq.redis"):
        start = time.monotonic()
        assert c.fetch(0.2) is None
        assert time.monotonic() - start < 2
    assert "Redis connection problem while fetching" in caplog.text

    class Clock:
        """The broker module's ``time``, recording sleeps instead of sleeping."""

        monotonic = staticmethod(time.monotonic)
        time = staticmethod(time.time)

        def __init__(self):
            self.sleeps = []

        def sleep(self, seconds):
            self.sleeps.append(seconds)

    clock = Clock()
    monkeypatch.setattr(redis_broker, "time", clock)
    # Settling retries twice, then gives up.
    with pytest.raises(redis.ConnectionError):
        c.requeue(Delivery(Message("t", queue="q"), handle="token"))
    assert clock.sleeps == [0.2, 0.4]
    b.close()


# --- against Redis ----------------------------------------------------------------------


@needs_redis
def test_settling_survives_a_transient_connection_error(app, monkeypatch):
    b = app.broker
    m = Message("t", queue=q(app))
    b.enqueue([m])
    c = b.consumer([q(app)], "w")
    d = c.fetch(2)
    real = b.run
    calls = []

    def flaky(*args, **kwargs):
        calls.append(args[0])
        if len(calls) == 1:
            raise redis.ConnectionError("Connection reset by peer")
        return real(*args, **kwargs)

    monkeypatch.setattr(b, "run", flaky)
    c.complete(d, ResultRecord(m.id, states.SUCCESS, result=1), [])
    assert calls == ["complete", "complete"]
    assert b.get_result(m.id).result == 1


@needs_redis
def test_new_client_after_fork(app):
    b = app.broker
    inherited = b.client
    b._pid = -1
    assert b.client is not inherited
    assert b._pid > 0
    b.client.ping()
    b._pid = -1
    b.close()  # leaves the parent's pool alone
    assert b._client is None


@needs_redis
@pytest.mark.parametrize(("policy", "warns"), [("allkeys-lru", True), ("noeviction", False), (None, False)])
def test_setup_checks_maxmemory_policy(app, monkeypatch, caplog, policy, warns):
    b = app.broker
    monkeypatch.setattr(b.client, "config_get", lambda name: {name: policy} if policy else {})
    with caplog.at_level(logging.WARNING, "potatoq.redis"):
        b.setup()
    assert ("may silently evict" in caplog.text) is warns


@needs_redis
def test_setup_tolerates_disabled_config_command(app, monkeypatch, caplog):
    b = app.broker

    def config_get(name):
        raise redis.ResponseError("unknown command 'CONFIG'")

    monkeypatch.setattr(b.client, "config_get", config_get)
    with caplog.at_level(logging.WARNING, "potatoq.redis"):
        b.setup()
    assert caplog.text == ""


@needs_redis
def test_enqueue_nothing(app):
    app.broker.enqueue([])
    assert app.broker.queue_sizes() == {}


@needs_redis
def test_result_without_expiry(app):
    b = app.broker
    b.store_result(ResultRecord("r", states.SUCCESS, result=1), None)
    assert b.client.pttl(f"{b.prefix}:result:r") == -1
    assert b.get_result("r").result == 1


@needs_redis
def test_wait_for_result_is_woken_by_store_result(app):
    b = app.broker
    b.store_result(ResultRecord("r", states.STARTED), 60)  # not ready: nobody is notified
    timer = threading.Timer(0.3, b.store_result, [ResultRecord("r", states.SUCCESS, result=2), 60])
    start = time.monotonic()
    timer.start()
    record = b.wait_for_result("r", None)
    timer.join()
    assert record.result == 2
    assert time.monotonic() - start < 1.5  # woken up, not the 2s poll
    assert not b.client.exists(f"{b.prefix}:waiting:r")
    # Timing out returns what is there.
    b.store_result(ResultRecord("s", states.STARTED), 60)
    assert b.wait_for_result("s", 0.1).state == states.STARTED


@needs_redis
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
    c.dead_letter(d, "boom", None)
    assert b.peek(ready.id) is None  # dead letters aren't pending


@needs_redis
def test_recover_dead_letters_exhausted_tasks(app):
    app.conf.task_max_deliveries = 1
    app.conf.worker_dead_after = 0.2
    b = app.broker
    m = Message("t.add", queue=q(app))
    b.enqueue([m])
    c = b.consumer([q(app)], "dead-node")
    assert c.fetch(2) is not None
    time.sleep(0.4)  # the lease expires
    recovered = b.recover(0.2)
    assert [(d.message.id, d.delivery_count) for d in recovered] == [(m.id, 1)]
    assert [(e["id"], e["reason"], e["task"], e["queue"]) for e in b.dead_letters()] == [
        (m.id, "worker lost too many times", "t.add", q(app))
    ]


@needs_redis
def test_recover_forgets_long_dead_workers(app):
    b = app.broker
    b.heartbeat("alive", {"pid": 1})
    b.client.hset(f"{b.prefix}:workers", "gone", '{"heartbeat": 1}')
    assert {w["id"] for w in b.workers()} == {"alive", "gone"}
    assert b.recover(60) == []
    assert [w["id"] for w in b.workers()] == ["alive"]


@needs_redis
def test_maintenance_trims_dead_letters(app):
    app.conf.dead_letter_max = 2
    app.conf.dead_letter_ttl = 3600
    b = app.broker
    now_ms = int(time.time() * 1000)
    for i, age in enumerate([7200, 30, 20, 10]):
        died = now_ms - age * 1000
        b.client.hset(
            f"{b.prefix}:job:d{i}",
            mapping={"q": "q", "p": Message("t", queue="q").encode(), "st": "dead", "r": "r", "died": died},
        )
        b.client.zadd(f"{b.prefix}:dead", {f"d{i}": died})
    # A dead-letter entry whose job is gone is skipped when listing.
    b.client.zadd(f"{b.prefix}:dead", {"orphan": now_ms - 25_000})
    assert [e["id"] for e in b.dead_letters()] == ["d3", "d2", "d1", "d0"]
    b.maintenance()
    assert [e["id"] for e in b.dead_letters()] == ["d3", "d2"]
    assert not b.client.exists(f"{b.prefix}:job:d0", f"{b.prefix}:job:d1")
    assert b.client.zcard(f"{b.prefix}:dead") == 2


@needs_redis
def test_unregister(app):
    b = app.broker
    b.heartbeat("w1", {"pid": 1})
    b.heartbeat("w2", {"pid": 2})
    b.unregister("w1")
    assert [(w["id"], w["pid"]) for w in b.workers()] == [("w2", 2)]


@needs_redis
def test_extend_keeps_running_tasks_from_being_recovered(app):
    app.conf.worker_dead_after = 1.0
    b = app.broker
    m = Message("t", queue=q(app))
    b.enqueue([m])
    c = b.consumer([q(app)], "w")
    d = c.fetch(2)
    b.extend([])  # nothing running: no-op
    for _ in range(3):
        time.sleep(0.4)
        b.extend([d])
    assert b.recover(1.0) == []
    assert b.peek(m.id)[1] == "running"  # 1.2s in, but the lease was extended
    time.sleep(1.2)
    assert b.recover(1.0) == []
    assert b.peek(m.id)[1] == "ready"
    stale = Delivery(d.message, handle=d.handle)
    d2 = c.fetch(2)
    assert d2.delivery_count == 2
    b.extend([stale])  # a lost claim isn't extended (nor resurrected)
    c.complete(d2, None, [])


@needs_redis
def test_interrupt_wakes_fetch(app):
    c = app.broker.consumer([q(app)], "w")
    timer = threading.Timer(0.2, c.interrupt)
    start = time.monotonic()
    timer.start()
    assert c.fetch(10) is None
    timer.join()
    assert time.monotonic() - start < 3
