"""Broker registry, the base ``Broker`` defaults and the in-memory broker's edge cases."""

from __future__ import annotations

import time

import pytest

from potatoq import Potatoq, states
from potatoq.brokers import base, broker_for_url
from potatoq.brokers.base import Broker, Consumer, Delivery, ResultRecord
from potatoq.brokers.memory import MemoryBroker
from potatoq.brokers.sqlite import SQLiteBroker
from potatoq.exceptions import ImproperlyConfigured
from potatoq.message import Message

# --- registry -------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("memory://", MemoryBroker),
        ("memory", MemoryBroker),
        ("SQLITE:///x.db", SQLiteBroker),
        ("sqla+sqlite:///x.db", SQLiteBroker),
    ],
)
def test_broker_for_url(url, expected):
    assert broker_for_url(url) is expected


def test_broker_for_url_unsupported_scheme():
    with pytest.raises(ImproperlyConfigured, match=r"Unsupported broker URL 'ftp://host'"):
        broker_for_url("ftp://host")


def test_broker_for_url_missing_extra(monkeypatch):
    import potatoq.brokers as registry

    def fail(name, package=None):
        raise ImportError("No module named 'redis'")

    monkeypatch.setattr(registry, "import_module", fail)
    with pytest.raises(ImproperlyConfigured, match=r"pip install 'potatoq\[redis\]'") as info:
        broker_for_url("valkey://localhost")
    assert isinstance(info.value.__cause__, ImportError)


# --- base defaults ----------------------------------------------------------------------


class _DictBroker(Broker):
    """Implements only results, to exercise the base class's defaults."""

    def __init__(self, url, app, **options):
        super().__init__(url, app, **options)
        self.records: dict[str, ResultRecord] = {}

    def get_result(self, task_id):
        return self.records.get(task_id)


def test_base_broker_defaults(memory_app):
    b = _DictBroker("dict://", memory_app, opt=1)
    assert b.options == {"opt": 1}
    b.setup()
    b.after_fork()
    b.unregister("w")
    b.extend([])
    b.tick()
    b.maintenance()
    b.close()
    assert b.peek("x") is None
    assert b.recover(1.0) == []
    assert b.lost_deliveries("w", 1) == []
    assert b.workers() == []
    assert b.queue_sizes() == {}
    assert b.dead_letters() == []
    for call in (
        lambda: b.enqueue([]),
        lambda: b.consumer([], "w"),
        lambda: b.enqueue_periodic("p", 1.0, Message("t")),
        lambda: b.store_result(ResultRecord("x"), None),
        lambda: b.forget("x"),
        lambda: b.heartbeat("w", {}),
        lambda: b.chord_part_done("g", 0, 1, None),
        lambda: b.revoke([], 1.0),
        lambda: b.purge("q"),
    ):
        with pytest.raises(NotImplementedError):
            call()


def test_base_consumer_defaults(memory_app):
    c = Consumer(memory_app.broker, ["q"], "w")
    assert c.pid > 0 and c.can_settle_foreign
    c.interrupt()
    c.close()
    d = Delivery(Message("t"))
    for call in (
        lambda: c.fetch(0),
        lambda: c.complete(d, None, []),
        lambda: c.retry(d, d.message, None),
        lambda: c.requeue(d),
        lambda: c.dead_letter(d, "x", None),
    ):
        with pytest.raises(NotImplementedError):
            call()


def test_base_wait_for_result_polls(memory_app, monkeypatch):
    b = _DictBroker("dict://", memory_app)
    b.records["t"] = ResultRecord("t", states.STARTED)
    # Times out with the unfinished record.
    start = time.monotonic()
    assert b.wait_for_result("t", 0.05).state == states.STARTED
    assert time.monotonic() - start < 1
    # No timeout: polls until the record is ready.
    calls = []

    def get_result(task_id):
        calls.append(task_id)
        if len(calls) == 3:
            return ResultRecord(task_id, states.SUCCESS, result=1)
        return None

    monkeypatch.setattr(b, "get_result", get_result)
    assert b.wait_for_result("t", None).result == 1
    assert len(calls) == 3


def test_result_record_roundtrip():
    record = ResultRecord("t", states.FAILURE, result="x", traceback="tb", retries=2, worker="w")
    assert ResultRecord.from_dict(record.to_dict()) == record
    assert record.ready
    assert not ResultRecord("t").ready
    assert base.ResultRecord.from_dict({"task_id": "t"}).state == states.PENDING


# --- memory broker ----------------------------------------------------------------------


@pytest.fixture
def mem():
    app = Potatoq("memcov", broker="memory://", set_as_current=False)
    app.conf.result_backend = "broker"
    yield app, app.broker
    app.close()


def test_memory_enqueue_dedupes_ids(mem):
    _, b = mem
    m = Message("t", queue="q")
    b.enqueue([m])
    b.enqueue([Message.from_dict(m.to_dict())])
    assert b.queue_sizes() == {"q": 1}


def test_memory_peek_states(mem):
    _, b = mem
    later = Message("t", queue="q", eta=time.time() + 60)
    now = Message("t", queue="q")
    b.enqueue([later, now])
    assert b.peek("missing") is None
    assert b.peek(later.id)[1] == "scheduled"
    message, state = b.peek(now.id)
    assert (message.id, state) == (now.id, "ready")
    c = b.consumer(["q"], "w")
    d = c.fetch(1)
    assert b.peek(d.message.id)[1] == "running"
    assert b.get_result(d.message.id).state == states.STARTED


def test_memory_workers(mem):
    _, b = mem
    b.heartbeat("w2", {"pid": 2})
    b.heartbeat("w1", {"pid": 1})
    assert [(w["id"], w["pid"]) for w in b.workers()] == [("w1", 1), ("w2", 2)]
    assert all("heartbeat" in w for w in b.workers())
    b.unregister("w1")
    b.unregister("never-registered")
    assert [w["id"] for w in b.workers()] == ["w2"]


def test_memory_requeue_dead_unknown(mem):
    _, b = mem
    assert b.requeue_dead("nope") is False


def test_memory_interrupt_wakes_fetch(mem):
    _, b = mem
    c = b.consumer(["q"], "w")

    class Interrupting:
        """A lock wrapper whose wait() simulates a signal handler interrupting fetch."""

        def __init__(self, lock):
            self.lock = lock

        def __enter__(self):
            return self.lock.__enter__()

        def __exit__(self, *exc):
            return self.lock.__exit__(*exc)

        def wait(self, timeout):
            c.interrupt()

    b.lock = Interrupting(b.lock)
    start = time.monotonic()
    assert c.fetch(10) is None
    assert time.monotonic() - start < 1


def test_memory_stale_deliveries_are_ignored(mem):
    _, b = mem
    m = Message("t", queue="q")
    b.enqueue([m])
    c = b.consumer(["q"], "w")
    stale = c.fetch(1)
    c.requeue(stale, count=True)  # as if recovered
    current = c.fetch(1)
    assert current.delivery_count == 2
    # The stale delivery can't settle the current one.
    c.complete(stale, ResultRecord(m.id, states.SUCCESS), [Message("t", queue="q")])
    c.requeue(stale)
    c.dead_letter(stale, "stale", None)
    assert b.dead_letters() == []
    assert b.peek(m.id)[1] == "running"
    assert b.queue_sizes() == {}
    # Unknown job (already gone) is ignored too.
    c.complete(current, None, [])
    c.complete(current, None, [])
    assert b.peek(m.id) is None


def test_memory_retry_and_dead_letter_store_results(mem):
    _, b = mem
    m = Message("t", queue="q")
    b.enqueue([m])
    c = b.consumer(["q"], "w")
    d = c.fetch(1)
    retried = Message.from_dict(m.to_dict())
    retried.retries = 1
    c.retry(d, retried, ResultRecord(m.id, states.RETRY))
    assert b.get_result(m.id).state == states.RETRY
    d = c.fetch(1)
    assert d.message.retries == 1
    follow = Message("t", queue="q")
    c.dead_letter(d, "boom", ResultRecord(m.id, states.FAILURE, result="boom"), [follow])
    assert b.get_result(m.id).state == states.FAILURE
    assert [e["reason"] for e in b.dead_letters()] == ["boom"]
    assert c.fetch(1).message.id == follow.id
