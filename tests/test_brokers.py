"""Contract tests every broker must pass (memory, SQLite, Postgres, Redis, RabbitMQ)."""

from __future__ import annotations

import time

import pytest

from potatoq import states
from potatoq.brokers.base import ResultRecord
from potatoq.message import Message
from potatoq.worker import executor


def msg(app, task="t.add", args=(1, 2), **kwargs) -> Message:
    return Message(task=task, args=list(args), queue=app.conf.task_default_queue, **kwargs)


def consumer(app, queues=None):
    return app.broker.consumer(queues or [app.conf.task_default_queue], "test-worker")


def fetch(c, timeout=5.0):
    return c.fetch(timeout=timeout)


def test_enqueue_fetch_complete(broker_app):
    app = broker_app
    m = msg(app)
    app.broker.enqueue([m])
    c = consumer(app)
    d = fetch(c)
    assert d is not None
    assert d.message.id == m.id
    assert d.message.args == [1, 2]
    assert d.delivery_count == 1
    record = ResultRecord(task_id=m.id, state=states.SUCCESS, result=3)
    if app.backend is c.broker:
        c.complete(d, record, [])
    else:
        app.backend.store_result(record, 60)
        c.complete(d, None, [])
    assert app.backend.get_result(m.id).result == 3
    assert fetch(c, 0.3) is None
    c.close()


def test_fifo_and_priority(broker_app):
    app = broker_app
    if app.kind == "rabbitmq":
        pytest.skip("RabbitMQ prioritises within a prefetch window only")
    low = [msg(app, args=(i,)) for i in range(3)]
    high = msg(app, args=("high",), priority=5)
    app.broker.enqueue(low)
    app.broker.enqueue([high])
    c = consumer(app)
    order = []
    for _ in range(4):
        d = fetch(c)
        order.append(d.message.args[0])
        c.complete(d, None, [])
    assert order == ["high", 0, 1, 2]
    c.close()


def test_eta_is_held_by_broker(broker_app):
    app = broker_app
    m = msg(app, eta=time.time() + 1.2)
    app.broker.enqueue([m])
    c = consumer(app)
    assert fetch(c, 0.3) is None
    deadline = time.time() + 6
    d = None
    while d is None and time.time() < deadline:
        app.broker.tick()
        d = fetch(c, 0.5)
    assert d is not None and d.message.id == m.id
    assert time.time() >= m.eta - 0.05
    c.complete(d, None, [])
    c.close()


def test_retry_replaces_message(broker_app):
    app = broker_app
    m = msg(app)
    app.broker.enqueue([m])
    c = consumer(app)
    d = fetch(c)
    new = Message.from_dict(m.to_dict())
    new.retries = 1
    c.retry(d, new, None)
    d2 = fetch(c)
    assert d2.message.id == m.id
    assert d2.message.retries == 1
    assert d2.delivery_count == 1  # a retry is not a redelivery
    c.complete(d2, None, [])
    c.close()


def test_requeue_counts_deliveries(broker_app):
    app = broker_app
    m = msg(app)
    app.broker.enqueue([m])
    c = consumer(app)
    d = fetch(c)
    c.requeue(d, count=True)
    d2 = fetch(c)
    assert d2.message.id == m.id
    assert d2.delivery_count == 2
    c.requeue(d2, count=False)
    d3 = fetch(c)
    if app.kind != "rabbitmq":  # RabbitMQ < 4.3 counts nacks too
        assert d3.delivery_count == 2
    c.complete(d3, None, [])
    c.close()


def test_dead_letter_and_replay(broker_app):
    app = broker_app
    m = msg(app)
    app.broker.enqueue([m])
    c = consumer(app)
    d = fetch(c)
    c.dead_letter(d, "boom", None)
    assert fetch(c, 0.3) is None
    dead = app.broker.dead_letters()
    assert [e["id"] for e in dead] == [m.id]
    assert "boom" in dead[0]["reason"]
    assert app.broker.requeue_dead(m.id)
    d2 = fetch(c)
    assert d2.message.id == m.id
    c.complete(d2, None, [])
    c.close()


def test_followups_are_enqueued_on_complete(broker_app):
    app = broker_app
    m = msg(app)
    follow = msg(app, args=(9,))
    app.broker.enqueue([m])
    c = consumer(app)
    d = fetch(c)
    c.complete(d, None, [follow])
    d2 = fetch(c)
    assert d2.message.id == follow.id
    c.complete(d2, None, [])
    c.close()


def test_chord_counter_completes_once(broker_app):
    app = broker_app
    backend = app.backend if app.kind == "rabbitmq" else app.broker
    assert backend.chord_part_done("g1", 1, 3, "b") is None
    assert backend.chord_part_done("g1", 1, 3, "b") is None  # duplicate delivery
    assert backend.chord_part_done("g1", 0, 3, "a") is None
    assert backend.chord_part_done("g1", 2, 3, {"c": 1}) == ["a", "b", {"c": 1}]
    # A part redelivered after completion (its worker died before acking) gets the
    # results again, so the callback can't be lost; the callback's fixed id dedupes it.
    assert backend.chord_part_done("g1", 2, 3, {"c": 1}) == ["a", "b", {"c": 1}]


def test_periodic_dedup(broker_app):
    app = broker_app
    if app.kind == "rabbitmq":  # the leader token arrives a moment after connecting
        deadline = time.monotonic() + 10
        while not app.broker._leader_poll() and time.monotonic() < deadline:
            time.sleep(0.05)
    first = app.broker.enqueue_periodic("every-minute", 1700000000.0, msg(app))
    second = app.broker.enqueue_periodic("every-minute", 1700000000.0, msg(app))
    assert first is True
    assert second is False
    c = consumer(app)
    d = fetch(c)
    c.complete(d, None, [])
    assert fetch(c, 0.3) is None
    c.close()


def test_last_periodic_runs(broker_app):
    from datetime import UTC, datetime

    app = broker_app
    now = time.time()
    app.broker.enqueue_periodic("hourly", now - 7200, msg(app))
    app.broker.enqueue_periodic("hourly", now - 3600, msg(app))
    app.broker.enqueue_periodic("daily", now - 60, msg(app))
    if app.broker.durable_periodic_claims:
        assert app.control.last_periodic_runs() == {
            "daily": datetime.fromtimestamp(now - 60, UTC),
            "hourly": datetime.fromtimestamp(now - 3600, UTC),
        }
    else:  # RabbitMQ: only the leader's memory knows
        assert app.control.last_periodic_runs() == {}


def test_revoke_waiting_task(broker_app):
    app = broker_app
    m = msg(app)
    app.broker.enqueue([m])
    app.control.revoke(m.id)
    c = consumer(app)
    d = fetch(c, 1.0)
    if d is not None:  # RabbitMQ can't delete messages; the executor skips it
        assert app.kind == "rabbitmq"
        outcome = executor.execute(app, d.message, delivery_count=d.delivery_count)
        assert outcome.state == states.REVOKED
        executor.settle(app, c, d, outcome)
    assert app.AsyncResult(m.id).state == states.REVOKED
    c.close()


def test_results_roundtrip_and_wait(broker_app):
    app = broker_app
    backend = app.backend
    record = ResultRecord(task_id="r1", state=states.SUCCESS, result={"x": [1, 2]})
    backend.store_result(record, 60)
    assert backend.get_result("r1").result == {"x": [1, 2]}
    assert backend.wait_for_result("r1", 1).state == states.SUCCESS
    assert backend.wait_for_result("missing", 0.2) is None
    backend.forget("r1")
    assert backend.get_result("r1") is None


def test_queue_sizes_and_purge(broker_app):
    app = broker_app
    app.broker.enqueue([msg(app) for _ in range(3)])
    time.sleep(0.2 if app.kind == "rabbitmq" else 0)
    assert app.broker.queue_sizes().get(app.conf.task_default_queue) == 3
    assert app.broker.purge(app.conf.task_default_queue) == 3
    assert not app.broker.queue_sizes().get(app.conf.task_default_queue)


def test_stale_claim_cannot_ack(broker_app):
    """A worker whose claim was recovered must not settle the task's new delivery,
    nor write results / follow-ups / dead letters for it."""
    app = broker_app
    if app.kind in ("rabbitmq", "memory"):
        pytest.skip("delivery tags are per channel")
    m = msg(app)
    app.broker.enqueue([m])
    c = consumer(app)
    d = fetch(c)
    c.requeue(d, count=True)  # as if recovered
    d2 = fetch(c)
    stale_record = ResultRecord(task_id=m.id, state=states.FAILURE, result="stale")
    c.complete(d, stale_record if app.backend is c.broker else None, [msg(app, args=("stale",))])  # ignored
    c.dead_letter(d, "stale", None)  # ignored
    assert app.broker.dead_letters() == []
    if app.backend is c.broker:
        assert app.backend.get_result(m.id).state == states.STARTED
    c.complete(d2, None, [])
    assert fetch(c, 0.3) is None
    c.close()


def test_recover_dead_node(broker_app):
    app = broker_app
    if app.kind in ("rabbitmq", "memory"):
        pytest.skip("RabbitMQ redelivers on connection loss by itself")
    app.conf.worker_dead_after = 0.5
    m = msg(app)
    app.broker.enqueue([m])
    c = app.broker.consumer([app.conf.task_default_queue], "dead-node")
    app.broker.heartbeat("dead-node", {})
    d = fetch(c)
    assert d is not None
    time.sleep(1.2)
    assert app.broker.recover(0.5) == []
    c2 = consumer(app)
    d2 = fetch(c2)
    assert d2 is not None and d2.message.id == m.id
    assert d2.delivery_count == 2
    c2.complete(d2, None, [])
    c.close()
    c2.close()


def test_task_started_state_visible(broker_app):
    app = broker_app
    if app.kind == "rabbitmq":
        pytest.skip("needs a database or Redis broker")
    m = msg(app)
    app.broker.enqueue([m])
    c = consumer(app)
    d = fetch(c)
    assert app.AsyncResult(m.id).state == states.STARTED
    c.complete(d, None, [])
    c.close()
