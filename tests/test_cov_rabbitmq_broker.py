"""RabbitMQ broker and consumer: URLs, topology, failure handling, inspection."""

from __future__ import annotations

import logging
import time
import uuid
from types import SimpleNamespace

import pytest
from conftest import make_app, rabbitmq_available

pika = pytest.importorskip("pika")

from pika.exceptions import AMQPConnectionError, StreamLostError  # noqa: E402

from potatoq import Potatoq, states  # noqa: E402
from potatoq.brokers import rabbitmq  # noqa: E402
from potatoq.brokers.base import Delivery, ResultRecord  # noqa: E402
from potatoq.brokers.rabbitmq import RabbitMQBroker, _check_queue_name, _params, delay_route  # noqa: E402
from potatoq.exceptions import ImproperlyConfigured  # noqa: E402
from potatoq.message import Message  # noqa: E402

RAW_URL = "amqp://guest:guest@localhost:5672/%2F"


def offline_broker(url: str = "amqp://guest:guest@localhost:5672//", **conf) -> RabbitMQBroker:
    """A broker object that hasn't connected (``app.broker`` would declare queues)."""
    app = Potatoq(f"offline-{uuid.uuid4().hex[:6]}", broker=url, set_as_current=False)
    for key, value in conf.items():
        app.conf[key] = value
    app._broker = RabbitMQBroker(url, app)
    return app._broker


# --- no server needed -------------------------------------------------------------


def test_url_parsing():
    params = _params("pyamqp://ann:secret@rabbit.example:5673//?heartbeat=30", 5)
    assert (params.host, params.port, params.virtual_host) == ("rabbit.example", 5673, "/")
    assert (params.credentials.username, params.credentials.password) == ("ann", "secret")
    assert params.heartbeat == 30
    assert params.socket_timeout == 5
    assert params.blocked_connection_timeout == 30.0
    assert _params("amqp://localhost/", 60).virtual_host == "/"
    assert _params("amqp://localhost/", 60).blocked_connection_timeout == 60
    assert _params("amqp://localhost/shop?heartbeat=5", 1).virtual_host == "shop"
    assert _params("amqps://localhost/%2Fshop", 1).virtual_host == "/shop"
    assert _params("amqp://localhost", 1).virtual_host == "/"


def test_queue_names_must_be_routing_key_words():
    for bad in ("a.b", "a#", "a*b"):
        with pytest.raises(ImproperlyConfigured, match="can't contain"):
            _check_queue_name(bad)
    _check_queue_name("emails-high_1")


def test_delay_route():
    zeros = ["0"] * 28

    def bits(*ones):
        b = list(zeros)
        for i in ones:
            b[27 - i] = "1"
        return ".".join(b)

    assert delay_route(5, "q") == ("potatoq.delay.L02", bits(2, 0) + ".q")
    assert delay_route(1, "q") == ("potatoq.delay.L00", bits(0) + ".q")
    assert delay_route(0, "q") == delay_route(1, "q")  # at least a second
    longest = delay_route(10**12, "q")  # clamped to the 28-level cascade
    assert longest == ("potatoq.delay.L27", bits(*range(28)) + ".q")


def test_results_and_coordination_need_a_result_backend():
    broker = offline_broker()
    assert broker.app.backend is None
    with pytest.raises(ImproperlyConfigured, match="can't store results"):
        broker.store_result(ResultRecord(task_id="x"), None)
    with pytest.raises(ImproperlyConfigured, match="can't store results"):
        broker.get_result("x")
    with pytest.raises(ImproperlyConfigured, match="Chords need a result backend"):
        broker.chord_part_done("g", 0, 2, 1)
    with pytest.raises(ImproperlyConfigured, match="Revoking tasks"):
        broker.revoke(["x"], 60)
    broker.heartbeat("w1", {"pid": 1})  # nowhere to record it: ignored
    broker.unregister("w1")
    assert broker.workers() == []


class Exploding:
    is_open = True

    def close(self):
        raise StreamLostError("gone")


def test_broker_close_tolerates_dead_connections():
    broker = offline_broker()
    broker._local.channel = SimpleNamespace(connection=Exploding())
    broker._leader = (Exploding(), None)
    broker.close()
    assert broker._leader is None
    assert getattr(broker._local, "channel", None) is None


def test_scheduler_leadership_survives_connection_failures(monkeypatch, caplog):
    broker = offline_broker()

    def refuse(params):
        raise AMQPConnectionError("refused")

    monkeypatch.setattr(rabbitmq.pika, "BlockingConnection", refuse)
    with caplog.at_level(logging.WARNING, logger="potatoq.rabbitmq"):
        assert broker._leader_poll() is False
    assert "Lost scheduler leadership connection" in caplog.text
    assert broker.enqueue_periodic("p", 1.0, Message(task="t")) is False

    class Lost:
        def process_data_events(self, time_limit):
            raise StreamLostError("lost")

    broker._leader = (Lost(), None)
    broker._holding_token = True
    broker.tick()  # polls the held token: the connection is gone, so is leadership
    assert broker._leader is None and broker._holding_token is False
    broker.tick()  # no leader connection: nothing to do


def test_fetch_when_rabbitmq_is_unreachable(caplog):
    broker = offline_broker("amqp://guest:guest@127.0.0.1:1//", broker_connection_timeout=0.5)
    consumer = broker.consumer(["default"], "w")
    with caplog.at_level(logging.WARNING, logger="potatoq.rabbitmq"):
        assert consumer.fetch(0.1) is None
    assert "Can't connect to RabbitMQ" in caplog.text
    assert consumer._broken
    with pytest.raises(AMQPConnectionError, match="consumer connection is down"):
        consumer.requeue(Delivery(Message(task="t"), handle=1))
    consumer.close()  # never connected: nothing to close


# --- against a RabbitMQ server ----------------------------------------------------------


@pytest.fixture
def rmq(tmp_path):
    if not rabbitmq_available():
        pytest.skip("RabbitMQ not available")
    app, cleanup = make_app("rabbitmq", tmp_path)
    app.extra_queues = []  # type: ignore[attr-defined]
    try:
        yield app
    finally:
        app.close()
        cleanup()
        delete_queues(*app.extra_queues)  # type: ignore[attr-defined]


def delete_queues(*names):
    conn = pika.BlockingConnection(pika.URLParameters(RAW_URL))
    ch = conn.channel()
    for name in names:
        for q in (name, f"{name}.dlq"):
            try:
                ch.queue_delete(q)
            except Exception:
                ch = conn.channel()
    conn.close()


def extra_queue(app, label):
    name = f"{label}{app.test_token}"
    app.extra_queues.append(name)
    return name


def msg(app, queue=None, **kwargs):
    kwargs.setdefault("args", [1, 2])
    return Message(task="t.add", queue=queue or app.conf.task_default_queue, **kwargs)


def wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def fetch_settled(consumer, timeout=5.0):
    delivery = consumer.fetch(timeout)
    assert delivery is not None
    consumer.complete(delivery, None, [])
    return delivery


def test_coordination_through_the_result_backend(rmq):
    broker = rmq.broker
    broker.heartbeat("node-1", {"pid": 7})
    assert [w["id"] for w in broker.workers()] == ["node-1"]
    broker.unregister("node-1")
    assert broker.workers() == []
    group = uuid.uuid4().hex
    assert broker.chord_part_done(group, 0, 2, "a") is None
    assert broker.chord_part_done(group, 1, 2, "b") == ["a", "b"]
    broker.revoke(["t1", "t2"], 60)
    assert rmq.backend.get_result("t2").state == states.REVOKED


def test_declare_queue_with_different_arguments(rmq):
    name = extra_queue(rmq, "classic")
    conn = pika.BlockingConnection(pika.URLParameters(RAW_URL))
    conn.channel().queue_declare(name, durable=True)  # what Celery would have created
    conn.close()
    broker = rmq.broker
    with pytest.raises(ImproperlyConfigured, match="already exists with different arguments"):
        broker.enqueue([msg(rmq, name)])
    # The channel the broker closed was replaced; the broker still works.
    broker.enqueue([msg(rmq)])
    assert broker.queue_sizes() == {rmq.conf.task_default_queue: 1}


def test_publishing_to_a_deleted_queue_redeclares_it(rmq):
    broker = rmq.broker
    queue = rmq.conf.task_default_queue
    broker.enqueue([msg(rmq)])
    assert queue in broker._declared
    delete_queues(queue)  # e.g. deleted by an operator while we were running
    broker.enqueue([msg(rmq)])  # unroutable: declared again, then published
    assert broker.queue_sizes() == {queue: 1}


def test_enqueue_reconnects_once_after_connection_loss(rmq, monkeypatch):
    broker = rmq.broker
    broker.setup()
    first_channel = broker._channel()
    real = broker.publish_on
    failures = [StreamLostError("connection reset")]

    def flaky(ch, message, headers=None):
        if failures:
            raise failures.pop()
        real(ch, message, headers)

    monkeypatch.setattr(broker, "publish_on", flaky)
    broker.enqueue([msg(rmq)])
    assert broker._channel() is not first_channel
    assert broker.queue_sizes() == {rmq.conf.task_default_queue: 1}
    first_channel.connection.close()

    failures[:] = [StreamLostError("again"), StreamLostError("still down")]
    with pytest.raises(StreamLostError, match="again"):
        broker.enqueue([msg(rmq)])


def test_channel_is_replaced_in_a_forked_child(rmq):
    broker = rmq.broker
    parent_channel = broker._channel()
    broker._pid = -1  # as if this were a forked child
    try:
        child_channel = broker._channel()
        assert child_channel is not parent_channel
        assert rabbitmq._INHERITED[-1][0].channel is parent_channel  # kept, never closed
        broker.enqueue([msg(rmq)])
        assert broker.queue_sizes() == {rmq.conf.task_default_queue: 1}
    finally:
        rabbitmq._INHERITED.pop()
        parent_channel.connection.close()


def test_periodic_dedup_memory_is_bounded(rmq, monkeypatch):
    broker = rmq.broker
    monkeypatch.setattr(broker, "_leader_poll", lambda: True)
    broker._fired = {(f"old{i}", float(i)): None for i in range(10_000)}
    assert broker.enqueue_periodic("p", 1.0, msg(rmq)) is True
    assert broker.enqueue_periodic("p", 1.0, msg(rmq)) is False  # same schedule slot
    assert len(broker._fired) == 10_000
    assert ("old0", 0.0) not in broker._fired and ("p", 1.0) in broker._fired
    assert broker.queue_sizes() == {rmq.conf.task_default_queue: 1}


def test_inspection_covers_routed_queues(rmq):
    routed = extra_queue(rmq, "routed")
    never = extra_queue(rmq, "never")  # routed to, but never declared
    rmq.conf.task_routes = {"t.a": routed, "t.b": {"queue": never}, "t.c": {"priority": 1}}
    broker = rmq.broker
    broker.enqueue([msg(rmq), msg(rmq, routed), msg(rmq, routed)])
    assert broker._known_queues() == {rmq.conf.task_default_queue, routed, never}
    assert broker.queue_sizes() == {rmq.conf.task_default_queue: 1, routed: 2}
    assert broker.dead_letters() == []  # never.dlq doesn't exist: skipped


def test_undecodable_messages_are_dead_lettered(rmq, caplog):
    broker = rmq.broker
    queue = rmq.conf.task_default_queue
    broker.setup()
    ch = broker._channel()
    ch.basic_publish("", queue, b"\xff not json", pika.BasicProperties(delivery_mode=2))
    ch.basic_publish("", queue, b'["json", "but not a task"]', pika.BasicProperties(delivery_mode=2))
    good = msg(rmq)
    broker.enqueue([good])
    consumer = broker.consumer([queue], "w")
    try:
        with caplog.at_level(logging.ERROR, logger="potatoq.rabbitmq"):
            delivery = consumer.fetch(5)
        assert delivery.message.id == good.id
        assert caplog.text.count("Undecodable message") == 2
        consumer.complete(delivery, None, [])
    finally:
        consumer.close()
    dead = broker.dead_letters()
    assert sorted(e["message"]["body"] for e in dead) == ['["json", "but not a task"]', "� not json"]
    assert {e["reason"] for e in dead} == {"rejected"}  # from RabbitMQ's x-death header
    assert broker.requeue_dead("no-such-task") is False


def test_requeue_one_of_several_dead_letters(rmq):
    broker = rmq.broker
    first, second, third = msg(rmq), msg(rmq), msg(rmq)
    broker.enqueue([first, second, third])
    consumer = broker.consumer([rmq.conf.task_default_queue], "w")
    try:
        for _ in range(3):
            consumer.dead_letter(consumer.fetch(5), "boom", None)
        assert broker.requeue_dead(second.id) is True
        assert consumer.fetch(5).message.id == second.id
        assert sorted(e["id"] for e in broker.dead_letters()) == sorted([first.id, third.id])
    finally:
        consumer.close()


def test_dead_letter_publishes_followups(rmq):
    broker = rmq.broker
    m, followup = msg(rmq), msg(rmq, args=["errback"])
    broker.enqueue([m])
    consumer = broker.consumer([rmq.conf.task_default_queue], "w")
    try:
        consumer.dead_letter(consumer.fetch(5), "boom", None, followups=[followup])
        assert fetch_settled(consumer).message.id == followup.id
        assert [e["id"] for e in broker.dead_letters()] == [m.id]
    finally:
        consumer.close()


def test_delayed_messages_share_the_delay_topology(rmq):
    broker = rmq.broker
    eta = time.time() + 1.2
    a, b = msg(rmq, eta=eta), msg(rmq, eta=eta)
    broker.enqueue([a, b])  # the cascade is declared once per channel
    consumer = broker.consumer([rmq.conf.task_default_queue], "w")
    try:
        assert consumer.fetch(0.3) is None
        got = {fetch_settled(consumer, 8).message.id, fetch_settled(consumer, 8).message.id}
        assert got == {a.id, b.id}
        assert time.time() >= eta - 0.05
    finally:
        consumer.close()


def test_consumer_falls_back_when_arguments_are_rejected(rmq, monkeypatch):
    consumer = rmq.broker.consumer([rmq.conf.task_default_queue], "w")
    # Like an older server rejecting x-consumer-timeout: the channel is closed with 406.
    monkeypatch.setattr(consumer, "_consume_arguments", lambda: {"x-priority": "not a number"})
    rmq.broker.enqueue([m := msg(rmq)])
    try:
        assert fetch_settled(consumer).message.id == m.id
    finally:
        consumer.close()


def test_consumer_reconnects_after_losing_its_connection(rmq, caplog):
    broker = rmq.broker
    m = msg(rmq)
    broker.enqueue([m])
    consumer = broker.consumer([rmq.conf.task_default_queue], "w")
    try:
        delivery = consumer.fetch(5)
        assert delivery.delivery_count == 1
        with caplog.at_level(logging.WARNING, logger="potatoq.rabbitmq"):
            consumer._call(consumer._conn.close)  # the connection drops mid-task
            assert wait_for(lambda: consumer._broken)
        assert "RabbitMQ consumer connection lost" in caplog.text
        with pytest.raises(AMQPConnectionError, match="consumer connection is down"):
            consumer.complete(delivery, None, [])  # the unacked message went back to the queue
        redelivered = fetch_settled(consumer)
        assert redelivered.message.id == m.id
        assert redelivered.delivery_count == 2
    finally:
        consumer.close()


def test_io_thread_errors_and_timeouts_reach_the_caller(rmq):
    broker = rmq.broker
    m = msg(rmq)
    broker.enqueue([m])
    consumer = broker.consumer([rmq.conf.task_default_queue], "w")
    try:
        delivery = consumer.fetch(5)
        bad = Message(task="t", queue="bad.queue")
        with pytest.raises(ImproperlyConfigured, match="can't contain"):
            consumer.complete(delivery, None, [bad])  # raised on the I/O thread
        consumer.call_timeout = 0.05
        with pytest.raises(AMQPConnectionError, match="timed out"):
            consumer._call(lambda: time.sleep(0.3))
        consumer.call_timeout = 60
        consumer.complete(delivery, None, [])  # nothing was acked before: still ours
        assert consumer.fetch(0.3) is None
    finally:
        consumer.close()


def test_multi_queue_consumer_pauses_other_queues(rmq):
    broker = rmq.broker
    q1, q2 = rmq.conf.task_default_queue, f"other{rmq.test_token}"
    a, b = msg(rmq, q1), msg(rmq, q2)
    broker.enqueue([a, b])
    consumer = broker.consumer([q1, q2], "w")
    try:
        consumer._ensure_started()
        assert wait_for(lambda: consumer._inbox.qsize() == 2)  # one prefetched per queue
        first = consumer.fetch(5)
        # The other queue's consumer was cancelled and its prefetched message given back.
        other_queue = q2 if first.message.queue == q1 else q1
        assert consumer._paused == {other_queue}
        assert consumer._inbox.qsize() == 0
        assert wait_for(lambda: broker.queue_sizes() == {other_queue: 1})
        consumer.complete(first, None, [])
        second = consumer.fetch(5)  # resumes every queue first
        assert {first.message.id, second.message.id} == {a.id, b.id}
        consumer.complete(second, None, [])
    finally:
        consumer.close()


def test_pausing_other_queues_is_best_effort(rmq, monkeypatch):
    broker = rmq.broker
    q1, q2 = rmq.conf.task_default_queue, f"other{rmq.test_token}"
    broker.enqueue([m := msg(rmq, q2)])
    consumer = broker.consumer([q1, q2], "w")

    def fail(keep):
        raise StreamLostError("lost")

    monkeypatch.setattr(consumer, "_pause_others", fail)
    try:
        assert fetch_settled(consumer).message.id == m.id
    finally:
        consumer.close()


def test_close_gives_back_prefetched_messages(rmq):
    broker = rmq.broker
    m = msg(rmq)
    broker.enqueue([m])
    consumer = broker.consumer([rmq.conf.task_default_queue], "w")
    consumer._ensure_started()
    assert wait_for(lambda: consumer._inbox.qsize() == 1)
    consumer.close()
    assert consumer._conn.is_closed
    other = broker.consumer([rmq.conf.task_default_queue], "w2")
    try:
        assert fetch_settled(other).message.id == m.id
    finally:
        other.close()


def test_close_with_a_dead_channel_still_closes_the_connection(rmq):
    broker = rmq.broker
    broker.enqueue([m := msg(rmq)])
    consumer = broker.consumer([rmq.conf.task_default_queue], "w")
    consumer._ensure_started()
    assert wait_for(lambda: consumer._inbox.qsize() == 1)
    consumer._call(consumer._ch.close)  # e.g. closed by the broker after a channel error
    consumer.close()  # the nack fails; the connection is closed anyway
    assert consumer._conn.is_closed
    other = broker.consumer([rmq.conf.task_default_queue], "w2")
    try:
        assert fetch_settled(other).message.id == m.id
    finally:
        other.close()
