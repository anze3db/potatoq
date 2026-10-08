"""Executor outcomes, settling and eager execution."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pytest

from potatoq import Potatoq, Task, chain, chord, group, states
from potatoq.canvas import Signature
from potatoq.exceptions import ChordError, Ignore, NotRegistered, WorkerTerminate
from potatoq.message import Message
from potatoq.testing import drain
from potatoq.worker import executor


@pytest.fixture
def add(memory_app):
    @memory_app.task
    def add(x, y):
        return x + y

    return add


@pytest.fixture
def tsum(memory_app):
    @memory_app.task
    def tsum(values):
        return sum(values)

    return tsum


def _run(app: Potatoq, message: Message, **kwargs) -> executor.Outcome:
    return executor.execute(app, message, hostname="test", **kwargs)


def test_in_task_body_only_while_the_task_runs(memory_app):
    seen = []

    @memory_app.task
    def probe():
        seen.append(executor.in_task_body())

    assert not executor.in_task_body()
    outcome = _run(memory_app, probe.build_message([], {}))
    assert outcome.state == states.SUCCESS
    assert seen == [True]
    assert not executor.in_task_body()


# --- signature_to_messages ------------------------------------------------------


def test_link_to_group_and_chord(memory_app, add, tsum):
    group_result = add.apply_async((1, 1), link=group(add.s(10), add.s(20)))
    chord_result = add.apply_async((2, 2), link=chord([add.s(1), add.s(2)], tsum.s()))
    drained = drain(memory_app)
    assert sorted(d.result for d in drained) == sorted([2, 12, 22, 4, 5, 6, 11])
    assert group_result.get() == 2 and chord_result.get() == 4
    callback = next(d for d in drained if d.name == tsum.name)
    assert callback.args == [[5, 6]]


def test_link_to_chord_with_empty_header(memory_app, add, tsum):
    add.apply_async((1, 1), link=chord([], tsum.s()))
    drained = drain(memory_app)
    assert [(d.name, d.args, d.result) for d in drained][1:] == [(tsum.name, [[]], 0)]


def test_signature_to_messages_for_unregistered_task(memory_app, add):
    sig = Signature("remote.task", (1,), app=memory_app, queue="remote")
    sig.link(add.s(1))
    sig.link_error({"task": "remote.errback", "args": [], "kwargs": {}, "options": {}})
    [msg] = executor.signature_to_messages(memory_app, sig, (0,), None)
    assert (msg.task, msg.args, msg.queue) == ("remote.task", [0, 1], "remote")
    assert [s["task"] for s in msg.link] == [add.name]
    assert [s["task"] for s in msg.link_error] == ["remote.errback"]
    assert msg.parent_id is None

    parent = add.build_message([1, 2], {})
    [child] = executor.signature_to_messages(memory_app, Signature("remote.task", app=memory_app), (), parent)
    assert (child.parent_id, child.root_id) == (parent.id, parent.root_id)
    assert child.queue == memory_app.conf.task_default_queue


def test_signature_to_messages_with_chain_and_parent(memory_app, add):
    parent = add.build_message([1, 2], {})
    [first] = executor.signature_to_messages(memory_app, chain(add.s(1), add.s(2)), (3,), parent)
    assert first.args == [3, 1]
    assert [s["task"] for s in first.link] == [add.name]
    assert first.parent_id == parent.id


# --- execute: early outcomes ----------------------------------------------------


def test_expired_message_is_revoked(memory_app, add):
    revoked = []

    def on_revoked(sender, **kw):
        revoked.append(kw["expired"])

    from potatoq import signals

    signals.task_revoked.connect(on_revoked)
    try:
        message = add.build_message([1, 2], {}, expires=-1)
        message.expires = 0.0
        outcome = _run(memory_app, message)
    finally:
        signals.task_revoked.disconnect(on_revoked)
    assert (outcome.action, outcome.state, outcome.reason) == (executor.COMPLETE, states.REVOKED, "expired")
    assert outcome.record.state == states.REVOKED
    assert revoked == [True]


def test_revoked_in_backend_is_not_run(memory_app):
    calls = []

    @memory_app.task
    def work():
        calls.append(1)

    message = work.build_message([], {})
    memory_app.store_result(message.id, states.REVOKED, None)
    outcome = _run(memory_app, message, delivery_count=2)
    assert (outcome.action, outcome.state, outcome.reason) == (executor.COMPLETE, states.REVOKED, "revoked")
    assert calls == []


def test_redelivered_success_rebuilds_link_and_chord_followups(memory_app, add, tsum):
    message = add.build_message(
        [1, 2],
        {},
        link=add.s(10),
        group_id="g-success",
        group_index=0,
        chord={"callback": tsum.s().to_dict(), "size": 1},
    )
    memory_app.store_result(message.id, states.SUCCESS, 3)
    outcome = _run(memory_app, message, delivery_count=2)
    assert (outcome.action, outcome.state, outcome.reason) == (executor.COMPLETE, states.IGNORED, "already finished")
    assert [(m.task, m.args) for m in outcome.followups] == [(add.name, [3, 10]), (tsum.name, [[3]])]


def test_redelivered_failure_rebuilds_errbacks(memory_app, add, tsum):
    @memory_app.task
    def errback(task_id):
        pass

    message = add.build_message(
        [1, 2],
        {},
        link_error=errback.s(),
        group_id="g-failure",
        group_index=0,
        chord={"callback": tsum.s().on_error(errback.s()).to_dict(), "size": 1},
    )
    memory_app.store_result(message.id, states.FAILURE, {"exc_type": "ValueError", "exc_message": ["boom"]}, "tb")
    outcome = _run(memory_app, message, delivery_count=2)
    assert (outcome.action, outcome.state, outcome.reason) == (executor.DEAD_LETTER, states.FAILURE, "tb")
    callback_id = outcome.followups[1].args[0]
    assert [(m.task, m.args) for m in outcome.followups] == [
        (errback.name, [message.id]),
        (errback.name, [callback_id]),
    ]
    with pytest.raises(ChordError, match="ValueError"):
        memory_app.AsyncResult(callback_id).get()

    # Without dead-lettering (and a result that isn't an exception dict).
    memory_app.conf.task_dead_letter_failures = False
    other = add.build_message([1, 2], {}, link_error=errback.s())
    memory_app.store_result(other.id, states.FAILURE, "not a dict")
    outcome = _run(memory_app, other, delivery_count=2)
    assert (outcome.action, outcome.state, outcome.reason) == (executor.COMPLETE, states.IGNORED, "already finished")
    assert [m.args for m in outcome.followups] == [[other.id]]


def test_redelivered_failure_without_traceback(memory_app, add):
    message = add.build_message([1, 2], {})
    memory_app.store_result(message.id, states.FAILURE, None)
    outcome = _run(memory_app, message, delivery_count=2)
    assert (outcome.action, outcome.reason) == (executor.DEAD_LETTER, "failed before redelivery")


def test_unregistered_task_without_results(memory_app):
    message = Message(task="nope.task", id="m1", ignore_result=True)
    outcome = _run(memory_app, message)
    assert (outcome.action, outcome.state, outcome.record) == (executor.DEAD_LETTER, states.FAILURE, None)
    assert isinstance(outcome.exc, NotRegistered)


# --- execute: task outcomes -----------------------------------------------------


def test_ignore(memory_app):
    @memory_app.task
    def skip():
        raise Ignore()

    outcome = _run(memory_app, skip.build_message([], {}))
    assert (outcome.action, outcome.state, outcome.record) == (executor.COMPLETE, states.IGNORED, None)


def test_worker_terminate_and_interrupts_propagate(memory_app):
    returns = []

    class Hooked(Task):
        def after_return(self, status, retval, task_id, args, kwargs, einfo):
            returns.append(status)

    @memory_app.task(base=Hooked)
    def terminate():
        raise WorkerTerminate()

    @memory_app.task(base=Hooked)
    def interrupt():
        raise KeyboardInterrupt()

    with pytest.raises(WorkerTerminate):
        _run(memory_app, terminate.build_message([], {}))
    with pytest.raises(KeyboardInterrupt):
        _run(memory_app, interrupt.build_message([], {}))
    outcome = _run(memory_app, interrupt.build_message([], {}), is_eager=True)
    assert outcome.state == states.FAILURE and isinstance(outcome.exc, KeyboardInterrupt)
    assert returns == [states.FAILURE] * 3
    assert terminate.request.id is None  # request stack popped


def test_failing_hooks_are_logged_not_raised(memory_app, caplog):
    einfos = []

    class Broken(Task):
        def on_success(self, retval, task_id, args, kwargs):
            raise RuntimeError("on_success")

        def on_retry(self, exc, task_id, args, kwargs, einfo):
            raise RuntimeError("on_retry")

        def on_failure(self, exc, task_id, args, kwargs, einfo):
            einfos.append(str(einfo))
            raise RuntimeError("on_failure")

        def after_return(self, status, retval, task_id, args, kwargs, einfo):
            raise RuntimeError("after_return")

    @memory_app.task(base=Broken, bind=True, max_retries=1)
    def flaky(self, fail):
        if fail == "retry":
            self.retry(countdown=0)
        if fail == "error":
            raise ValueError("task error")
        return "ok"

    caplog.set_level(logging.ERROR, logger="potatoq.worker")
    assert _run(memory_app, flaky.build_message(["none"], {})).state == states.SUCCESS
    assert _run(memory_app, flaky.build_message(["retry"], {})).state == states.RETRY
    assert _run(memory_app, flaky.build_message(["error"], {})).state == states.FAILURE
    messages = [r.getMessage() for r in caplog.records]
    for hook in ("on_success", "on_retry", "on_failure", "after_return"):
        assert any(f"{hook} handler of {flaky.name} failed" in m for m in messages), hook
    assert "ValueError: task error" in einfos[0]


def test_retry_options(memory_app):
    when = datetime.now(UTC) + timedelta(hours=1)

    @memory_app.task(bind=True)
    def moving(self):
        self.retry(args=[1], kwargs={"k": 2}, eta=when, queue="elsewhere", priority=7, max_retries=5)

    @memory_app.task(bind=True)
    def past(self):
        self.retry(eta=datetime.now(UTC) - timedelta(hours=1))

    outcome = _run(memory_app, moving.build_message([], {}))
    new = outcome.retry_message
    assert (new.args, new.kwargs, new.queue, new.priority) == ([1], {"k": 2}, "elsewhere", 7)
    assert new.eta == pytest.approx(when.timestamp())
    assert new.options["max_retries"] == 5 and new.retries == 1

    assert _run(memory_app, past.build_message([], {})).retry_message.eta is None


def test_autoretry_skips_dont_autoretry_for(memory_app):
    class Permanent(ValueError):
        pass

    @memory_app.task(autoretry_for=(ValueError,), dont_autoretry_for=(Permanent,), retry_kwargs={"max_retries": 3})
    def work(permanent):
        raise Permanent() if permanent else ValueError()

    assert _run(memory_app, work.build_message([True], {})).state == states.FAILURE
    assert _run(memory_app, work.build_message([False], {})).state == states.RETRY


# --- failure_outcome ------------------------------------------------------------


def test_failure_outcome_for_dead_process(memory_app, add, tsum, caplog):
    failures = []

    class Broken(Task):
        def on_failure(self, exc, task_id, args, kwargs, einfo):
            failures.append((exc, einfo.exception))
            raise RuntimeError("hook")

    @memory_app.task(base=Broken)
    def killed():
        pass

    @memory_app.task
    def errback(task_id):
        pass

    exc = MemoryError("oom")
    message = killed.build_message(
        [],
        {},
        link_error=errback.s(),
        group_id="g-dead",
        group_index=0,
        chord={"callback": tsum.s().to_dict(), "size": 1},
    )
    outcome = executor.failure_outcome(memory_app, message, exc, "host")
    assert (outcome.action, outcome.state, outcome.traceback) == (executor.COMPLETE, states.FAILURE, "MemoryError: oom")
    assert outcome.record.state == states.FAILURE and outcome.record.worker == "host"
    assert failures == [(exc, exc)]
    assert [m.task for m in outcome.followups] == [errback.name]  # chord callback failed instead
    assert "on_failure handler" in caplog.text


def test_failure_outcome_for_unknown_task_without_backend(caplog):
    app = Potatoq("nobackend", broker="memory://")
    app.conf.result_backend = "disabled"
    try:
        message = Message(task="gone.task", id="x", group_id="g", group_index=0, chord={"callback": {}, "size": 2})
        outcome = executor.failure_outcome(app, message, RuntimeError("died"))
        assert outcome.record is None and outcome.followups == []
        assert "Failed to record chord failure for x" in caplog.text
    finally:
        app.close()


# --- settle ---------------------------------------------------------------------


def test_settle_stores_results_in_a_separate_backend(tmp_path, add):
    app = Potatoq("separate", broker="memory://", set_as_current=True)
    app.conf.result_backend = f"sqlite:///{tmp_path}/results.db"
    try:

        @app.task
        def mul(x, y):
            return x * y

        result = mul.delay(3, 4)
        drain(app)
        assert app.backend is not app.broker
        assert app.backend.get_result(result.id).state == states.SUCCESS
        assert result.get(timeout=1) == 12
    finally:
        app.close()


def test_settle_retry_publishes_followups(memory_app, add):
    add.delay(1, 2)
    consumer = memory_app.broker.consumer([memory_app.conf.task_default_queue], "w")
    delivery = consumer.fetch(timeout=0)
    retry_message = Message.from_dict(delivery.message.to_dict())
    retry_message.retries = 1
    followup = add.build_message([5, 5], {})
    outcome = executor.Outcome(executor.RETRY, states.RETRY, retry_message=retry_message, followups=[followup])
    executor.settle(memory_app, consumer, delivery, outcome)
    drained = drain(memory_app)
    assert sorted((d.id, d.result) for d in drained) == sorted([(retry_message.id, 3), (followup.id, 10)])


def test_settle_requeue(memory_app, add):
    add.delay(1, 2)
    consumer = memory_app.broker.consumer([memory_app.conf.task_default_queue], "w")
    delivery = consumer.fetch(timeout=0)
    executor.settle(memory_app, consumer, delivery, executor.Outcome(executor.REQUEUE, states.REJECTED))
    again = consumer.fetch(timeout=0)
    assert again.message.id == delivery.message.id and again.delivery_count == 2


# --- eager execution ------------------------------------------------------------


def test_execute_eagerly_follows_retries_links_and_stores_results(memory_app, add):
    attempts = []

    @memory_app.task(bind=True, max_retries=3)
    def flaky(self, x):
        attempts.append(self.request.retries)
        if len(attempts) < 3:
            self.retry(countdown=60)
        return x

    memory_app.conf.task_store_eager_result = True
    result = flaky.apply((5,), link=add.s(1))
    assert result.get() == 5 and attempts == [0, 1, 2]
    assert memory_app.backend.get_result(result.id).state == states.SUCCESS
    link_results = [r for r in memory_app.broker.results if r != result.id]
    assert [memory_app.backend.get_result(r).result for r in link_results] == [6]


def test_execute_eagerly_without_throw_returns_failure(memory_app):
    @memory_app.task
    def boom():
        raise ValueError("eager")

    result = boom.apply(throw=False)
    assert result.state == states.FAILURE and isinstance(result.result, ValueError)
    with pytest.raises(ValueError):
        boom.apply(throw=True)


def test_settle_refuses_unknown_actions(memory_app, add):
    add.delay(1, 2)
    consumer = memory_app.broker.consumer([memory_app.conf.task_default_queue], "w")
    delivery = consumer.fetch(timeout=0)
    with pytest.raises(ValueError, match="Unknown outcome bogus"):
        executor.settle(memory_app, consumer, delivery, executor.Outcome("bogus", states.SUCCESS))
