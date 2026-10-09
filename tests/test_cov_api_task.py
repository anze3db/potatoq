"""The ``Task`` class: options, publishing helpers, signatures, retry and replace."""

from __future__ import annotations

import asyncio
import pickle
import time
from datetime import timedelta

import pytest

from potatoq import Task, states
from potatoq.canvas import group
from potatoq.exceptions import Replace, Retry
from potatoq.message import Message
from potatoq.task import Context, run_coroutine
from potatoq.testing import drain


@pytest.fixture
def add(memory_app):
    @memory_app.task
    def add(x, y):
        return x + y

    return add


def test_context_helpers():
    ctx = Context(id="abc", retries=2)
    assert ctx.correlation_id == "abc"
    assert ctx.get("retries") == 2 and ctx.get("missing", "dflt") == "dflt"
    assert repr(ctx) == "<Context: {'id': 'abc', 'retries': 2}>"


# --- run_coroutine ------------------------------------------------------------------


def test_run_coroutine_propagates_exceptions():
    async def fails():
        raise ValueError("nope")

    with pytest.raises(ValueError, match="nope"):
        run_coroutine(fails())


class _Interrupt(SystemExit):
    """Stands in for a signal handler (hard time limit) interrupting the event loop."""


def test_run_coroutine_cancels_the_task_when_the_loop_is_interrupted():
    cancelled = []

    def interrupt():
        raise _Interrupt()

    async def slow():
        asyncio.get_running_loop().call_soon(interrupt)
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    with pytest.raises(_Interrupt):
        run_coroutine(slow())
    assert cancelled == [True]

    async def ok():
        return "loop still usable"

    assert run_coroutine(ok()) == "loop still usable"


# --- option resolution ----------------------------------------------------------------


def test_resolved_max_retries(memory_app):
    @memory_app.task(retry_kwargs={"max_retries": 7})
    def from_retry_kwargs():
        pass

    @memory_app.task
    def from_app():
        pass

    @memory_app.task(max_retries=2, retry_kwargs={"max_retries": 7})
    def explicit():
        pass

    memory_app.conf.task_max_retries = 9
    assert from_retry_kwargs.resolved_max_retries() == 7
    assert from_app.resolved_max_retries() == 9
    assert explicit.resolved_max_retries() == 2


def test_resolved_time_limits(memory_app):
    @memory_app.task(time_limit=timedelta(seconds=10), soft_time_limit=timedelta(seconds=20))
    def soft_above_hard():
        pass

    assert soft_above_hard.resolved_time_limits() == (10.0, None)
    assert soft_above_hard.resolved_time_limits({"soft_time_limit": 5}) == (10.0, 5)
    memory_app.conf.task_time_limit = None
    memory_app.conf.task_soft_time_limit = 3

    @memory_app.task
    def defaults():
        pass

    assert defaults.resolved_time_limits() == (None, 3)


# --- publishing ---------------------------------------------------------------------


def test_build_message_eta_and_countdown(memory_app, add):
    with pytest.raises(ValueError, match="either countdown or eta"):
        add.apply_async((1, 2), countdown=1, eta=timedelta(seconds=1))
    before = time.time()
    message = add.build_message([1, 2], {}, eta=timedelta(seconds=60))
    assert message.eta is not None and message.eta >= before + 60
    message = add.build_message([1, 2], {}, countdown=timedelta(seconds=30))
    assert message.eta is not None and message.eta >= before + 30
    assert add.build_message([1, 2], {}, countdown=-5).eta is None


def test_signature_check_skipped_when_run_has_no_signature(memory_app):
    class NoSignature:
        __signature__ = "not a signature"

        def __call__(self, *args, **kwargs):
            return args

    class Opaque(Task):
        name = "cov.opaque"
        run = NoSignature()

    task = memory_app.register_task(Opaque)
    result = task.delay(1, 2, 3)  # can't be checked, so it isn't
    drain(memory_app)
    assert result.get() == [1, 2, 3]


def test_publishing_aliases(memory_app, add):
    add.delay_on_commit(1, 1)
    add.apply_async_on_commit((2, 2), countdown=60)
    r3 = add.enqueue(3, 3)
    r4 = add.send(4, 4)
    r5 = asyncio.run(add.adelay(5, 5))
    r6 = asyncio.run(add.aapply_async((6, 6), priority=3))
    assert memory_app.broker.peek(r6.id)[0].priority == 3
    drained = drain(memory_app)
    assert sorted(d.result for d in drained) == [2, 4, 6, 8, 10, 12]
    assert [r.get() for r in (r3, r4, r5, r6)] == [6, 8, 10, 12]


def test_publishing_on_commit_goes_through_transaction_hooks(memory_app, add):
    deferred = []

    class Hook:
        def publish(self, app, messages, using, on_commit=True):
            deferred.extend((m.args, on_commit) for m in messages)
            return True

    memory_app.add_transaction_hook(Hook())
    add.delay_on_commit(1, 1)
    add.apply_async_on_commit((2, 2))
    add.apply_async((3, 3), enqueue_on_commit=False)  # hooks see it too (Django: its connection)
    assert deferred == [([1, 1], True), ([2, 2], True), ([3, 3], False)]
    assert memory_app.broker.queue_sizes() == {}


# --- signatures ---------------------------------------------------------------------


def test_signature_variants(add):
    sig = add.signature((1,), kwargs={"y": 2}, options={"queue": "q"}, countdown=3)
    assert sig.args == (1,) and sig.kwargs == {"y": 2}
    assert sig.options == {"queue": "q", "countdown": 3}
    star = add.subtask(1, 2)
    assert star.args == (1, 2) and star.kwargs == {} and star.options == {}


def test_map_starmap_chunks(memory_app, add):
    @memory_app.task
    def double(x):
        return x * 2

    mapped = double.map([1, 2, 3])
    assert isinstance(mapped, group) and len(mapped) == 3
    starmapped = add.starmap([(1, 2), (3, 4)])
    chunked = add.chunks([(1, 2), (3, 4), (5, 6)], 2)
    assert len(chunked) == 2

    results = [mapped.delay(), starmapped.delay(), chunked.delay()]
    drain(memory_app)
    assert [r.get() for r in results] == [[2, 4, 6], [3, 7], [[3, 7], [11]]]


# --- retry --------------------------------------------------------------------------


def test_retry_when_called_directly(memory_app):
    @memory_app.task(bind=True)
    def direct(self, with_exc):
        if with_exc:
            raise self.retry(exc=KeyError("original"))
        raise self.retry()

    with pytest.raises(KeyError, match="original"):
        direct(True)
    with pytest.raises(Retry, match="Task can be retried"):  # like Celery
        direct(False)


def test_retry_without_throw_returns_retry_with_backoff_delay(memory_app):
    @memory_app.task(retry_backoff=False, default_retry_delay=7)
    def fixed():
        pass

    fixed.push_request(id="tid", called_directly=False, retries=0)
    try:
        retry = fixed.retry(args=(1,), kwargs={"a": 1}, throw=False, queue="other")
    finally:
        fixed.pop_request()
    assert isinstance(retry, Retry)
    assert retry.when == 7.0
    assert retry.args_override == (1,) and retry.kwargs_override == {"a": 1}  # type: ignore[attr-defined]
    assert retry.options == {"queue": "other"}  # type: ignore[attr-defined]


def test_backoff_delay_falls_back_to_app_default_retry_delay(memory_app):
    memory_app.conf.task_retry_backoff = 0
    memory_app.conf.task_default_retry_delay = 42

    @memory_app.task
    def plain():
        pass

    assert plain.backoff_delay(3) == 42.0


# --- replace / state ----------------------------------------------------------------


def test_replace_requires_a_running_task_and_a_signature(memory_app, add):
    with pytest.raises(RuntimeError, match="inside a running task"):
        add.replace(add.s(1, 2))
    add.push_request(id="tid", called_directly=False, message=Message(task=add.name))
    try:
        with pytest.raises(TypeError, match="needs a signature"):
            add.replace(None)
    finally:
        add.pop_request()


def test_replace_inherits_errbacks_and_chord(memory_app, add):
    errback = add.s(0, 0).to_dict()
    chord = {"callback": add.s(1).to_dict(), "size": 2}
    message = Message(task=add.name, link_error=[errback], chord=chord, group_id="g", group_index=1)
    add.push_request(id="tid", called_directly=False, message=message)
    try:
        with pytest.raises(Replace) as info:
            add.replace(add.s(5, 5).on_error(add.s(9, 9)))
    finally:
        add.pop_request()
    sig = info.value.sig
    assert sig.options["task_id"] == "tid"
    assert [list(e["args"]) for e in sig.options["link_error"]] == [[9, 9], [0, 0]]
    assert sig.options["chord"] == chord
    assert sig.options["group_id"] == "g" and sig.options["group_index"] == 1


def test_update_state_and_backend(memory_app, add):
    assert add.backend is memory_app.backend
    add.update_state(state="PROGRESS", meta={"done": 1})  # no current task: ignored
    add.update_state(task_id="explicit", state="PROGRESS", meta={"done": 2})
    result = add.AsyncResult("explicit")
    assert result.state == "PROGRESS" and result.info == {"done": 2}
    add.update_state(task_id="started")
    assert add.AsyncResult("started").state == states.STARTED


def test_tasks_pickle_by_name(memory_app, add):
    assert pickle.loads(pickle.dumps(add)) is add
