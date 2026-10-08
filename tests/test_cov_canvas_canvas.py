"""Canvas primitives: signatures, chains, groups and chords (eager and via ``drain``)."""

from __future__ import annotations

import pickle

import pytest

from potatoq import chain, chord, group, signature, states
from potatoq.canvas import Signature, maybe_signature, xmap, xstarmap
from potatoq.exceptions import ChordError
from potatoq.result import EagerResult, GroupResult
from potatoq.testing import drain
from potatoq.worker import executor


@pytest.fixture
def tasks(memory_app):
    @memory_app.task
    def add(x, y):
        return x + y

    @memory_app.task
    def tsum(values):
        return sum(values)

    @memory_app.task
    def double(x):
        return x * 2

    return add, tsum, double


# --- signatures ---------------------------------------------------------------


def test_signature_construction_and_calling(memory_app, tasks):
    add, _, _ = tasks
    raw = {"task": add.name, "args": [1], "kwargs": {"y": 2}, "options": {}, "subtask_type": None, "immutable": False}
    sig = Signature(raw, app=memory_app)
    assert dict(sig) == raw
    assert sig.type is add
    assert sig() == 3  # called directly, like the task itself

    from_task = Signature(add, (2, 3))  # app taken from the task
    assert from_task.app is memory_app
    assert from_task.task == add.name
    assert from_task.apply().get() == 5
    assert add.s(2).apply((10,)).get() == 12  # partial args are prepended


def test_signature_for_unregistered_task_is_sent_by_name(memory_app):
    result = Signature("elsewhere.task", (1,), app=memory_app).apply_async()
    drained = drain(memory_app)
    assert [(d.id, d.name, d.args) for d in drained] == [(result.id, "elsewhere.task", [1])]
    assert drained[0].state == states.FAILURE  # not registered in this process


def test_clone_set_and_freeze(memory_app, tasks):
    add, _, _ = tasks
    sig = add.s(1)
    clone = sig.clone((5,), {"z": 1}, countdown=3)
    assert clone.args == (5, 1)
    assert clone.kwargs == {"z": 1}
    assert clone.options == {"countdown": 3}
    assert sig.args == (1,) and sig.kwargs == {} and sig.options == {}

    immutable = add.si(1, 2).clone((9,), {"z": 1})
    assert immutable.args == (1, 2) and immutable.kwargs == {}

    assert sig.set(immutable=True, queue="q") is sig
    assert sig.immutable and sig.options == {"queue": "q"}
    sig.set_immutable(False)
    assert not sig.immutable

    res = sig.freeze("fixed-id", group_id="g1")
    assert res.id == "fixed-id" == sig.id
    assert sig.options["group_id"] == "g1"
    assert sig.freeze().id == "fixed-id"  # idempotent


def test_link_error_and_on_error(memory_app, tasks):
    add, tsum, _ = tasks
    sig = add.s(1, 2)
    assert sig.link_error(tsum.s()) == tsum.s()
    assert sig.on_error({"task": add.name, "args": [], "kwargs": {}, "options": {}}) is sig
    assert [s.task for s in sig.options["link_error"]] == [tsum.name, add.name]


def test_or_operator(memory_app, tasks):
    add, tsum, double = tasks
    c = add.s(1, 1) | chain(double.s(), double.s())
    assert isinstance(c, chain)
    assert [t.task for t in c.tasks] == [add.name, double.name, double.name]

    g = group(add.s(1, 1), add.s(2, 2))
    c2 = double.s(1) | g
    assert isinstance(c2, chain) and isinstance(c2.tasks[1], group)
    assert isinstance(g | tsum.s(), chord)
    with pytest.raises(TypeError):
        add.s() | 5

    joined = chain(add.s(1, 1), double.s()) | chain(double.s(), double.s())
    assert len(joined.tasks) == 4
    assert [t.task for t in (chain(add.s(1, 1)) | double.s()).tasks] == [add.name, double.name]
    with pytest.raises(TypeError):
        chain(add.s(1, 1)) | 5


def test_pickling_and_repr(memory_app, tasks):
    add, tsum, double = tasks
    sig = add.s(1, y=2)
    restored = pickle.loads(pickle.dumps(sig))
    assert type(restored) is Signature and restored == sig
    assert restored.app is memory_app  # resolved through the current app
    assert repr(sig) == f"{add.name}(1, y=2)"
    assert repr(add.si(1, 2)) == f"{add.name}(1, 2)!"

    c = chain(add.s(1, 1), double.s())
    restored_chain = pickle.loads(pickle.dumps(c))
    assert isinstance(restored_chain, chain)
    assert repr(restored_chain) == f"{add.name}(1, 1) | {double.name}()"
    assert repr(group(double.s(1), double.s(2))) == f"group([{double.name}(1), {double.name}(2)])"
    assert repr(chord([double.s(1)], tsum.s())) == f"chord([{double.name}(1)], {tsum.name}())"


def test_signature_and_maybe_signature_helpers(memory_app, tasks):
    add, _, _ = tasks
    sig = add.s(1, 2)
    copy = signature(sig)
    assert copy == sig and copy is not sig
    from_dict = signature(sig.to_dict(), app=memory_app)
    assert type(from_dict) is Signature and from_dict.to_dict() == sig.to_dict()
    by_name = signature(add.name, (1, 2), app=memory_app)
    assert by_name == sig and by_name.app is memory_app

    assert maybe_signature(None) is None
    assert maybe_signature(sig) is sig
    assert maybe_signature(add) == add.s()
    with pytest.raises(TypeError, match="Expected a signature"):
        maybe_signature(42)


def test_xmap_and_xstarmap(memory_app, tasks):
    add, _, double = tasks
    assert [t.args for t in xmap(double, [1, 2])] == [(1,), (2,)]
    assert [t.args for t in xstarmap(add, [(1, 2), (3, 4)])] == [(1, 2), (3, 4)]
    result = xstarmap(add, [(1, 2), (3, 4)]).apply_async()
    drain(memory_app)
    assert result.get() == [3, 7]


# --- chains -------------------------------------------------------------------


def test_chain_flattens_lists_and_takes_partial_args(memory_app, tasks):
    add, _, double = tasks
    c = chain(add.s(1), [double.s(), (double.s(),)])
    assert len(c.tasks) == 3
    result = c.apply_async((10,))
    drain(memory_app)
    assert result.get() == 44


def test_empty_chain_is_refused(memory_app):
    with pytest.raises(ValueError, match="Empty chain"):
        chain(app=memory_app).apply_async()


def test_chain_with_group_followed_by_task_becomes_chord(memory_app, tasks):
    add, tsum, double = tasks
    result = chain(add.s(1, 1), group(add.s(1), add.s(2)), tsum.s(), double.s()).apply_async()
    drained = drain(memory_app)
    assert result.get() == 14  # (2+1 + 2+2) * 2
    assert len(drained) == 5


def test_chain_ending_in_group_or_chord(memory_app, tasks):
    add, tsum, _ = tasks
    first = chain(add.s(1, 1), group(add.s(1), add.s(2))).apply_async()
    drain(memory_app)
    assert first.get() == [3, 4]  # the group's results

    result = chain(add.s(1, 1), chord([add.s(10), add.s(20)], tsum.s())).apply_async()
    drain(memory_app)
    assert result.get() == 34


def test_nested_chain_runs_in_order(memory_app, tasks):
    add, _, _ = tasks
    result = chain(add.s(1, 1), chain(add.s(10), add.s(100)), add.s(1000)).apply_async()
    drained = drain(memory_app)
    assert [d.result for d in drained] == [2, 12, 112, 1112]
    assert result.get() == 1112


def test_chain_clone_gives_partial_args_to_the_first_step(memory_app, tasks):
    add, _, double = tasks
    c = chain(add.s(1), double.s())
    clone = c.clone((5,), queue="q")
    assert isinstance(clone, chain)
    assert [t.args for t in clone.tasks] == [(5, 1), ()]
    assert clone.options == {"queue": "q"}
    assert [t.args for t in c.tasks] == [(1,), ()] and c.options == {}


def test_chain_as_link_callback_receives_parent_result(memory_app, tasks):
    add, _, double = tasks
    first = add.apply_async((1, 2), link=chain(add.s(10), double.s()))
    drained = drain(memory_app)
    assert first.get() == 3
    assert [d.result for d in drained] == [3, 13, 26]


def test_chain_eager(memory_app, tasks):
    add, tsum, double = tasks
    memory_app.conf.task_always_eager = True
    c = chain(add.s(1), group(add.s(1), add.s(2)), tsum.s(), chain(double.s(), double.s()))
    result = c.apply_async((1,))
    assert isinstance(result, EagerResult)
    assert result.get() == 28  # (2+1 + 2+2) * 4
    assert c.apply((2,)).get() == 36


# --- groups -------------------------------------------------------------------


def test_group_protocol_and_partial_args(memory_app, tasks):
    add, _, _ = tasks
    g = group([add.s(1), add.s(2)])
    assert len(g) == 2
    assert [t.args for t in g] == [(1,), (2,)]
    assert [t.args for t in g.clone((10,)).tasks] == [(10, 1), (10, 2)]
    assert g.id is None
    frozen = g.freeze()
    assert g.id == frozen.id

    result = g(10)  # __call__ == apply_async with partial args
    assert isinstance(result, GroupResult)
    drain(memory_app)
    assert result.get() == [11, 12]


def test_group_link_applies_to_every_member(memory_app, tasks):
    add, _, double = tasks
    g = group(add.s(1, 1), add.s(2, 2))
    assert g.link(double.s()) == double.s()
    for member in g.tasks:
        assert member.options["link"] == [double.s()]


def test_group_eager_and_apply(memory_app, tasks):
    add, _, _ = tasks
    g = group(add.s(1), add.s(2))
    applied = g.apply((5,))
    assert isinstance(applied, GroupResult)
    assert applied.get() == [6, 7]
    memory_app.conf.task_always_eager = True
    assert g.apply_async((1,)).get() == [2, 3]
    assert memory_app.broker.queue_sizes() == {}


def test_empty_group_publishes_nothing(memory_app):
    result = group([], app=memory_app).apply_async()
    assert result.results == []
    assert memory_app.broker.queue_sizes() == {}


# --- chords -------------------------------------------------------------------


def test_chord_round_trips_through_dicts(memory_app, tasks):
    add, tsum, _ = tasks
    c = chord([add.s(1, 1)], tsum.s())
    restored = Signature.from_dict(c.to_dict(), app=memory_app)
    assert isinstance(restored, chord)
    assert isinstance(restored["kwargs"]["body"], dict) and not isinstance(restored["kwargs"]["body"], Signature)
    assert isinstance(restored.body, Signature)  # upgraded on access
    assert restored.body.task == tsum.name
    assert restored.id is None
    restored.freeze()
    assert restored.id == restored.body.id is not None
    assert chord([add.s(1, 1)]).id is None


def test_chord_without_body_is_refused(memory_app, tasks):
    add, _, _ = tasks
    with pytest.raises(ValueError, match="needs a body"):
        chord([add.s(1, 1)]).apply_async()


def test_chord_with_partial_args(memory_app, tasks):
    add, tsum, _ = tasks
    c = chord([add.s(1), add.s(2)], tsum.s())
    assert [t.args for t in c.clone((10,)).tasks] == [(10, 1), (10, 2)]
    result = c.apply_async((10,))
    drain(memory_app)
    assert result.get() == 23
    assert result.parent.get() == [11, 12]


def test_chord_with_empty_header_runs_body_with_no_results(memory_app, tasks):
    _, tsum, _ = tasks
    result = chord([], tsum.s(), app=memory_app).apply_async()
    drained = drain(memory_app)
    assert [d.args for d in drained] == [[[]]]
    assert result.get() == 0


def test_chord_eager(memory_app, tasks):
    add, tsum, _ = tasks
    c = chord([add.s(1), add.s(2)], tsum.s())
    assert c.apply((1,)).get() == 5
    memory_app.conf.task_always_eager = True
    assert c.apply_async((2,)).get() == 7
    assert chord([add.s(1, 1), add.s(2, 2)])(tsum.s()).get() == 6  # body given at call time


def test_chord_errback_receives_callback_id(memory_app, tasks):
    add, tsum, _ = tasks
    seen = []

    @memory_app.task
    def boom(x):
        raise ValueError(x)

    @memory_app.task
    def on_error(task_id):
        seen.append(task_id)

    body = tsum.s().on_error(on_error.s())
    result = chord([add.s(1, 1), boom.s(2)], body).apply_async()
    drain(memory_app)
    assert seen == [result.id]
    assert result.state == states.FAILURE


def test_chains_nested_in_groups_and_chords(memory_app):
    """Celery allows chains as group members and in chord headers; the chain's last
    step is the member's result."""
    from potatoq import chain, chord, group
    from potatoq.testing import drain

    @memory_app.task
    def add(x, y):
        return x + y

    @memory_app.task
    def total(xs):
        return sum(xs)

    g = group(add.s(1, 1), chain(add.s(2, 2), add.s(10))).apply_async()
    c = chord([add.s(1, 1), chain(add.s(2, 2), add.s(10))], total.s()).apply_async()
    tail = chain(add.s(1, 1), group(add.s(10), add.s(20))).apply_async()
    drain(memory_app)
    assert g.get() == [2, 14]
    assert c.get() == 16
    assert tail.get() == [12, 22]  # a chain ending in a group returns the group's results


def test_only_chains_ending_in_a_task_can_be_nested(memory_app):
    import pytest

    from potatoq import chain, group

    @memory_app.task
    def add(x, y):
        return x + y

    with pytest.raises(TypeError, match="must end with a task"):
        group(chain(add.s(1, 1), group(add.s(1), add.s(2)))).apply_async()
    with pytest.raises(TypeError, match="Only chains"):
        group(group(add.s(1, 1))).apply_async()


# --- workflow options, failures and composite shapes ----------------------------


@pytest.fixture
def wf(memory_app):
    return _workflow_tasks(memory_app)


def _workflow_tasks(app):
    """Tasks for workflow tests; every call is recorded in ``ns.calls``."""
    from types import SimpleNamespace

    calls: list = []

    @app.task
    def add(x, y):
        calls.append(("add", x, y))
        return x + y

    @app.task
    def tsum(values):
        calls.append(("tsum", values))
        return sum(values)

    @app.task
    def double(x):
        calls.append(("double", x))
        return x * 2

    @app.task
    def boom(*args):
        calls.append(("boom", args))
        raise ValueError("boom")

    @app.task
    def cb(x):
        calls.append(("cb", x))
        return x

    @app.task
    def eb(task_id):
        calls.append(("eb", task_id))

    @app.task(bind=True)
    def to_chain(self, x):
        self.replace(chain(double.s(x), double.s()))

    @app.task(bind=True)
    def to_group(self, x):
        self.replace(group(double.s(x), double.s(x + 1)))

    tasks = (add, tsum, double, boom, cb, eb, to_chain, to_group)
    return SimpleNamespace(app=app, calls=calls, **{t.name.rsplit(".", 1)[-1]: t for t in tasks})


def run_all(app) -> None:
    """Run everything queued, on any broker, until the queue stays empty."""
    consumer = app.broker.consumer([app.conf.task_default_queue], "canvas-tests")
    idle = 1.0 if getattr(app, "kind", None) == "rabbitmq" else 0.2
    try:
        while (delivery := consumer.fetch(timeout=idle)) is not None:
            outcome = executor.execute(app, delivery.message, delivery_count=delivery.delivery_count, hostname="t")
            executor.settle(app, consumer, delivery, outcome)
    finally:
        consumer.close()


def _errbacks_called(wf):
    return [c[1] for c in wf.calls if c[0] == "eb"]


def test_chain_and_chord_call_options_go_to_the_right_tasks(broker_app):
    wf = _workflow_tasks(broker_app)
    # link: after the last step; task_id: the last step's id; link_error: every step.
    last_id = f"last-{broker_app.test_token}"
    result = chain(wf.add.s(1, 1), wf.double.s(), wf.double.s()).apply_async(
        link=wf.cb.s(), link_error=wf.eb.s(), task_id=last_id
    )
    run_all(broker_app)
    assert result.id == last_id
    assert result.get(timeout=5) == 8
    assert wf.calls == [("add", 1, 1), ("double", 2), ("double", 4), ("cb", 8)]

    wf.calls.clear()
    failing = chain(wf.add.s(1, 1), wf.boom.s(), wf.double.s()).apply_async(link_error=wf.eb.s())
    run_all(broker_app)
    with pytest.raises(ValueError, match="boom"):
        failing.get(timeout=5)
    [errback_arg] = _errbacks_called(wf)  # once, with the failed step's id
    assert errback_arg != failing.id
    assert broker_app.AsyncResult(errback_arg).state == states.FAILURE

    # A chord's link and task_id belong to its body, not to each header task.
    wf.calls.clear()
    body_id = f"body-{broker_app.test_token}"
    result = chord([wf.add.s(1, 1), wf.add.s(2, 2)], wf.tsum.s()).apply_async(link=wf.cb.s(), task_id=body_id)
    run_all(broker_app)
    assert result.id == body_id
    assert result.get(timeout=5) == 6
    assert [c for c in wf.calls if c[0] == "cb"] == [("cb", 6)]


def test_failure_reaches_the_end_of_chains_groups_and_chords(broker_app):
    wf = _workflow_tasks(broker_app)
    plain = chain(wf.boom.s(), wf.double.s()).apply_async()
    members = group(chain(wf.boom.s(), wf.double.s()), wf.add.s(2, 2)).apply_async()
    body = wf.tsum.s().on_error(wf.eb.s())
    in_chord = chord([chain(wf.boom.s(), wf.double.s()), wf.add.s(2, 2)], body).apply_async()
    after_chord = chain(chord([wf.boom.s(), wf.add.s(1, 1)], wf.tsum.s()), wf.double.s()).apply_async()
    run_all(broker_app)
    assert not any(c[0] in ("double", "tsum") for c in wf.calls)  # never ran
    with pytest.raises(ValueError, match="boom"):
        plain.get(timeout=5)
    with pytest.raises(ValueError, match="boom"):
        members.results[0].get(timeout=5)
    assert members.results[1].get(timeout=5) == 4
    with pytest.raises(ChordError, match="boom"):
        in_chord.get(timeout=5)
    assert _errbacks_called(wf) == [in_chord.id]  # the body's errback, exactly once
    with pytest.raises(ChordError):
        after_chord.get(timeout=5)


def test_composite_signatures_as_bodies_steps_and_members(broker_app):
    wf = _workflow_tasks(broker_app)
    add, tsum, double, cb = wf.add, wf.tsum, wf.double, wf.cb
    chain_body = chord([add.s(1, 1), add.s(2, 2)], chain(tsum.s(), double.s())).apply_async()
    after_chain_body = chain(
        add.s(1, 1), chord([double.s(), double.s()], chain(tsum.s(), double.s())), cb.s()
    ).apply_async()
    group_body = chain(add.s(1, 1), chord([double.s(), double.s()], group(cb.s(), cb.s()))).apply_async()
    failing_group_body = chord([wf.boom.s(), add.s(2, 2)], group(cb.s(), cb.s())).apply_async()
    chain_member = chain(add.s(1, 1), group(chain(double.s(), double.s()), double.s())).apply_async()
    chain_in_header = chain(add.s(1, 1), chord([chain(double.s(), double.s()), double.s()], tsum.s())).apply_async()
    group_after_group = (group(add.s(1, 1), add.s(2, 2)) | group(tsum.s(), tsum.s()) | cb.s()).apply_async()
    nested_body = chord([add.s(1, 1), add.s(2, 2)], chord([tsum.s(), tsum.s()], tsum.s())).apply_async()
    after_chord = chain(chord([add.s(1, 1), add.s(2, 2)], tsum.s()), double.s()).apply_async()
    run_all(broker_app)
    assert chain_body.get(timeout=5) == 12
    assert after_chain_body.get(timeout=5) == 16
    assert isinstance(group_body, GroupResult)
    assert group_body.get(timeout=5) == [[4, 4], [4, 4]]
    with pytest.raises(ChordError):
        failing_group_body.get(timeout=5)
    assert chain_member.get(timeout=5) == [8, 4]
    assert chain_in_header.get(timeout=5) == 12
    assert group_after_group.get(timeout=5) == [6, 6]
    assert wf.calls.count(("cb", [6, 6])) == 1
    assert nested_body.get(timeout=5) == 12
    assert after_chord.get(timeout=5) == 12


def test_replace_with_a_chain_or_group_keeps_id_callbacks_and_chord(broker_app):
    wf = _workflow_tasks(broker_app)
    in_chain = chain(wf.add.s(1, 1), wf.to_chain.s(), wf.cb.s()).apply_async()
    in_header = chord([wf.to_chain.s(1), wf.add.s(2, 2)], wf.tsum.s()).apply_async()
    by_group = wf.to_group.delay(1)
    group_in_header = chord([wf.to_group.s(1), wf.add.s(2, 2)], wf.cb.s()).apply_async()
    run_all(broker_app)
    assert in_chain.get(timeout=5) == 8
    assert ("cb", 8) in wf.calls
    assert in_header.get(timeout=5) == 8
    assert by_group.get(timeout=5) == [2, 4]
    assert group_in_header.get(timeout=5) == [[2, 4], 4]


def test_retrying_header_tasks_keep_their_chord_membership(broker_app):
    attempts = []

    @broker_app.task(bind=True, max_retries=2)
    def flaky(self, x):
        attempts.append(x)
        if self.request.retries < 1:
            raise self.retry(countdown=0)
        return x

    wf = _workflow_tasks(broker_app)
    result = chord([flaky.s(1), chain(wf.add.s(1, 1), flaky.s())], wf.tsum.s()).apply_async()
    run_all(broker_app)
    assert result.get(timeout=5) == 3
    assert sorted(attempts) == [1, 1, 2, 2]


def test_chain_freeze_and_ids(wf):
    c = chain(wf.add.s(1, 1), wf.double.s())
    assert c.id is None and chain(app=wf.app).id is None
    assert c.clone().set(task_id="x").id == "x"
    frozen = c.freeze()
    assert c.id == frozen.id == c.tasks[-1].id
    result = c.apply_async()
    assert result.id == frozen.id
    drain(wf.app)
    assert frozen.get() == 4

    ending_in_chord = chain(wf.add.s(1, 1), chord([wf.double.s()], wf.tsum.s()))
    assert ending_in_chord.freeze("fixed").id == "fixed" == ending_in_chord.tasks[-1].body.id
    assert len(chain(wf.add.s(1, 1), chain(app=wf.app)).freeze().id) > 0  # an empty nested chain is skipped
    with pytest.raises(ValueError, match="Empty chain"):
        chain(app=wf.app).freeze()


def test_link_error_and_options_on_chains_groups_and_chords(wf):
    for sig in (
        chain(wf.add.s(1, 1), wf.boom.s(), wf.double.s()),
        group(wf.add.s(1, 1), wf.boom.s()),
        chord([wf.add.s(1, 1), wf.boom.s()], wf.tsum.s()),
    ):
        wf.calls.clear()
        assert sig.on_error(wf.eb.s()) is sig
        sig.apply_async()
        drain(wf.app)
        assert len(_errbacks_called(wf)) == 1, sig

    wf.calls.clear()
    c = chord([wf.add.s(1, 1), wf.add.s(2, 2)], wf.tsum.s())
    c.link(wf.cb.s())
    c.apply_async()
    drain(wf.app)
    assert wf.calls[-1] == ("cb", 6)

    # Options set on a workflow reach its tasks; countdown only delays the start.
    queue = wf.app.conf.task_default_queue
    sig = chain(wf.add.s(1, 1), wf.double.s(), app=wf.app).set(priority=7, countdown=60, queue=None)
    [first] = executor.signature_to_messages(wf.app, sig, (), None)
    assert (first.priority, first.queue) == (7, queue) and first.eta is not None
    [second] = first.link
    assert second["options"]["priority"] == 7 and "countdown" not in second["options"]
    chord_sig = chord([wf.add.s(1, 1)], wf.tsum.s()).set(priority=3)
    [header] = executor.signature_to_messages(wf.app, chord_sig, (), None)
    assert header.priority == 3 and header.chord["callback"]["options"]["priority"] == 3
    [member] = executor.signature_to_messages(wf.app, group(wf.add.s(1, 1)).set(priority=2), (), None)
    assert member.priority == 2


def test_apply_async_publish_options_and_argument_check(wf):
    result = chain(wf.add.s(1, 1), wf.double.s()).apply_async(enqueue_on_commit=False, producer=None)
    drain(wf.app)
    assert result.get() == 4
    with pytest.raises(TypeError, match="add"):
        group(wf.add.s(1)).apply_async()


def test_group_as_chord_header_keeps_its_id_and_options(wf):
    g = group(wf.add.s(1, 1), wf.add.s(2, 2)).set(priority=4)
    gid = g.freeze().id
    c = chord(g, wf.tsum.s())
    assert c["options"]["group_id"] == gid
    assert [t.options["priority"] for t in c.tasks] == [4, 4]
    result = c.apply_async()
    drain(wf.app)
    assert result.get() == 6 and result.parent.id == gid
    assert c.clone().set(task_id="x").id == "x"
    with pytest.raises(ValueError, match="needs a body"):
        chord([wf.add.s(1, 1)]).freeze()


def test_chord_body_chain_ending_in_group_followed_by_a_step(wf):
    body = chain(wf.tsum.s(), group(wf.double.s(), wf.double.s()))
    result = chain(chord([wf.add.s(1, 1), wf.add.s(2, 2)], body), wf.cb.s()).apply_async()
    drain(wf.app)
    assert result.get() == [12, 12]
    assert [c for c in wf.calls if c[0] == "cb"] == [("cb", [12, 12])]


def test_name_only_signatures_in_a_group(wf):
    result = group(Signature("remote.x", (1,), app=wf.app), wf.add.s(1, 1)).apply_async()
    drained = drain(wf.app)
    assert sorted(d.name for d in drained) == sorted(["remote.x", wf.add.name])
    assert result.results[1].get() == 2


# --- eager parity ---------------------------------------------------------------


def test_eager_routes_options_like_async(wf):
    wf.app.conf.task_always_eager = True
    assert chain(wf.add.s(1, 1), wf.double.s()).apply_async(link=wf.cb.s()).get() == 4
    assert wf.calls == [("add", 1, 1), ("double", 2), ("cb", 4)]

    wf.calls.clear()
    with pytest.raises(ValueError):
        chain(wf.add.s(1, 1), wf.boom.s()).on_error(wf.eb.s()).apply()
    assert len(_errbacks_called(wf)) == 1
    failed = chain(wf.boom.s(), wf.double.s()).apply(throw=False)
    assert failed.state == states.FAILURE and isinstance(failed.result, ValueError)


def test_eager_group_runs_every_member_before_raising(wf):
    with pytest.raises(ValueError):
        group(wf.boom.s(), wf.add.s(1, 1)).apply()
    assert wf.calls == [("boom", ()), ("add", 1, 1)]
    results = group(wf.boom.s(), wf.add.s(1, 1)).apply(throw=False)
    assert results.get(propagate=False)[1] == 2


def test_eager_chord_header_failure_fails_the_body(wf):
    body = wf.tsum.s().on_error(wf.eb.s())
    with pytest.raises(ChordError):
        chord([wf.boom.s(), wf.add.s(1, 1)], body).apply()
    assert len(_errbacks_called(wf)) == 1

    wf.calls.clear()
    chain_body = chain(wf.tsum.s(), wf.double.s()).on_error(wf.eb.s())
    result = chord([wf.boom.s()], chain_body).apply(throw=False)
    assert result.state == states.FAILURE and isinstance(result.result, ChordError)
    assert len(_errbacks_called(wf)) == 1


def test_eager_replace_returns_the_replacement_result(wf):
    assert wf.to_chain.apply((1,)).get() == 4
    assert chain(wf.add.s(1, 1), wf.to_chain.s(), wf.cb.s()).apply().get() == 8


def test_call_links_add_to_the_signature_links(wf):
    sig = wf.add.s(1, 1)
    sig.link(wf.double.s())
    result = sig.apply_async(link=wf.cb.s())
    drain(wf.app)
    assert result.get() == 2
    assert sorted(wf.calls[1:]) == [("cb", 2), ("double", 2)]
