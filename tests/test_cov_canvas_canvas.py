"""Canvas primitives: signatures, chains, groups and chords (eager and via ``drain``)."""

from __future__ import annotations

import pickle

import pytest

from potatoq import chain, chord, group, signature, states
from potatoq.canvas import Signature, maybe_signature, xmap, xstarmap
from potatoq.result import EagerResult, GroupResult
from potatoq.testing import drain


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
