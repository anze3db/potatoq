"""Result objects: ``AsyncResult``, ``EagerResult``, ``ResultSet``, ``GroupResult``."""

from __future__ import annotations

import asyncio
import datetime as dt
import pickle

import pytest

from potatoq import Potatoq, states
from potatoq import app as app_module
from potatoq.exceptions import ResultBackendDisabled, TaskRevokedError
from potatoq.exceptions import TimeoutError as PotatoqTimeoutError
from potatoq.result import AsyncResult, EagerResult, GroupResult, ResultSet, result_from_tuple
from potatoq.task import Context
from potatoq.testing import drain


@pytest.fixture
def add(memory_app):
    @memory_app.task
    def add(x, y):
        return x + y

    return add


def test_defaults_to_current_app(memory_app):
    assert AsyncResult("x").app is memory_app
    assert ResultSet([]).app is memory_app
    assert result_from_tuple((("x", None), None)).app is memory_app


def test_identity_and_dunder_methods(memory_app):
    a = AsyncResult("abc", app=memory_app)
    assert repr(a) == "<AsyncResult: abc>" and str(a) == "abc" and a.task_id == "abc"
    assert a == AsyncResult("abc", app=memory_app) and a == "abc"
    assert a != AsyncResult("other", app=memory_app) and a != "other"
    assert a.__eq__(1) is NotImplemented
    assert len({a, AsyncResult("abc", app=memory_app)}) == 1
    assert a.children == []
    with pytest.raises(NotImplementedError):
        a.build_graph()
    with pytest.raises(NotImplementedError, match="chains"):
        a.then(print)


def test_pending_result_metadata(memory_app):
    r = memory_app.AsyncResult("pending", task_name="some.task")
    assert r.state == r.status == states.PENDING
    assert r.info is None and r.result is None and r.traceback is None
    assert r.date_done is None and r.args is None and r.kwargs is None
    assert r.retries == 0 and r.worker is None
    assert r.name == "some.task"
    assert not r.ready() and not r.successful() and not r.failed()


def test_finished_result_metadata(memory_app):
    request = Context(args=[1, 2], kwargs={"z": 3}, retries=2, hostname="w1")
    memory_app.store_result("done", states.SUCCESS, 3, task_name="tasks.add", request=request)
    r = memory_app.AsyncResult("done")
    assert r.successful() and r.info == 3
    assert isinstance(r.date_done, dt.datetime) and r.date_done.tzinfo is not None
    assert r.name == "tasks.add"
    assert r.args == [1, 2] and r.kwargs == {"z": 3} and r.retries == 2 and r.worker == "w1"
    seen = []
    assert r.get(callback=lambda task_id, value: seen.append((task_id, value))) == 3
    assert seen == [("done", 3)]
    assert list(r.collect()) == [(r, 3)]


def test_failed_result(memory_app):
    @memory_app.task
    def boom():
        raise KeyError("boom")

    r = boom.delay()
    drain(memory_app)
    assert r.failed() and isinstance(r.info, KeyError) and "KeyError" in r.traceback
    with pytest.raises(KeyError):
        r.get()
    assert isinstance(r.get(propagate=False), KeyError)


def test_revoked_result_raises_task_revoked(memory_app):
    memory_app.store_result("rev", states.REVOKED, None)
    r = memory_app.AsyncResult("rev")
    with pytest.raises(TaskRevokedError):
        r.get()
    assert r.get(propagate=False) is None


def test_get_times_out(memory_app):
    with pytest.raises(PotatoqTimeoutError, match="timed out"):
        memory_app.AsyncResult("never").get(timeout=0.01)


def test_ignored_result_cannot_be_waited_for(memory_app):
    r = AsyncResult("x", app=memory_app, ignored=True)
    with pytest.raises(ResultBackendDisabled, match="ignores its result"):
        r.get()


def test_aget(memory_app, add):
    r = add.delay(1, 2)
    drain(memory_app)
    assert asyncio.run(r.aget(timeout=1)) == 3


def test_forget_and_revoke(memory_app, add):
    r = add.delay(1, 2)
    drain(memory_app)
    assert r.get() == 3
    r.forget()
    assert r.state == states.PENDING

    waiting = add.delay(2, 2)
    waiting.revoke()
    assert drain(memory_app) == []


def test_as_tuple_round_trip_and_pickle(memory_app):
    parent = AsyncResult("parent", app=memory_app)
    child = AsyncResult("child", app=memory_app, parent=parent)
    assert child.as_tuple() == (("child", (("parent", None), None)), None)
    restored = result_from_tuple(child.as_tuple(), app=memory_app)
    assert restored == child and restored.parent == parent and restored.parent.parent is None
    unpickled = pickle.loads(pickle.dumps(child))
    assert unpickled.id == "child" and unpickled.app is memory_app and unpickled.parent == parent


def test_eager_result(memory_app):
    ok = EagerResult("e1", 5, states.SUCCESS, app=memory_app, name="t")
    assert repr(ok) == "<EagerResult: e1>"
    assert ok.state == ok.status == states.SUCCESS
    assert ok.info == ok.result == 5 and ok.traceback is None and ok.get() == 5
    ok.forget()
    ok.revoke()
    assert ok.state == states.REVOKED

    exc = ValueError("bad")
    failed = EagerResult("e2", exc, states.FAILURE, traceback="tb", app=memory_app)
    assert failed.traceback == "tb" and failed.failed()
    with pytest.raises(ValueError):
        failed.get()
    assert failed.get(propagate=False) is exc


def test_result_set(memory_app, add):
    a, b = add.delay(1, 1), add.delay(2, 2)
    rs = ResultSet([a], app=memory_app)
    rs.add(b)
    rs.add(b)  # no duplicates
    assert len(rs) == 2 and list(rs) == [a, b] and rs[1] is b
    assert repr(rs) == f"<ResultSet: {[a.id, b.id]}>"
    assert rs.waiting() and not rs.ready() and rs.completed_count() == 0
    drain(memory_app)
    assert rs.ready() and rs.successful() and not rs.failed() and rs.completed_count() == 2
    assert rs.get(timeout=1) == [2, 4]
    assert rs.join() == [2, 4]
    rs.forget()
    assert a.state == b.state == states.PENDING


def test_result_set_revoke(memory_app, add):
    rs = ResultSet([add.delay(1, 1), add.delay(2, 2)], app=memory_app)
    rs.revoke()
    assert drain(memory_app) == []


def test_result_set_get_refused_inside_task(memory_app, add):
    rs = ResultSet([], app=memory_app)
    add.push_request(id="running")
    app_module.set_current_task(add)
    try:
        with pytest.raises(RuntimeError, match=r"Never call result\.get"):
            rs.get()
        assert rs.get(disable_sync_subtasks=False) == []
    finally:
        app_module.set_current_task(None)
        add.pop_request()


def test_group_result_save_restore_delete(memory_app, add):
    results = [add.delay(1, 2), add.delay(3, 4)]
    group_result = GroupResult("gid", results, app=memory_app)
    assert repr(group_result) == f"<GroupResult: gid [{results[0].id}, {results[1].id}]>"
    assert group_result.save() is group_result

    restored = GroupResult.restore("gid", app=memory_app)
    assert restored is not None and restored.id == "gid" and restored.results == results
    drain(memory_app)
    assert restored.get() == [3, 7]

    memory_app.set_current()
    assert GroupResult.restore("gid").results == results
    group_result.delete()
    assert GroupResult.restore("gid", app=memory_app) is None


def test_group_result_requires_backend():
    app = Potatoq("nores", broker="memory://", backend="disabled", set_as_current=False)
    with pytest.raises(ResultBackendDisabled):
        GroupResult("gid", [], app=app).save()
