"""The instrumentation hooks that tracing and APM integrations build on."""

from __future__ import annotations

from contextlib import contextmanager

import pytest

from potatoq import Potatoq, instrumentation
from potatoq.instrumentation import Instrumentation, TaskRun
from potatoq.testing import drain


@pytest.fixture
def app():
    app = Potatoq("instr", broker="memory://", set_as_current=True)
    yield app
    app.close()


class Recorder(Instrumentation):
    def __init__(self):
        self.created = []
        self.runs = []

    def message_created(self, message):
        message.headers["seen"] = "yes"
        self.created.append(message.task)

    @contextmanager
    def task_run(self, run):
        self.runs.append(run)
        yield


@pytest.fixture
def recorder():
    recorder = Recorder()
    instrumentation.register(recorder)
    yield recorder
    instrumentation.unregister(recorder)


def test_hooks_see_every_message_and_attempt(app, recorder):
    @app.task(name="t.flaky", autoretry_for=(ValueError,), max_retries=1, retry_backoff=False)
    def flaky():
        raise ValueError("no")

    flaky.delay()
    drain(app)
    assert recorder.created == ["t.flaky", "t.flaky"]  # the call, then the retry
    first, second = recorder.runs
    assert (first.state, second.state) == ("RETRY", "FAILURE")
    assert first.retry_eta is not None
    assert isinstance(second.exception, ValueError) and second.failed
    assert second.retries == 1 and second.queue == "default" and second.name == "t.flaky"
    assert second.message.headers["seen"] == "yes"
    assert second.wait_time >= 0 and second.runtime >= 0
    assert second.periodic is None and not second.is_eager and not second.process_died
    assert repr(second) == f"<TaskRun t.flaky[{second.message.id}] FAILURE>"


def test_register_replaces_an_instance_of_the_same_class(recorder):
    other = Recorder()
    instrumentation.register(other)
    assert instrumentation.registered().count(recorder) == 0
    assert other in instrumentation.registered()
    instrumentation.unregister(other)
    instrumentation.unregister(other)  # already gone: fine


def test_a_failing_instrumentation_never_breaks_a_task(app, caplog):
    class Broken(Instrumentation):
        def message_created(self, message):
            raise RuntimeError("created")

        def task_run(self, run):
            raise RuntimeError("enter")

    class BrokenExit(Instrumentation):
        @contextmanager
        def task_run(self, run):
            yield
            raise RuntimeError("exit")

    class Default(Instrumentation):
        pass

    broken, broken_exit, default = Broken(), BrokenExit(), Default()
    for i in (broken, broken_exit, default):
        instrumentation.register(i)
    try:

        @app.task(name="t.ok")
        def ok():
            return 1

        ok.delay()
        [done] = drain(app)
    finally:
        for i in (broken, broken_exit, default):
            instrumentation.unregister(i)
    assert done.state == "SUCCESS"
    assert "failed on message t.ok" in caplog.text
    assert "failed to start <TaskRun t.ok" in caplog.text
    assert "failed to finish <TaskRun t.ok" in caplog.text


def test_task_run_sees_exceptions_that_end_the_attempt(app):
    seen = []

    class Watch(Instrumentation):
        def task_run(self, run):
            outer = self

            class Manager:
                def __enter__(self):
                    return None

                def __exit__(self, *exc_info):
                    seen.append((outer, exc_info[0]))

            return Manager()

    watch = Watch()
    instrumentation.register(watch)
    try:
        with pytest.raises(KeyboardInterrupt), instrumentation.task_run(TaskRun(app, None, None)):  # type: ignore[arg-type]
            raise KeyboardInterrupt
    finally:
        instrumentation.unregister(watch)
    assert seen == [(watch, KeyboardInterrupt)]
