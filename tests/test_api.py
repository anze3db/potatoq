"""The Celery-compatible API, exercised through the memory broker and ``drain``."""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal

import pytest

from potatoq import Celery, Potatoq, Task, chain, chord, group, shared_task, signature, states
from potatoq.exceptions import (
    MaxRetriesExceededError,
    NotRegistered,
    Reject,
    ResultBackendDisabled,
    SoftTimeLimitExceeded,
)
from potatoq.testing import drain


def test_celery_alias_and_task_naming(memory_app):
    assert Celery is Potatoq

    @memory_app.task
    def add(x, y):
        return x + y

    assert add.name == f"{__name__}.add"
    assert add(2, 3) == 5  # direct call
    assert add.request.called_directly
    assert repr(add).startswith("<@task: ")


def test_delay_and_drain(memory_app):
    @memory_app.task
    def add(x, y):
        return x + y

    result = add.delay(2, 3)
    assert result.state == states.PENDING
    drained = drain(memory_app)
    assert [d.state for d in drained] == ["SUCCESS"]
    assert result.get(timeout=1) == 5
    assert result.successful() and result.ready()


def test_arguments_checked_at_delay_time(memory_app):
    @memory_app.task
    def add(x, y):
        return x + y

    with pytest.raises(TypeError, match="add"):
        add.delay(1, 2, 3)


def test_unserializable_arguments_fail_fast(memory_app):
    @memory_app.task
    def store(obj):
        return obj

    with pytest.raises(TypeError, match="not JSON serializable"):
        store.delay(object())


def test_rich_types_round_trip(memory_app):
    @memory_app.task
    def echo(value):
        return value

    value = {
        "when": dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.UTC),
        "day": dt.date(2026, 1, 2),
        "id": uuid.UUID(int=7),
        "price": Decimal("9.99"),
        "raw": b"\x00\x01",
        "tags": {"a"},
    }
    result = echo.delay(value)
    drain(memory_app)
    assert result.get() == value


def test_bind_and_request_context(memory_app):
    @memory_app.task(bind=True)
    def info(self):
        return {
            "id": self.request.id,
            "retries": self.request.retries,
            "eager": self.request.is_eager,
            "task": self.name,
        }

    result = info.delay()
    drain(memory_app)
    assert result.get() == {"id": result.id, "retries": 0, "eager": False, "task": info.name}


def test_retry_with_countdown(memory_app):
    calls = []

    @memory_app.task(bind=True, max_retries=2)
    def flaky(self):
        calls.append(self.request.retries)
        if self.request.retries < 2:
            raise self.retry(countdown=30)
        return "ok"

    result = flaky.delay()
    drained = drain(memory_app)
    assert [d.state for d in drained] == ["RETRY", "RETRY", "SUCCESS"]
    assert calls == [0, 1, 2]
    assert result.get() == "ok"


def test_retry_exhausted_reraises_exc(memory_app):
    @memory_app.task(bind=True, max_retries=1)
    def always(self):
        raise self.retry(exc=ValueError("nope"), countdown=1)

    result = always.delay()
    drained = drain(memory_app)
    assert [d.state for d in drained] == ["RETRY", "FAILURE"]
    with pytest.raises(ValueError, match="nope"):
        result.get()
    assert [e["task"] for e in memory_app.broker.dead_letters()] == [always.name]


def test_retry_without_exc_raises_max_retries(memory_app):
    @memory_app.task(bind=True, max_retries=0)
    def once(self):
        raise self.retry()

    result = once.delay()
    drain(memory_app)
    with pytest.raises(MaxRetriesExceededError):
        result.get()


def test_autoretry_for_with_backoff(memory_app):
    attempts = []

    @memory_app.task(bind=True, autoretry_for=(ConnectionError,), retry_backoff=True, max_retries=3)
    def fetch(self):
        attempts.append(self.request.retries)
        if len(attempts) < 3:
            raise ConnectionError("down")
        return "fetched"

    result = fetch.delay()
    drain(memory_app)
    assert attempts == [0, 1, 2]
    assert result.get() == "fetched"


def test_default_retry_delay_uses_backoff(memory_app):
    @memory_app.task
    def noop():
        pass

    delays = [noop.backoff_delay(n) for n in range(8)]
    assert 5 <= delays[0] <= 10  # equal jitter around 10s
    assert all(d <= memory_app.conf.task_retry_backoff_max for d in delays)

    @memory_app.task(default_retry_delay=42)
    def fixed():
        pass

    assert fixed.backoff_delay(3) == 42  # an explicit Celery-style delay wins


def test_failure_is_dead_lettered_and_propagates(memory_app):
    @memory_app.task
    def boom():
        raise KeyError("missing")

    result = boom.delay()
    drain(memory_app)
    assert result.failed()
    with pytest.raises(KeyError):
        result.get()
    assert result.get(propagate=False).__class__ is KeyError
    assert "KeyError" in result.traceback
    dead = memory_app.broker.dead_letters()
    assert dead[0]["id"] == result.id
    assert memory_app.broker.requeue_dead(result.id)
    assert drain(memory_app)[0].state == "FAILURE"


def test_reject_requeue_and_dead_letter(memory_app):
    @memory_app.task(bind=True)
    def picky(self):
        if self.request.delivery_count == 1:
            raise Reject("try again", requeue=True)
        raise Reject("never")

    picky.delay()
    drained = drain(memory_app)
    assert [d.state for d in drained] == ["REJECTED", "REJECTED"]
    assert memory_app.broker.dead_letters()[0]["reason"] == "never"


def test_ignore_result_get_raises(memory_app):
    @memory_app.task(ignore_result=True)
    def fire_and_forget():
        return 1

    result = fire_and_forget.delay()
    drain(memory_app)
    with pytest.raises(ResultBackendDisabled):
        result.get()


def test_results_off_by_default_on_redis_like_brokers():
    app = Potatoq("noresults", broker="memory://")

    @app.task
    def add(x, y):
        return x + y

    assert add.resolved_ignore_result()  # memory/Redis/RabbitMQ: opt-in, like Celery
    sqlite_app = Potatoq("db", broker="sqlite:///unused.db")

    @sqlite_app.task
    def sub(x, y):
        return x - y

    assert not sub.resolved_ignore_result()  # database brokers: free, so on


def test_expires_discards_task(memory_app):
    @memory_app.task
    def late():
        return "ran"

    result = late.apply_async(expires=-1)
    drained = drain(memory_app)
    assert drained[0].state == states.REVOKED
    assert result.state == states.REVOKED


def test_eta_requires_aware_datetime(memory_app):
    @memory_app.task
    def noop():
        pass

    with pytest.raises(ValueError, match="timezone-aware"):
        noop.apply_async(eta=dt.datetime.now())
    noop.apply_async(eta=dt.datetime.now(dt.UTC) + dt.timedelta(hours=1))
    assert memory_app.broker.queue_sizes() == {"default": 1}


def test_chain(memory_app):
    @memory_app.task
    def add(x, y):
        return x + y

    @memory_app.task
    def mul(x, y):
        return x * y

    result = chain(add.s(2, 2), mul.s(10), add.s(1)).apply_async()
    drain(memory_app)
    assert result.get() == 41
    result2 = (add.s(1, 1) | mul.s(3)).delay()
    drain(memory_app)
    assert result2.get() == 6


def test_immutable_signature_in_chain(memory_app):
    @memory_app.task
    def const(value):
        return value

    result = (const.s(1) | const.si(2)).delay()
    drain(memory_app)
    assert result.get() == 2


def test_group(memory_app):
    @memory_app.task
    def square(x):
        return x * x

    result = group(square.s(i) for i in range(5)).apply_async()
    drain(memory_app)
    assert result.get() == [0, 1, 4, 9, 16]
    assert result.completed_count() == 5


def test_chord(memory_app):
    @memory_app.task
    def square(x):
        return x * x

    @memory_app.task
    def total(values):
        return sum(values)

    result = chord(square.s(i) for i in range(4))(total.s())
    drain(memory_app)
    assert result.get() == 14


def test_group_then_task_becomes_chord(memory_app):
    @memory_app.task
    def square(x):
        return x * x

    @memory_app.task
    def total(values):
        return sum(values)

    @memory_app.task
    def double(x):
        return x * 2

    result = (group(square.s(i) for i in range(3)) | total.s() | double.s()).delay()
    drain(memory_app)
    assert result.get() == 10


def test_chord_with_failing_header_fails_callback(memory_app):
    @memory_app.task
    def maybe(x):
        if x == 1:
            raise ValueError("bad")
        return x

    @memory_app.task
    def total(values):
        return sum(values)

    result = chord([maybe.s(0), maybe.s(1)], total.s()).apply_async()
    drain(memory_app)
    from potatoq.exceptions import ChordError

    with pytest.raises(ChordError):
        result.get()


def test_link_error_called_with_task_id(memory_app):
    seen = []

    @memory_app.task
    def boom():
        raise RuntimeError("x")

    @memory_app.task
    def on_error(task_id):
        seen.append(task_id)

    result = boom.apply_async(link_error=on_error.s())
    drain(memory_app)
    assert seen == [result.id]


def test_signature_serialization_round_trip(memory_app):
    @memory_app.task
    def add(x, y):
        return x + y

    sig = add.s(1).set(countdown=10)
    clone = signature(dict(sig), app=memory_app)
    assert clone.task == add.name and clone.args == (1,) and clone.options == {"countdown": 10}
    assert clone.clone((2,)).args == (2, 1)


def test_shared_task_binds_to_current_app():
    @shared_task
    def hello(name):
        return f"hello {name}"

    app = Potatoq("shared", broker="memory://", result_backend="broker")
    app.finalize()
    assert hello.name in app.tasks
    result = hello.delay("world")
    drain(app)
    assert result.get() == "hello world"


def test_send_task_by_name(memory_app):
    @memory_app.task(name="custom.name")
    def named(x):
        return x

    result = memory_app.send_task("custom.name", [5])
    drain(memory_app)
    assert result.get() == 5


def test_unregistered_task_is_dead_lettered(memory_app):
    memory_app.send_task("does.not.exist", [1])
    drained = drain(memory_app)
    assert drained[0].state == "FAILURE"
    assert isinstance(drained[0].exception, NotRegistered)
    assert memory_app.broker.dead_letters()[0]["task"] == "does.not.exist"


def test_task_routes(memory_app):
    memory_app.conf.task_routes = {"billing.*": {"queue": "billing"}, "emails.send": "emails"}

    @memory_app.task(name="billing.charge")
    def charge():
        pass

    @memory_app.task(name="emails.send")
    def send():
        pass

    charge.delay()
    send.delay()
    assert memory_app.broker.queue_sizes() == {"billing": 1, "emails": 1}


def test_custom_task_base_class_and_hooks(memory_app):
    events = []

    class Tracked(Task):
        def on_success(self, retval, task_id, args, kwargs):
            events.append(("success", retval))

        def on_failure(self, exc, task_id, args, kwargs, einfo):
            events.append(("failure", type(exc).__name__))

        def after_return(self, status, retval, task_id, args, kwargs, einfo):
            events.append(("after", status))

    @memory_app.task(base=Tracked)
    def ok():
        return 1

    @memory_app.task(base=Tracked)
    def bad():
        raise ValueError

    ok.delay()
    bad.delay()
    drain(memory_app)
    assert events == [("success", 1), ("after", "SUCCESS"), ("failure", "ValueError"), ("after", "FAILURE")]


def test_signals(memory_app):
    from potatoq import signals

    seen = []

    @signals.task_prerun.connect
    def prerun(sender=None, task_id=None, **kwargs):
        seen.append(("prerun", sender.name))

    @signals.task_success.connect
    def success(sender=None, result=None, **kwargs):
        seen.append(("success", result))

    try:

        @memory_app.task
        def add(x, y):
            return x + y

        add.delay(1, 1)
        drain(memory_app)
        assert seen == [("prerun", add.name), ("success", 2)]
    finally:
        signals.task_prerun.disconnect(prerun)
        signals.task_success.disconnect(success)


def test_eager_mode_still_serializes(memory_app):
    memory_app.conf.task_always_eager = True

    @memory_app.task
    def echo(value):
        return value

    assert echo.delay({"a": 1}).get() == {"a": 1}
    with pytest.raises(TypeError):
        echo.delay(object())


def test_eager_failure_propagates(memory_app):
    memory_app.conf.task_always_eager = True

    @memory_app.task
    def boom():
        raise ValueError("eager")

    with pytest.raises(ValueError):
        boom.delay()
    memory_app.conf.task_eager_propagates = False
    assert boom.delay().state == states.FAILURE


def test_get_inside_task_is_refused(memory_app):
    @memory_app.task
    def inner():
        return 1

    @memory_app.task
    def outer():
        return inner.delay().get(timeout=1)

    result = outer.delay()
    drain(memory_app)
    with pytest.raises(RuntimeError, match=r"Never call result\.get"):
        result.get()


def test_async_task(memory_app):
    import asyncio

    @memory_app.task
    async def fetch(x):
        await asyncio.sleep(0)
        return x * 2

    result = fetch.delay(21)
    drain(memory_app)
    assert result.get() == 42


def test_async_task_soft_time_limit(memory_app):
    import asyncio

    @memory_app.task(soft_time_limit=0.05, time_limit=5)
    async def slow():
        await asyncio.sleep(1)

    result = slow.delay()
    drain(memory_app)
    with pytest.raises(SoftTimeLimitExceeded):
        result.get()


@pytest.mark.parametrize(
    "settings,expected",
    [
        ({"CELERY_BROKER_URL": "redis://x"}, ("broker_url", "redis://x")),
        ({"CELERY_TASK_ALWAYS_EAGER": True}, ("task_always_eager", True)),
        ({"CELERY_BEAT_SCHEDULE": {"a": 1}}, ("beat_schedule", {"a": 1})),
        ({"CELERY_RESULT_BACKEND": "redis://y"}, ("result_backend", "redis://y")),
    ],
)
def test_config_from_object_with_celery_namespace(settings, expected):
    class Settings:
        pass

    for key, value in settings.items():
        setattr(Settings, key, value)
    app = Potatoq("cfg", set_as_current=False)
    app.config_from_object(Settings, namespace="CELERY")
    assert app.conf[expected[0]] == expected[1]


def test_old_style_uppercase_settings():
    app = Potatoq("old", set_as_current=False)
    app.config_from_object({"BROKER_URL": "redis://z", "CELERYD_CONCURRENCY": 3, "CELERY_ALWAYS_EAGER": True})
    assert app.conf.broker_url == "redis://z"
    assert app.conf.worker_concurrency == 3
    assert app.conf.task_always_eager is True


def test_obsolete_celery_settings_are_accepted():
    app = Potatoq("obsolete", set_as_current=False)
    app.conf.update(
        broker_connection_retry_on_startup=True,
        worker_prefetch_multiplier=1,
        task_serializer="json",
        accept_content=["json"],
    )
    assert app.conf.task_serializer == "json"


def test_add_periodic_task_via_on_after_configure(memory_app):
    from potatoq import crontab

    @memory_app.task
    def report():
        pass

    @memory_app.on_after_configure.connect
    def setup_periodic_tasks(sender, **kwargs):
        sender.add_periodic_task(10.0, report.s(), name="every 10s")
        sender.add_periodic_task(crontab(minute=0), report.s(), name="hourly", expires=60)

    memory_app.loader_import_default_modules()
    schedule = memory_app.conf.beat_schedule
    assert schedule["every 10s"]["task"] == report.name
    assert schedule["hourly"]["options"] == {"expires": 60}


def test_scheduler_enqueues_due_entries_once(memory_app):
    from datetime import UTC, datetime, timedelta

    from potatoq.worker.scheduler import Scheduler

    @memory_app.task
    def report():
        pass

    memory_app.conf.beat_schedule = {"r": {"task": report.name, "schedule": 60.0}}
    scheduler = Scheduler(memory_app)
    start = datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC)
    scheduler.last_check = start
    assert scheduler.tick(start + timedelta(seconds=40)) == 1
    other = Scheduler(memory_app)  # a second worker's scheduler
    other.last_check = start
    assert other.tick(start + timedelta(seconds=40)) == 0
    assert memory_app.broker.queue_sizes() == {"default": 1}


def test_replace(memory_app):
    @memory_app.task
    def final(x):
        return x * 10

    @memory_app.task(bind=True)
    def first(self, x):
        self.replace(final.s(x + 1))

    @memory_app.task
    def after(x):
        return x + 1

    result = (first.s(1) | after.s()).delay()
    drained = drain(memory_app)
    assert [d.name.rsplit(".", 1)[-1] for d in drained] == ["first", "final", "after"]
    assert result.get() == 21


def test_get_task_logger_import_path():
    from potatoq.utils.log import get_task_logger

    assert get_task_logger(__name__).name == f"potatoq.task.{__name__}"
