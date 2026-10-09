"""Django integration: zero-config broker, transactional enqueue, settings."""

from __future__ import annotations

import pytest


def jobs(app):
    return app.broker.conn.execute("SELECT task FROM potatoq_jobs ORDER BY seq").fetchall()


def test_zero_config_uses_django_database(django_env):
    from django.conf import settings

    app = django_env
    assert app.conf.broker_url == "sqlite:///" + str(settings.DATABASES["default"]["NAME"])
    assert app.conf.task_default_queue == "shop"  # POTATOQ dict
    assert app.conf.timezone == "Europe/Ljubljana"  # from TIME_ZONE
    assert app.conf.task_acks_late is True


def test_delay_inside_atomic_joins_the_transaction(django_env):
    from django.db import connection, transaction
    from djangoproj.shop.tasks import send_receipt

    app = django_env
    app.broker.purge("shop")
    with transaction.atomic():
        with connection.cursor() as cur:
            cur.execute("INSERT INTO shop_order (id, total) VALUES (1, 100)")
        send_receipt.delay(1)
        # Written through Django's connection: invisible to others until COMMIT.
        assert app.broker.queue_sizes() == {}
    assert app.broker.queue_sizes() == {"shop": 1}

    from potatoq.testing import drain

    drained = drain(app, ["shop"])
    assert drained[0].result == {"order": 1, "total": 100}


def test_rollback_discards_task(django_env):
    from django.db import transaction
    from djangoproj.shop.tasks import send_receipt

    app = django_env
    app.broker.purge("shop")
    with pytest.raises(RuntimeError), transaction.atomic():
        send_receipt.delay(2)
        raise RuntimeError("rollback")
    assert app.broker.queue_sizes() == {}


def test_enqueue_on_commit_opt_out_and_other_broker(django_env, tmp_path):
    from django.db import transaction
    from djangoproj.shop.tasks import audit

    from potatoq import Potatoq
    from potatoq.contrib.django import install

    other = Potatoq("other", broker="memory://")
    install(other)
    task = other._task_from_fun(audit.__wrapped__, name="audit2", enqueue_on_commit=False)
    deferred = other._task_from_fun(audit.__wrapped__, name="deferred", enqueue_on_commit=None)
    with transaction.atomic():
        task.delay("now")  # opted out: sent immediately
        deferred.delay("later")  # different broker: sent on commit
        assert other.broker.queue_sizes() == {"default": 1}
    assert other.broker.queue_sizes() == {"default": 2}
    with pytest.raises(RuntimeError), transaction.atomic():
        deferred.delay("never")
        raise RuntimeError
    assert other.broker.queue_sizes() == {"default": 2}


def test_separate_queue_database_waits_for_the_default_transaction(django_env, tmp_path, monkeypatch):
    """POTATOQ_DATABASE = "queue": the broker is another database, but the transaction
    that matters is the caller's (on "default"), so delay() waits for its COMMIT."""
    from django.conf import settings
    from django.db import connection, connections, transaction

    from potatoq import Potatoq
    from potatoq.contrib.django import DjangoTransactionHook, database_url

    monkeypatch.setitem(
        connections.databases,
        "queue",
        {**connections.databases["default"], "NAME": str(tmp_path / "queue.db")},
    )
    monkeypatch.setattr(settings, "POTATOQ_DATABASE", "queue", raising=False)
    app = Potatoq("queue-db", broker=database_url(), set_as_current=False)
    app.add_transaction_hook(DjangoTransactionHook())
    try:
        app.broker.setup()
        task = app._task_from_fun(lambda order_id: order_id, name="queue_db_receipt")
        with pytest.raises(RuntimeError), transaction.atomic():
            with connection.cursor() as cur:
                cur.execute("INSERT INTO shop_order (id, total) VALUES (10, 1)")
            task.delay(10)
            assert app.broker.queue_sizes() == {}  # not before COMMIT
            raise RuntimeError("rollback")
        assert app.broker.queue_sizes() == {}  # and never after a ROLLBACK
        with transaction.atomic():
            task.delay(11)
            assert app.broker.queue_sizes() == {}
        assert app.broker.queue_sizes() == {"default": 1}
        # A transaction on the queue alias itself is the broker's database: joined.
        with transaction.atomic(using="queue"):
            task.apply_async((12,), using="queue")
            assert app.broker.queue_sizes() == {"default": 1}  # written, but not committed
        assert app.broker.queue_sizes() == {"default": 2}
    finally:
        connections["queue"].close()
        del connections["queue"]
        app.close()


def test_management_command(django_env, capsys):
    from django.core.management import call_command

    call_command("potatoq", "queues")
    assert capsys.readouterr().out


def test_django_objects_are_rejected_with_a_helpful_message(django_env):
    """Models and querysets never go over the wire (no pickle): passing one fails at
    .delay() time, in the web process, with advice on what to pass instead."""
    from django.contrib.contenttypes.models import ContentType
    from django.core.management import call_command
    from django.utils.translation import gettext_lazy

    from potatoq import Potatoq

    call_command("migrate", "contenttypes", verbosity=0)
    app = Potatoq("ser", broker="memory://", set_as_current=False)

    @app.task
    def handle(obj):
        return obj

    instance = ContentType.objects.first()
    with pytest.raises(TypeError, match=r"Django model instance.*primary key \(obj\.pk\)"):
        handle.delay(instance)
    with pytest.raises(TypeError, match=r"Django queryset.*values_list"):
        handle.delay(ContentType.objects.all())
    assert app.broker.queue_sizes() == {}  # nothing was enqueued
    handle.delay(gettext_lazy("Hello"))  # lazy strings are fine: sent as plain text
    handle.delay(instance.pk)  # what you should pass instead
    assert app.broker.queue_sizes() == {"default": 2}


def test_broker_follows_the_test_database(django_env, tmp_path, monkeypatch):
    """Django's test runner renames the database after potatoq was configured; tasks
    must go to the test database, not the real one a dev worker may be consuming."""
    from django.db import connections
    from djangoproj.shop.tasks import send_receipt

    app = django_env
    destroyed = []
    creation = connections["default"].creation
    monkeypatch.setattr(creation, "destroy_test_db", lambda *a, **k: destroyed.append(app._broker))
    monkeypatch.setattr(app._broker_follows, "_hooked", False)
    settings_dict = connections.databases["default"]
    real = settings_dict["NAME"]
    try:
        settings_dict["NAME"] = str(tmp_path / "test_db.sqlite3")
        connections["default"].close()  # as the test runner does when it switches
        send_receipt.delay(1)
        assert app.broker.url == f"sqlite:///{tmp_path}/test_db.sqlite3"
        assert [r[0] for r in jobs(app)] == ["djangoproj.shop.tasks.send_receipt"]

        # Before the test runner drops the test database, potatoq lets go of it
        # (Postgres refuses to drop a database with other sessions).
        creation.destroy_test_db(":memory:", verbosity=0)
        assert destroyed == [None]

        settings_dict["NAME"] = "file:memorydb_default?mode=memory&cache=shared"
        assert app.broker.url == "memory://"
        assert app.backend is app.broker
    finally:
        settings_dict["NAME"] = real
        connections["default"].close()
    assert app.broker.url == f"sqlite:///{real}"


def test_worker_imports_the_urlconf_only_when_asked(django_env, monkeypatch, caplog):
    import types

    from django.conf import settings

    from potatoq import signals

    app = django_env
    worker = types.SimpleNamespace(app=app)
    name = "djangoproj.shop.views.refresh_preview"
    monkeypatch.setattr(settings, "ROOT_URLCONF", "djangoproj.shop.views", raising=False)  # as urls.py would
    signals.worker_init.send(sender=worker)
    assert name not in app.tasks  # by default only tasks.py modules are discovered

    monkeypatch.setitem(app.conf, "worker_import_urlconf", True)
    signals.worker_init.send(sender=None)  # not a worker: nothing to do
    assert name not in app.tasks
    signals.worker_init.send(sender=worker)
    assert name in app.tasks

    monkeypatch.setattr(settings, "ROOT_URLCONF", "djangoproj.missing_urls")
    signals.worker_init.send(sender=worker)
    assert "Could not import ROOT_URLCONF 'djangoproj.missing_urls'" in caplog.text

    monkeypatch.setattr(settings, "ROOT_URLCONF", None)
    signals.worker_init.send(sender=worker)  # nothing to import


def test_eager_task_inside_atomic_keeps_the_callers_connection(django_env):
    from django.db import connection, transaction
    from djangoproj.shop.tasks import send_receipt

    app = django_env
    app.conf.task_always_eager = True
    try:
        with transaction.atomic():
            assert send_receipt.delay(1).get()["order"] == 1
            with connection.cursor() as cur:  # still open: the task ran in our transaction
                cur.execute("SELECT 1")
    finally:
        app.conf.task_always_eager = False


def test_eager_delay_on_commit_waits_for_the_commit_like_celery(django_env):
    from django.db import transaction
    from djangoproj.shop.tasks import audit

    from potatoq import signals

    app = django_env
    ran = []

    def record(sender=None, args=None, **kwargs):
        ran.append(args)

    signals.task_prerun.connect(record)
    app.conf.task_always_eager = True
    try:
        with transaction.atomic():
            audit.delay_on_commit("created")
            assert ran == []  # not inside the transaction
        assert ran == [["created"]]
        with transaction.atomic():
            audit.delay_on_commit("rolled back")
            transaction.set_rollback(True)
        assert ran == [["created"]]
    finally:
        app.conf.task_always_eager = False
        signals.task_prerun.disconnect(record)


def test_delay_without_a_transaction_goes_through_djangos_connection(django_env, monkeypatch):
    """So tests use the test database, and pytest-django's guard ("Database access not
    allowed") covers tasks too instead of queueing into the development database."""
    from django.db.backends.base.base import BaseDatabaseWrapper
    from djangoproj.shop.tasks import send_receipt

    app = django_env
    before = len(jobs(app))
    send_receipt.delay(1)
    assert len(jobs(app)) == before + 1  # committed right away

    def blocked(self):
        raise RuntimeError('Database access not allowed, use the "django_db" mark')

    monkeypatch.setattr(BaseDatabaseWrapper, "ensure_connection", blocked)  # what pytest-django does
    with pytest.raises(RuntimeError, match="Database access not allowed"):
        send_receipt.delay(2)
    monkeypatch.undo()
    assert len(jobs(app)) == before + 1  # nothing slipped through potatoq's own connection
    app.broker.purge("shop")


def test_delay_from_async_code_uses_potatoqs_connection(django_env):
    """Django refuses its connection in async code, so .delay() there keeps working."""
    import asyncio

    from djangoproj.shop.tasks import send_receipt

    app = django_env
    before = len(jobs(app))

    async def view():
        send_receipt.delay(3)

    asyncio.run(view())
    assert len(jobs(app)) == before + 1
    app.broker.purge("shop")
