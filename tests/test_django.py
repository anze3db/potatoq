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


def test_management_command(django_env, capsys):
    from django.core.management import call_command

    call_command("potatoq", "queues")
    assert capsys.readouterr().out
