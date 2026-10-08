from __future__ import annotations

import os
import socket
import uuid
from collections.abc import Iterator

import pytest

from potatoq import Potatoq

POSTGRES_URL = os.environ.get("POTATOQ_TEST_POSTGRES", "postgresql://localhost/potatoq_test")
REDIS_URL = os.environ.get("POTATOQ_TEST_REDIS", "redis://localhost:6379/15")
RABBITMQ_URL = os.environ.get("POTATOQ_TEST_RABBITMQ", "amqp://guest:guest@localhost:5672//")


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def postgres_available() -> bool:
    try:
        import psycopg

        psycopg.connect(POSTGRES_URL, connect_timeout=2).close()
        return True
    except Exception:
        return False


def redis_available() -> bool:
    return _port_open("localhost", 6379)


def rabbitmq_available() -> bool:
    return _port_open("localhost", 5672)


BROKERS = ["memory", "sqlite", "postgres", "redis", "rabbitmq"]


def make_app(kind: str, tmp_path, *, results: bool = True, **conf) -> tuple[Potatoq, callable]:
    """Create an isolated app for ``kind``; returns (app, cleanup)."""
    token = uuid.uuid4().hex[:8]
    cleanup = lambda: None  # noqa: E731
    queue = f"q{token}"
    options: dict = {}
    if kind == "memory":
        broker = "memory://"
    elif kind == "sqlite":
        broker = f"sqlite:///{tmp_path}/broker-{token}.db"
    elif kind == "postgres":
        if not postgres_available():
            pytest.skip("Postgres not available")
        broker = POSTGRES_URL
        schema = f"test_{token}"
        options = {"schema": schema}

        def cleanup() -> None:
            import psycopg

            with psycopg.connect(POSTGRES_URL, autocommit=True) as conn:
                conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

    elif kind == "redis":
        if not redis_available():
            pytest.skip("Redis not available")
        broker = REDIS_URL
        prefix = f"ptest-{token}"
        options = {"global_keyprefix": prefix}

        def cleanup() -> None:
            import redis

            client = redis.Redis.from_url(REDIS_URL)
            keys = list(client.scan_iter(f"{prefix}:*", count=1000))
            if keys:
                client.delete(*keys)

    elif kind == "rabbitmq":
        if not rabbitmq_available():
            pytest.skip("RabbitMQ not available")
        broker = RABBITMQ_URL
        if results and not redis_available():
            pytest.skip("RabbitMQ tests need Redis for results")
        prefix = f"ptest-{token}"

        def cleanup() -> None:
            import pika
            import redis

            conn = pika.BlockingConnection(pika.URLParameters("amqp://guest:guest@localhost:5672/%2F"))
            ch = conn.channel()
            for name in (queue, f"{queue}.dlq", "other" + token, f"other{token}.dlq"):
                try:
                    ch.queue_delete(name)
                except Exception:
                    ch = conn.channel()
            conn.close()
            client = redis.Redis.from_url(REDIS_URL)
            keys = list(client.scan_iter(f"{prefix}:*", count=1000))
            if keys:
                client.delete(*keys)

    else:  # pragma: no cover
        raise ValueError(kind)
    app = Potatoq(f"test_{kind}", broker=broker, set_as_current=True)
    app.conf.task_default_queue = queue
    app.conf.broker_transport_options = options
    app.conf.worker_heartbeat_interval = 0.5
    if kind == "rabbitmq" and results:
        app.conf.result_backend = REDIS_URL
        app.conf.result_backend_transport_options = {"global_keyprefix": prefix}
    elif results:
        app.conf.result_backend = "broker"
    for key, value in conf.items():
        app.conf[key] = value
    app.test_token = token  # type: ignore[attr-defined]
    return app, cleanup


@pytest.fixture(params=BROKERS)
def broker_app(request, tmp_path) -> Iterator[Potatoq]:
    app, cleanup = make_app(request.param, tmp_path)
    app.kind = request.param  # type: ignore[attr-defined]
    try:
        yield app
    finally:
        app.close()
        cleanup()


@pytest.fixture
def memory_app() -> Iterator[Potatoq]:
    app = Potatoq("tests", broker="memory://", set_as_current=True)
    app.conf.result_backend = "broker"
    yield app
    app.close()


TESTS = __import__("pathlib").Path(__file__).parent


@pytest.fixture(scope="session")
def django_session(tmp_path_factory):
    """Django configured once per test run (tests/djangoproj), like a fresh project."""
    import sys

    sys.path.insert(0, str(TESTS))
    os.environ["DJANGO_SETTINGS_MODULE"] = "djangoproj.settings"
    os.environ["TEST_DJANGO_DB"] = str(tmp_path_factory.mktemp("dj") / "db.sqlite3")
    import django

    from potatoq import app as app_module

    # No app created explicitly: potatoq.contrib.django configures the default one.
    app_module._current_app = None
    app_module._default_app = None
    django.setup()
    from django.db import connection

    with connection.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS shop_order (id integer primary key, total integer)")
    app = app_module.current_app()
    yield app
    os.environ.pop("DJANGO_SETTINGS_MODULE", None)


@pytest.fixture
def django_env(django_session):
    """The Django project's potatoq app, made current again for this test."""
    from django.tasks import task_backends

    django_session.set_current()
    task_backends["default"]._app = None
    return django_session
