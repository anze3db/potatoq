"""Broker registry: picks the implementation from the URL scheme."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

from ..exceptions import ImproperlyConfigured

if TYPE_CHECKING:
    from .base import Broker

_SCHEMES = {
    "redis": "redis:RedisBroker",
    "rediss": "redis:RedisBroker",
    "valkey": "redis:RedisBroker",
    "valkeys": "redis:RedisBroker",
    "redis+socket": "redis:RedisBroker",
    "unix": "redis:RedisBroker",
    "amqp": "rabbitmq:RabbitMQBroker",
    "amqps": "rabbitmq:RabbitMQBroker",
    "pyamqp": "rabbitmq:RabbitMQBroker",
    "postgres": "postgres:PostgresBroker",
    "postgresql": "postgres:PostgresBroker",
    "postgresql+psycopg": "postgres:PostgresBroker",
    "postgresql+psycopg2": "postgres:PostgresBroker",
    "sqla+postgresql": "postgres:PostgresBroker",
    "sqlite": "sqlite:SQLiteBroker",
    "sqla+sqlite": "sqlite:SQLiteBroker",
    "memory": "memory:MemoryBroker",
}

_EXTRAS = {"redis": "redis", "rabbitmq": "rabbitmq", "postgres": "postgres"}


def broker_for_url(url: str) -> type[Broker]:
    scheme = url.split("://", 1)[0].lower() if "://" in url else url.lower()
    target = _SCHEMES.get(scheme)
    if target is None:
        raise ImproperlyConfigured(
            f"Unsupported broker URL {url!r}. Use redis://, amqp://, postgresql://, sqlite:// or memory://"
        )
    module_name, cls_name = target.split(":")
    try:
        module = import_module(f".{module_name}", __name__)
    except ImportError as exc:
        extra = _EXTRAS.get(module_name, module_name)
        raise ImproperlyConfigured(
            f"The {module_name} broker needs extra dependencies: pip install 'potatoq[{extra}]' ({exc})"
        ) from exc
    return getattr(module, cls_name)
