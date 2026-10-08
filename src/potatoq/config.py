"""Configuration with production-ready defaults.

Setting names match Celery's lowercase settings so existing configuration keeps
working. Old uppercase ``CELERY_*`` names and the Django ``namespace="CELERY"`` style
are translated. Settings that only existed to work around Celery defaults are
accepted and ignored (see ``OBSOLETE``).
"""

from __future__ import annotations

import importlib
import logging
import os
from collections.abc import Iterator, Mapping, MutableMapping
from typing import Any

logger = logging.getLogger("potatoq.config")

#: Every default here is a deliberate choice. See docs/defaults.md for the reasoning.
DEFAULTS: dict[str, Any] = {
    # --- broker / results -------------------------------------------------------
    "broker_url": None,  # falls back to $POTATOQ_BROKER_URL, Django's database, then sqlite
    "result_backend": None,  # None = use the broker if it can store results
    "result_expires": 24 * 60 * 60,  # seconds
    # None = store results when they are free (database brokers, where the result is
    # written in the same transaction as the ack) or when result_backend is set.
    "task_ignore_result": None,
    "broker_transport_options": {},
    "broker_connection_timeout": 10.0,
    # --- task execution ---------------------------------------------------------
    "task_default_queue": "default",
    "task_default_priority": 0,
    "task_routes": {},
    "task_acks_late": True,  # at-least-once: ack after the task finishes
    "task_reject_on_worker_lost": True,  # redeliver when the process dies (bounded below)
    "task_max_deliveries": 5,  # poison-message guard: dead-letter after N crashed deliveries
    "task_time_limit": 30 * 60,  # hard limit, seconds; None disables
    "task_soft_time_limit": None,  # defaults to a bit before the hard limit
    "task_default_retry_delay": 180,  # used only when backoff is disabled
    "task_max_retries": 3,  # for self.retry()/autoretry_for, Celery compatible
    "task_retry_backoff": 10,  # exponential: ~10s, 20s, 40s ... (full jitter)
    "task_retry_backoff_max": 600,
    "task_retry_jitter": True,
    "task_dead_letter_failures": True,  # keep terminally failed tasks for inspection/replay
    "task_enqueue_on_commit": True,  # inside a DB transaction, enqueue when it commits
    "dead_letter_max": 10_000,
    "dead_letter_ttl": 30 * 24 * 60 * 60,
    "task_track_started": False,  # DB brokers report STARTED for free anyway
    "task_always_eager": False,
    "task_eager_propagates": True,
    "task_store_eager_result": False,
    "task_publish_retry": True,
    "task_annotations": None,
    # --- worker -----------------------------------------------------------------
    "worker_concurrency": None,  # processes; None = number of CPUs
    "worker_threads": 1,  # task threads per process (I/O-bound workloads); see docs/guide/workers.md
    "worker_prefetch_multiplier": 1,  # each idle process fetches exactly one task
    "worker_max_tasks_per_child": 1000,  # recycle processes to contain memory leaks
    "worker_max_memory_per_child": None,  # KiB (Celery compatible) or "512MB"
    "worker_shutdown_timeout": 25.0,  # seconds to finish running tasks on SIGTERM
    "worker_heartbeat_interval": 5.0,
    "worker_dead_after": 60.0,  # a worker silent this long is dead; its tasks are recovered
    "worker_hijack_root_logger": False,
    "worker_log_format": "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s",
    "worker_task_log_format": ("[%(asctime)s: %(levelname)s/%(processName)s] %(task_name)s[%(task_id)s]: %(message)s"),
    "worker_enable_scheduler": True,  # every worker runs the (deduplicated) scheduler
    # --- scheduling -------------------------------------------------------------
    "beat_schedule": {},
    "timezone": "UTC",
    "enable_utc": True,
    # --- misc -------------------------------------------------------------------
    "imports": (),
    "include": (),
    "task_create_missing_queues": True,
    "database_auto_create_schema": True,
}

#: Celery settings that are unnecessary in Potatoq; accepted silently.
OBSOLETE = frozenset(
    {
        "accept_content",
        "task_serializer",
        "result_serializer",
        "result_accept_content",
        "broker_connection_retry",
        "broker_connection_retry_on_startup",
        "broker_connection_max_retries",
        "broker_pool_limit",
        "broker_heartbeat",
        "broker_channel_error_retry",
        "worker_cancel_long_running_tasks_on_connection_loss",
        "worker_send_task_events",
        "task_send_sent_event",
        "worker_enable_remote_control",
        "worker_disable_rate_limits",
        "result_extended",
        "result_persistent",
        "event_queue_expires",
        "event_queue_ttl",
        "task_acks_on_failure_or_timeout",
        "worker_pool",
        "worker_pool_restarts",
        "broker_use_ssl",
        "redis_backend_health_check_interval",
        "redis_socket_keepalive",
        "task_queue_max_priority",
        "task_default_exchange",
        "task_default_exchange_type",
        "task_default_routing_key",
        "task_queues",
        "worker_direct",
        "worker_lost_wait",
        "result_chord_join_timeout",
        "result_backend_always_retry",
        "beat_scheduler",
        "beat_max_loop_interval",
        "beat_schedule_filename",
        "worker_proc_alive_timeout",
    }
)

#: Old (Celery 3) uppercase names -> new names, for the ones that differ.
_OLD_NAMES = {
    "BROKER_URL": "broker_url",
    "CELERY_RESULT_BACKEND": "result_backend",
    "CELERY_TASK_RESULT_EXPIRES": "result_expires",
    "CELERY_ALWAYS_EAGER": "task_always_eager",
    "CELERY_EAGER_PROPAGATES_EXCEPTIONS": "task_eager_propagates",
    "CELERY_IGNORE_RESULT": "task_ignore_result",
    "CELERY_ACKS_LATE": "task_acks_late",
    "CELERYD_CONCURRENCY": "worker_concurrency",
    "CELERYD_MAX_TASKS_PER_CHILD": "worker_max_tasks_per_child",
    "CELERYD_PREFETCH_MULTIPLIER": "worker_prefetch_multiplier",
    "CELERYD_TASK_TIME_LIMIT": "task_time_limit",
    "CELERYD_TASK_SOFT_TIME_LIMIT": "task_soft_time_limit",
    "CELERYBEAT_SCHEDULE": "beat_schedule",
    "CELERY_TIMEZONE": "timezone",
    "CELERY_ROUTES": "task_routes",
    "CELERY_DEFAULT_QUEUE": "task_default_queue",
    "CELERY_IMPORTS": "imports",
    "CELERY_INCLUDE": "include",
}


def normalize_key(key: str, namespace: str | None = None) -> str | None:
    """Map a user supplied setting name to a Potatoq setting name (or None to skip)."""
    if namespace:
        prefix = namespace.rstrip("_") + "_"
        if not key.startswith(prefix):
            return None
        key = key[len(prefix) :]
        old = _OLD_NAMES.get(f"CELERY_{key}") or _OLD_NAMES.get(key)
        return old or key.lower()
    if key in _OLD_NAMES:
        return _OLD_NAMES[key]
    for prefix in ("POTATOQ_", "CELERY_", "CELERYD_", "CELERYBEAT_"):
        if key.startswith(prefix):
            return key[len(prefix) :].lower()
    if key.islower():
        return key
    return None


class Settings(MutableMapping[str, Any]):
    """``app.conf``: a dict with attribute access and Celery-compatible helpers."""

    def __init__(self, initial: Mapping[str, Any] | None = None):
        object.__setattr__(self, "_data", dict(DEFAULTS))
        object.__setattr__(self, "_changed", set())
        if initial:
            self.update(initial)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        normalized = normalize_key(key) or key
        if normalized in OBSOLETE:
            logger.debug("Ignoring setting %r: not needed with Potatoq", key)
        self._data[normalized] = value
        self._changed.add(normalized)

    def __delitem__(self, key: str) -> None:
        del self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __getattr__(self, key: str) -> Any:
        try:
            return self._data[key]
        except KeyError:
            raise AttributeError(key) from None

    def __setattr__(self, key: str, value: Any) -> None:
        self[key] = value

    def changed(self) -> dict[str, Any]:
        return {k: self._data[k] for k in self._changed}

    def update_from_mapping(self, mapping: Mapping[str, Any], namespace: str | None = None) -> None:
        for key, value in mapping.items():
            new_key = normalize_key(key, namespace)
            if new_key is not None:
                self[new_key] = value

    def update_from_object(self, obj: Any, namespace: str | None = None) -> None:
        if isinstance(obj, str):
            obj = load_object(obj)
        if isinstance(obj, Mapping):
            self.update_from_mapping(obj, namespace)
            return
        self.update_from_mapping({k: getattr(obj, k) for k in dir(obj) if not k.startswith("_")}, namespace)

    def get_env(self, *names: str) -> str | None:
        for name in names:
            value = os.environ.get(name)
            if value:
                return value
        return None


def load_object(path: str) -> Any:
    """Import ``"pkg.module:attr"`` or ``"pkg.module.attr"`` or ``"pkg.module"``."""
    if ":" in path:
        module_name, attr = path.split(":", 1)
        obj: Any = importlib.import_module(module_name)
        for part in attr.split("."):
            obj = getattr(obj, part)
        return obj
    try:
        return importlib.import_module(path)
    except ImportError:
        module_name, _, attr = path.rpartition(".")
        if not module_name:
            raise
        return getattr(importlib.import_module(module_name), attr)
