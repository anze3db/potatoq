"""Signals, compatible with ``celery.signals``.

Receivers are connected with ``@signal.connect`` (optionally ``sender=...``) and are
called with ``sender`` plus signal-specific keyword arguments. A receiver that raises
is logged and does not break the worker, matching Celery.
"""

from __future__ import annotations

import logging
import threading
import weakref
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("potatoq.signals")

__all__ = [
    "Signal",
    "after_setup_logger",
    "after_task_publish",
    "beat_init",
    "before_task_publish",
    "import_modules",
    "setup_logging",
    "task_failure",
    "task_internal_error",
    "task_postrun",
    "task_prerun",
    "task_received",
    "task_rejected",
    "task_retry",
    "task_revoked",
    "task_success",
    "task_unknown",
    "worker_init",
    "worker_process_init",
    "worker_process_shutdown",
    "worker_ready",
    "worker_shutdown",
    "worker_shutting_down",
]


def _make_ref(receiver: Callable[..., Any], weak: bool) -> Any:
    if not weak:
        return lambda: receiver
    if hasattr(receiver, "__self__") and hasattr(receiver, "__func__"):
        return weakref.WeakMethod(receiver)  # type: ignore[arg-type]
    return weakref.ref(receiver)


class Signal:
    def __init__(self, name: str | None = None, providing_args: Any = None, use_caching: bool = False):
        self.name = name or "Signal"
        self._receivers: list[tuple[Any, Any, Any]] = []  # (lookup_key, sender, ref)
        self._lock = threading.Lock()

    def connect(
        self,
        receiver: Callable[..., Any] | None = None,
        *,
        sender: Any = None,
        weak: bool = False,
        dispatch_uid: Any = None,
    ) -> Any:
        """Connect a receiver. Works as ``sig.connect(fn)``, ``@sig.connect`` or ``@sig.connect(sender=X)``."""

        def _connect(fn: Callable[..., Any]) -> Callable[..., Any]:
            key = (dispatch_uid or id(fn), id(sender) if sender is not None else None)
            with self._lock:
                if not any(k == key for k, _, _ in self._receivers):
                    self._receivers.append((key, sender, _make_ref(fn, weak)))
            return fn

        if receiver is None:
            return _connect
        return _connect(receiver)

    def disconnect(
        self, receiver: Callable[..., Any] | None = None, *, sender: Any = None, dispatch_uid: Any = None
    ) -> bool:
        key = (dispatch_uid or id(receiver), id(sender) if sender is not None else None)
        with self._lock:
            before = len(self._receivers)
            self._receivers = [r for r in self._receivers if r[0] != key]
            return len(self._receivers) != before

    def _live_receivers(self, sender: Any) -> list[Callable[..., Any]]:
        live = []
        dead = False
        for _, rsender, ref in self._receivers:
            fn = ref()
            if fn is None:
                dead = True
                continue
            if rsender is None or rsender is sender or rsender == sender:
                live.append(fn)
            elif isinstance(rsender, str) and getattr(sender, "name", None) == rsender:
                live.append(fn)
        if dead:
            with self._lock:
                self._receivers = [r for r in self._receivers if r[2]() is not None]
        return live

    @property
    def receivers(self) -> list[Any]:
        return list(self._receivers)

    def has_receivers(self) -> bool:
        return bool(self._receivers)

    def send(self, sender: Any = None, **named: Any) -> list[tuple[Callable[..., Any], Any]]:
        if not self._receivers:
            return []
        responses = []
        for fn in self._live_receivers(sender):
            try:
                responses.append((fn, fn(signal=self, sender=sender, **named)))
            except Exception as exc:
                logger.exception("Signal handler %r for %s raised: %r", fn, self.name, exc)
                responses.append((fn, exc))
        return responses

    send_robust = send

    def __repr__(self) -> str:
        return f"<Signal: {self.name}>"


before_task_publish = Signal("before_task_publish")
after_task_publish = Signal("after_task_publish")
task_received = Signal("task_received")
task_prerun = Signal("task_prerun")
task_postrun = Signal("task_postrun")
task_success = Signal("task_success")
task_failure = Signal("task_failure")
task_retry = Signal("task_retry")
task_revoked = Signal("task_revoked")
task_internal_error = Signal("task_internal_error")
task_unknown = Signal("task_unknown")
task_rejected = Signal("task_rejected")
worker_init = Signal("worker_init")
worker_ready = Signal("worker_ready")
worker_shutting_down = Signal("worker_shutting_down")
worker_shutdown = Signal("worker_shutdown")
worker_process_init = Signal("worker_process_init")
worker_process_shutdown = Signal("worker_process_shutdown")
beat_init = Signal("beat_init")
import_modules = Signal("import_modules")
setup_logging = Signal("setup_logging")
after_setup_logger = Signal("after_setup_logger")
