"""Logging helpers: ``get_task_logger`` and a formatter that knows the current task."""

from __future__ import annotations

import logging
import sys
from typing import Any

__all__ = ["TaskFormatter", "get_logger", "get_task_logger", "setup_logging"]

_TASK_LOGGER_ROOT = "potatoq.task"


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def get_task_logger(name: str) -> logging.Logger:
    """A logger whose records include ``task_id`` and ``task_name`` (Celery compatible)."""
    if name in (_TASK_LOGGER_ROOT, "celery.task"):
        return logging.getLogger(_TASK_LOGGER_ROOT)
    logger = logging.getLogger(f"{_TASK_LOGGER_ROOT}.{name}")
    return logger


class TaskContextFilter(logging.Filter):
    """Adds ``task_id`` / ``task_name`` attributes to every record."""

    def filter(self, record: logging.LogRecord) -> bool:
        from .app import _state

        task = getattr(_state, "current_task", None)
        request = task.request if task is not None else None
        record.task_id = getattr(request, "id", None) or "???"
        record.task_name = getattr(task, "name", None) or "???"
        return True


class TaskFormatter(logging.Formatter):
    """Uses the task format for records emitted while a task runs."""

    def __init__(self, fmt: str | None = None, task_fmt: str | None = None, **kwargs: Any):
        super().__init__(fmt, **kwargs)
        self._task_formatter = logging.Formatter(task_fmt, **kwargs) if task_fmt else None

    def format(self, record: logging.LogRecord) -> str:
        if self._task_formatter is not None and getattr(record, "task_id", "???") != "???":
            return self._task_formatter.format(record)
        return super().format(record)


def setup_logging(app: Any, loglevel: str | int = "INFO", logfile: str | None = None) -> None:
    """Configure logging for the worker without clobbering an existing setup.

    Celery replaces the root logger's handlers by default; Potatoq only adds a handler
    when nothing is configured yet (or when ``worker_hijack_root_logger`` is set).
    """
    from . import signals

    if isinstance(loglevel, str):
        loglevel = logging.getLevelName(loglevel.upper())
    results = signals.setup_logging.send(sender=None, loglevel=loglevel, logfile=logfile, format=None, colorize=None)
    if any(r for _, r in results if r is not None) or signals.setup_logging.has_receivers():
        return
    root = logging.getLogger()
    hijack = app.conf.worker_hijack_root_logger
    if root.handlers and not hijack:
        logging.getLogger("potatoq").setLevel(loglevel)
        for handler in root.handlers:
            handler.addFilter(TaskContextFilter())
        return
    if hijack:
        for handler in list(root.handlers):
            root.removeHandler(handler)
    handler: logging.Handler = logging.FileHandler(logfile) if logfile else logging.StreamHandler(sys.stderr)
    handler.addFilter(TaskContextFilter())
    handler.setFormatter(TaskFormatter(app.conf.worker_log_format, app.conf.worker_task_log_format))
    root.addHandler(handler)
    root.setLevel(loglevel)
    signals.after_setup_logger.send(sender=None, logger=root, loglevel=loglevel, logfile=logfile, format=None, colorize=None)
