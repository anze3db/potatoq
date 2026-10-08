"""Logging helpers: ``get_task_logger`` and a formatter that knows the current task."""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

from .console import Painter, _isatty

__all__ = ["PotatoqFormatter", "TaskFormatter", "get_logger", "get_task_logger", "setup_logging"]

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

        if hasattr(record, "potatoq_event"):
            return True  # potatoq's own task events carry their task already
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


#: ``potatoq_event`` of a record → (emoji, color of the outcome).
_EVENTS = {
    "success": ("✅", "green"),
    "retry": ("🔁", "yellow"),
    "failure": ("❌", "red"),
    "revoked": ("🚫", "grey"),
    "ignored": ("💤", "grey"),
    "rejected": ("⛔", "yellow"),
    "lost": ("🚨", "red"),
}
_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))


def _is_potatoq_frame(filename: str) -> bool:
    return os.path.abspath(filename).startswith(_PACKAGE_DIR + os.sep)


_LEVELS = {"DEBUG": "blue", "INFO": "green", "WARNING": "yellow", "ERROR": "red", "CRITICAL": "on_red"}


class PotatoqFormatter(logging.Formatter):
    """The worker's log format: FastAPI-style on a terminal, one plain line otherwise.

    On a terminal (or with ``FORCE_COLOR``)::

        12:00:01      INFO  ✅ shop.send_receipt[3f2a…] succeeded in 12ms

    Elsewhere (files, journald, containers)::

        [2026-10-09 12:00:01,120: INFO/ForkPoolWorker-1] ✅ Task shop.send_receipt[3f2a…] succeeded in 12ms

    Messages stay plain text in the log records; only this formatter adds colors,
    emojis and the tag gutter, so other handlers (and ``caplog``) never see them.
    Records can carry ``potatoq_tag`` (a gutter label instead of the level),
    ``potatoq_icon`` and ``potatoq_event``.
    """

    def __init__(
        self, stream: Any = None, *, pretty: bool | None = None, color: bool | None = None, emoji: bool | None = None
    ):
        super().__init__()
        stream = sys.stderr if stream is None else stream
        self.paint = Painter(stream, color=color, emoji=emoji)
        self.pretty = (_isatty(stream) or bool(os.environ.get("FORCE_COLOR"))) if pretty is None else pretty

    def format(self, record: logging.LogRecord) -> str:
        paint = self.paint
        message = self._message(record)
        tag = getattr(record, "potatoq_tag", None)
        if self.pretty:
            when = paint.style(self.formatTime(record, "%H:%M:%S"), "dim")
            if tag:
                label = paint.tag(tag, "on_potato" if tag == "potatoq" else "cyan")
            else:
                label = paint.tag(record.levelname, _LEVELS.get(record.levelname, "cyan"))
            line = f"{when}{label}  {message}"
        else:
            prefix = f"{tag}: " if tag and tag != "potatoq" else ""
            line = f"[{self.formatTime(record)}: {record.levelname}/{record.processName}] {prefix}{message}"
        if record.exc_info:
            record.exc_text = record.exc_text or self.formatException(record.exc_info)
        if record.exc_text:
            line += "\n" + record.exc_text
        if record.stack_info:
            line += "\n" + self.formatStack(record.stack_info)
        return line

    def formatException(self, ei: Any) -> str:
        """Without potatoq's own frames on top (the task's frames start the traceback),
        and colored like Python's own tracebacks on a terminal (3.13+)."""
        import traceback

        te = traceback.TracebackException(ei[0], ei[1], ei[2])
        frames = list(te.stack)
        while len(frames) > 1 and _is_potatoq_frame(frames[0].filename):
            frames.pop(0)  # the worker calling the task
        while len(frames) > 1 and _is_potatoq_frame(frames[-1].filename):
            frames.pop()  # e.g. retry() re-raising the task's exception
        te.stack = traceback.StackSummary.from_list(frames)
        try:
            lines = te.format(colorize=self.paint.color)  # type: ignore[call-arg]
        except TypeError:  # pragma: no cover - Python < 3.13 (coverage runs on 3.13)
            lines = te.format()
        return "".join(lines).rstrip("\n")

    def _message(self, record: logging.LogRecord) -> str:
        paint = self.paint
        text = record.getMessage()
        event = getattr(record, "potatoq_event", None)
        name, task_id = getattr(record, "task_name", "???"), getattr(record, "task_id", "???")
        icon = getattr(record, "potatoq_icon", None)
        if event in _EVENTS:
            icon, color = _EVENTS[event]
            prefix = f"Task {name}[{task_id}] "
            if self.pretty and text.startswith(prefix):
                head, sep, tail = text[len(prefix) :].partition(": ")
                task = paint.style(name, "bold") + paint.style(f"[{task_id}]", "dim")
                text = f"{task} {paint.style(head, color)}{sep}{tail}"
        elif task_id != "???":  # logged by a task while it runs
            label = f"{name}[{task_id}]"
            text = f"{paint.style(label, 'dim')} {text}" if self.pretty else f"{label}: {text}"
        return paint.icon(icon) + text if icon else text


def setup_logging(app: Any, loglevel: str | int = "INFO", logfile: str | None = None) -> None:
    """Configure logging for the worker without clobbering an existing setup.

    Celery replaces the root logger's handlers by default; Potatoq only adds a handler
    when nothing is configured yet (or when ``worker_hijack_root_logger`` is set).
    """
    from . import signals

    if isinstance(loglevel, str):
        loglevel = logging.getLevelName(loglevel.upper())
    # Client libraries are chatty at INFO (pika logs every connection step).
    # pika logs every failed address of a connection attempt (e.g. IPv6 ::1) at ERROR;
    # Potatoq reports connection problems itself.
    logging.getLogger("pika").setLevel(max(logging.CRITICAL, loglevel))
    results = signals.setup_logging.send(sender=None, loglevel=loglevel, logfile=logfile, format=None, colorize=None)
    if any(r for _, r in results if r is not None) or signals.setup_logging.has_receivers():
        return
    root = logging.getLogger()
    hijack = app.conf.worker_hijack_root_logger
    if root.handlers and not hijack:
        logging.getLogger("potatoq").setLevel(loglevel)
        for existing in root.handlers:
            existing.addFilter(TaskContextFilter())
        return
    if hijack:
        for existing in list(root.handlers):
            root.removeHandler(existing)
    handler: logging.Handler = logging.FileHandler(logfile) if logfile else logging.StreamHandler(sys.stderr)
    handler.addFilter(TaskContextFilter())
    conf = app.conf
    if {"worker_log_format", "worker_task_log_format"} & set(conf.changed()):
        # Celery-style formats set explicitly: use them as given.
        formatter: logging.Formatter = TaskFormatter(conf.worker_log_format, conf.worker_task_log_format)
    else:
        from . import console

        color = console._overrides["color"]
        emoji = console._overrides["emoji"]
        formatter = PotatoqFormatter(
            getattr(handler, "stream", None),
            color=conf.worker_log_color if color is None else color,
            emoji=conf.worker_log_emoji if emoji is None else emoji,
        )
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(loglevel)
    signals.after_setup_logger.send(
        sender=None, logger=root, loglevel=loglevel, logfile=logfile, format=None, colorize=None
    )
