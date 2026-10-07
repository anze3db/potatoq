"""Potatoq: a Celery-compatible task queue with production-ready defaults.

    from potatoq import Potatoq

    app = Potatoq("proj")              # broker from $POTATOQ_BROKER_URL, Django, or sqlite

    @app.task
    def add(x, y):
        return x + y

    add.delay(2, 2)

Migrating from Celery is usually ``s/celery/potatoq/``: ``Celery`` and ``shared_task``
are available under the same names.
"""

from .app import Celery, Potatoq, current_app, shared_task
from .canvas import chain, chord, group, signature, subtask
from .result import AsyncResult, GroupResult
from .schedules import crontab, schedule
from .task import Task

__version__ = "0.1.0"

__all__ = [
    "AsyncResult",
    "Celery",
    "GroupResult",
    "Potatoq",
    "Task",
    "chain",
    "chord",
    "crontab",
    "current_app",
    "group",
    "schedule",
    "shared_task",
    "signature",
    "subtask",
]
