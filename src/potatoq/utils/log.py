"""``from potatoq.utils.log import get_task_logger`` (as in ``celery.utils.log``)."""

from ..log import get_logger, get_task_logger

__all__ = ["get_logger", "get_task_logger"]
