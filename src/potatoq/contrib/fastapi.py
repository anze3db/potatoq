"""FastAPI / Starlette helpers.

Nothing is required to use Potatoq from FastAPI. In ``async def`` endpoints prefer the
non-blocking variants:

    @app.post("/signup")
    async def signup(...):
        result = await send_welcome.adelay(user.id)       # enqueue without blocking the loop
        return {"task_id": result.id}

    @app.get("/tasks/{task_id}")
    async def status(task_id: str):
        return task_status(task_id)                        # JSON-friendly status

With SQLAlchemy sessions, call ``potatoq.contrib.sqlalchemy.install(app)`` once so
tasks enqueued inside a session transaction are sent on commit.
"""

from __future__ import annotations

from typing import Any

from ..app import Potatoq, current_app


def task_status(task_id: str, app: Potatoq | None = None) -> dict[str, Any]:
    """A JSON-serializable status dict for a task id (for polling endpoints)."""
    app = app or current_app()
    result = app.AsyncResult(task_id)
    state = result.state
    info: dict[str, Any] = {"id": task_id, "state": state, "ready": state in ("SUCCESS", "FAILURE", "REVOKED")}
    if state == "SUCCESS":
        info["result"] = result.result
    elif state == "FAILURE":
        info["error"] = repr(result.result)
    return info


def lifespan(app: Potatoq | None = None) -> Any:
    """Lifespan context manager that closes broker connections on shutdown."""
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _lifespan(_: Any) -> Any:
        yield
        (app or current_app()).close()

    return _lifespan
