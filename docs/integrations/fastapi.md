# FastAPI

No integration is required. In `async def` endpoints use the non-blocking variants, so
the broker round trip never blocks the event loop:

```python
from fastapi import FastAPI
from potatoq.contrib.fastapi import lifespan, task_status

from .worker import app as potatoq, send_welcome

api = FastAPI(lifespan=lifespan(potatoq))   # closes broker connections on shutdown


@api.post("/signup")
async def signup(email: str):
    user = await create_user(email)
    result = await send_welcome.adelay(user.id)
    return {"task_id": result.id}


@api.get("/tasks/{task_id}")
async def status(task_id: str):
    return task_status(task_id, potatoq)
    # {"id": ..., "state": "SUCCESS", "ready": true, "result": ...}
```

Tasks themselves can be `async def` too: see [async tasks](../guide/tasks.md#async-def-tasks).

With SQLAlchemy sessions, call [`potatoq.contrib.sqlalchemy.install`](sqlalchemy.md)
once so tasks enqueued inside a session transaction are sent on commit.
