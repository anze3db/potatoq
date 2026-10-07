from __future__ import annotations

from potatoq import Potatoq
from potatoq.testing import drain


def test_flask_init_app_reads_config_and_pushes_app_context():
    from flask import Flask, current_app

    from potatoq.contrib.flask import init_app

    flask_app = Flask("shop")
    flask_app.config.update(CELERY_BROKER_URL="memory://", POTATOQ_RESULT_BACKEND="broker", GREETING="hi")
    app = Potatoq("flaskapp", set_as_current=False)

    @app.task
    def defined_before():
        return current_app.config["GREETING"]

    potatoq = init_app(flask_app, app)

    @potatoq.task
    def defined_after(name):
        return f"{current_app.config['GREETING']} {name}"

    assert potatoq.conf.broker_url == "memory://"
    assert flask_app.extensions["potatoq"] is potatoq
    r1 = defined_before.delay()
    r2 = defined_after.delay("ann")
    drain(potatoq)
    assert r1.get() == "hi"
    assert r2.get() == "hi ann"


def test_fastapi_async_enqueue_and_status():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from potatoq.contrib.fastapi import lifespan, task_status

    app = Potatoq("fastapiapp", broker="memory://", result_backend="broker")

    @app.task
    def add(x, y):
        return x + y

    api = FastAPI(lifespan=lifespan(app))

    @api.post("/add")
    async def enqueue(x: int, y: int):
        result = await add.adelay(x, y)
        return {"id": result.id}

    @api.get("/tasks/{task_id}")
    async def status(task_id: str):
        return task_status(task_id, app)

    with TestClient(api) as client:
        task_id = client.post("/add", params={"x": 2, "y": 3}).json()["id"]
        assert client.get(f"/tasks/{task_id}").json()["state"] == "PENDING"
        drain(app)
        assert client.get(f"/tasks/{task_id}").json() == {"id": task_id, "state": "SUCCESS", "ready": True, "result": 5}
