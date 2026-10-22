"""The web dashboard: its JSON API, actions, auth, and the WSGI, ASGI, Django and CLI entry points."""

from __future__ import annotations

import asyncio
import base64
import json
import threading
import time
import urllib.request
from io import BytesIO
from types import SimpleNamespace
from wsgiref.util import setup_testing_defaults

import pytest

from potatoq import Potatoq, crontab
from potatoq import dashboard as dashboard_module
from potatoq.cli import main
from potatoq.dashboard import Dashboard, serve
from potatoq.message import Message
from potatoq.testing import drain

ACTION = {"x-potatoq-dashboard": "1"}


def call(dashboard, method="GET", path="/", query="", headers=None, script_name=""):
    """Run one WSGI request; returns (status code, headers, body)."""
    environ: dict = {"REQUEST_METHOD": method, "PATH_INFO": path, "QUERY_STRING": query, "SCRIPT_NAME": script_name}
    for name, value in (headers or {}).items():
        environ["HTTP_" + name.upper().replace("-", "_")] = value
    setup_testing_defaults(environ)
    environ["wsgi.input"] = BytesIO()
    seen = {}

    def start_response(status, response_headers):
        seen["status"] = int(status.split()[0])
        seen["headers"] = dict(response_headers)

    body = b"".join(dashboard(environ, start_response))
    return seen["status"], seen["headers"], body


def api(dashboard, path, method="GET", **kwargs):
    status, _, body = call(dashboard, method, f"/api/{path}", **kwargs)
    return status, json.loads(body)


@pytest.fixture
def tasks(memory_app):
    @memory_app.task
    def add(x, y):
        return x + y

    @memory_app.task
    def charge(order_id):
        raise ConnectionError(f"gateway timed out for order {order_id}")

    @memory_app.task
    def bare():
        raise KeyError

    return SimpleNamespace(add=add, charge=charge, bare=bare)


def test_serves_the_page_and_its_assets_with_security_headers(memory_app):
    dashboard = Dashboard(memory_app)
    status, headers, body = call(dashboard)
    assert status == 200 and b"<title>potatoq dashboard</title>" in body
    assert headers["Content-Type"].startswith("text/html")
    assert "frame-ancestors 'none'" in headers["Content-Security-Policy"]
    assert headers["X-Content-Type-Options"] == "nosniff"
    for asset, kind in (("app.js", "text/javascript"), ("app.css", "text/css"), ("logo.svg", "image/svg+xml")):
        status, headers, body = call(dashboard, path=f"/{asset}")
        assert status == 200 and headers["Content-Type"].startswith(kind) and body
    status, headers, body = call(dashboard, "HEAD")
    assert status == 200 and body == b"" and int(headers["Content-Length"]) > 0
    assert call(dashboard, path="/secret.py")[0] == 404
    assert call(dashboard, "POST", "/")[0] == 405


def test_redirects_to_the_trailing_slash_where_it_is_mounted(memory_app):
    status, headers, _ = call(Dashboard(memory_app), path="", script_name="/ops/potatoq")
    assert status == 301 and headers["Location"] == "/ops/potatoq/"


def test_overview(memory_app, tasks):
    memory_app.conf.beat_schedule = {
        "nightly": {"task": tasks.add.name, "schedule": crontab(minute=0, hour=3), "args": (1, 2)},
        "often": {"task": tasks.add.name, "schedule": 30.0, "args": (1, 2)},
    }
    broker = memory_app.broker
    broker.heartbeat("live@a", {"hostname": "a", "pid": 1, "queues": ["default"], "concurrency": 2, "threads": 4,
                                "running": ["t1"]})  # fmt: skip
    broker.heartbeat("late@b", {"hostname": "b", "queues": ["default"]})
    broker.heartbeat("gone@c", {"hostname": "c"})
    broker.workers_["late@b"]["heartbeat"] -= 10 * memory_app.conf.worker_heartbeat_interval
    broker.workers_["gone@c"]["heartbeat"] -= 2 * memory_app.conf.worker_dead_after
    tasks.add.delay(1, 2)
    tasks.charge.delay(7)
    broker.enqueue_periodic("often", time.time() - 5, Message(task=tasks.add.name, args=[1, 2]))

    status, data = api(Dashboard(memory_app), "overview")
    assert status == 200
    assert data["app"] == "tests" and data["broker"] == "memory://" and data["results"] == "memory://"
    assert data["read_only"] is False
    assert [w["id"] for w in data["workers"]] == ["late@b", "live@a"]
    live = data["workers"][1]
    assert (live["concurrency"], live["threads"], live["running"], live["stale"]) == (2, 4, ["t1"], False)
    assert data["workers"][0]["stale"] is True and data["workers"][0]["concurrency"] == 1
    assert data["queues"] == {"default": 3}
    assert data["dead"] == {"count": 0, "more": False}
    nightly, often = data["schedule"]
    assert (nightly["name"], nightly["schedule"], nightly["last_sent"]) == ("nightly", "0 3 * * *", None)
    assert nightly["next_run"] > data["now"]
    assert often["schedule"] == "every 30s" and often["last_sent"] is not None


def test_overview_counts_dead_letters_up_to_the_limit(memory_app, tasks, monkeypatch):
    monkeypatch.setattr(dashboard_module, "DEAD_LETTER_LIMIT", 2)
    for i in range(2):
        tasks.charge.delay(i)
    drain(memory_app)
    dashboard = Dashboard(memory_app)
    assert api(dashboard, "overview")[1]["dead"] == {"count": 2, "more": False}
    tasks.charge.delay(3)
    drain(memory_app)
    assert api(dashboard, "overview")[1]["dead"] == {"count": 2, "more": True}


def test_a_schedule_that_cannot_say_when_it_runs_next(memory_app, tasks, monkeypatch):
    memory_app.conf.beat_schedule = {"broken": {"task": tasks.add.name, "schedule": 60.0}}

    def boom(self, now):
        raise ValueError("no")

    monkeypatch.setattr("potatoq.schedules.schedule.next_after", boom)
    [entry] = api(Dashboard(memory_app), "overview")[1]["schedule"]
    assert entry["next_run"] is None


def test_dead_letters(memory_app, tasks):
    result = tasks.charge.delay(42)
    drain(memory_app)
    dashboard = Dashboard(memory_app)
    status, [entry] = api(dashboard, "dead")
    assert status == 200
    assert (entry["id"], entry["task"], entry["args"], entry["kwargs"]) == (result.id, tasks.charge.name, [42], {})
    assert "gateway timed out for order 42" in entry["reason"] and entry["died_at"] > 0
    assert api(dashboard, "dead", query="limit=0")[1] == [entry]  # at least one
    assert api(dashboard, "dead", query="limit=lots") == (400, {"error": "limit must be a number"})


def test_task_details(memory_app, tasks):
    dashboard = Dashboard(memory_app)
    done = tasks.add.delay(2, 3)
    failed = tasks.charge.delay(9)
    bare = tasks.bare.delay()
    drain(memory_app)
    waiting = tasks.add.apply_async((1, 1), countdown=600)

    data = api(dashboard, f"tasks/{done.id}")[1]
    assert (data["state"], data["task"], data["args"], data["result"]) == ("SUCCESS", tasks.add.name, [2, 3], 5)
    assert data["waiting"] is None and data["done_at"] >= data["started_at"] > 0

    data = api(dashboard, f"tasks/{failed.id}")[1]
    assert data["state"] == "FAILURE" and data["error"] == "ConnectionError: gateway timed out for order 9"
    assert "Traceback" in data["traceback"]
    assert data["dead"]["args"] == [9] and "gateway timed out" in data["dead"]["reason"]
    assert api(dashboard, f"tasks/{bare.id}")[1]["error"] == "KeyError"

    data = api(dashboard, f"tasks/{waiting.id}")[1]
    assert data["waiting"]["state"] == "scheduled" and data["waiting"]["args"] == [1, 1]
    assert data["waiting"]["eta"] > time.time()

    assert api(dashboard, "tasks/nope")[1] == {"id": "nope", "state": None, "waiting": None, "dead": None}


def test_task_details_without_a_result_backend():
    app = Potatoq("nores", broker="memory://", set_as_current=False)
    app.conf.result_backend = "disabled"
    try:
        assert app.backend is None
        assert api(Dashboard(app), "tasks/x")[1] == {"id": "x", "state": None, "waiting": None, "dead": None}
        assert api(Dashboard(app), "overview")[1]["results"] is None
    finally:
        app.close()


def test_dead_letters_are_found_without_stored_results():
    """Redis doesn't store results unless asked: a failed task is only a dead letter."""
    app = Potatoq("nores", broker="memory://", set_as_current=False)
    app.conf.result_backend = "disabled"
    try:

        @app.task
        def charge(order_id):
            raise ConnectionError(f"gateway timed out for order {order_id}")

        result = charge.delay(3)
        drain(app)
        data = api(Dashboard(app), f"tasks/{result.id}")[1]
        assert data["state"] is None and data["waiting"] is None
        assert (data["dead"]["id"], data["dead"]["task"], data["dead"]["args"]) == (result.id, charge.name, [3])
        assert "gateway timed out for order 3" in data["dead"]["reason"]
    finally:
        app.close()


def test_retry_and_revoke(memory_app, tasks):
    dashboard = Dashboard(memory_app)
    dead = tasks.charge.delay(1)
    drain(memory_app)
    assert api(dashboard, f"dead/{dead.id}/retry", "POST", headers=ACTION) == (200, {"ok": True})
    assert memory_app.broker.dead_letters() == [] and memory_app.broker.peek(dead.id) is not None
    assert api(dashboard, f"dead/{dead.id}/retry", "POST", headers=ACTION) == (
        404, {"error": f"{dead.id} isn't in the dead letters"},
    )  # fmt: skip

    later = tasks.add.apply_async((1, 2), countdown=600)
    assert api(dashboard, f"tasks/{later.id}/revoke", "POST", headers=ACTION) == (200, {"ok": True})
    assert memory_app.broker.peek(later.id) is None

    assert api(dashboard, "tasks/x/explode", "POST", headers=ACTION) == (404, {"error": "No such action"})


def test_actions_need_the_dashboard_header_and_its_origin(memory_app):
    dashboard = Dashboard(memory_app)
    refused = (403, {"error": "Actions must come from the dashboard page"})
    assert api(dashboard, "tasks/x/revoke", "POST") == refused
    evil = {**ACTION, "origin": "https://evil.example", "host": "ops.example"}
    assert api(dashboard, "tasks/x/revoke", "POST", headers=evil) == refused
    same = {**ACTION, "origin": "https://ops.example", "host": "ops.example"}
    assert api(dashboard, "tasks/x/revoke", "POST", headers=same) == (200, {"ok": True})


def test_read_only(memory_app):
    dashboard = Dashboard(memory_app, read_only=True)
    assert api(dashboard, "overview")[1]["read_only"] is True
    assert api(dashboard, "tasks/x/revoke", "POST", headers=ACTION) == (403, {"error": "The dashboard is read-only"})


def test_unknown_endpoints_and_methods(memory_app):
    dashboard = Dashboard(memory_app)
    assert api(dashboard, "nothing") == (404, {"error": "No such endpoint"})
    assert api(dashboard, "tasks/") == (404, {"error": "No such endpoint"})
    assert api(dashboard, "overview", "DELETE") == (405, {"error": "Use GET or POST"})


def test_broker_errors_become_a_json_500(memory_app, monkeypatch, caplog):
    def down():
        raise ConnectionError("broker is down")

    monkeypatch.setattr(memory_app.broker, "queue_sizes", down)
    assert api(Dashboard(memory_app), "overview") == (500, {"error": "ConnectionError: broker is down"})
    assert "Dashboard request failed: GET api/overview" in caplog.text


def basic(credentials: str) -> dict[str, str]:
    return {"authorization": "Basic " + base64.b64encode(credentials.encode()).decode()}


def test_basic_auth(memory_app):
    dashboard = Dashboard(memory_app, auth=("ops", "s3cret"))
    status, headers, _ = call(dashboard)
    assert status == 401 and headers["WWW-Authenticate"].startswith("Basic")
    assert call(dashboard, headers=basic("ops:wrong"))[0] == 401
    assert call(dashboard, headers=basic("nope:s3cret"))[0] == 401
    assert call(dashboard, headers={"authorization": "Bearer s3cret"})[0] == 401
    assert call(dashboard, headers={"authorization": "Basic !!!"})[0] == 401
    assert call(dashboard, headers={"authorization": "Basic " + base64.b64encode(b"\xff").decode()})[0] == 401
    assert call(dashboard, headers=basic("ops:s3cret"))[0] == 200
    assert api(dashboard, "overview", headers=basic("ops:s3cret"))[0] == 200


def asgi(dashboard, method="GET", path="/", root_path="", query=b"", headers=()):
    sent = []

    async def receive():  # pragma: no cover - the dashboard reads no request bodies
        return {"type": "http.request", "body": b""}

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "method": method, "path": path, "root_path": root_path, "query_string": query,
             "headers": [(k.encode(), v.encode()) for k, v in headers]}  # fmt: skip
    asyncio.run(dashboard.asgi(scope, receive, send))
    start, body = sent
    return start["status"], dict(start["headers"]), body["body"]


def test_asgi(memory_app):
    dashboard = Dashboard(memory_app)
    status, headers, body = asgi(dashboard)
    assert status == 200 and b"potatoq dashboard" in body and headers[b"content-type"].startswith(b"text/html")
    # Starlette's Mount: the full path, with the mount point as root_path.
    status, _, body = asgi(dashboard, path="/ops/api/overview", root_path="/ops")
    assert status == 200 and json.loads(body)["broker"] == "memory://"
    status, headers, _ = asgi(dashboard, path="", root_path="/ops")
    assert status == 301 and headers[b"location"] == b"/ops/"
    assert asgi(dashboard, "HEAD")[2] == b""
    status, _, body = asgi(dashboard, "POST", "/api/tasks/x/revoke", headers=[("X-Potatoq-Dashboard", "1")])
    assert (status, json.loads(body)) == (200, {"ok": True})
    status, _, body = asgi(dashboard, "GET", "/api/dead", query=b"limit=nan")
    assert status == 400


def test_asgi_ignores_lifespan_events(memory_app):
    async def never(*args):  # pragma: no cover
        raise AssertionError("not called")

    asyncio.run(Dashboard(memory_app).asgi({"type": "lifespan"}, never, never))


def test_serve(memory_app):
    server = serve(Dashboard(memory_app), "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address[:2]
        with urllib.request.urlopen(f"http://{host}:{port}/api/overview", timeout=10) as response:
            assert json.loads(response.read())["broker"] == "memory://"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


class FakeServer:
    def __init__(self, dashboard, host, port):
        self.dashboard = dashboard
        self.server_address = (host, port)
        self.closed = False

    def serve_forever(self):
        raise KeyboardInterrupt

    def server_close(self):
        self.closed = True


@pytest.fixture
def fake_serve(monkeypatch):
    servers = []

    def fake(dashboard, host, port):
        servers.append(FakeServer(dashboard, host, port))
        return servers[-1]

    monkeypatch.setattr(dashboard_module, "serve", fake)
    monkeypatch.delenv("POTATOQ_DASHBOARD_AUTH", raising=False)
    return servers


def test_cli(memory_app, fake_serve, capsys):
    assert main(["-A", memory_app, "dashboard", "--port", "5601", "--read-only"]) == 0
    [server] = fake_serve
    assert server.closed and server.dashboard.read_only and server.dashboard.auth is None
    assert server.server_address == ("127.0.0.1", 5601)
    out = capsys.readouterr().out
    assert "http://127.0.0.1:5601/ for memory://" in out and "without --auth" not in out


def test_cli_warns_when_listening_beyond_localhost_without_auth(memory_app, fake_serve, capsys):
    assert main(["-A", memory_app, "dashboard", "--host", "::"]) == 0
    out = capsys.readouterr().out
    assert "http://[::]:5555/" in out and "Listening beyond localhost without --auth" in out


def test_cli_auth(memory_app, fake_serve, monkeypatch, capsys):
    assert main(["-A", memory_app, "dashboard", "--host", "0.0.0.0", "--auth", "ops:pa:ss"]) == 0
    assert fake_serve[-1].dashboard.auth == ("ops", "pa:ss")
    assert "without --auth" not in capsys.readouterr().out
    monkeypatch.setenv("POTATOQ_DASHBOARD_AUTH", "env:pw")
    assert main(["-A", memory_app, "dashboard"]) == 0
    assert fake_serve[-1].dashboard.auth == ("env", "pw")
    assert main(["-A", memory_app, "dashboard", "--auth", "nopassword"]) == 2
    assert "--auth must be user:password" in capsys.readouterr().err


@pytest.mark.parametrize("kind", ["memory", "sqlite", "postgres", "redis", "rabbitmq"])
def test_every_broker(kind, tmp_path):
    from conftest import make_app

    app, cleanup = make_app(kind, tmp_path)
    try:

        @app.task
        def fail(n):  # pragma: no cover - dead-lettered without running
            raise RuntimeError(n)

        result = fail.delay(5)
        consumer = app.broker.consumer([app.conf.task_default_queue], "dashboard-test")
        delivery = consumer.fetch(5)
        consumer.dead_letter(delivery, "RuntimeError: boom", None)
        dashboard = Dashboard(app)
        status, overview = api(dashboard, "overview")
        assert status == 200 and overview["dead"]["count"] == 1 and overview["workers"] == []
        [entry] = api(dashboard, "dead")[1]
        assert entry["id"] == result.id and entry["args"] == [5] and "boom" in entry["reason"]
        assert api(dashboard, f"tasks/{result.id}")[0] == 200
        assert api(dashboard, f"dead/{result.id}/retry", "POST", headers=ACTION) == (200, {"ok": True})
        assert api(dashboard, "overview")[1]["dead"]["count"] == 0
        consumer.complete(consumer.fetch(5), None, [])
        consumer.close()
    finally:
        app.close()
        cleanup()


class User(SimpleNamespace):
    is_authenticated = True
    is_active = True
    is_staff = True


@pytest.fixture
def urlconf(django_env):
    from django.test import override_settings

    with override_settings(ROOT_URLCONF="potatoq.contrib.django.dashboard"):
        yield


def test_django(django_env, urlconf):
    from django.test import RequestFactory, override_settings

    from potatoq.contrib.django import dashboard as dj

    factory = RequestFactory()

    def get(path, user, **extra):
        request = factory.get(f"/potatoq/{path}", **extra)
        if user is not None:
            request.user = user
        return dj.dashboard(request, path)

    response = get("", None)
    assert response.status_code == 302 and response["Location"] == "/accounts/login/?next=%2Fpotatoq%2F"
    assert get("", User(is_authenticated=False)).status_code == 302
    assert get("", User(is_staff=False)).status_code == 403
    assert get("", User(is_active=False)).status_code == 403

    response = get("", User())
    assert response.status_code == 200 and b"potatoq dashboard" in response.content
    assert "frame-ancestors" in response["Content-Security-Policy"]
    response = get("api/overview", User())
    assert response.status_code == 200 and json.loads(response.content)["app"] == django_env.main
    assert dj._dashboard() is dj._dashboard() and dj._dashboard().app is django_env

    request = factory.post("/potatoq/api/tasks/x/revoke", HTTP_X_POTATOQ_DASHBOARD="1")
    request.user = User()
    response = dj.dashboard(request, "api/tasks/x/revoke")
    assert response.status_code == 200 and json.loads(response.content) == {"ok": True}

    other = Potatoq("other", broker="memory://", set_as_current=True)
    try:
        with override_settings(POTATOQ_DASHBOARD_READ_ONLY=True):
            assert dj._dashboard().app is other and dj._dashboard().read_only is True
    finally:
        other.close()
        django_env.set_current()


def test_django_urlconf(django_env, urlconf):
    from django.urls import resolve

    from potatoq.contrib.django import dashboard as dj

    match = resolve("/api/overview")
    assert match.func is dj.dashboard and match.kwargs == {"path": "api/overview"}
