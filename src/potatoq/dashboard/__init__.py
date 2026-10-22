"""A web dashboard: workers, queues, periodic tasks, dead letters and task lookup.

Standard library only, so ``pip install potatoq`` is enough. The core is a WSGI app::

    from potatoq.dashboard import Dashboard

    dashboard = Dashboard(app)              # WSGI: Flask, gunicorn, wsgiref…
    asgi_dashboard = dashboard.asgi         # ASGI: FastAPI/Starlette app.mount()

Run it on its own with ``potatoq dashboard``, or mount it in Django with
``path("potatoq/", include("potatoq.contrib.django.dashboard"))``.

The page is static HTML that polls a small JSON API under ``api/``. Actions (replaying
a dead letter, revoking a waiting task) are POSTs that need the ``X-Potatoq-Dashboard``
header, which browsers won't send cross-site without CORS, and are off with
``read_only=True``.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hmac
import json
import logging
import threading
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from importlib import resources
from socketserver import ThreadingMixIn
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlsplit
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from ..config import redact_url
from ..worker.scheduler import describe_schedule, load_entries

if TYPE_CHECKING:
    from ..app import Potatoq

logger = logging.getLogger("potatoq.dashboard")

#: Dead letters fetched for the overview; the page says "100+" beyond that.
DEAD_LETTER_LIMIT = 100

ACTION_HEADER = "x-potatoq-dashboard"

_ASSETS = {
    "": ("index.html", "text/html; charset=utf-8"),
    "app.js": ("app.js", "text/javascript; charset=utf-8"),
    "app.css": ("app.css", "text/css; charset=utf-8"),
    "logo.svg": ("logo.svg", "image/svg+xml"),
}

_SECURITY_HEADERS = [
    ("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'"),
    ("X-Content-Type-Options", "nosniff"),
    ("Referrer-Policy", "same-origin"),
]

_REASONS = {200: "OK", 301: "Moved Permanently", 400: "Bad Request", 401: "Unauthorized", 403: "Forbidden", 404: "Not Found",
            405: "Method Not Allowed", 500: "Internal Server Error"}  # fmt: skip


class Response:
    def __init__(self, status: int, body: bytes, content_type: str, headers: Iterable[tuple[str, str]] = ()):
        self.status = status
        self.body = body
        self.headers = [("Content-Type", content_type), ("Content-Length", str(len(body))), *_SECURITY_HEADERS]
        self.headers.extend(headers)

    @property
    def status_line(self) -> str:
        return f"{self.status} {_REASONS[self.status]}"


def _json(status: int, data: Any) -> Response:
    body = json.dumps(data, default=repr).encode()
    return Response(status, body, "application/json", [("Cache-Control", "no-store")])


def _error(status: int, message: str) -> Response:
    return _json(status, {"error": message})


class Dashboard:
    """The dashboard for ``app``, as a WSGI application (``.asgi`` for ASGI).

    ``auth`` is an optional ``(username, password)`` pair checked with HTTP basic auth.
    Leave it out when your framework already guards the URL. ``read_only`` turns off
    the actions.
    """

    def __init__(self, app: Potatoq, *, read_only: bool = False, auth: tuple[str, str] | None = None):
        self.app = app
        self.read_only = read_only
        self.auth = auth
        # Broker clients aren't all thread-safe (pika isn't), and a dashboard sees a
        # handful of requests a second at most.
        self._lock = threading.Lock()
        self._modules_imported = False

    def handle(
        self, method: str, path: str, query: str = "", headers: dict[str, str] | None = None, prefix: str = ""
    ) -> Response:
        """Answer one request. ``path`` is relative to where the dashboard is mounted,
        ``prefix`` is the mount point (for the redirect to its trailing slash), and
        ``headers`` have lower-case names."""
        headers = headers or {}
        if self.auth is not None and not self._authorized(headers.get("authorization", "")):
            return Response(401, b"Authentication required\n", "text/plain; charset=utf-8",
                            [("WWW-Authenticate", 'Basic realm="potatoq", charset="UTF-8"')])  # fmt: skip
        if path == "":
            return Response(301, b"", "text/plain", [("Location", prefix + "/")])
        path = path.removeprefix("/")
        if path.startswith("api/"):
            try:
                with self._lock:
                    return self._api(method, path[4:], parse_qs(query), headers)
            except Exception as exc:
                logger.exception("Dashboard request failed: %s %s", method, path)
                return _error(500, f"{type(exc).__name__}: {exc}")
        if path not in _ASSETS:
            return Response(404, b"Not found\n", "text/plain; charset=utf-8")
        if method not in ("GET", "HEAD"):
            return Response(405, b"Method not allowed\n", "text/plain; charset=utf-8")
        name, content_type = _ASSETS[path]
        body = resources.files(__package__).joinpath("static", name).read_bytes()
        return Response(200, body, content_type, [("Cache-Control", "no-cache")])

    def _authorized(self, header: str) -> bool:
        assert self.auth is not None
        scheme, _, encoded = header.partition(" ")
        if scheme.lower() != "basic":
            return False
        try:
            username, _, password = base64.b64decode(encoded, validate=True).decode().partition(":")
        except (binascii.Error, UnicodeDecodeError):
            return False
        # Both comparisons run, so the time taken doesn't reveal which one failed.
        ok_user = hmac.compare_digest(username.encode(), self.auth[0].encode())
        ok_password = hmac.compare_digest(password.encode(), self.auth[1].encode())
        return ok_user and ok_password

    def _api(self, method: str, path: str, query: dict[str, list[str]], headers: dict[str, str]) -> Response:
        parts = path.split("/")
        if method == "POST":
            if self.read_only:
                return _error(403, "The dashboard is read-only")
            if headers.get(ACTION_HEADER) != "1" or not _same_origin(headers):
                return _error(403, "Actions must come from the dashboard page")
            if len(parts) == 3 and parts[0] == "dead" and parts[2] == "retry":
                if not self.app.broker.requeue_dead(parts[1]):  # type: ignore[attr-defined]
                    return _error(404, f"{parts[1]} isn't in the dead letters")
                return _json(200, {"ok": True})
            if len(parts) == 3 and parts[0] == "tasks" and parts[2] == "revoke":
                self.app.control.revoke(parts[1])
                return _json(200, {"ok": True})
            return _error(404, "No such action")
        if method not in ("GET", "HEAD"):
            return _error(405, "Use GET or POST")
        if path == "overview":
            return _json(200, self.overview())
        if path == "dead":
            try:
                limit = int(query.get("limit", [str(DEAD_LETTER_LIMIT)])[0])
            except ValueError:
                return _error(400, "limit must be a number")
            return _json(200, self.dead_letters(min(max(limit, 1), 1000)))
        if len(parts) == 2 and parts[0] == "tasks" and parts[1]:
            return _json(200, self.task(parts[1]))
        return _error(404, "No such endpoint")

    def overview(self) -> dict[str, Any]:
        """Everything the main page shows, in one round trip per broker call."""
        app = self.app
        now = time.time()
        dead_after = float(app.conf.worker_dead_after)
        stale_after = 3 * float(app.conf.worker_heartbeat_interval)
        workers = []
        for w in app.broker.workers():
            age = now - float(w.get("heartbeat", now))
            if age > dead_after:
                continue
            workers.append({
                "id": w["id"], "hostname": w.get("hostname"), "pid": w.get("pid"), "queues": w.get("queues", []),
                "concurrency": w.get("concurrency") or 1, "threads": w.get("threads") or 1,
                "running": w.get("running", []), "heartbeat_age": age, "stale": age > stale_after,
            })  # fmt: skip
        dead = app.broker.dead_letters(DEAD_LETTER_LIMIT + 1)
        return {
            "app": app.main,
            "broker": redact_url(app.broker.url),
            "results": redact_url(app.backend.url) if app.backend is not None else None,
            "now": now,
            "read_only": self.read_only,
            "workers": sorted(workers, key=lambda w: w["id"]),
            "queues": dict(sorted(app.broker.queue_sizes().items())),
            "dead": {"count": min(len(dead), DEAD_LETTER_LIMIT), "more": len(dead) > DEAD_LETTER_LIMIT},
            "schedule": self.schedule(now),
        }

    def schedule(self, now: float) -> list[dict[str, Any]]:
        if not self._modules_imported:
            self.app.loader_import_default_modules()  # periodic tasks may live in task modules
            self._modules_imported = True
        last = self.app.broker.last_periodic_runs()
        rows = []
        for entry in load_entries(self.app):
            try:
                next_run: float | None = entry.schedule.next_after(datetime.fromtimestamp(now, UTC)).timestamp()
            except Exception:
                next_run = None
            rows.append({
                "name": entry.name, "task": entry.task, "schedule": describe_schedule(entry.schedule),
                "next_run": next_run, "last_sent": last.get(entry.name),
            })  # fmt: skip
        return rows

    def dead_letters(self, limit: int) -> list[dict[str, Any]]:
        entries = []
        for e in self.app.broker.dead_letters(limit):
            message = e.get("message") or {}
            entries.append({
                "id": e.get("id"), "task": e.get("task"), "queue": e.get("queue"), "died_at": e.get("died_at"),
                "reason": e.get("reason") or "", "args": message.get("args", []), "kwargs": message.get("kwargs", {}),
                "retries": message.get("retries", 0),
            })  # fmt: skip
        return entries

    def task(self, task_id: str) -> dict[str, Any]:
        """What the result backend and the broker know about one task."""
        app = self.app
        out: dict[str, Any] = {"id": task_id, "state": None, "waiting": None}
        record = app.backend.get_result(task_id) if app.backend is not None else None
        if record is not None:
            out.update({
                "state": record.state, "task": record.task_name, "args": record.args, "kwargs": record.kwargs,
                "result": record.result, "traceback": record.traceback, "retries": record.retries,
                "worker": record.worker, "enqueued_at": record.enqueued_at, "started_at": record.date_started,
                "done_at": record.date_done,
            })  # fmt: skip
            error = record.result
            if record.state == "FAILURE" and isinstance(error, dict) and "exc_type" in error:
                details = error.get("exc_message")
                text = ", ".join(map(str, details)) if isinstance(details, list) else str(details or "")
                out["error"] = f"{error['exc_type']}: {text}" if text else str(error["exc_type"])
        peeked = app.broker.peek(task_id)
        if peeked is not None:
            message, state = peeked
            out["waiting"] = {
                "state": state, "task": message.task, "queue": message.queue, "args": message.args,
                "kwargs": message.kwargs, "eta": message.eta, "enqueued_at": message.enqueued_at,
            }  # fmt: skip
        return out

    def __call__(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> list[bytes]:
        """The WSGI entry point."""
        headers = {
            key[5:].replace("_", "-").lower(): value for key, value in environ.items() if key.startswith("HTTP_")
        }
        response = self.handle(
            environ["REQUEST_METHOD"], environ.get("PATH_INFO", ""), environ.get("QUERY_STRING", ""),
            headers, environ.get("SCRIPT_NAME", ""),
        )  # fmt: skip
        start_response(response.status_line, response.headers)
        return [] if environ["REQUEST_METHOD"] == "HEAD" else [response.body]

    async def asgi(self, scope: dict[str, Any], receive: Callable[..., Any], send: Callable[..., Any]) -> None:
        """The ASGI entry point: ``fastapi_app.mount("/potatoq", dashboard.asgi)``."""
        if scope["type"] != "http":  # lifespan events of a standalone ASGI server
            return
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
        root_path = scope.get("root_path", "")
        path = scope.get("path", "")
        # Starlette's Mount keeps the full path in "path" and the mount point in
        # "root_path"; other servers strip it.
        if root_path and path.startswith(root_path):
            path = path[len(root_path) :]
        response = await asyncio.to_thread(
            self.handle, scope["method"], path, scope.get("query_string", b"").decode("latin-1"), headers, root_path
        )
        await send({
            "type": "http.response.start", "status": response.status,
            "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in response.headers],
        })  # fmt: skip
        await send({"type": "http.response.body", "body": b"" if scope["method"] == "HEAD" else response.body})


def _same_origin(headers: dict[str, str]) -> bool:
    """A browser's ``Origin`` header, when there is one, must name this host."""
    origin = headers.get("origin")
    if not origin:
        return True
    return urlsplit(origin).netloc == headers.get("host", "")


def serve(dashboard: Dashboard, host: str = "127.0.0.1", port: int = 5555) -> Any:
    """A threaded wsgiref server for ``dashboard`` (call ``serve_forever()`` on it)."""

    class Server(ThreadingMixIn, WSGIServer):
        daemon_threads = True

    class QuietHandler(WSGIRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            logger.debug("%s - %s", self.address_string(), format % args)

    return make_server(host, port, dashboard, server_class=Server, handler_class=QuietHandler)
