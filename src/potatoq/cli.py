"""``potatoq`` command line: ``potatoq -A proj worker``, mirroring ``celery``."""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import socket
import sys
import threading
import time
from typing import Any

from . import console
from .app import Potatoq, current_app
from .config import redact_url
from .exceptions import ImproperlyConfigured, OperationalError

logger = logging.getLogger("potatoq")

#: Celery flags that are unnecessary in Potatoq (accepted, ignored).
IGNORED_FLAGS = ("--without-gossip", "--without-mingle", "--without-heartbeat", "-E", "--task-events", "--events")


def find_app(spec: str | Potatoq | None) -> Potatoq:
    """Resolve ``-A``: ``proj``, ``proj.celery``, ``proj.celery:app`` or an instance."""
    if isinstance(spec, Potatoq):
        return spec
    sys.path.insert(0, os.getcwd())
    if not spec:
        _setup_django()
        return current_app()
    app = _import_app(spec)
    # After the import: a celery.py-style module sets DJANGO_SETTINGS_MODULE itself.
    _setup_django()
    return app


def _setup_django() -> None:
    """``django.setup()`` for a Django project (DJANGO_SETTINGS_MODULE) not set up yet."""
    if not os.environ.get("DJANGO_SETTINGS_MODULE"):
        return
    try:
        import django
        from django.apps import apps
    except ImportError:
        return
    if not apps.ready:
        django.setup()


def _import_app(spec: str) -> Potatoq:
    module_name, _, attr = spec.partition(":")
    module = importlib.import_module(module_name)
    if attr:
        obj: Any = module
        for part in attr.split("."):
            obj = getattr(obj, part)
        return obj() if callable(obj) and not isinstance(obj, Potatoq) else obj
    for name in ("app", "potatoq", "celery"):
        obj = getattr(module, name, None)
        if isinstance(obj, Potatoq):
            return obj
    for submodule in ("potatoq", "celery"):
        try:
            sub = importlib.import_module(f"{module_name}.{submodule}")
        except ModuleNotFoundError as exc:
            if exc.name != f"{module_name}.{submodule}":
                raise
            continue
        for name in ("app", "potatoq", "celery"):
            obj = getattr(sub, name, None)
            if isinstance(obj, Potatoq):
                return obj
    instances = [v for v in vars(module).values() if isinstance(v, Potatoq)]
    if instances:
        return instances[0]
    raise SystemExit(f"Could not find a Potatoq app in {spec!r}. Pass -A module:attribute.")


def build_parser(prog: str = "potatoq") -> argparse.ArgumentParser:
    extra: dict[str, Any] = {"color": console.color_enabled(sys.stdout)} if sys.version_info >= (3, 14) else {}
    parser = argparse.ArgumentParser(
        prog=prog, description="potatoq: the Python task queue with production-ready defaults", **extra
    )
    parser.add_argument("-A", "--app", help="Application, e.g. proj or proj.celery:app")
    parser.add_argument(
        "--color", action=argparse.BooleanOptionalAction, default=None,
        help="Colors in output and logs (default: on for terminals; NO_COLOR and FORCE_COLOR work too)",
    )  # fmt: skip
    parser.add_argument(
        "--emoji", action=argparse.BooleanOptionalAction, default=None,
        help="Emojis in output and logs (default: on; or set POTATOQ_NO_EMOJI=1)",
    )  # fmt: skip
    parser.add_argument("-b", "--broker", help="Broker URL (overrides configuration)")
    parser.add_argument("--result-backend", help="Result backend URL")
    parser.add_argument("--workdir", help="Change to this directory first")
    sub = parser.add_subparsers(dest="command")

    w = sub.add_parser("worker", help="Start a worker")
    w.add_argument("-c", "--concurrency", type=int, help="Number of worker processes (default: available CPUs)")
    w.add_argument(
        "-t",
        "--threads",
        type=int,
        help="Task threads per process (default 1). For I/O-bound tasks; see the docs on time limits",
    )
    w.add_argument("-Q", "--queues", help="Comma separated queues to consume (default: default queue)")
    w.add_argument("-l", "--loglevel", default=None, help="DEBUG, INFO, WARNING, ERROR")
    w.add_argument("-f", "--logfile")
    w.add_argument("-n", "--hostname")
    w.add_argument(
        "-P",
        "--pool",
        default="prefork",
        help="prefork (default), threads (= -c 1 --threads N, Celery compatible) or solo",
    )
    w.add_argument("--max-tasks-per-child", type=int, default=-1)
    w.add_argument("--max-memory-per-child", default=-1, help="e.g. 512MB, or KiB like Celery")
    w.add_argument("--shutdown-timeout", type=float)
    w.add_argument("--no-scheduler", action="store_true", help="Don't run periodic tasks on this worker")
    w.add_argument(
        "-B", "--beat", action="store_true", help="(Celery compat) the scheduler already runs in every worker"
    )
    w.add_argument("-O", dest="optimization", help="(Celery compat, ignored) fair scheduling is the default")
    w.add_argument("--time-limit", type=float)
    w.add_argument("--soft-time-limit", type=float)
    w.add_argument("--pidfile")
    w.add_argument("--autoscale", help="(Celery compat, ignored) use -c")
    w.add_argument(
        "--prefetch-multiplier", type=int, help="(Celery compat, ignored) prefetch is always one task per idle process"
    )
    for flag in IGNORED_FLAGS:
        w.add_argument(flag, action="store_true", help=argparse.SUPPRESS)

    b = sub.add_parser("beat", help="Run only the periodic task scheduler (optional: workers already do this)")
    b.add_argument("-l", "--loglevel", default="INFO")
    b.add_argument("-s", "--schedule", help="(Celery compat, ignored)")

    s = sub.add_parser("status", help="Show live workers")
    s.add_argument("--json", action="store_true")

    q = sub.add_parser("queues", help="Show queue sizes")
    q.add_argument("--json", action="store_true")

    p = sub.add_parser("purge", help="Delete waiting tasks")
    p.add_argument("-Q", "--queues", help="Queues to purge (default: default queue)")
    p.add_argument("-f", "--force", action="store_true")

    d = sub.add_parser("dead", help="Inspect and replay dead-lettered tasks")
    dsub = d.add_subparsers(dest="dead_command")
    dl = dsub.add_parser("list")
    dl.add_argument("--limit", type=int, default=20)
    dl.add_argument("--json", action="store_true")
    dr = dsub.add_parser("retry")
    dr.add_argument("task_ids", nargs="+")

    c = sub.add_parser("call", help="Enqueue a task by name")
    c.add_argument("name")
    c.add_argument("-a", "--args", default="[]", help="JSON list")
    c.add_argument("-k", "--kwargs", default="{}", help="JSON object")
    c.add_argument("--countdown", type=float)
    c.add_argument("-Q", "--queue")

    r = sub.add_parser("result", help="Show a task result")
    r.add_argument("task_id")
    r.add_argument("--wait", type=float, default=None, help="Seconds to wait for it")

    sub.add_parser("schedule", help="List periodic tasks and when they run next")
    i = sub.add_parser("inspect", help="Inspect live workers: active, registered, stats, ping, active_queues")
    i.add_argument("what", choices=["active", "registered", "stats", "ping", "active_queues", "scheduled", "reserved"])
    i.add_argument("-d", "--destination")

    sub.add_parser("migrate", help="Create the broker schema (tables/queues)")
    rv = sub.add_parser("revoke", help="Revoke tasks that haven't started")
    rv.add_argument("task_ids", nargs="+")
    sub.add_parser("shell", help="Python shell with the app loaded")
    return parser


def main(argv: list[Any] | None = None, prog: str = "potatoq") -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    app_obj = None
    if len(argv) >= 2 and argv[0] in ("-A", "--app") and isinstance(argv[1], Potatoq):
        app_obj = argv[1]
        argv = argv[2:]
    args = build_parser(prog).parse_args([str(a) for a in argv])
    console.configure(color=args.color, emoji=args.emoji)
    try:
        return _run(args, app_obj)
    except (ImproperlyConfigured, OperationalError) as exc:
        err = console.painter(sys.stderr)
        print(err.line("error", f"{err.icon('❌')}{exc}", "red"), file=sys.stderr)
        return 1


def _run(args: argparse.Namespace, app_obj: Potatoq | None) -> int:
    if args.workdir:
        os.chdir(args.workdir)
    if app_obj is not None:
        _setup_django()  # app.worker_main() in a Django project: as find_app would
    app = app_obj or find_app(args.app)
    if args.broker:
        app.conf.broker_url = args.broker
    if args.result_backend:
        app.conf.result_backend = args.result_backend
    command = args.command or "worker"
    handler = globals()[f"cmd_{command}"]
    return int(handler(app, args) or 0)


def _macos_fork_safety() -> None:
    """Restart once with ``OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES`` on macOS.

    Once a process has started a thread (logfire, Sentry, New Relic and many other SDKs
    do at import), macOS kills its forked children the first time they touch certain
    system frameworks, e.g. ``socket.getfqdn()`` in Django's ``send_mail``. The variable
    only takes effect at process start, hence the re-exec (same PID, same arguments)."""
    if sys.platform != "darwin" or os.environ.get("OBJC_DISABLE_INITIALIZE_FORK_SAFETY"):
        return
    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
    sys.stdout.flush()
    sys.stderr.flush()
    os.execv(sys.executable, [sys.executable, *sys.orig_argv[1:]])


def expand_hostname(name: str) -> str:
    """Celery's ``-n`` placeholders: ``%h`` (host.domain), ``%n`` (host), ``%d`` (domain)."""
    full = socket.gethostname()
    host, _, domain = full.partition(".")
    return name.replace("%%", "\0").replace("%h", full).replace("%n", host).replace("%d", domain).replace("\0", "%")


def cmd_worker(app: Potatoq, args: argparse.Namespace) -> int:
    if sys.platform == "win32":
        raise SystemExit(
            "potatoq workers need fork() and POSIX signals, so Windows isn't supported. "
            "Run the worker under WSL or in a Linux container."
        )
    if (args.pool or "prefork").lower() != "solo":
        _macos_fork_safety()
    if args.hostname:
        args.hostname = expand_hostname(args.hostname)
    from .worker.supervisor import Supervisor

    if args.time_limit:
        app.conf.task_time_limit = args.time_limit
    if args.soft_time_limit:
        app.conf.task_soft_time_limit = args.soft_time_limit
    if args.pidfile:
        with open(args.pidfile, "w") as f:
            f.write(str(os.getpid()))
    pool = (args.pool or "prefork").lower()
    if pool == "threads":
        # Celery's `-P threads -c N` = N threads in one process. Same here, except
        # that time limits are still enforced.
        args.threads = args.threads or args.concurrency or 10
        args.concurrency = 1
        pool = "prefork"
    elif pool not in ("prefork", "processes", "solo"):
        print(f"potatoq: pool {pool!r} is not supported; using prefork", file=sys.stderr)
        pool = "prefork"
    if pool == "solo":
        return run_solo(app, args)
    worker = Supervisor(
        app,
        concurrency=args.concurrency,
        queues=args.queues,
        hostname=args.hostname,
        loglevel=args.loglevel,
        logfile=args.logfile,
        max_tasks_per_child=args.max_tasks_per_child,
        max_memory_per_child=args.max_memory_per_child,
        scheduler=False if args.no_scheduler else None,
        shutdown_timeout=args.shutdown_timeout,
        threads=args.threads,
    )
    return worker.start()


def run_solo(app: Potatoq, args: argparse.Namespace) -> int:
    """Run tasks in this process, one at a time (handy for debugging with pdb)."""
    import signal

    from .log import setup_logging
    from .worker import executor

    setup_logging(app, args.loglevel or "INFO", args.logfile)
    app.loader_import_default_modules()
    queues = [q for q in (args.queues or app.conf.task_default_queue).split(",") if q]
    hostname = args.hostname or f"potatoq@{socket.gethostname()}"
    node_id = f"{hostname}:{os.getpid()}:solo"
    consumer = app.broker.consumer(queues, node_id)
    stop = {"flag": False}

    def _stop(*_: Any) -> None:
        stop["flag"] = True
        consumer.interrupt()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    logger.info("potatoq solo worker %s consuming %s", hostname, ",".join(queues))
    scheduler = None
    if not args.no_scheduler:
        from .worker.scheduler import Scheduler

        scheduler = Scheduler(app) or None
        if scheduler:
            scheduler.start()
    current: dict[str, Any] = {}
    from .control import publish_registered

    info = {"hostname": hostname, "pid": os.getpid(), "queues": queues, "concurrency": 1}
    info["registered"] = publish_registered(app)
    done = threading.Event()

    def heartbeat() -> None:
        # In a thread, so a long task doesn't make this worker look dead (others would
        # recover and rerun its task). Brokers keep one connection per thread.
        while True:
            delivery = current.get("delivery")
            try:
                app.broker.heartbeat(node_id, {**info, "running": [delivery.message.id] if delivery else []})
                if delivery is not None:
                    app.broker.extend([delivery])
            except Exception:
                logger.exception("Heartbeat failed")
            if done.wait(app.conf.worker_heartbeat_interval):
                return

    beat = threading.Thread(target=heartbeat, name="potatoq-heartbeat", daemon=True)
    beat.start()
    try:
        while not stop["flag"]:
            if scheduler is not None:
                scheduler.tick()  # between tasks: a long task delays sends (60 s catch-up)
            app.broker.tick()
            delivery = consumer.fetch(timeout=1.0)
            if delivery is None:
                continue
            current["delivery"] = delivery
            try:
                outcome = executor.execute(
                    app, delivery.message, delivery_count=delivery.delivery_count, hostname=hostname
                )
                executor.settle(app, consumer, delivery, outcome)
            finally:
                current.pop("delivery", None)
            executor.log_done(delivery.message, outcome)
    finally:
        done.set()
        beat.join()
    app.broker.unregister(node_id)
    return 0


def cmd_beat(app: Potatoq, args: argparse.Namespace) -> int:
    from .log import setup_logging
    from .worker.scheduler import Scheduler

    setup_logging(app, args.loglevel)
    app.loader_import_default_modules()
    scheduler = Scheduler(app)
    if not scheduler:
        logger.warning("beat_schedule is empty; nothing to do")
    scheduler.start()
    logger.info(
        "Every potatoq worker already runs the scheduler; a separate beat is optional",
        extra={"potatoq_tag": "note"},
    )
    try:
        while True:
            scheduler.tick()
            time.sleep(min(1.0, max(0.05, scheduler.seconds_until_next())))
    except KeyboardInterrupt:
        return 0


def _print(data: Any, as_json: bool) -> None:
    if as_json:
        print(json.dumps(data, indent=2, default=str))


def _out() -> console.Painter:
    return console.painter(sys.stdout)


def _coarse(seconds: float) -> str:
    """``42s``, ``5m``, ``2h13m``, ``3d4h``: precise enough for a glance."""
    seconds = int(abs(seconds))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        hours, minutes = divmod(seconds // 60, 60)
        return f"{hours}h{minutes:02d}m" if minutes else f"{hours}h"
    days, hours = divmod(seconds // 3600, 24)
    return f"{days}d{hours}h" if hours else f"{days}d"


def _ago(seconds: float) -> str:
    return "just now" if seconds < 1 else f"{_coarse(seconds)} ago"


def cmd_schedule(app: Potatoq, args: argparse.Namespace) -> int:
    from datetime import UTC, datetime

    from .worker.scheduler import describe_schedule, load_entries, local_time

    out = _out()
    app.loader_import_default_modules()
    entries = load_entries(app)
    if not entries:
        print(out.line("schedule", "No periodic tasks (beat_schedule is empty)", "yellow"))
        return 0
    last = app.broker.last_periodic_runs()
    now = datetime.now(UTC)
    rows = []
    for entry in entries:
        try:
            upcoming = entry.schedule.next_after(now)
            next_run = local_time(app, upcoming)[: -len(app.conf.timezone or "UTC") - 1]
            wait = (upcoming - now).total_seconds()
            next_run += out.style(f" (in {_coarse(wait)})" if wait >= 1 else " (due now)", "dim")
        except Exception as exc:
            next_run = out.style(f"unknown ({exc})", "red")
        sent = last.get(entry.name)
        rows.append([
            out.style(entry.name, "bold"), entry.task, describe_schedule(entry.schedule), next_run,
            _ago(now.timestamp() - sent) if sent is not None else out.style("—", "dim"),
        ])  # fmt: skip
    zone = app.conf.timezone or "UTC"
    n = len(entries)
    print(out.line("schedule", f"{out.icon('⏰')}{n} periodic task{'s' if n != 1 else ''}, times in {zone}"))
    print()
    for line in out.table(["ENTRY", "TASK", "SCHEDULE", "NEXT RUN", "LAST SENT"], rows):
        print(line)
    return 0


def cmd_status(app: Potatoq, args: argparse.Namespace) -> int:
    workers = app.broker.workers()
    if args.json:
        _print(workers, True)
        return 0
    out = _out()
    if not workers:
        print(out.line("workers", "No live workers", "yellow"))
        return 1
    now = time.time()
    stale_after = 3 * float(app.conf.worker_heartbeat_interval)
    rows = []
    for w in workers:
        age = now - float(w.get("heartbeat", now))
        processes, threads = w.get("concurrency") or 1, w.get("threads") or 1
        concurrency = f"{processes * threads} ({processes}×{threads})" if threads > 1 else str(processes)  # noqa: RUF001
        rows.append([
            out.dot(age < stale_after) + w["id"], ",".join(w.get("queues", [])), concurrency,
            str(len(w.get("running", []))), _ago(age),
        ])  # fmt: skip
    print(out.line("workers", f"{len(workers)} live", "green"))
    print()
    for line in out.table(["WORKER", "QUEUES", "CONCURRENCY", "RUNNING", "HEARTBEAT"], rows, "<<>>>"):
        print(line)
    return 0


def cmd_queues(app: Potatoq, args: argparse.Namespace) -> int:
    sizes = app.broker.queue_sizes()
    if args.json:
        _print(sizes, True)
        return 0
    out = _out()
    if not sizes:
        print(out.line("queues", f"{out.icon('✨')}All queues are empty", "green"))
        return 0
    total = sum(sizes.values())
    print(out.line("queues", f"{total} task{'s' if total != 1 else ''} waiting"))
    print()
    for line in out.table(["QUEUE", "WAITING"], [[n, str(c)] for n, c in sorted(sizes.items())], "<>"):
        print(line)
    return 0


def cmd_purge(app: Potatoq, args: argparse.Namespace) -> int:
    out = _out()
    queues = [q for q in (args.queues or app.conf.task_default_queue).split(",") if q]
    if not args.force:
        answer = input(out.line("purge", f"Delete all waiting tasks in {', '.join(queues)}? [y/N] ", "yellow"))
        if answer.lower() not in ("y", "yes"):
            return 1
    for queue in queues:
        n = app.broker.purge(queue)
        print(out.line("purged", f"{out.icon('🧹')}{n} task{'s' if n != 1 else ''} from {queue}", "green"))
    return 0


def cmd_dead(app: Potatoq, args: argparse.Namespace) -> int:
    out = _out()
    if args.dead_command == "retry":
        for task_id in args.task_ids:
            if app.broker.requeue_dead(task_id):  # type: ignore[attr-defined]
                print(out.line("requeued", f"{out.icon('🔁')}{task_id}", "green"))
            else:
                print(out.line("missing", f"{task_id} isn't in the dead letters", "red"))
        return 0
    limit = getattr(args, "limit", 20)
    entries = app.broker.dead_letters(limit)
    if getattr(args, "json", False):
        _print(entries, True)
        return 0
    if not entries:
        print(out.line("dead", f"{out.icon('🎉')}No dead-lettered tasks", "green"))
        return 0
    now = time.time()
    rows = []
    for e in entries:
        reason = (e.get("reason") or "").strip().splitlines()
        why = reason[-1] if reason else ""
        why = why if len(why) <= 70 else why[:69] + "…"
        died = _ago(now - e["died_at"]) if e.get("died_at") else "?"
        rows.append([out.style(str(e.get("task")), "bold"), str(e.get("id")), str(e.get("queue")), died, why])
    n = len(entries)
    print(out.line("dead", f"{out.icon('💀')}{n} dead-lettered task{'s' if n != 1 else ''}, newest first", "red"))
    print()
    for line in out.table(["TASK", "ID", "QUEUE", "DIED", "REASON"], rows):
        print(line)
    print()
    print(out.line("tip", "Run one again with: potatoq dead retry <ID>", "grey"))
    return 0


def cmd_call(app: Potatoq, args: argparse.Namespace) -> int:
    app.loader_import_default_modules()
    options: dict[str, Any] = {}
    if args.queue:
        options["queue"] = args.queue
    result = app.send_task(
        args.name, json.loads(args.args), json.loads(args.kwargs), countdown=args.countdown, **options
    )
    out = _out()
    if not out.pretty:
        print(result.id)  # for scripts: just the id
        return 0
    queue = args.queue or app.route_for(args.name).get("queue") or app.conf.task_default_queue
    when = f" in {_coarse(args.countdown)}" if args.countdown else ""
    print(out.line("sent", f"{out.icon('📨')}{out.style(args.name, 'bold')} to {queue}{when}", "green"))
    print(out.line("id", result.id))
    return 0


_STATE_STYLES = {"SUCCESS": "green", "FAILURE": "red", "RETRY": "yellow", "REVOKED": "grey", "STARTED": "cyan"}


def cmd_result(app: Potatoq, args: argparse.Namespace) -> int:
    out = _out()
    result = app.AsyncResult(args.task_id)
    if args.wait is not None:
        try:
            value = result.get(timeout=args.wait, propagate=False)
        except Exception as exc:
            print(out.line("state", out.style(result.state, _STATE_STYLES.get(result.state, "yellow"))))
            print(out.line("error", str(exc), "red"))
            return 1
    else:
        value = result.result
    print(out.line("state", out.style(result.state, _STATE_STYLES.get(result.state, "cyan"), "bold")))
    print(
        out.line(
            "error" if result.state == "FAILURE" else "result",
            repr(value),
            "red" if result.state == "FAILURE" else "cyan",
        )
    )
    if result.traceback:
        print(out.style(result.traceback.rstrip(), "dim"))
    return 0


def cmd_inspect(app: Potatoq, args: argparse.Namespace) -> int:
    destination = args.destination.split(",") if args.destination else None
    replies = getattr(app.control.inspect(destination=destination), args.what)()
    if not replies:
        print(_out().line("error", "No workers replied (is a worker running? see potatoq status)", "red"))
        return 1
    print(json.dumps(replies, indent=2, default=str))
    return 0


def cmd_migrate(app: Potatoq, args: argparse.Namespace) -> int:
    out = _out()
    app.broker.setup()
    print(out.line("broker", f"{out.icon('✅')}Set up {redact_url(app.broker.url)}", "green"))
    if app.backend is not None and app.backend is not app.broker:
        app.backend.setup()
        print(out.line("results", f"{out.icon('✅')}Set up {redact_url(app.backend.url)}", "green"))
    return 0


def cmd_revoke(app: Potatoq, args: argparse.Namespace) -> int:
    app.control.revoke(args.task_ids)
    n = len(args.task_ids)
    print(_out().line("revoked", f"{_out().icon('🚫')}{n} task{'s' if n != 1 else ''}", "green"))
    return 0


def cmd_shell(app: Potatoq, args: argparse.Namespace) -> int:
    import code

    app.loader_import_default_modules()
    namespace = {
        "app": app,
        **{t.name.rsplit(".", 1)[-1]: t for t in app.tasks.values() if not t.name.startswith("potatoq.")},
    }
    code.interact(local=namespace, banner=f"potatoq shell ({app})")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
