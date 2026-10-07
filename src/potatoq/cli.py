"""``potatoq`` command line: ``potatoq -A proj worker``, mirroring ``celery``."""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import sys
import time
from typing import Any

from .app import Potatoq, current_app

logger = logging.getLogger("potatoq")

#: Celery flags that are unnecessary in Potatoq (accepted, ignored).
IGNORED_FLAGS = ("--without-gossip", "--without-mingle", "--without-heartbeat", "-E", "--task-events", "--events")


def find_app(spec: str | Potatoq | None) -> Potatoq:
    """Resolve ``-A``: ``proj``, ``proj.celery``, ``proj.celery:app`` or an instance."""
    if isinstance(spec, Potatoq):
        return spec
    sys.path.insert(0, os.getcwd())
    if not spec:
        if os.environ.get("DJANGO_SETTINGS_MODULE"):
            import django

            django.setup()
        return current_app()
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
        if obj is not None and hasattr(obj, "__path__"):
            continue
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="potatoq", description="Potatoq task queue")
    parser.add_argument("-A", "--app", help="Application, e.g. proj or proj.celery:app")
    parser.add_argument("-b", "--broker", help="Broker URL (overrides configuration)")
    parser.add_argument("--result-backend", help="Result backend URL")
    parser.add_argument("--workdir", help="Change to this directory first")
    sub = parser.add_subparsers(dest="command")

    w = sub.add_parser("worker", help="Start a worker")
    w.add_argument("-c", "--concurrency", type=int, help="Number of worker processes (default: available CPUs)")
    w.add_argument("-Q", "--queues", help="Comma separated queues to consume (default: default queue)")
    w.add_argument("-l", "--loglevel", default=None, help="DEBUG, INFO, WARNING, ERROR")
    w.add_argument("-f", "--logfile")
    w.add_argument("-n", "--hostname")
    w.add_argument("-P", "--pool", default="prefork", help="prefork (default) or solo (one task at a time, in-process)")
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

    sub.add_parser("migrate", help="Create the broker schema (tables/queues)")
    rv = sub.add_parser("revoke", help="Revoke tasks that haven't started")
    rv.add_argument("task_ids", nargs="+")
    sub.add_parser("shell", help="Python shell with the app loaded")
    return parser


def main(argv: list[Any] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    app_obj = None
    if len(argv) >= 2 and argv[0] in ("-A", "--app") and isinstance(argv[1], Potatoq):
        app_obj = argv[1]
        argv = argv[2:]
    args = build_parser().parse_args([str(a) for a in argv])
    if args.workdir:
        os.chdir(args.workdir)
    app = app_obj or find_app(args.app)
    if args.broker:
        app.conf.broker_url = args.broker
    if args.result_backend:
        app.conf.result_backend = args.result_backend
    command = args.command or "worker"
    handler = globals()[f"cmd_{command}"]
    return int(handler(app, args) or 0)


def cmd_worker(app: Potatoq, args: argparse.Namespace) -> int:
    from .worker.supervisor import Supervisor

    if args.time_limit:
        app.conf.task_time_limit = args.time_limit
    if args.soft_time_limit:
        app.conf.task_soft_time_limit = args.soft_time_limit
    if args.pidfile:
        with open(args.pidfile, "w") as f:
            f.write(str(os.getpid()))
    pool = (args.pool or "prefork").lower()
    if pool not in ("prefork", "processes", "solo"):
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
    )
    return worker.start()


def run_solo(app: Potatoq, args: argparse.Namespace) -> int:
    """Run tasks in this process, one at a time (handy for debugging with pdb)."""
    import signal
    import socket

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
    last_beat = 0.0
    while not stop["flag"]:
        if time.monotonic() - last_beat > app.conf.worker_heartbeat_interval:
            app.broker.heartbeat(
                node_id, {"hostname": hostname, "pid": os.getpid(), "queues": queues, "concurrency": 1}
            )
            app.broker.tick()
            last_beat = time.monotonic()
        delivery = consumer.fetch(timeout=1.0)
        if delivery is None:
            continue
        outcome = executor.execute(app, delivery.message, delivery_count=delivery.delivery_count, hostname=hostname)
        executor.settle(app, consumer, delivery, outcome)
        logger.info(
            "Task %s[%s] %s in %.3fs",
            delivery.message.task,
            delivery.message.id,
            outcome.state.lower(),
            outcome.runtime,
        )
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
    logger.info("Note: every potatoq worker already runs the scheduler; a separate beat is optional")
    try:
        while True:
            scheduler.tick()
            time.sleep(min(1.0, max(0.05, scheduler.seconds_until_next())))
    except KeyboardInterrupt:
        return 0


def _print(data: Any, as_json: bool) -> None:
    if as_json:
        print(json.dumps(data, indent=2, default=str))


def cmd_status(app: Potatoq, args: argparse.Namespace) -> int:
    workers = app.broker.workers()
    if args.json:
        _print(workers, True)
        return 0
    if not workers:
        print("No live workers")
        return 1
    now = time.time()
    for w in workers:
        age = now - float(w.get("heartbeat", now))
        print(
            f"{w['id']}: queues={','.join(w.get('queues', []))} concurrency={w.get('concurrency')} running={len(w.get('running', []))} heartbeat={age:.0f}s ago"
        )
    return 0


def cmd_queues(app: Potatoq, args: argparse.Namespace) -> int:
    sizes = app.broker.queue_sizes()
    if args.json:
        _print(sizes, True)
        return 0
    for name, size in sorted(sizes.items()):
        print(f"{name}: {size}")
    if not sizes:
        print("All queues are empty")
    return 0


def cmd_purge(app: Potatoq, args: argparse.Namespace) -> int:
    queues = [q for q in (args.queues or app.conf.task_default_queue).split(",") if q]
    if not args.force:
        answer = input(f"Delete all waiting tasks in {', '.join(queues)}? [y/N] ")
        if answer.lower() not in ("y", "yes"):
            return 1
    for queue in queues:
        print(f"{queue}: purged {app.broker.purge(queue)} tasks")
    return 0


def cmd_dead(app: Potatoq, args: argparse.Namespace) -> int:
    if args.dead_command == "retry":
        for task_id in args.task_ids:
            ok = app.broker.requeue_dead(task_id)  # type: ignore[attr-defined]
            print(f"{task_id}: {'requeued' if ok else 'not found'}")
        return 0
    limit = getattr(args, "limit", 20)
    entries = app.broker.dead_letters(limit)
    if getattr(args, "json", False):
        _print(entries, True)
        return 0
    for e in entries:
        reason = (e.get("reason") or "").strip().splitlines()
        died = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(e["died_at"])) if e.get("died_at") else "?"
        print(f"{e['id']}  {e['task']}  queue={e['queue']}  died={died}  {reason[-1] if reason else ''}")
    if not entries:
        print("No dead-lettered tasks")
    return 0


def cmd_call(app: Potatoq, args: argparse.Namespace) -> int:
    app.loader_import_default_modules()
    options: dict[str, Any] = {}
    if args.queue:
        options["queue"] = args.queue
    result = app.send_task(
        args.name, json.loads(args.args), json.loads(args.kwargs), countdown=args.countdown, **options
    )
    print(result.id)
    return 0


def cmd_result(app: Potatoq, args: argparse.Namespace) -> int:
    result = app.AsyncResult(args.task_id)
    if args.wait is not None:
        try:
            value = result.get(timeout=args.wait, propagate=False)
        except Exception as exc:
            print(f"{result.state}: {exc}")
            return 1
    else:
        value = result.result
    print(f"{result.state}: {value!r}")
    if result.traceback:
        print(result.traceback)
    return 0


def cmd_migrate(app: Potatoq, args: argparse.Namespace) -> int:
    app.broker.setup()
    if app.backend is not None and app.backend is not app.broker:
        app.backend.setup()
    print("Schema is up to date")
    return 0


def cmd_revoke(app: Potatoq, args: argparse.Namespace) -> int:
    app.control.revoke(args.task_ids)
    print(f"Revoked {len(args.task_ids)} task(s)")
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
