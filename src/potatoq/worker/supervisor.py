"""The worker's main process: forks children, watches them, keeps the node alive.

The supervisor is deliberately single-threaded (so forking replacement children is
always safe) and never runs tasks. Its loop:

* reads "started"/"finished" events from each child's pipe, so it always knows which
  task every child is running and when its hard time limit expires;
* SIGKILLs children that blow their hard time limit and records the failure;
* when a child dies unexpectedly (OOM killer, segfault), immediately requeues its
  task, or dead-letters it if it already crashed workers ``task_max_deliveries`` times;
* heartbeats the node and extends task leases, recovers tasks of dead nodes,
  promotes scheduled tasks and runs the deduplicated periodic-task scheduler;
* on SIGTERM stops fetching, waits ``worker_shutdown_timeout`` for running tasks,
  then interrupts and requeues whatever is left (Kubernetes/Heroku friendly).
"""

from __future__ import annotations

import errno
import logging
import math
import os
import random
import selectors
import signal
import socket
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .. import serialization, signals
from ..brokers.base import Delivery
from ..config import redact_url
from ..control import publish_registered
from ..exceptions import TimeLimitExceeded, WorkerLostError
from ..message import Message
from . import executor
from .child import child_main
from .scheduler import Scheduler

if TYPE_CHECKING:
    from ..app import Potatoq

logger = logging.getLogger("potatoq.worker")


def default_concurrency() -> int:
    """CPUs actually available to this process: honours affinity and cgroup quotas
    (Celery uses the host's CPU count, which over-subscribes containers)."""
    if hasattr(os, "process_cpu_count"):
        n = os.process_cpu_count() or 1
    elif hasattr(os, "sched_getaffinity"):
        n = len(os.sched_getaffinity(0))
    else:
        n = os.cpu_count() or 1
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            quota, period = f.read().split()
        if quota != "max":
            n = min(n, max(1, math.ceil(int(quota) / int(period))))
    except (OSError, ValueError):
        pass
    return max(1, n)


def parse_memory(value: Any) -> int | None:
    """``"512MB"``/``"2GiB"``/``262144`` (KiB, like Celery) -> KiB."""
    if value in (None, "", 0):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().upper().replace("IB", "B")
    units = {"KB": 1, "K": 1, "MB": 1024, "M": 1024, "GB": 1024**2, "G": 1024**2}
    for suffix, factor in sorted(units.items(), key=lambda kv: -len(kv[0])):
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * factor)
    return int(text)


@dataclass
class Running:
    delivery: Delivery
    started: float
    hard_deadline: float | None
    #: The task's hard time limit in seconds.
    limit: float | None = None


@dataclass
class ChildProc:
    pid: int
    index: int
    read_fd: int
    started_at: float = field(default_factory=time.monotonic)
    buffer: bytes = b""
    #: Tasks running in this process (several with ``--threads``), by task id.
    running: dict[str, Running] = field(default_factory=dict)
    #: Ids of the tasks whose hard time limit made us kill this process.
    timed_out: set[str] = field(default_factory=set)
    killed_for_timeout: bool = False
    term_sent: bool = False
    abort_sent: bool = False


class Supervisor:
    #: Seconds between re-storing the registered task names (they expire after a week).
    registered_refresh = 3600.0

    def __init__(
        self,
        app: Potatoq,
        *,
        concurrency: int | None = None,
        queues: list[str] | str | None = None,
        hostname: str | None = None,
        loglevel: str | None = None,
        logfile: str | None = None,
        max_tasks_per_child: int | None = -1,
        max_memory_per_child: Any = -1,
        scheduler: bool | None = None,
        shutdown_timeout: float | None = None,
        threads: int | None = None,
        **kwargs: Any,
    ):
        self.app = app
        conf = app.conf
        self.concurrency = int(concurrency or conf.worker_concurrency or default_concurrency())
        self.threads = max(1, int(threads or conf.worker_threads or 1))
        if isinstance(queues, str):
            queues = [q.strip() for q in queues.split(",") if q.strip()]
        self.queues = list(queues or [conf.task_default_queue])
        self.hostname = hostname or f"potatoq@{socket.gethostname()}"
        self.registered_digest: str | None = None
        #: Set by SIGHUP: once stopped, the worker starts again (see cli.cmd_worker).
        self.reload_requested = False
        #: Signals received but not acted on yet (see _record_signal).
        self._signals: deque[int] = deque()
        self.node_id = f"{self.hostname}:{os.getpid()}:{random.randrange(16**6):06x}"
        self.loglevel = loglevel
        self.logfile = logfile
        self.max_tasks_per_child = conf.worker_max_tasks_per_child if max_tasks_per_child == -1 else max_tasks_per_child
        self.max_memory_kib = parse_memory(
            conf.worker_max_memory_per_child if max_memory_per_child == -1 else max_memory_per_child
        )
        self.scheduler_enabled = conf.worker_enable_scheduler if scheduler is None else scheduler
        self.shutdown_timeout = conf.worker_shutdown_timeout if shutdown_timeout is None else shutdown_timeout
        self.children: dict[int, ChildProc] = {}
        self.selector = selectors.DefaultSelector()
        self.shutting_down = False
        self.cold = False
        self.shutdown_deadline: float | None = None
        self._wake_r, self._wake_w = os.pipe()
        self._recent_crashes: list[float] = []
        self.consumer: Any = None
        self.scheduler: Scheduler | None = None
        self.exitcode = 0

    # --- lifecycle -----------------------------------------------------------------

    def start(self) -> int:
        app = self.app
        if self.loglevel or not logging.getLogger().handlers:
            from ..log import setup_logging

            setup_logging(app, self.loglevel or "INFO", self.logfile)
        app.loader_import_default_modules()
        signals.worker_init.send(sender=self)
        broker = app.broker  # connects and creates the schema
        self.registered_digest = publish_registered(app)
        self.consumer = broker.consumer(self.queues, self.node_id, pid=0)
        broker.heartbeat(self.node_id, self._info())
        self._install_signals()
        if self.scheduler_enabled and app.conf.beat_schedule:
            self.scheduler = Scheduler(app)
        self._banner()
        if self.scheduler is not None:
            self.scheduler.start()  # lists the entries, after the banner
        # Children must not inherit our broker connections.
        broker.close()
        for index in range(self.concurrency):
            self._spawn(index)
        signals.worker_ready.send(sender=self)
        try:
            self._loop()
        finally:
            self._finish()
        return self.exitcode

    def _info(self) -> dict[str, Any]:
        return {
            "hostname": self.hostname,
            "pid": os.getpid(),
            "queues": self.queues,
            "concurrency": self.concurrency,
            "threads": self.threads,
            "running": [task_id for c in self.children.values() for task_id in c.running],
            "registered": self.registered_digest,
        }

    def _banner(self) -> None:
        from .. import __version__
        from .executor import duration

        app = self.app
        conf = app.conf
        backend = app.backend
        results = redact_url(backend.url) if backend else "disabled"
        ignore = conf.task_ignore_result
        if backend and (ignore or (ignore is None and not app.results_enabled_by_default())):
            # Tasks can still opt in with ignore_result=False.
            results = f"not stored by default (backend {results})"
        if self.threads > 1:
            processes = f"{self.concurrency} process{'es' if self.concurrency != 1 else ''}"
            workers = f"{self.concurrency * self.threads} = {processes} × {self.threads} threads"  # noqa: RUF001
        else:
            workers = f"{self.concurrency} process{'es' if self.concurrency != 1 else ''}"
        hard, soft = conf.task_time_limit, conf.task_soft_time_limit
        limits = f"{duration(hard)} per task" if hard else "no time limit"
        if hard and soft:
            limits += f" (soft {duration(soft)})"
        if self.max_tasks_per_child:
            limits += f", new process every {self.max_tasks_per_child} tasks"
        tasks = sorted(n for n in app.tasks if not n.startswith("potatoq."))
        shown = ", ".join(tasks[:6]) + (f", … ({len(tasks) - 6} more)" if len(tasks) > 6 else "")

        def say(tag: str, text: str, *args: Any, level: int = logging.INFO, icon: str | None = None) -> None:
            logger.log(level, text, *args, extra={"potatoq_tag": tag, "potatoq_icon": icon})

        say("potatoq", "Worker %s is ready (potatoq %s)", self.hostname, __version__, icon="🥔")
        say("broker", "%s", redact_url(app.broker.url))
        say("results", "%s", results)
        say("queues", "%s", ", ".join(self.queues))
        say("workers", "%s", workers)
        say("limits", "%s", limits)
        n = len(self.scheduler.entries) if self.scheduler else 0
        say("schedule", "%s", f"{n} periodic task{'s' if n != 1 else ''}" if n else "off")
        say("tasks", "%d registered: %s", len(tasks), shown or "none")
        logger.debug("Registered tasks: %s", ", ".join(tasks))
        if self.threads > 1:
            say(
                "note",
                "Threads: soft time limits interrupt a task at its next Python instruction (not inside a "
                "blocking C call); a hard time limit kills the whole process and requeues the other %d task(s) "
                "running in it.",
                self.threads - 1,
            )
        if not self.consumer.can_settle_foreign and backend is None:
            say(
                "warning",
                "No result backend: tasks killed for exceeding their hard time limit will be redelivered by "
                "RabbitMQ (up to task_max_deliveries times). Set result_backend to record them as failed instead.",
                level=logging.WARNING,
            )

    def _install_signals(self) -> None:
        os.set_blocking(self._wake_w, False)
        os.set_blocking(self._wake_r, False)
        signal.set_wakeup_fd(self._wake_w, warn_on_full_buffer=False)
        self.selector.register(self._wake_r, selectors.EVENT_READ, None)
        for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT, signal.SIGHUP):
            signal.signal(signum, self._record_signal)
        signal.signal(signal.SIGCHLD, lambda *a: None)  # wakes select via the wakeup fd

    def _record_signal(self, signum: int, frame: Any) -> None:
        """The signal handler: only note the signal. Python runs handlers between any two
        bytecodes, including inside another handler (systemd and ``uv run`` can each
        send a SIGTERM at the same moment), so logging or signalling children here
        would race. The wakeup fd makes the main loop act on it right away."""
        self._signals.append(signum)

    def _handle_signals(self) -> None:
        """Act on recorded signals, in the main loop, one at a time."""
        while self._signals:
            signum = self._signals.popleft()
            if signum == signal.SIGINT:
                self._on_int(signum, None)
            elif signum == signal.SIGQUIT:
                self._on_cold(signum, None)
            elif signum == signal.SIGHUP:
                self._on_hup(signum, None)
            else:
                self._on_term(signum, None)

    def _on_term(self, signum: int, frame: Any) -> None:
        self._begin_shutdown(cold=False, sig="SIGTERM")

    def _on_int(self, signum: int, frame: Any) -> None:
        self._begin_shutdown(cold=self.shutting_down, sig="SIGINT")  # second Ctrl-C = cold

    def _on_cold(self, signum: int, frame: Any) -> None:
        self._begin_shutdown(cold=True, sig="SIGQUIT")

    def _on_hup(self, signum: int, frame: Any) -> None:
        """Reload: a warm shutdown, after which the CLI starts the worker again in this
        same process (``os.execv``), so it runs the code and settings on disk now."""
        if self.shutting_down:
            return  # already stopping; a reload can't override that
        self.reload_requested = True
        self._begin_shutdown(cold=False, sig="SIGHUP")

    def _begin_shutdown(self, cold: bool, sig: str = "SIGTERM") -> None:
        if not self.shutting_down:
            running = sum(len(c.running) for c in self.children.values())
            what = "Reloading" if self.reload_requested else "Warm shutdown"
            if running:
                logger.info(
                    "%s: waiting up to %gs for %d running task(s) (Ctrl+C to stop now)",
                    what, self.shutdown_timeout, running, extra={"potatoq_icon": "👋"},
                )  # fmt: skip
            else:
                logger.info("Reloading" if self.reload_requested else "Shutting down", extra={"potatoq_icon": "👋"})
            signals.worker_shutting_down.send(sender=self.hostname, sig=sig, how="Warm", exitcode=0)
            self.shutting_down = True
            self.shutdown_deadline = time.monotonic() + self.shutdown_timeout
            for child in self.children.values():
                self._kill(child, signal.SIGTERM)
                child.term_sent = True
        if cold and not self.cold:
            logger.info("Cold shutdown: interrupting running tasks", extra={"potatoq_icon": "🛑"})
            self.cold = True
            self.shutdown_deadline = time.monotonic()

    # --- children ------------------------------------------------------------------

    def _spawn(self, index: int) -> None:
        read_fd, write_fd = os.pipe()
        sys.stdout.flush()
        sys.stderr.flush()
        pid = os.fork()
        if pid == 0:  # child
            os.close(read_fd)
            os.close(self._wake_r)
            os.close(self._wake_w)
            signal.set_wakeup_fd(-1)
            child_main(
                self.app, node_id=self.node_id, queues=self.queues, write_fd=write_fd, index=index,
                hostname=self.hostname, max_tasks=self.max_tasks_per_child, max_memory_kib=self.max_memory_kib,
                threads=self.threads,
            )  # fmt: skip
        os.close(write_fd)
        os.set_blocking(read_fd, False)
        child = ChildProc(pid=pid, index=index, read_fd=read_fd)
        self.children[pid] = child
        self.selector.register(read_fd, selectors.EVENT_READ, child)

    def _kill(self, child: ChildProc, sig: int) -> None:
        try:
            os.kill(child.pid, sig)
        except ProcessLookupError:
            pass

    def _read_child(self, child: ChildProc) -> None:
        while True:
            try:
                chunk = os.read(child.read_fd, 65536)
            except BlockingIOError:
                break
            except OSError as exc:
                if exc.errno == errno.EINTR:
                    continue
                break
            if not chunk:
                break
            child.buffer += chunk
        *lines, child.buffer = child.buffer.split(b"\n")
        for line in lines:
            if not line:
                continue
            event = serialization.loads(line)
            if event["e"] == "start":
                delivery = Delivery(
                    Message.from_dict(event["message"]),
                    delivery_count=event["count"],
                    handle=_to_handle(event["handle"]),
                )
                hard = event.get("hard")
                limit = hard - event["started"] if hard else None
                child.running[delivery.message.id] = Running(
                    delivery=delivery,
                    started=event["started"],
                    # A little grace so the soft limit's exception can be handled first.
                    hard_deadline=time.monotonic() + limit + 1.0 if limit else None,
                    limit=limit,
                )
            elif event["e"] == "done":
                child.running.pop(event["id"], None)

    def _reap(self) -> None:
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return
            if pid == 0:
                return
            child = self.children.pop(pid, None)
            if child is None:
                continue
            self._read_child(child)  # final events
            self.selector.unregister(child.read_fd)
            os.close(child.read_fd)
            self._on_child_exit(child, status)

    def _on_child_exit(self, child: ChildProc, status: int) -> None:
        code = os.waitstatus_to_exitcode(status)
        lost: list[Delivery] = [r.delivery for r in child.running.values()]
        if self.consumer.can_settle_foreign:
            known = {d.message.id for d in lost}
            try:
                lost += [
                    d for d in self.app.broker.lost_deliveries(self.node_id, child.pid) if d.message.id not in known
                ]
            except Exception:
                logger.exception("Could not look up tasks of dead child %d", child.pid)
        for delivery in lost:
            try:
                self._handle_lost(child, delivery, code)
            except Exception:
                # The broker is unreachable: the task stays claimed by this (dead)
                # process and is recovered once the broker is back (lease expiry /
                # recover()); never let it take the supervisor down.
                logger.exception("Could not recover task %s of dead child %d", delivery.message.id, child.pid)
        if code != 0 and not child.killed_for_timeout and not (self.shutting_down and child.abort_sent):
            logger.error("Child %d exited unexpectedly (code %s)", child.pid, code)
            self._recent_crashes = [t for t in self._recent_crashes if time.monotonic() - t < 10] + [time.monotonic()]
        if not self.shutting_down:
            if len(self._recent_crashes) > 3 * self.concurrency:
                logger.critical("Children keep crashing; backing off for 1s")
                time.sleep(1)
                self._recent_crashes.clear()
            self._spawn(child.index)

    def _handle_lost(self, child: ChildProc, delivery: Delivery, code: int) -> None:
        app = self.app
        message = delivery.message
        consumer = self.consumer
        if message.id in child.timed_out:
            running = child.running.get(message.id)
            limit = running.limit if running else None
            exc: BaseException = TimeLimitExceeded(
                f"Task {message.task}[{message.id}] exceeded its time limit ({limit:g}s) and was killed"
                if limit
                else "time limit exceeded"
            )
            # Logged once, when the process was killed (_enforce_time_limits).
            outcome = executor.failure_outcome(app, message, exc, self.hostname)
        elif (self.shutting_down and child.abort_sent) or child.killed_for_timeout:
            # Interrupted by shutdown, or an innocent bystander of another task's hard
            # time limit in the same (threaded) process: not this task's fault.
            reason = "shutdown" if not child.killed_for_timeout else "another task's time limit"
            logger.warning("Requeueing %s[%s] interrupted by %s", message.task, message.id, reason)
            if consumer.can_settle_foreign:
                consumer.requeue(delivery, count=False)
            return
        else:
            limit = int(app.conf.task_max_deliveries)
            if delivery.delivery_count < limit and app.conf.task_reject_on_worker_lost:
                logger.warning(
                    "Process running %s[%s] died (code %s); requeueing (delivery %d of %d)",
                    message.task, message.id, code, delivery.delivery_count, limit,
                )  # fmt: skip
                if consumer.can_settle_foreign:
                    consumer.requeue(delivery, count=True)
                return
            exc = WorkerLostError(
                f"Worker exited prematurely (code {code}) while running {message.task}[{message.id}], {delivery.delivery_count} time(s)"
            )
            logger.error("%s; dead-lettering", exc)
            outcome = executor.failure_outcome(app, message, exc, self.hostname)
        if consumer.can_settle_foreign:
            executor.settle(app, consumer, delivery, outcome)
        else:
            # e.g. RabbitMQ: the broker redelivers by itself; the stored final result
            # makes the redelivered copy a no-op (see executor.execute).
            if outcome.record is not None and app.backend is not None:
                app.backend.store_result(outcome.record, expires=app.conf.result_expires)
            if outcome.followups:
                app.publish_now(outcome.followups)

    # --- main loop -----------------------------------------------------------------

    def _loop(self) -> None:
        conf = self.app.conf
        broker = self.app.broker
        now = time.monotonic()
        heartbeat_every = float(conf.worker_heartbeat_interval)
        timers = {
            "heartbeat": now + heartbeat_every,
            "recover": now + random.uniform(1, 5),
            "tick": now + 1.0,
            "maintenance": now + random.uniform(30, 90),
            "registered": now + self.registered_refresh,
            "scheduler": now + 0.5,
        }
        while True:
            self._handle_signals()
            now = time.monotonic()
            if self.shutting_down:
                if not self.children:
                    return
                self._shutdown_step(now)
            timeout = max(0.0, min(min(timers.values()) - now, 0.5))
            for key, _ in self.selector.select(timeout):
                if key.data is None:
                    try:
                        while os.read(self._wake_r, 512):
                            pass
                    except BlockingIOError:
                        pass
                else:
                    self._read_child(key.data)
            try:
                self._reap()
            except Exception:
                logger.exception("Error while handling exited children")
            now = time.monotonic()
            self._enforce_time_limits(now)
            try:
                if now >= timers["heartbeat"]:
                    timers["heartbeat"] = now + heartbeat_every
                    broker.heartbeat(self.node_id, self._info())
                    running = [r.delivery for c in self.children.values() for r in c.running.values()]
                    if running:
                        broker.extend(running)
                if self.shutting_down:
                    continue
                if now >= timers["tick"]:
                    timers["tick"] = now + 1.0
                    broker.tick()
                if now >= timers["recover"]:
                    timers["recover"] = now + random.uniform(5, 15)
                    for delivery in broker.recover(float(conf.worker_dead_after)):
                        exc = WorkerLostError(
                            f"Worker node died while running {delivery.message.task}[{delivery.message.id}] {delivery.delivery_count} time(s)"
                        )
                        outcome = executor.failure_outcome(self.app, delivery.message, exc, self.hostname)
                        if outcome.record is not None and self.app.backend is not None:
                            self.app.backend.store_result(outcome.record, expires=conf.result_expires)
                        if outcome.followups:
                            self.app.publish_now(outcome.followups)
                if now >= timers["maintenance"]:
                    timers["maintenance"] = now + random.uniform(45, 75)
                    broker.maintenance()
                if now >= timers["registered"]:
                    timers["registered"] = now + self.registered_refresh
                    publish_registered(self.app)  # refresh before it expires
                if self.scheduler is not None and now >= timers["scheduler"]:
                    self.scheduler.tick()
                    timers["scheduler"] = now + min(1.0, max(0.05, self.scheduler.seconds_until_next()))
            except Exception:
                logger.exception("Supervisor housekeeping failed (will retry)")

    def _enforce_time_limits(self, now: float) -> None:
        for child in list(self.children.values()):
            if child.killed_for_timeout:
                continue
            expired = [
                tid for tid, r in child.running.items() if r.hard_deadline is not None and now >= r.hard_deadline
            ]
            if expired:
                child.killed_for_timeout = True
                child.timed_out.update(expired)
                others = len(child.running) - len(expired)
                for tid in expired:
                    r = child.running[tid]
                    logger.error(
                        "Task %s[%s] exceeded its time limit (%gs); killing process %d%s",
                        r.delivery.message.task, tid, r.limit, child.pid,
                        f" and requeueing its {others} other task(s)" if others else "",
                    )  # fmt: skip
                self._kill(child, signal.SIGKILL)

    def _shutdown_step(self, now: float) -> None:
        if self.shutdown_deadline is None or now < self.shutdown_deadline:
            return
        for child in self.children.values():
            if not child.abort_sent:
                child.abort_sent = True
                self._kill(child, signal.SIGUSR1)  # interrupt the task; it gets requeued
        if now >= self.shutdown_deadline + 5.0:
            for child in self.children.values():
                self._kill(child, signal.SIGKILL)

    def _finish(self) -> None:
        self.shutting_down = True  # no respawning from here on
        for child in list(self.children.values()):
            self._kill(child, signal.SIGKILL)
        deadline = time.monotonic() + 5
        while self.children and time.monotonic() < deadline:
            self._reap()
            time.sleep(0.05)
        try:
            self.app.broker.unregister(self.node_id)
        except Exception:
            logger.exception("Could not unregister worker")
        signals.worker_shutdown.send(sender=self)
        stopped = "stopped, reloading" if self.reload_requested else "stopped"
        logger.info("Worker %s %s", self.hostname, stopped, extra={"potatoq_icon": "🥔"})


def _to_handle(handle: Any) -> Any:
    return tuple(handle) if isinstance(handle, list) else handle
