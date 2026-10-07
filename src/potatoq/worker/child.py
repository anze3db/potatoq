"""A forked worker process: fetches one task at a time and runs it.

Each child owns its broker connection and only fetches when it is idle, so a
long-running task never holds other tasks hostage (Celery's prefetch problem).
It tells the supervisor which task it is running over a pipe, so the supervisor
can enforce hard time limits and recover the task if this process dies.
"""

from __future__ import annotations

import inspect
import json
import logging
import os
import resource
import signal
import sys
import time
from typing import TYPE_CHECKING, Any

from .. import signals
from ..exceptions import SoftTimeLimitExceeded, WorkerTerminate
from . import executor

if TYPE_CHECKING:
    from ..app import Potatoq
    from ..brokers.base import Delivery

logger = logging.getLogger("potatoq.worker")


def _rss_kib() -> int:
    """Current resident set size in KiB (peak RSS where current isn't available)."""
    try:
        with open("/proc/self/statm") as f:
            pages = int(f.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE") // 1024
    except (OSError, ValueError, IndexError):
        maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return maxrss // 1024 if sys.platform == "darwin" else maxrss


class Child:
    def __init__(
        self,
        app: Potatoq,
        node_id: str,
        queues: list[str],
        write_fd: int,
        index: int,
        hostname: str,
        max_tasks: int | None,
        max_memory_kib: int | None,
    ):
        self.app = app
        self.node_id = node_id
        self.queues = queues
        self.write_fd = write_fd
        self.index = index
        self.hostname = hostname
        self.max_tasks = max_tasks
        self.max_memory_kib = max_memory_kib
        self.parent_pid = os.getppid()
        self.stopping = False
        self.fetching = False
        self.consumer: Any = None

    # --- signals -------------------------------------------------------------------

    def _install_signals(self) -> None:
        signal.signal(signal.SIGINT, signal.SIG_IGN)  # Ctrl-C is handled by the supervisor
        signal.signal(signal.SIGTERM, self._on_term)
        signal.signal(signal.SIGUSR1, self._on_abort)
        signal.signal(signal.SIGALRM, self._on_soft_limit)
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)

    def _on_term(self, signum: int, frame: Any) -> None:
        # Never raise here: we might be in the middle of a claim. fetch() returns
        # within a second anyway, and a running task is allowed to finish.
        self.stopping = True
        if self.fetching and self.consumer is not None:
            self.consumer.interrupt()

    def _on_abort(self, signum: int, frame: Any) -> None:
        self.stopping = True
        if executor.IN_TASK_BODY:
            raise WorkerTerminate("worker shutting down")
        if self.fetching and self.consumer is not None:
            self.consumer.interrupt()

    def _on_soft_limit(self, signum: int, frame: Any) -> None:
        if executor.IN_TASK_BODY:
            raise SoftTimeLimitExceeded("soft time limit exceeded")

    # --- reporting -----------------------------------------------------------------

    def _report(self, event: dict[str, Any]) -> None:
        data = (json.dumps(event, separators=(",", ":"), default=str) + "\n").encode()
        while data:
            try:
                written = os.write(self.write_fd, data)
            except InterruptedError:
                continue
            data = data[written:]

    # --- main loop -----------------------------------------------------------------

    def run(self) -> int:
        import multiprocessing

        app = self.app
        multiprocessing.current_process().name = f"ForkPoolWorker-{self.index + 1}"
        self._install_signals()
        app._after_fork()
        signals.worker_process_init.send(sender=None)
        self.consumer = app.broker.consumer(self.queues, self.node_id)
        processed = 0
        try:
            while not self.stopping:
                if os.getppid() != self.parent_pid:
                    logger.warning("Supervisor died; child exiting")
                    break
                self.fetching = True
                try:
                    delivery = self.consumer.fetch(timeout=1.0)
                finally:
                    self.fetching = False
                if delivery is None:
                    continue
                if self.stopping:
                    self.consumer.requeue(delivery, count=False)
                    break
                if not self._process(delivery):
                    break
                processed += 1
                if self.max_tasks and processed >= self.max_tasks:
                    logger.info("Child %d recycling after %d tasks", os.getpid(), processed)
                    break
                if self.max_memory_kib and _rss_kib() > self.max_memory_kib:
                    logger.warning(
                        "Child %d recycling: memory %d KiB over limit %d KiB", os.getpid(), _rss_kib(), self.max_memory_kib
                    )
                    break
        finally:
            signals.worker_process_shutdown.send(sender=None, pid=os.getpid(), exitcode=0)
            try:
                self.consumer.close()
            except Exception:
                pass
        return 0

    def _process(self, delivery: Delivery) -> bool:
        """Run one task. Returns False if the child must exit afterwards."""
        app = self.app
        message = delivery.message
        task = app.tasks.get(message.task)
        hard, soft = task.resolved_time_limits(message.options) if task else (None, None)
        started = time.time()
        self._report(
            {
                "e": "start",
                "message": message.to_dict(),
                "count": delivery.delivery_count,
                "handle": delivery.handle,
                "hard": started + hard if hard else None,
                "started": started,
            }
        )
        is_async = task is not None and inspect.iscoroutinefunction(getattr(task.run, "__func__", task.run))
        if soft and not is_async:
            signal.setitimer(signal.ITIMER_REAL, soft)
        try:
            outcome = executor.execute(app, message, delivery_count=delivery.delivery_count, hostname=self.hostname)
        except WorkerTerminate:
            signal.setitimer(signal.ITIMER_REAL, 0)
            logger.warning("Task %s[%s] interrupted by shutdown; requeueing", message.task, message.id)
            self.consumer.requeue(delivery, count=False)
            self._report({"e": "done", "id": message.id})
            return False
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
        try:
            executor.settle(app, self.consumer, delivery, outcome)
        except Exception:
            logger.exception("Failed to settle task %s[%s]", message.task, message.id)
        level = logging.INFO if outcome.state in ("SUCCESS", "RETRY", "IGNORED") else logging.WARNING
        logger.log(level, "Task %s[%s] %s in %.3fs", message.task, message.id, outcome.state.lower(), outcome.runtime)
        self._report({"e": "done", "id": message.id})
        return True


def child_main(app: Potatoq, **kwargs: Any) -> None:
    """Entry point in the forked process; never returns."""
    code = 1
    try:
        code = Child(app, **kwargs).run()
    except BaseException:
        logger.exception("Worker child crashed")
    finally:
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        finally:
            os._exit(code)
