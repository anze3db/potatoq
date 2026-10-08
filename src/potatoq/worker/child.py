"""A forked worker process: runs ``threads`` task slots, each fetching one task at a time.

Each slot owns its broker connection and only fetches when it is idle, so a
long-running task never holds other tasks hostage (Celery's prefetch problem). The
child tells the supervisor which tasks it is running over a pipe, so the supervisor
can enforce hard time limits and recover the tasks if this process dies.

Time limits:

* ``threads=1`` (the default): the task runs on the main thread and the soft limit is
  a ``SIGALRM`` that raises ``SoftTimeLimitExceeded``. Signals interrupt blocking
  system calls, so this works even while the task waits on a socket.
* ``threads>1``: a thread can't receive signals, so the main thread *injects*
  ``SoftTimeLimitExceeded`` into the task's thread (``PyThreadState_SetAsyncExc``).
  It is raised at the next Python bytecode the thread executes: a thread blocked
  inside a C call (a socket read without timeout, a long C computation) only sees it
  when that call returns. The hard limit still applies to the whole process.

In both modes the hard limit is enforced by the supervisor killing the process;
other tasks that happened to share the process are requeued without penalty.
"""

from __future__ import annotations

import ctypes
import inspect
import logging
import os
import resource
import signal
import sys
import threading
import time
from typing import TYPE_CHECKING, Any

from .. import serialization, signals
from ..exceptions import SoftTimeLimitExceeded, WorkerTerminate
from . import executor

if TYPE_CHECKING:
    from ..app import Potatoq
    from ..brokers.base import Delivery

logger = logging.getLogger("potatoq.worker")

_SetAsyncExc = ctypes.pythonapi.PyThreadState_SetAsyncExc


def _rss_kib() -> int:
    """Current resident set size in KiB (peak RSS where current isn't available)."""
    try:
        with open("/proc/self/statm") as f:
            pages = int(f.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE") // 1024
    except (OSError, ValueError, IndexError):
        maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return maxrss // 1024 if sys.platform == "darwin" else maxrss


class Slot:
    """One task slot (a thread, or the main thread when ``threads=1``).

    ``enter``/``exit`` bracket the user's task code. Exceptions are only injected while
    the slot is inside that bracket and while holding ``lock``; ``exit`` clears any
    injection still pending, so an interruption can never land in broker bookkeeping.
    """

    def __init__(self, index: int):
        self.index = index
        self.lock = threading.Lock()
        self.thread_id: int | None = None
        self.in_body = False
        self.task_id: str | None = None
        self.soft_deadline: float | None = None
        self.soft_fired = False
        self.consumer: Any = None
        self.fetching = False

    def enter(self) -> None:
        with self.lock:
            self.in_body = True

    def exit(self) -> None:
        with self.lock:
            self.in_body = False
            if self.thread_id is not None:
                _SetAsyncExc(ctypes.c_ulong(self.thread_id), None)  # drop a pending injection

    def inject(self, exc_type: type[BaseException], task_id: str | None = None) -> bool:
        with self.lock:
            if not self.in_body or self.thread_id is None or (task_id is not None and task_id != self.task_id):
                return False
            return _SetAsyncExc(ctypes.c_ulong(self.thread_id), ctypes.py_object(exc_type)) == 1


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
        threads: int = 1,
    ):
        self.app = app
        self.node_id = node_id
        self.queues = queues
        self.write_fd = write_fd
        self.index = index
        self.hostname = hostname
        self.max_tasks = max_tasks
        self.max_memory_kib = max_memory_kib
        self.threads = max(1, int(threads or 1))
        self.parent_pid = os.getppid()
        self.stopping = False
        self.aborting = False
        self.exit_code = 0
        self.processed = 0
        self.slots = [Slot(i) for i in range(self.threads)]
        self._report_lock = threading.Lock()
        self._count_lock = threading.Lock()

    # --- signals (always delivered to the main thread) -------------------------------

    def _install_signals(self) -> None:
        signal.signal(signal.SIGINT, signal.SIG_IGN)  # Ctrl-C is handled by the supervisor
        signal.signal(signal.SIGTERM, self._on_term)
        signal.signal(signal.SIGUSR1, self._on_abort)
        signal.signal(signal.SIGALRM, self._on_soft_limit)
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)

    def _interrupt_fetches(self) -> None:
        for slot in self.slots:
            if slot.fetching and slot.consumer is not None:
                slot.consumer.interrupt()

    def _on_term(self, signum: int, frame: Any) -> None:
        # Never raise here: we might be in the middle of a claim. fetch() returns
        # within a second anyway, and running tasks are allowed to finish.
        self.stopping = True
        self._interrupt_fetches()

    def _on_abort(self, signum: int, frame: Any) -> None:
        self.stopping = True
        self.aborting = True
        self._interrupt_fetches()
        if self.threads == 1 and executor.in_task_body():
            raise WorkerTerminate("worker shutting down")

    def _on_soft_limit(self, signum: int, frame: Any) -> None:
        if executor.in_task_body():
            raise SoftTimeLimitExceeded("soft time limit exceeded")

    # --- reporting -----------------------------------------------------------------

    def _report(self, event: dict[str, Any]) -> None:
        data = (serialization.dumps(event) + "\n").encode()
        with self._report_lock:  # whole lines only; writes > PIPE_BUF aren't atomic
            while data:
                try:
                    written = os.write(self.write_fd, data)
                except InterruptedError:
                    continue
                data = data[written:]

    # --- lifecycle -----------------------------------------------------------------

    def run(self) -> int:
        import multiprocessing

        app = self.app
        multiprocessing.current_process().name = f"ForkPoolWorker-{self.index + 1}"
        self._install_signals()
        app._after_fork()
        signals.worker_process_init.send(sender=None)
        try:
            if self.threads == 1:
                self._slot_loop(self.slots[0])
            else:
                self._run_threads()
        finally:
            signals.worker_process_shutdown.send(sender=None, pid=os.getpid(), exitcode=self.exit_code)
        return self.exit_code

    def _run_threads(self) -> None:
        workers = [
            threading.Thread(target=self._slot_loop, args=(slot,), name=f"potatoq-slot-{slot.index + 1}", daemon=True)
            for slot in self.slots
        ]
        for t in workers:
            t.start()
        aborted = False
        while any(t.is_alive() for t in workers):
            if os.getppid() != self.parent_pid and not self.stopping:
                logger.warning("Supervisor died; child exiting")
                self.stopping = True
            if self.aborting and not aborted:
                aborted = True
                for slot in self.slots:
                    slot.inject(WorkerTerminate)
            self._check_soft_limits()
            time.sleep(0.05)

    def _check_soft_limits(self) -> None:
        now = time.monotonic()
        for slot in self.slots:
            deadline = slot.soft_deadline
            if deadline is not None and now >= deadline and not slot.soft_fired:
                slot.soft_fired = True
                if slot.inject(SoftTimeLimitExceeded, slot.task_id):
                    logger.warning("Soft time limit exceeded for task %s; interrupting it", slot.task_id)

    def _slot_loop(self, slot: Slot) -> None:
        slot.thread_id = threading.get_ident()
        if self.threads > 1:
            executor.set_body_guard(slot)
        slot.consumer = self.app.broker.consumer(self.queues, self.node_id)
        try:
            while not self.stopping:
                if self.threads == 1 and os.getppid() != self.parent_pid:
                    logger.warning("Supervisor died; child exiting")
                    break
                slot.fetching = True
                try:
                    delivery = slot.consumer.fetch(timeout=1.0)
                finally:
                    slot.fetching = False
                if delivery is None:
                    continue
                if self.stopping:
                    slot.consumer.requeue(delivery, count=False)
                    break
                if not self._process(slot, delivery):
                    break
                if self._done_one():
                    break
        except BaseException:
            logger.exception("Task slot %d crashed", slot.index + 1)
            self.exit_code = self.exit_code or 1
            self.stopping = True
        finally:
            try:
                slot.consumer.close()
            except Exception:
                pass

    def _done_one(self) -> bool:
        """Count a finished task; True when this process should be recycled."""
        with self._count_lock:
            self.processed += 1
            processed = self.processed
        if self.max_tasks and processed >= self.max_tasks:
            if not self.stopping:
                logger.info("Child %d recycling after %d tasks", os.getpid(), processed)
            self.stopping = True
        elif self.max_memory_kib and _rss_kib() > self.max_memory_kib:
            if not self.stopping:
                logger.warning(
                    "Child %d recycling: memory %d KiB over limit %d KiB", os.getpid(), _rss_kib(), self.max_memory_kib
                )
            self.stopping = True
        return self.stopping

    # --- one task --------------------------------------------------------------------

    def _process(self, slot: Slot, delivery: Delivery) -> bool:
        """Run one task. Returns False if this slot must stop afterwards."""
        app = self.app
        message = delivery.message
        task = app.resolve_task(message.task)
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
        use_alarm = soft and not is_async and self.threads == 1
        slot.task_id = message.id
        slot.soft_fired = False
        slot.soft_deadline = time.monotonic() + soft if soft and not is_async and self.threads > 1 else None
        if use_alarm:
            signal.setitimer(signal.ITIMER_REAL, soft)
        try:
            outcome = executor.execute(app, message, delivery_count=delivery.delivery_count, hostname=self.hostname)
        except WorkerTerminate:
            logger.warning("Task %s[%s] interrupted by shutdown; requeueing", message.task, message.id)
            slot.consumer.requeue(delivery, count=False)
            self._report({"e": "done", "id": message.id})
            return False
        finally:
            if use_alarm:
                signal.setitimer(signal.ITIMER_REAL, 0)
            slot.soft_deadline = None
            slot.task_id = None
        try:
            executor.settle(app, slot.consumer, delivery, outcome)
        except Exception:
            # Don't carry on with a task still claimed (SQL) or unacked (RabbitMQ):
            # stop without reporting "done" so the supervisor recovers it.
            logger.exception("Failed to settle task %s[%s]; restarting this process", message.task, message.id)
            self.exit_code = 3
            self.stopping = True
            return False
        level = logging.INFO if outcome.state in ("SUCCESS", "RETRY", "IGNORED") else logging.WARNING
        logger.log(level, "Task %s[%s] %s in %.3fs", message.task, message.id, outcome.state.lower(), outcome.runtime)
        self._report({"e": "done", "id": message.id})
        return True


def child_main(app: Potatoq, **kwargs: Any) -> None:
    """Entry point in the forked process; never returns."""
    code = 1
    try:
        code = Child(app, **kwargs).run()
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
    except BaseException:
        logger.exception("Worker child crashed")
    finally:
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        finally:
            os._exit(code)
