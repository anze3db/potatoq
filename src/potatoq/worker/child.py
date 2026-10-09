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


#: What the main thread injects into slot threads.
_INJECTED = (SoftTimeLimitExceeded, WorkerTerminate)


class Slot:
    """One task slot (a thread, or the main thread when ``threads=1``).

    ``run`` brackets the user's task code. Exceptions are only injected while the slot
    is inside that bracket and while holding ``lock``; ``run`` clears any injection still
    pending on the way out, so an interruption can never land in broker bookkeeping.
    """

    def __init__(self, index: int):
        self.index = index
        # An RLock only so that ``run`` can tell whether it holds it (``_is_owned``).
        self.lock = threading.RLock()
        self.thread_id: int | None = None
        self.in_body = False
        self.task_id: str | None = None
        self.soft_deadline: float | None = None
        self.soft_fired = False
        self.consumer: Any = None
        self.fetching = False

    def run(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
        """Call ``fn`` (on this slot's thread) as the task body."""
        try:
            self.in_body = True
            return fn(*args, **kwargs)
        finally:
            # An inject() that saw in_body just before the store below still lands, at
            # the next eval-breaker check: a call returning (e.g. right after acquiring
            # the lock), a function entry, a loop back-edge. It must neither escape into
            # broker code nor leak the lock, so all of this runs in this frame (a helper
            # would check on entry) and is retried, releasing the lock if the injection
            # interrupted us holding it, until the injection is cleared under the lock.
            # The store comes first and is plain (no check), so every inject() taking
            # the lock after us refuses: only an injection already under way lands here.
            # The body has returned or raised by then, so its result stands and the
            # late interruption is dropped (an abort still stops the slot: ``stopping``
            # is set before injecting).
            self.in_body = False
            while True:
                try:
                    while self.lock._is_owned():  # type: ignore[attr-defined]
                        self.lock.release()
                    self.lock.acquire()  # waits out an inject() in progress
                    if self.thread_id is not None:
                        _SetAsyncExc(ctypes.c_ulong(self.thread_id), None)  # drop a pending injection
                    self.lock.release()
                    break
                except _INJECTED:
                    pass

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
        signal.signal(signal.SIGHUP, signal.SIG_IGN)  # so is reloading (and a closed terminal)
        signal.signal(signal.SIGTERM, self._on_term)
        signal.signal(signal.SIGUSR1, self._on_abort)
        signal.signal(signal.SIGALRM, self._on_soft_limit)
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        # Our handlers are in place: deliver whatever arrived since the fork.
        signal.pthread_sigmask(signal.SIG_UNBLOCK, FORK_BLOCKED)

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
                except BrokenPipeError:
                    # The supervisor is gone (killed, or crashed). Finish what we're
                    # doing and exit, like the parent-PID check does.
                    if not self.stopping:
                        logger.warning("Supervisor died; child exiting")
                    self.stopping = True
                    return
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
            with slot.lock:  # one task's id and deadline: the slot may have moved on to the next
                task_id, deadline = slot.task_id, slot.soft_deadline
                if deadline is None or now < deadline or slot.soft_fired:
                    continue
                slot.soft_fired = True
            if slot.inject(SoftTimeLimitExceeded, task_id):
                logger.warning("Soft time limit exceeded for task %s; interrupting it", task_id)

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
        use_alarm = bool(soft) and not is_async and self.threads == 1
        with slot.lock:
            slot.task_id = message.id
            slot.soft_fired = False
            slot.soft_deadline = time.monotonic() + soft if soft and not is_async and self.threads > 1 else None
        if use_alarm and soft:
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
            with slot.lock:
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
        executor.log_done(message, outcome)
        self._report({"e": "done", "id": message.id})
        return True


#: Signals the supervisor sends to (or shares with) its children. They're blocked across
#: fork() and unblocked once the child has its own handlers: in between, the child would
#: still run the supervisor's handlers and a SIGTERM would be lost (the supervisor would
#: then wait out the whole shutdown timeout for an idle child).
FORK_BLOCKED = frozenset({signal.SIGTERM, signal.SIGINT, signal.SIGQUIT, signal.SIGHUP, signal.SIGUSR1})


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
