"""Runs one task message and decides what should happen to it.

The executor never talks to the broker's queueing side directly: it returns an
``Outcome`` (complete / retry / requeue / dead-letter) together with the result to
store and the follow-up messages (chain links, chord callbacks) to enqueue. The
consumer then settles all of it at once, which on database brokers means a single
transaction: the task's ack, its result and its callbacks commit together.
"""

from __future__ import annotations

import logging
import threading
import time
import traceback as tb
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .. import serialization, signals, states
from ..app import set_current_task
from ..brokers.base import ResultRecord
from ..canvas import Signature
from ..exceptions import (
    ChordError,
    Ignore,
    MaxRetriesExceededError,
    NotRegistered,
    Reject,
    Replace,
    Retry,
    WorkerTerminate,
)
from ..message import Message, new_id
from ..task import Context

if TYPE_CHECKING:
    from ..app import Potatoq
    from ..result import EagerResult
    from ..task import Task

logger = logging.getLogger("potatoq.worker")

#: Per thread: whether user task code is running right now. Time limits and forced
#: shutdown only ever interrupt a task while this is set, never broker bookkeeping.
#: ``guard`` (optional, set by threaded workers) is entered/exited around the body.
_body = threading.local()


def in_task_body() -> bool:
    return getattr(_body, "active", False)


def set_body_guard(guard: Any) -> None:
    """Install an object with ``enter()``/``exit()`` for this thread's task bodies."""
    _body.guard = guard


COMPLETE = "complete"  # ack (task succeeded or failed terminally)
RETRY = "retry"  # replace with ``retry_message``
REQUEUE = "requeue"  # give back unchanged
DEAD_LETTER = "dead_letter"  # park for humans


@dataclass
class Outcome:
    action: str
    state: str
    record: ResultRecord | None = None
    retry_message: Message | None = None
    followups: list[Message] = field(default_factory=list)
    retval: Any = None
    exc: BaseException | None = None
    traceback: str | None = None
    runtime: float = 0.0
    reason: str | None = None


class ExceptionInfo:
    """Minimal ``billiard.einfo.ExceptionInfo`` stand-in passed to ``on_failure``."""

    def __init__(self, exc: BaseException):
        self.exception = exc
        self.type = type(exc)
        self.tb = exc.__traceback__
        self.traceback = "".join(tb.format_exception(type(exc), exc, exc.__traceback__))

    def __str__(self) -> str:
        return self.traceback


def build_request(
    message: Message,
    delivery_count: int,
    hostname: str | None,
    is_eager: bool,
    timelimit: tuple[float | None, float | None],
) -> Context:
    return Context(
        id=message.id,
        task=message.task,
        args=message.args,
        kwargs=message.kwargs,
        retries=message.retries,
        delivery_count=delivery_count,
        eta=message.eta,
        expires=message.expires,
        is_eager=is_eager,
        called_directly=False,
        hostname=hostname,
        root_id=message.root_id,
        parent_id=message.parent_id,
        group=message.group_id,
        group_index=message.group_index,
        headers=message.headers,
        delivery_info={"routing_key": message.queue, "priority": message.priority, "redelivered": delivery_count > 1},
        timelimit=timelimit,
        message=message,
        ignore_result=message.ignore_result,
        chord=message.chord,
        callbacks=message.link,
        errbacks=message.link_error,
    )


def _record(
    message: Message, state: str, result: Any, request: Context | None, traceback: str | None = None
) -> ResultRecord:
    return ResultRecord(
        task_id=message.id,
        state=state,
        result=result,
        traceback=traceback,
        date_done=time.time() if state in states.READY_STATES else None,
        task_name=message.task,
        args=message.args,
        kwargs=message.kwargs,
        retries=message.retries,
        worker=request.hostname if request else None,
        date_started=getattr(request, "started_at", None) if request else None,
        enqueued_at=message.enqueued_at,
    )


def _callbacks(app: Potatoq, sigs: list[dict[str, Any]], args: tuple[Any, ...], parent: Message) -> list[Message]:
    """Build messages for ``link``/``link_error`` callbacks without publishing them."""
    out = []
    for raw in sigs:
        sig = Signature.from_dict(raw, app=app)
        out.extend(signature_to_messages(app, sig, args, parent))
    return out


def signature_to_messages(app: Potatoq, sig: Signature, args: tuple[Any, ...], parent: Message | None) -> list[Message]:
    """Turn a (possibly composite) signature into messages, prepending ``args``."""
    from ..canvas import _chain, _chord, group

    sig = sig.clone(args) if args else sig.clone()
    parent_opts = {"parent_id": parent.id, "root_id": parent.root_id} if parent else {}
    if isinstance(sig, _chain):
        steps = sig._prepare_steps()
        return signature_to_messages(app, steps[0], (), parent)
    if isinstance(sig, group):
        sig.freeze()
        return [m for t in sig.tasks for m in signature_to_messages(app, t, (), parent)]
    if isinstance(sig, _chord):
        sig.freeze()
        callback = sig.body.to_dict()
        gid = sig["options"]["group_id"]
        header = sig.tasks
        if not header:
            return signature_to_messages(app, sig.body, ([],), parent)
        msgs = []
        for t in header:
            targs, tkwargs, topts = t._merge(None, None, None)
            tid = topts.pop("task_id")
            index = topts.pop("group_index")
            topts.pop("group_id", None)
            task = app.tasks[t.task]
            msgs.append(
                task.build_message(
                    list(targs), tkwargs, tid, group_id=gid, group_index=index,
                    chord={"callback": callback, "size": len(header)}, ignore_result=False,
                    **{**parent_opts, **topts},
                )
            )  # fmt: skip
        return msgs
    targs, tkwargs, topts = sig._merge(None, None, None)
    target = app.tasks.get(sig.task)
    task_id = topts.pop("task_id", None)
    if target is None:
        msg = Message(
            task=sig.task,
            args=list(targs),
            kwargs=tkwargs,
            id=task_id or new_id(),
            queue=topts.get("queue") or app.conf.task_default_queue,
        )
        msg.link = [s.to_dict() if isinstance(s, Signature) else s for s in topts.get("link", [])]
        msg.link_error = [s.to_dict() if isinstance(s, Signature) else s for s in topts.get("link_error", [])]
        if parent:
            msg.parent_id, msg.root_id = parent.id, parent.root_id
        return [msg]
    link = topts.pop("link", None)
    link_error = topts.pop("link_error", None)
    return [
        target.build_message(
            list(targs), tkwargs, task_id, link=link, link_error=link_error, **{**parent_opts, **topts}
        )
    ]


def _chord_followups(app: Potatoq, message: Message, value: Any, failed_exc: BaseException | None) -> list[Message]:
    """Record a finished chord header task; return the callback message if it was the last."""
    if not message.chord or message.group_id is None:
        return []
    backend = app.require_backend()
    part = {"__chord_error__": serialization.exception_to_dict(failed_exc)} if failed_exc else value
    results = backend.chord_part_done(message.group_id, message.group_index or 0, int(message.chord["size"]), part)
    if results is None:
        return []
    callback = Signature.from_dict(message.chord["callback"], app=app)
    errors = [r for r in results if isinstance(r, dict) and "__chord_error__" in r]
    if errors:
        exc = serialization.exception_from_dict(errors[0]["__chord_error__"])
        chord_exc = ChordError(f"Dependency of chord {message.group_id} raised {exc!r}")
        callback_id = callback.id or new_id()
        app.store_result(
            callback_id, states.FAILURE, serialization.exception_to_dict(chord_exc), task_name=callback.task
        )
        errbacks = callback.options.get("link_error") or []
        fake_parent = Message(task=callback.task, id=callback_id, root_id=message.root_id)
        return _callbacks(
            app,
            [Signature.from_dict(e, app=app).to_dict() if isinstance(e, dict) else e for e in errbacks],
            (callback_id,),
            fake_parent,
        )
    return signature_to_messages(app, callback, (results,), message)


def execute(
    app: Potatoq,
    message: Message,
    *,
    delivery_count: int = 1,
    hostname: str | None = None,
    is_eager: bool = False,
) -> Outcome:
    """Run ``message`` and return what should happen next. Never raises."""
    start = time.monotonic()
    task: Task | None = app.resolve_task(message.task)
    limits = task.resolved_time_limits(message.options) if task is not None else (None, None)
    request = build_request(message, delivery_count, hostname, is_eager, limits)

    if message.is_expired():
        logger.info("Task %s[%s] expired; discarding", message.task, message.id)
        signals.task_revoked.send(sender=task, request=request, terminated=False, signum=None, expired=True)
        rec = _record(message, states.REVOKED, None, request)
        return Outcome(COMPLETE, states.REVOKED, record=rec, reason="expired")

    backend = app.backend
    previous = None
    if backend is not None and (delivery_count > 1 or app.broker.needs_revoke_check):
        previous = backend.get_result(message.id)
    if previous is not None and previous.state == states.REVOKED:
        # Brokers that can't delete queued messages (RabbitMQ) mark revoked tasks in
        # the result backend instead.
        signals.task_revoked.send(sender=task, request=request, terminated=False, signum=None, expired=False)
        return Outcome(COMPLETE, states.REVOKED, reason="revoked")
    if previous is not None and previous.ready and delivery_count > 1:
        # Redelivered after a crash, but the previous attempt got as far as storing a
        # final result: don't run the task again.
        logger.info("Task %s[%s] already finished (%s); skipping redelivery", message.task, message.id, previous.state)
        # The previous attempt may have died between storing the result and enqueueing
        # its callbacks: rebuild them (their ids are fixed, so brokers dedupe repeats).
        if previous.state == states.FAILURE:
            exc = serialization.exception_from_dict(previous.result) if isinstance(previous.result, dict) else None
            followups = _callbacks(app, message.link_error, (message.id,), message)
            followups += _chord_followups(app, message, None, exc or Exception("failed"))
            if app.conf.task_dead_letter_failures:
                return Outcome(
                    DEAD_LETTER,
                    states.FAILURE,
                    followups=followups,
                    reason=previous.traceback or "failed before redelivery",
                )
            return Outcome(COMPLETE, states.IGNORED, followups=followups, reason="already finished")
        # READY_STATES minus REVOKED (handled above) and FAILURE: it succeeded.
        followups = _callbacks(app, message.link, (previous.result,), message)
        followups += _chord_followups(app, message, previous.result, None)
        return Outcome(COMPLETE, states.IGNORED, followups=followups, reason="already finished")

    if task is None:
        exc = NotRegistered(message.task)
        logger.error("Received unregistered task %r (id %s); dead-lettering it", message.task, message.id)
        signals.task_unknown.send(sender=None, name=message.task, id=message.id, message=message, exc=exc)
        record = None
        if backend is not None and not message.ignore_result:
            record = _record(message, states.FAILURE, serialization.exception_to_dict(exc), request, str(exc))
        return Outcome(DEAD_LETTER, states.FAILURE, record=record, exc=exc, reason=f"unregistered task {message.task}")

    ignore_result = message.ignore_result
    store = backend is not None and not ignore_result
    request.started_at = time.time()
    task.push_request(**request.__dict__)
    set_current_task(task)
    retval: Any = None
    try:
        signals.task_prerun.send(sender=task, task_id=message.id, task=task, args=message.args, kwargs=message.kwargs)
        task.before_start(message.id, message.args, message.kwargs)
        guard = getattr(_body, "guard", None)
        try:
            _body.active = True
            if guard is not None:
                guard.enter()
            try:
                retval = task(*message.args, **message.kwargs)
            finally:
                if guard is not None:
                    guard.exit()
                _body.active = False
        except Exception as exc:
            if (
                task.autoretry_for
                and isinstance(exc, task.autoretry_for)
                and not isinstance(exc, (Retry, Ignore, Reject))
            ):
                if not (task.dont_autoretry_for and isinstance(exc, task.dont_autoretry_for)):
                    try:
                        retry_opts = {
                            k: v
                            for k, v in task.retry_kwargs.items()
                            if k not in ("max_retries", "countdown", "retry_backoff")
                        }
                        task.retry(exc=exc, countdown=task.backoff_delay(message.retries), **retry_opts)
                    except Retry as retry_exc:
                        raise retry_exc from exc
            raise
        outcome = _on_success(app, task, message, request, retval, store)
    except Retry as exc:
        outcome = _on_retry(app, task, message, request, exc, store)
    except Replace as exc:
        followups = signature_to_messages(app, exc.sig, (), message)
        outcome = Outcome(COMPLETE, states.IGNORED, followups=followups, reason="replaced")
    except Ignore:
        outcome = Outcome(COMPLETE, states.IGNORED)
    except Reject as exc:
        signals.task_rejected.send(sender=task, message=message, exc=exc)
        if exc.requeue:
            outcome = Outcome(REQUEUE, states.REJECTED, reason=str(exc.reason))
        else:
            outcome = Outcome(DEAD_LETTER, states.REJECTED, reason=str(exc.reason or "rejected"))
    except BaseException as exc:
        if isinstance(exc, WorkerTerminate) or (isinstance(exc, (KeyboardInterrupt, SystemExit)) and not is_eager):
            raise
        outcome = _on_failure(app, task, message, request, exc, store)
    finally:
        outcome_state = locals().get("outcome")
        state = outcome_state.state if outcome_state else states.FAILURE
        try:
            task.after_return(state, retval, message.id, message.args, message.kwargs, None)
        except Exception:
            logger.exception("after_return handler of %s failed", task.name)
        signals.task_postrun.send(
            sender=task,
            task_id=message.id,
            task=task,
            args=message.args,
            kwargs=message.kwargs,
            retval=retval,
            state=state,
        )
        task.pop_request()
        set_current_task(None)
    outcome.runtime = time.monotonic() - start
    return outcome


def _on_success(app: Potatoq, task: Task, message: Message, request: Context, retval: Any, store: bool) -> Outcome:
    serialization.check_serializable(retval) if store else None
    followups = _callbacks(app, message.link, (retval,), message)
    followups += _chord_followups(app, message, retval, None)
    record = _record(message, states.SUCCESS, retval, request) if store else None
    try:
        task.on_success(retval, message.id, message.args, message.kwargs)
    except Exception:
        logger.exception("on_success handler of %s failed", task.name)
    signals.task_success.send(sender=task, result=retval)
    return Outcome(COMPLETE, states.SUCCESS, record=record, followups=followups, retval=retval)


def _on_retry(app: Potatoq, task: Task, message: Message, request: Context, exc: Retry, store: bool) -> Outcome:
    when = exc.when
    now = time.time()
    if hasattr(when, "timestamp"):
        eta = when.timestamp()  # type: ignore[union-attr]
    else:
        eta = now + float(when or 0)
    new = Message.from_dict(message.to_dict())
    new.retries = message.retries + 1
    new.eta = eta if eta > now else None
    args_override = getattr(exc, "args_override", None)
    kwargs_override = getattr(exc, "kwargs_override", None)
    if args_override is not None:
        new.args = list(args_override)
    if kwargs_override is not None:
        new.kwargs = dict(kwargs_override)
    options = getattr(exc, "options", None) or {}
    if options.get("queue"):
        new.queue = options["queue"]
    if options.get("priority") is not None:
        new.priority = int(options["priority"])
    max_retries = getattr(exc, "max_retries", None)
    if max_retries is not None:
        new.options["max_retries"] = max_retries
    cause = exc.exc or exc
    einfo = ExceptionInfo(cause)
    try:
        task.on_retry(cause, message.id, message.args, message.kwargs, einfo)
    except Exception:
        logger.exception("on_retry handler of %s failed", task.name)
    signals.task_retry.send(sender=task, request=request, reason=exc, einfo=einfo)
    record = (
        _record(message, states.RETRY, serialization.exception_to_dict(cause), request, einfo.traceback)
        if store
        else None
    )
    logger.info("Task %s[%s] retry %s: %s", message.task, message.id, exc.humanize(), cause)
    return Outcome(RETRY, states.RETRY, record=record, retry_message=new, exc=cause, traceback=einfo.traceback)


def _on_failure(
    app: Potatoq, task: Task, message: Message, request: Context, exc: BaseException, store: bool
) -> Outcome:
    einfo = ExceptionInfo(exc)
    if isinstance(exc, MaxRetriesExceededError):
        logger.error("Task %s[%s] max retries exceeded", message.task, message.id)
    else:
        logger.error(
            "Task %s[%s] raised unexpected: %r",
            message.task,
            message.id,
            exc,
            exc_info=(type(exc), exc, exc.__traceback__),
        )
    store = store or (app.backend is not None and task.store_errors_even_if_ignored)
    record = (
        _record(message, states.FAILURE, serialization.exception_to_dict(exc), request, einfo.traceback)
        if store
        else None
    )
    try:
        task.on_failure(exc, message.id, message.args, message.kwargs, einfo)
    except Exception:
        logger.exception("on_failure handler of %s failed", task.name)
    signals.task_failure.send(
        sender=task, task_id=message.id, exception=exc, args=message.args, kwargs=message.kwargs,
        traceback=exc.__traceback__, einfo=einfo,
    )  # fmt: skip
    followups = _callbacks(app, message.link_error, (message.id,), message)
    followups += _chord_followups(app, message, None, exc)
    return Outcome(COMPLETE, states.FAILURE, record=record, followups=followups, exc=exc, traceback=einfo.traceback)


def failure_outcome(app: Potatoq, message: Message, exc: BaseException, hostname: str | None = None) -> Outcome:
    """Outcome for a task whose process died (hard time limit, OOM, segfault)."""
    task = app.tasks.get(message.task)
    request = build_request(message, 1, hostname, False, (None, None))
    store = app.backend is not None and not message.ignore_result
    traceback = f"{type(exc).__name__}: {exc}"
    record = (
        _record(message, states.FAILURE, serialization.exception_to_dict(exc), request, traceback) if store else None
    )
    if task is not None:
        try:
            task.on_failure(exc, message.id, message.args, message.kwargs, ExceptionInfo(exc))
        except Exception:
            logger.exception("on_failure handler of %s failed", message.task)
        signals.task_failure.send(
            sender=task,
            task_id=message.id,
            exception=exc,
            args=message.args,
            kwargs=message.kwargs,
            traceback=None,
            einfo=None,
        )
    followups = _callbacks(app, message.link_error, (message.id,), message)
    try:
        followups += _chord_followups(app, message, None, exc)
    except Exception:
        logger.exception("Failed to record chord failure for %s", message.id)
    return Outcome(COMPLETE, states.FAILURE, record=record, followups=followups, exc=exc, traceback=traceback)


# --- settling -------------------------------------------------------------------


def settle(app: Potatoq, consumer: Any, delivery: Any, outcome: Outcome) -> None:
    """Apply an ``Outcome`` through ``consumer``: ack/retry/requeue/dead-letter, store
    the result and enqueue follow-ups (in one transaction on database brokers)."""
    record = outcome.record
    backend = app.backend
    if record is not None and backend is not None and backend is not consumer.broker:
        backend.store_result(record, expires=app.conf.result_expires)
        record = None
    # Consumers enqueue follow-ups atomically with the ack (SQL transaction, Redis Lua
    # script) or publish-then-ack (RabbitMQ), so a crash never loses a callback.
    followups = outcome.followups
    action = outcome.action
    if action == COMPLETE and outcome.state == states.FAILURE and app.conf.task_dead_letter_failures:
        action = DEAD_LETTER
    if action == COMPLETE:
        consumer.complete(delivery, record, followups)
    elif action == RETRY:
        if followups:
            app.publish_now(followups)
        consumer.retry(delivery, outcome.retry_message, record)
    elif action == REQUEUE:
        consumer.requeue(delivery, count=True)
    elif action == DEAD_LETTER:
        reason = outcome.reason or outcome.traceback or (repr(outcome.exc) if outcome.exc else "dead-lettered")
        consumer.dead_letter(delivery, reason, record, followups)
    else:
        raise ValueError(f"Unknown outcome {action}")


# --- eager execution -----------------------------------------------------------


def execute_eagerly(app: Potatoq, message: Message, throw: bool = True) -> EagerResult:
    """``task.apply()``: run now, in this process, following retries and callbacks."""
    from ..result import EagerResult

    while True:
        outcome = execute(app, message, is_eager=True, hostname="eager")
        if outcome.action == RETRY and outcome.retry_message is not None:
            message = outcome.retry_message
            message.eta = None
            continue
        break
    if app.conf.task_store_eager_result and outcome.record is not None and app.backend is not None:
        app.backend.store_result(outcome.record, expires=app.conf.result_expires)
    for followup in outcome.followups:
        followup.eta = None
        execute_eagerly(app, followup, throw=throw)
    if outcome.state == states.FAILURE and outcome.exc is not None:
        if throw:
            raise outcome.exc
        return EagerResult(message.id, outcome.exc, states.FAILURE, outcome.traceback, app=app, name=message.task)
    value = outcome.retval if outcome.state == states.SUCCESS else None
    return EagerResult(message.id, value, outcome.state, app=app, name=message.task)
