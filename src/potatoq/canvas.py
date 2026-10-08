"""Workflow primitives: ``signature``, ``chain``, ``group``, ``chord``.

Chains are implemented with ``link`` callbacks carried inside the message, and chords
with an atomic per-group counter in the broker, so no process ever polls
("chord_unlock") or blocks waiting for other tasks.
"""

from __future__ import annotations

import copy
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Self, cast

from . import states
from .message import Message, new_id

if TYPE_CHECKING:
    from .app import Potatoq
    from .result import AsyncResult, GroupResult

__all__ = ["Signature", "chain", "chord", "group", "maybe_signature", "signature", "subtask", "xmap", "xstarmap"]


class Signature(dict):
    """A serializable description of a task call (task name, args, kwargs, options)."""

    def __init__(
        self,
        task: Any = None,
        args: Any = None,
        kwargs: dict[str, Any] | None = None,
        options: dict[str, Any] | None = None,
        type: Any = None,
        subtask_type: str | None = None,
        immutable: bool = False,
        app: Potatoq | None = None,
        **ex: Any,
    ):
        self._app = app
        if isinstance(task, dict) and not hasattr(task, "name"):
            super().__init__(task)
            return
        name = task if isinstance(task, str) else getattr(task, "name", task)
        if app is None and not isinstance(task, str) and hasattr(task, "app"):
            self._app = task.app
        super().__init__(
            task=name,
            args=tuple(args or ()),
            kwargs=dict(kwargs or {}),
            options=dict(options or {}, **ex),
            subtask_type=subtask_type,
            immutable=immutable,
        )

    # --- properties ------------------------------------------------------------

    @property
    def app(self) -> Potatoq:
        if self._app is not None:
            return self._app
        from .app import current_app

        return current_app()

    task = property(lambda self: self["task"])
    args = property(lambda self: tuple(self["args"]))
    kwargs = property(lambda self: self["kwargs"])
    options = property(lambda self: self["options"])
    subtask_type = property(lambda self: self.get("subtask_type"))
    immutable = property(lambda self: self.get("immutable", False))

    @property
    def id(self) -> str | None:
        return self["options"].get("task_id")

    @property
    def type(self) -> Any:
        return self.app.tasks[self["task"]]

    @classmethod
    def from_dict(cls, d: dict[str, Any], app: Potatoq | None = None) -> Signature:
        typ = d.get("subtask_type")
        target = {"chain": _chain, "group": group, "chord": _chord}.get(typ or "", Signature)
        return target._from_dict(d, app)  # type: ignore[attr-defined]

    @classmethod
    def _from_dict(cls, d: dict[str, Any], app: Potatoq | None = None) -> Self:
        sig = cls.__new__(cls)
        dict.__init__(sig, d)
        sig._app = app
        return sig

    def to_dict(self) -> dict[str, Any]:
        """Plain JSON-serializable dict (nested signatures included)."""

        def convert(value: Any) -> Any:
            if isinstance(value, dict):
                return {k: convert(v) for k, v in value.items()}
            if isinstance(value, (list, tuple)):
                return [convert(v) for v in value]
            return value

        return convert(dict(self))

    # --- calling ---------------------------------------------------------------

    def __call__(self, *partial_args: Any, **partial_kwargs: Any) -> Any:
        args, kwargs, _ = self._merge(partial_args, partial_kwargs, None)
        return self.type(*args, **kwargs)

    def delay(self, *partial_args: Any, **partial_kwargs: Any) -> AsyncResult:
        return self.apply_async(partial_args, partial_kwargs)

    def apply_async(
        self, args: Any = None, kwargs: dict[str, Any] | None = None, route_name: Any = None, **options: Any
    ) -> AsyncResult:
        args, kwargs, options = self._merge(args, kwargs, options)
        task = self.app.tasks.get(self["task"])
        if task is None:
            return self.app.send_task(self["task"], args, kwargs, **options)
        return task.apply_async(args, kwargs, **options)

    def apply(self, args: Any = None, kwargs: dict[str, Any] | None = None, **options: Any) -> Any:
        args, kwargs, options = self._merge(args, kwargs, options)
        return self.type.apply(args, kwargs, **options)

    def _merge(
        self, args: Any, kwargs: dict[str, Any] | None, options: dict[str, Any] | None
    ) -> tuple[tuple[Any, ...], dict[str, Any], dict[str, Any]]:
        args = tuple(args or ())
        kwargs = dict(kwargs or {})
        merged_options = dict(self["options"])
        _add_options(merged_options, options or {})
        if self.immutable:
            return self.args, dict(self.kwargs), merged_options
        return args + self.args, {**self.kwargs, **kwargs}, merged_options

    # --- building --------------------------------------------------------------

    def clone(self, args: Any = None, kwargs: dict[str, Any] | None = None, **opts: Any) -> Signature:
        sig = self._from_dict(copy.deepcopy(dict(self)), self._app)
        if not sig.immutable:
            if args:
                sig["args"] = tuple(args) + tuple(sig["args"])
            if kwargs:
                sig["kwargs"] = {**sig["kwargs"], **kwargs}
        _add_options(sig["options"], opts)
        return sig

    partial = clone

    def set(self, immutable: bool | None = None, **options: Any) -> Signature:
        if immutable is not None:
            self["immutable"] = immutable
        self["options"].update(options)
        return self

    def set_immutable(self, immutable: bool) -> None:
        self["immutable"] = immutable

    def freeze(
        self,
        _id: str | None = None,
        group_id: str | None = None,
        chord: dict[str, Any] | None = None,
        group_index: int | None = None,
        **kwargs: Any,
    ) -> AsyncResult:
        """Fix the id (``_id``, or a new one) so the result is known before sending.
        ``group_id``/``group_index``/``chord`` make it a group member or chord part;
        a chain or chord passes them on to the task whose result is its result."""
        opts = self["options"]
        if _id is not None:
            opts["task_id"] = _id
        opts.setdefault("task_id", new_id())
        if group_id is not None:
            opts["group_id"] = group_id
        if group_index is not None:
            opts["group_index"] = group_index
        if chord is not None:
            opts["chord"] = chord
        return self.app.AsyncResult(opts["task_id"])

    def link(self, callback: Any) -> Any:
        self["options"].setdefault("link", []).append(maybe_signature(callback, self._app))
        return callback

    def link_error(self, errback: Any) -> Any:
        self["options"].setdefault("link_error", []).append(maybe_signature(errback, self._app))
        return errback

    def on_error(self, errback: Any) -> Signature:
        self.link_error(errback)
        return self

    def __or__(self, other: Any) -> Any:
        if isinstance(other, _chain):
            return _chain(self, *other.tasks, app=self._app)
        if isinstance(other, group) and not isinstance(self, group):
            return _chain(self, other, app=self._app)
        if isinstance(self, group) and isinstance(other, Signature):
            return chord(self, body=other, app=self._app)
        if isinstance(other, Signature):
            return _chain(self, other, app=self._app)
        return NotImplemented

    def __deepcopy__(self, memo: dict[int, Any]) -> Signature:
        return self._from_dict(copy.deepcopy(dict(self), memo), self._app)

    def __reduce__(self) -> Any:
        return (_rebuild_signature, (dict(self),))

    def __repr__(self) -> str:
        args = ", ".join([*(repr(a) for a in self.args), *(f"{k}={v!r}" for k, v in self.kwargs.items())])
        return f"{self['task']}({args})" + ("!" if self.immutable else "")


def _rebuild_signature(d: dict[str, Any]) -> Signature:
    return Signature.from_dict(d)


def signature(varies: Any, *args: Any, **kwargs: Any) -> Signature:
    app = kwargs.pop("app", None)
    if isinstance(varies, dict):
        if isinstance(varies, Signature):
            return varies.clone()
        return Signature.from_dict(varies, app=app)
    kwargs["app"] = app
    return Signature(varies, *args, **kwargs)


subtask = signature


def maybe_signature(d: Any, app: Potatoq | None = None) -> Signature | None:
    if d is None:
        return None
    if isinstance(d, Signature):
        return d
    if isinstance(d, dict):
        return Signature.from_dict(d, app=app)
    if hasattr(d, "s"):  # a task
        return d.s()
    raise TypeError(f"Expected a signature, got {d!r}")


def signatures_to_list(value: Any) -> list[dict[str, Any]]:
    if not value:
        return []
    if isinstance(value, (Signature, dict)):
        value = [value]
    return [maybe_signature(v).to_dict() for v in value]  # type: ignore[union-attr]


def _as_list(value: Any) -> list[Any]:
    """``link``/``link_error`` values: one signature or a list of them."""
    if not value:
        return []
    if isinstance(value, dict):
        return [value]
    return list(value)


def _add_options(options: dict[str, Any], new: dict[str, Any]) -> None:
    """``options.update(new)``, except that ``link``/``link_error`` callbacks add up."""
    for key, value in new.items():
        if key in ("link", "link_error"):
            options[key] = [*_as_list(options.get(key)), *_as_list(value)]
        else:
            options[key] = value


#: Options that delay the start of a workflow: they go to the tasks it starts with.
_START_OPTIONS = frozenset({"countdown", "eta"})
#: ``apply_async`` options about publishing, not about the messages.
_PUBLISH_OPTIONS = ("connection", "using", "enqueue_on_commit")


def _push_options(
    sig: Signature, last: list[Signature], first: list[Signature], every: list[Signature], keep: tuple[str, ...] = ()
) -> None:
    """Move a chain's, group's or chord's own options to its tasks, and off it, so this
    can run again: ``task_id`` and ``link`` to the task(s) whose result is its result
    (``last``), ``countdown``/``eta`` to the ones it starts with, the rest (``link_error``
    included) to all of them. A nested chain, group or chord passes them on in turn."""
    opts = sig["options"]
    for key in [k for k in opts if k not in keep]:
        value = opts.pop(key)
        if value is None:
            continue
        if key == "link":
            for t in last:
                for callback in _as_list(value):
                    t.link(callback)
        elif key == "link_error":
            for t in every:
                for errback in _as_list(value):
                    t.link_error(errback)
        elif key == "task_id":
            last[0].set(task_id=value)
        else:
            for t in first if key in _START_OPTIONS else every:
                t.set(**{key: value})


def _flatten(tasks: Iterable[Any]) -> list[Any]:
    out = []
    for t in tasks:
        if isinstance(t, (list, tuple)) and not isinstance(t, Signature):
            out.extend(_flatten(t))
        else:
            out.append(t)
    return out


def _throw(app: Potatoq, options: dict[str, Any]) -> bool:
    throw = options.pop("throw", None)
    return app.conf.task_eager_propagates if throw is None else throw


def _send(sig: Signature, args: Any, kwargs: dict[str, Any] | None, options: dict[str, Any]) -> tuple[Any, Any]:
    """``apply_async`` of a chain, group or chord: (the frozen copy that was sent, its result)."""
    publish = {k: options.pop(k) for k in _PUBLISH_OPTIONS if k in options}
    options.pop("producer", None)
    c = sig.clone(args, kwargs, **options)
    result = c.freeze()
    sig.app.publish(
        _messages(sig.app, c, check=True),
        connection=publish.get("connection"),
        on_commit=publish.get("enqueue_on_commit"),
        using=publish.get("using"),
    )
    return c, result


class _chain(Signature):
    def __init__(self, *tasks: Any, app: Potatoq | None = None, **options: Any):
        tasks_list = [maybe_signature(t, app) for t in _flatten(tasks)]
        Signature.__init__(self, "potatoq.chain", (), {"tasks": tasks_list}, options, subtask_type="chain", app=app)

    @property
    def tasks(self) -> list[Signature]:
        return [maybe_signature(t, self._app) for t in self["kwargs"]["tasks"]]  # type: ignore[misc]

    @property
    def id(self) -> str | None:
        """The chain's result id: its last task's (or last group's, or last chord body's)."""
        task_id = self["options"].get("task_id")
        if task_id is not None:
            return task_id
        tasks = self.tasks
        return tasks[-1].id if tasks else None

    def freeze(
        self,
        _id: str | None = None,
        group_id: str | None = None,
        chord: dict[str, Any] | None = None,
        group_index: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """Lay out the steps and give them ids; returns the chain's result.

        Nested chains are spliced in, a step ending in a group is joined with the next
        step into a chord (the next step waits for the whole group), and the chain's own
        options move to the steps. Running it again changes nothing."""
        if _id is not None:
            self["options"]["task_id"] = _id
        flat: list[Signature] = []
        for t in self.tasks:
            t = t.clone() if t.id is None else t
            if isinstance(t, _chain):
                if t.tasks:
                    t.freeze()
                    flat.extend(t.tasks)
            else:
                flat.append(t)
        steps: list[Signature] = []
        for t in flat:
            if steps and _ends_in_group(steps[-1]):
                steps[-1] = _join(steps[-1], t)
            else:
                steps.append(t)
        if not steps:
            raise ValueError("Empty chain")
        if (chord is not None or group_index is not None) and _ends_in_group(steps[-1]):
            raise TypeError("A chain nested in a group or chord must end with a task or a chord, not a group")
        _push_options(self, last=steps[-1:], first=steps[:1], every=steps)
        for step in steps[:-1]:
            step.freeze()
        result = steps[-1].freeze(group_id=group_id, chord=chord, group_index=group_index)
        self["kwargs"]["tasks"] = steps
        return result

    def apply_async(
        self, args: Any = None, kwargs: dict[str, Any] | None = None, route_name: Any = None, **options: Any
    ) -> AsyncResult:
        if self.app.conf.task_always_eager:
            return self.apply(args, kwargs, **options)
        return _send(self, args, kwargs, options)[1]

    def clone(self, args: Any = None, kwargs: dict[str, Any] | None = None, **opts: Any) -> _chain:
        """Partial arguments go to the first step (a parent's result, when linked)."""
        c = _chain._from_dict(copy.deepcopy(dict(self)), self._app)
        if args or kwargs:
            tasks = c.tasks
            tasks[0] = tasks[0].clone(args, kwargs)
            c["kwargs"]["tasks"] = tasks
        _add_options(c["options"], opts)
        return c

    def apply(self, args: Any = None, kwargs: dict[str, Any] | None = None, **options: Any) -> Any:
        from .result import EagerResult

        throw = _throw(self.app, options)
        c = self.clone(args, kwargs, **options)
        c.freeze()
        result = None
        for step in c.tasks:
            if result is not None:
                if result.failed():
                    # The remaining steps never run; the chain fails with this error.
                    return EagerResult(c.id, result.result, states.FAILURE, result.traceback, app=self.app)  # type: ignore[arg-type]
                step = step.clone((result.get(),))
            result = step.apply(throw=throw)
        return result

    def __or__(self, other: Any) -> Any:
        if isinstance(other, _chain):
            return _chain(*self.tasks, *other.tasks, app=self._app)
        if isinstance(other, Signature):
            return _chain(*self.tasks, other, app=self._app)
        return NotImplemented

    def __repr__(self) -> str:
        return " | ".join(repr(t) for t in self.tasks)


#: ``chain(a.s(), b.s(), c.s())`` runs a, then b(a_result), then c(b_result).
chain = _chain


class group(Signature):
    """Run tasks in parallel. ``group(a.s(1), a.s(2))()`` returns a ``GroupResult``."""

    def __init__(self, *tasks: Any, app: Potatoq | None = None, **options: Any):
        if len(tasks) == 1 and not isinstance(tasks[0], Signature) and not isinstance(tasks[0], dict):
            tasks = tuple(tasks[0])
        tasks_list = [maybe_signature(t, app) for t in tasks]
        Signature.__init__(self, "potatoq.group", (), {"tasks": tasks_list}, options, subtask_type="group", app=app)

    @property
    def tasks(self) -> list[Signature]:
        return [maybe_signature(t, self._app) for t in self["kwargs"]["tasks"]]  # type: ignore[misc]

    def __iter__(self) -> Any:
        return iter(self.tasks)

    def __len__(self) -> int:
        return len(self["kwargs"]["tasks"])

    def freeze(  # type: ignore[override]
        self,
        _id: str | None = None,
        group_id: str | None = None,
        chord: dict[str, Any] | None = None,
        group_index: int | None = None,
        **kwargs: Any,
    ) -> GroupResult:
        if chord is not None or group_index is not None:
            raise TypeError("Only chains and chords can be nested in a group or chord, not a group")
        opts = self["options"]
        gid = opts["task_id"] = group_id or _id or opts.get("task_id") or new_id()
        tasks = [t.clone() if t.id is None else t for t in self.tasks]
        _push_options(self, last=tasks, first=tasks, every=tasks, keep=("task_id",))
        results = [t.freeze(group_id=gid, group_index=index) for index, t in enumerate(tasks)]
        self["kwargs"]["tasks"] = tasks
        return self.app.GroupResult(gid, results)

    def _unroll(self) -> list[Signature]:
        """The members, with the group's own options moved to them."""
        g = self.clone()
        tasks = g.tasks
        _push_options(g, last=tasks, first=tasks, every=tasks, keep=("task_id",))
        return tasks

    def clone(self, args: Any = None, kwargs: dict[str, Any] | None = None, **opts: Any) -> group:
        g = group._from_dict(copy.deepcopy(dict(self)), self._app)
        if args or kwargs:
            g["kwargs"]["tasks"] = [t.clone(args, kwargs) for t in g.tasks]
        _add_options(g["options"], opts)
        return g

    def apply_async(  # type: ignore[override]
        self, args: Any = None, kwargs: dict[str, Any] | None = None, route_name: Any = None, **options: Any
    ) -> GroupResult:
        if self.app.conf.task_always_eager:
            return self.apply(args, kwargs, **options)
        return _send(self, args, kwargs, options)[1]

    def __call__(self, *partial_args: Any, **options: Any) -> GroupResult:
        return self.apply_async(partial_args, **options)

    def apply(self, args: Any = None, kwargs: dict[str, Any] | None = None, **options: Any) -> Any:
        throw = _throw(self.app, options)
        g = self.clone(args, kwargs, **options)
        result = g.freeze()
        # Every member runs, like on a worker; then the first error propagates.
        result.results = [t.apply(throw=False) for t in g.tasks]
        if throw:
            for r in result.results:
                if r.failed():
                    r.get()
        return result

    def link(self, callback: Any) -> Any:
        tasks = self.tasks
        for t in tasks:
            t.link(callback)
        self["kwargs"]["tasks"] = tasks
        return callback

    def __repr__(self) -> str:
        return f"group([{', '.join(repr(t) for t in self.tasks)}])"


class _chord(Signature):
    def __init__(self, header: Any, body: Any = None, app: Potatoq | None = None, **options: Any):
        if isinstance(header, group):
            if header.id is not None:
                options.setdefault("group_id", header.id)
            header = header._unroll()
        header_list = [maybe_signature(t, app) for t in header]
        Signature.__init__(
            self, "potatoq.chord", (), {"header": header_list, "body": maybe_signature(body, app) if body is not None else None},
            options, subtask_type="chord", app=app,
        )  # fmt: skip

    @property
    def tasks(self) -> list[Signature]:
        return [maybe_signature(t, self._app) for t in self["kwargs"]["header"]]  # type: ignore[misc]

    header = tasks

    @property
    def body(self) -> Signature:
        body = self["kwargs"]["body"]
        if body is not None and not isinstance(body, Signature):
            body = maybe_signature(body, self._app)
            self["kwargs"]["body"] = body
        return cast(Signature, body)  # None until chord(header)(body)

    def __call__(self, body: Any = None, **options: Any) -> AsyncResult:
        if body is not None:
            self["kwargs"]["body"] = maybe_signature(body, self._app)
        return self.apply_async(**options)

    def freeze(
        self,
        _id: str | None = None,
        group_id: str | None = None,
        chord: dict[str, Any] | None = None,
        group_index: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """Give the header (its own group id) and the body ids; returns the body's result.
        The chord's own options go to the body, ``countdown``/``eta`` and other options
        to the header too; ``group_id``/``group_index``/``chord`` go to the body."""
        body = self.body
        if body is None:
            raise ValueError("chord needs a body (callback)")
        opts = self["options"]
        if _id is not None:
            opts["task_id"] = _id
        gid = opts["group_id"] = opts.get("group_id") or new_id()
        header = [t.clone() if t.id is None else t for t in self.tasks]
        _push_options(self, last=[body], first=header or [body], every=[*header, body], keep=("group_id",))
        for index, t in enumerate(header):
            t.freeze(group_id=gid, group_index=index)
        self["kwargs"]["header"] = header
        return body.freeze(group_id=group_id, chord=chord, group_index=group_index)

    @property
    def id(self) -> str | None:
        task_id = self["options"].get("task_id")
        if task_id is not None:
            return task_id
        return self.body.id if self.body is not None else None

    def clone(self, args: Any = None, kwargs: dict[str, Any] | None = None, **opts: Any) -> _chord:
        c = _chord._from_dict(copy.deepcopy(dict(self)), self._app)
        if args or kwargs:
            c["kwargs"]["header"] = [t.clone(args, kwargs) for t in c.tasks]
        _add_options(c["options"], opts)
        return c

    def apply_async(
        self, args: Any = None, kwargs: dict[str, Any] | None = None, route_name: Any = None, **options: Any
    ) -> AsyncResult:
        if self.body is None:
            raise ValueError("chord needs a body (callback)")
        if self.app.conf.task_always_eager:
            return self.apply(args, kwargs, **options)
        c, result = _send(self, args, kwargs, options)
        result.parent = self.app.GroupResult(c["options"]["group_id"], [t.freeze() for t in c.tasks])
        return result

    def apply(self, args: Any = None, kwargs: dict[str, Any] | None = None, **options: Any) -> Any:
        from .exceptions import ChordError
        from .result import EagerResult

        throw = _throw(self.app, options)
        c = self.clone(args, kwargs, **options)
        c.freeze()
        header = [t.apply(throw=False) for t in c.tasks]
        failed = next((r for r in header if r.failed()), None)
        if failed is not None:
            # Like on a worker: the body fails with ChordError and its errbacks run.
            exc = ChordError(f"Dependency of chord {c['options']['group_id']} raised {failed.result!r}")
            for errback in _errbacks(c.body):
                maybe_signature(errback, self._app).apply((c.body.id,))  # type: ignore[union-attr]
            if throw:
                raise exc
            return EagerResult(c.body.id, exc, states.FAILURE, app=self.app)  # type: ignore[arg-type]
        return c.body.clone(([r.get() for r in header],)).apply(throw=throw)

    def __repr__(self) -> str:
        return f"chord([{', '.join(repr(t) for t in self.tasks)}], {self.body!r})"


#: ``chord(header, body)`` runs ``header`` in parallel, then ``body`` with their results.
chord = _chord


def xmap(task: Any, it: Iterable[Any]) -> Signature:
    return task.map(it)


def xstarmap(task: Any, it: Iterable[Any]) -> Signature:
    return task.starmap(it)


def _ends_in_group(sig: Signature) -> bool:
    """Whether ``sig``'s result is a group's: a group, or a chain or chord body ending in one."""
    if isinstance(sig, group):
        return True
    if isinstance(sig, _chord):
        return sig.body is not None and _ends_in_group(sig.body)
    if isinstance(sig, _chain):
        tasks = sig.tasks
        return bool(tasks) and _ends_in_group(tasks[-1])
    return False


def _join(sig: Signature, nxt: Signature) -> Signature:
    """``sig``, which ends in a group, followed by ``nxt``: that group becomes the
    header of a chord with ``nxt`` as its body, so ``nxt`` runs once, with all results."""
    if isinstance(sig, group):
        return _chord(sig, nxt, app=sig._app)
    sig.freeze()  # a chain or chord: its own options go to its tasks first
    if isinstance(sig, _chord):
        sig["kwargs"]["body"] = _join(sig.body, nxt)
    else:
        tasks = cast(_chain, sig).tasks
        tasks[-1] = _join(tasks[-1], nxt)
        sig["kwargs"]["tasks"] = tasks
    return sig


def _errbacks(sig: Signature) -> list[dict[str, Any]]:
    """The distinct errbacks of ``sig``'s tasks (once frozen, they carry all of them)."""
    if not isinstance(sig, (_chain, group, _chord)):
        return signatures_to_list(sig.options.get("link_error"))
    parts = [*sig.tasks, sig.body] if isinstance(sig, _chord) else sig.tasks
    out: list[dict[str, Any]] = []
    for part in parts:
        out += [e for e in _errbacks(part) if e not in out]
    return out


def _messages(app: Potatoq, sig: Signature, parent: Message | None = None, check: bool = False) -> list[Message]:
    """The messages that start ``sig``: a chain's first step, every group member, every
    chord header task. ``sig`` is frozen first, so their ids are the ones its result
    refers to. ``check`` validates the arguments (on the producer side)."""
    if isinstance(sig, _chain):
        sig.freeze()
        steps = sig.tasks
        # Link from the back: each step's completion callback is the next step.
        for prev, nxt in zip(reversed(steps[:-1]), reversed(steps[1:]), strict=True):
            prev.link(nxt)
        return _messages(app, steps[0], parent, check)
    if isinstance(sig, group):
        sig.freeze()
        return [m for t in sig.tasks for m in _messages(app, t, parent, check)]
    if isinstance(sig, _chord):
        sig.freeze()
        header = sig.tasks
        if not header:
            return _messages(app, sig.body.clone(([],)), parent, check)
        info = {"callback": sig.body.to_dict(), "size": len(header)}
        messages = []
        for t in header:
            t = t.clone()
            t.freeze(chord=info)
            messages += _messages(app, t, parent, check)
        return messages
    return [_message(app, sig, parent, check)]


def _message(app: Potatoq, sig: Signature, parent: Message | None, check: bool) -> Message:
    args, kwargs, options = sig._merge(None, None, None)
    task_id = options.pop("task_id", None)
    if options.get("chord"):
        options["ignore_result"] = False  # the chord needs it
    if parent is not None:
        options.setdefault("parent_id", parent.id)
        options.setdefault("root_id", parent.root_id)
    task = app.tasks.get(sig.task)
    if task is None:
        # Sent by name: the task's code (and its defaults) live elsewhere.
        return Message(
            task=sig.task, args=list(args), kwargs=kwargs, id=task_id or new_id(),
            queue=options.get("queue") or app.conf.task_default_queue,
            parent_id=options.get("parent_id"), root_id=options.get("root_id"),
            group_id=options.get("group_id"), group_index=options.get("group_index"), chord=options.get("chord"),
            link=signatures_to_list(options.get("link")), link_error=signatures_to_list(options.get("link_error")),
        )  # fmt: skip
    if check and task.typing:
        task._check_arguments(list(args), kwargs)
    return task.build_message(list(args), kwargs, task_id, **options)
