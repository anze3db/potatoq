"""Workflow primitives: ``signature``, ``chain``, ``group``, ``chord``.

Chains are implemented with ``link`` callbacks carried inside the message, and chords
with an atomic per-group counter in the broker, so no process ever polls
("chord_unlock") or blocks waiting for other tasks.
"""

from __future__ import annotations

import copy
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from .message import new_id

if TYPE_CHECKING:
    from .app import Potatoq
    from .result import AsyncResult, GroupResult

__all__ = ["Signature", "chain", "chord", "group", "maybe_signature", "signature", "subtask", "xmap", "xstarmap"]


class Signature(dict):  # type: ignore[type-arg]
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
    def _from_dict(cls, d: dict[str, Any], app: Potatoq | None = None) -> Signature:
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

    def apply_async(self, args: Any = None, kwargs: dict[str, Any] | None = None, route_name: Any = None, **options: Any) -> AsyncResult:
        args, kwargs, options = self._merge(args, kwargs, options)
        task = self.app.tasks.get(self["task"])
        if task is None:
            return self.app.send_task(self["task"], args, kwargs, **options)
        return task.apply_async(args, kwargs, **options)

    def apply(self, args: Any = None, kwargs: dict[str, Any] | None = None, **options: Any) -> Any:
        args, kwargs, options = self._merge(args, kwargs, options)
        return self.type.apply(args, kwargs, **options)

    def _merge(self, args: Any, kwargs: dict[str, Any] | None, options: dict[str, Any] | None) -> tuple[tuple[Any, ...], dict[str, Any], dict[str, Any]]:
        args = tuple(args or ())
        kwargs = dict(kwargs or {})
        options = dict(options or {})
        merged_options = {**self["options"], **options}
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
        if opts:
            sig["options"].update(opts)
        return sig

    partial = clone

    def set(self, immutable: bool | None = None, **options: Any) -> Signature:
        if immutable is not None:
            self["immutable"] = immutable
        self["options"].update(options)
        return self

    def set_immutable(self, immutable: bool) -> None:
        self["immutable"] = immutable

    def freeze(self, _id: str | None = None, group_id: str | None = None, **kwargs: Any) -> AsyncResult:
        opts = self["options"]
        if _id is not None:
            opts["task_id"] = _id
        opts.setdefault("task_id", new_id())
        if group_id is not None:
            opts["group_id"] = group_id
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
    return Signature(varies, *args, app=app, **kwargs)


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


def _flatten(tasks: Iterable[Any]) -> list[Any]:
    out = []
    for t in tasks:
        if isinstance(t, (list, tuple)) and not isinstance(t, Signature):
            out.extend(_flatten(t))
        else:
            out.append(t)
    return out


class _chain(Signature):
    def __init__(self, *tasks: Any, app: Potatoq | None = None, **options: Any):
        tasks_list = [maybe_signature(t, app) for t in _flatten(tasks)]
        Signature.__init__(self, "potatoq.chain", (), {"tasks": tasks_list}, options, subtask_type="chain", app=app)

    @property
    def tasks(self) -> list[Signature]:
        return [maybe_signature(t, self._app) for t in self["kwargs"]["tasks"]]  # type: ignore[misc]

    def apply_async(self, args: Any = None, kwargs: dict[str, Any] | None = None, **options: Any) -> AsyncResult:
        if self.app.conf.task_always_eager:
            return self.apply(args, kwargs, **options)
        steps = self._prepare_steps(args, kwargs)
        if not steps:
            raise ValueError("Empty chain")
        first = steps[0]
        res = first.apply_async(**{k: v for k, v in options.items() if k not in ("task_id",)})
        return self._last_result(steps) or res

    def _prepare_steps(self, args: Any = None, kwargs: dict[str, Any] | None = None) -> list[Signature]:
        """Clone the steps, upgrade ``group | sig`` to chords, and wire them with links."""
        raw = [t.clone() for t in self.tasks]
        steps: list[Signature] = []
        i = 0
        while i < len(raw):
            step = raw[i]
            if isinstance(step, group) and i + 1 < len(raw):
                nxt = raw[i + 1]
                step = chord(step.tasks, body=nxt, app=self._app)
                i += 1
            steps.append(step)
            i += 1
        if args or kwargs:
            steps[0] = steps[0].clone(args, kwargs)
        # Assign ids up front so the chain's result is known immediately.
        for step in steps:
            step.freeze()
        # Link from the back: each step's completion callback is the next step.
        for prev, nxt in zip(reversed(steps[:-1]), reversed(steps[1:]), strict=True):
            _link_after(prev, nxt)
        return steps

    def _last_result(self, steps: list[Signature]) -> AsyncResult | None:
        last = steps[-1]
        if isinstance(last, group):
            return None
        if isinstance(last, _chord):
            return self.app.AsyncResult(last.body.id)  # type: ignore[arg-type]
        return self.app.AsyncResult(last.id)  # type: ignore[arg-type]

    def apply(self, args: Any = None, kwargs: dict[str, Any] | None = None, **options: Any) -> Any:
        result = None
        last = None
        for i, step in enumerate(self.tasks):
            step = step.clone()
            if i == 0:
                step = step.clone(args, kwargs)
            elif last is not None:
                step = step.clone((last.get(),))
            res = step.apply(**options)
            if isinstance(res, list):  # group
                last = _EagerList(res)
            else:
                last = res
            result = last
        return result

    def __or__(self, other: Any) -> Any:
        if isinstance(other, _chain):
            return _chain(*self.tasks, *other.tasks, app=self._app)
        if isinstance(other, Signature):
            return _chain(*self.tasks, other, app=self._app)
        return NotImplemented

    def __repr__(self) -> str:
        return " | ".join(repr(t) for t in self.tasks)


class _EagerList(list):  # type: ignore[type-arg]
    def get(self, **kwargs: Any) -> list[Any]:
        return [r.get(**kwargs) for r in self]


#: ``chain(a.s(), b.s(), c.s())`` runs a, then b(a_result), then c(b_result).
chain = _chain


def _link_after(prev: Signature, nxt: Signature) -> None:
    if isinstance(prev, _chord):
        prev.body.link(nxt)
    elif isinstance(prev, group):
        # A group followed by something is converted to a chord in _prepare_steps;
        # a trailing group needs no link.
        raise ValueError("group must be followed by a task to form a chord")
    else:
        prev.link(nxt)


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

    def __iter__(self) -> Any:  # type: ignore[override]
        return iter(self.tasks)

    def __len__(self) -> int:
        return len(self["kwargs"]["tasks"])

    def freeze(self, _id: str | None = None, group_id: str | None = None, **kwargs: Any) -> GroupResult:  # type: ignore[override]
        opts = self["options"]
        gid = group_id or _id or opts.get("task_id") or new_id()
        opts["task_id"] = gid
        tasks = []
        for index, t in enumerate(self.tasks):
            t = t.clone() if t.id is None else t
            t.freeze(group_id=gid)
            t["options"]["group_index"] = index
            tasks.append(t)
        self["kwargs"]["tasks"] = tasks
        return self.app.GroupResult(gid, [self.app.AsyncResult(t.id) for t in tasks])  # type: ignore[arg-type]

    @property
    def id(self) -> str | None:
        return self["options"].get("task_id")

    def clone(self, args: Any = None, kwargs: dict[str, Any] | None = None, **opts: Any) -> group:  # type: ignore[override]
        g = group._from_dict(copy.deepcopy(dict(self)), self._app)
        if args or kwargs:
            g["kwargs"]["tasks"] = [t.clone(args, kwargs) for t in g.tasks]
        return g  # type: ignore[return-value]

    def apply_async(self, args: Any = None, kwargs: dict[str, Any] | None = None, **options: Any) -> GroupResult:  # type: ignore[override]
        if self.app.conf.task_always_eager:
            return self.apply(args, kwargs, **options)  # type: ignore[return-value]
        g = self.clone(args, kwargs)
        result = g.freeze()
        messages = []
        for t in g.tasks:
            task = self.app.tasks[t.task]
            targs, tkwargs, topts = t._merge(None, None, options)
            topts["task_id"] = t.id
            messages.append(task.build_message(list(targs), tkwargs, **{k: v for k, v in topts.items() if k != "task_id"}, task_id=t.id))
        if messages:
            self.app.publish(messages)
        return result

    def __call__(self, *partial_args: Any, **options: Any) -> GroupResult:  # type: ignore[override]
        return self.apply_async(partial_args, **options)

    def apply(self, args: Any = None, kwargs: dict[str, Any] | None = None, **options: Any) -> Any:
        from .result import GroupResult

        g = self.clone(args, kwargs)
        gid = g.freeze().id
        return GroupResult(gid, [t.apply(**options) for t in g.tasks], app=self.app)

    def link(self, callback: Any) -> Any:
        for t in self["kwargs"]["tasks"]:
            maybe_signature(t, self._app).link(callback)  # type: ignore[union-attr]
        return callback

    def __repr__(self) -> str:
        return f"group([{', '.join(repr(t) for t in self.tasks)}])"


class _chord(Signature):
    def __init__(self, header: Any, body: Any = None, app: Potatoq | None = None, **options: Any):
        if isinstance(header, group):
            header = header.tasks
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
        return body

    def __call__(self, body: Any = None, **options: Any) -> AsyncResult:  # type: ignore[override]
        if body is not None:
            self["kwargs"]["body"] = maybe_signature(body, self._app)
        return self.apply_async(**options)

    def freeze(self, _id: str | None = None, group_id: str | None = None, **kwargs: Any) -> AsyncResult:
        opts = self["options"]
        gid = opts.setdefault("group_id", group_id or new_id())
        header = []
        for index, t in enumerate(self.tasks):
            t.freeze(group_id=gid)
            t["options"]["group_index"] = index
            header.append(t)
        self["kwargs"]["header"] = header
        return self.body.freeze(_id)

    @property
    def id(self) -> str | None:
        return self.body.id if self.body is not None else None

    def clone(self, args: Any = None, kwargs: dict[str, Any] | None = None, **opts: Any) -> _chord:  # type: ignore[override]
        c = _chord._from_dict(copy.deepcopy(dict(self)), self._app)
        if args or kwargs:
            c["kwargs"]["header"] = [t.clone(args, kwargs) for t in c.tasks]
        return c  # type: ignore[return-value]

    def apply_async(self, args: Any = None, kwargs: dict[str, Any] | None = None, **options: Any) -> AsyncResult:
        if self.body is None:
            raise ValueError("chord needs a body (callback)")
        if self.app.conf.task_always_eager:
            return self.apply(args, kwargs, **options)
        c = self.clone(args, kwargs)
        result = c.freeze()
        header = c.tasks
        if not header:
            return c.body.apply_async(([],))
        callback = c.body.to_dict()
        size = len(header)
        gid = c["options"]["group_id"]
        messages = []
        for t in header:
            task = self.app.tasks[t.task]
            targs, tkwargs, topts = t._merge(None, None, options)
            tid = topts.pop("task_id")
            topts.pop("group_id", None)
            index = topts.pop("group_index")
            messages.append(
                task.build_message(
                    list(targs), tkwargs, tid, group_id=gid, group_index=index,
                    chord={"callback": callback, "size": size}, ignore_result=False, **topts,
                )
            )  # fmt: skip
        self.app.publish(messages)
        from .result import GroupResult

        result.parent = GroupResult(gid, [self.app.AsyncResult(t.id) for t in header], app=self.app)  # type: ignore[arg-type]
        return result

    def apply(self, args: Any = None, kwargs: dict[str, Any] | None = None, **options: Any) -> Any:
        header_results = group(*self.clone(args, kwargs).tasks, app=self._app).apply()
        values = [r.get() for r in header_results]
        return self.body.clone().apply((values,), **options)

    def __repr__(self) -> str:
        return f"chord([{', '.join(repr(t) for t in self.tasks)}], {self.body!r})"


#: ``chord(header, body)`` runs ``header`` in parallel, then ``body`` with their results.
chord = _chord


def xmap(task: Any, it: Iterable[Any]) -> Signature:
    return task.map(it)


def xstarmap(task: Any, it: Iterable[Any]) -> Signature:
    return task.starmap(it)
