"""The task message that travels through every broker."""

from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from . import serialization

#: Bumped only for incompatible changes. Optional new fields don't need a bump:
#: decoders ignore fields they don't know.
PROTOCOL_VERSION = 1


def new_id() -> str:
    return str(uuid.uuid4())


@dataclass(slots=True)
class Message:
    task: str
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=new_id)
    queue: str = "default"
    priority: int = 0
    #: Unix timestamp before which the task must not run.
    eta: float | None = None
    #: Unix timestamp after which the task is discarded instead of run.
    expires: float | None = None
    #: How many times the task has been retried via ``Task.retry`` / autoretry.
    retries: int = 0
    root_id: str | None = None
    parent_id: str | None = None
    group_id: str | None = None
    group_index: int | None = None
    #: Per-call overrides of task options (time limits, max_retries, ...).
    options: dict[str, Any] = field(default_factory=dict)
    #: Serialized signatures to call with the result (``link``) or on failure.
    link: list[dict[str, Any]] = field(default_factory=list)
    link_error: list[dict[str, Any]] = field(default_factory=list)
    #: Chord callback info: ``{"callback": sig, "size": n}``.
    chord: dict[str, Any] | None = None
    ignore_result: bool = False
    headers: dict[str, Any] = field(default_factory=dict)
    enqueued_at: float = field(default_factory=time.time)
    v: int = PROTOCOL_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_wire(self) -> dict[str, Any]:
        """Compact form: fields still at their default are omitted, so messages stay
        small and adding fields later costs nothing on the wire."""
        data = self.to_dict()
        return {k: v for k, v in data.items() if k in _ALWAYS or v != _DEFAULTS[k]}

    def encode(self) -> str:
        return serialization.dumps(self.to_wire())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Message:
        version = data.get("v", PROTOCOL_VERSION)
        if not isinstance(version, int) or version > PROTOCOL_VERSION:
            raise ValueError(
                f"Message {data.get('id')} uses protocol v{version}, this worker understands up to "
                f"v{PROTOCOL_VERSION}: upgrade potatoq on the workers first."
            )
        # Unknown fields are ignored: newer producers can add optional fields freely.
        known = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        return cls(**known)

    @classmethod
    def decode(cls, data: str | bytes) -> Message:
        decoded = serialization.loads(data)
        if not isinstance(decoded, dict) or "task" not in decoded or "id" not in decoded:
            # Without an id, every redelivery would get a new one, so "already finished"
            # and revocation checks couldn't recognise it.
            raise ValueError(_not_ours(decoded))
        return cls.from_dict(decoded)

    @property
    def delay(self) -> float:
        """Seconds until the message may run (0 if due)."""
        if self.eta is None:
            return 0.0
        return max(0.0, self.eta - time.time())

    def is_expired(self, now: float | None = None) -> bool:
        return self.expires is not None and (now or time.time()) >= self.expires


#: Fields written even when they hold their default value.
_ALWAYS = frozenset({"task", "id", "v", "queue", "enqueued_at"})


def _field_defaults() -> dict[str, Any]:
    from dataclasses import MISSING, fields

    out: dict[str, Any] = {}
    for f in fields(Message):
        if f.default is not MISSING:
            out[f.name] = f.default
        elif f.default_factory is not MISSING:
            out[f.name] = f.default_factory()
    return out


_DEFAULTS = _field_defaults()


def _not_ours(data: Any) -> str:
    """Why a message on a potatoq queue can't be run, in words a person can act on."""
    if isinstance(data, list) and len(data) == 3 and isinstance(data[2], dict):
        return (
            "This is a Celery (protocol 2) message, not a potatoq one. potatoq has its own "
            "message format: send it with potatoq, or see the message format docs for other languages."
        )
    if isinstance(data, dict) and "body" in data and "headers" in data:
        return "This is a Celery/kombu message envelope, not a potatoq message. See the message format docs."
    if isinstance(data, dict) and "task" in data:
        return "Not a potatoq message: it has no 'id'. Producers must set a unique id (e.g. a UUID)."
    return f"Not a potatoq message: expected a JSON object with 'task' and 'id' fields, got {type(data).__name__}."
