"""The task message that travels through every broker."""

from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from . import serialization

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

    def encode(self) -> str:
        return serialization.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Message:
        known = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        return cls(**known)

    @classmethod
    def decode(cls, data: str | bytes) -> Message:
        return cls.from_dict(serialization.loads(data))

    @property
    def delay(self) -> float:
        """Seconds until the message may run (0 if due)."""
        if self.eta is None:
            return 0.0
        return max(0.0, self.eta - time.time())

    def is_expired(self, now: float | None = None) -> bool:
        return self.expires is not None and (now or time.time()) >= self.expires
