"""JSON serialization with round-tripping of common Python types.

Pickle is never used: it turns broker write access into remote code execution. Plain
JSON loses types that tasks commonly pass (datetimes, UUIDs, Decimals), so they are
encoded as ``{"__type__": ..., "__value__": ...}`` objects, the same convention
kombu uses, and decoded back on the other side.
"""

from __future__ import annotations

import base64
import datetime as dt
import decimal
import enum
import json
import os
import sys
import uuid
from typing import Any

from .exceptions import RemoteError

_TYPE = "__type__"
_VALUE = "__value__"


def _default(obj: Any) -> Any:
    # datetime must be tested before date (it is a subclass).
    if isinstance(obj, dt.datetime):
        return {_TYPE: "datetime", _VALUE: obj.isoformat()}
    if isinstance(obj, dt.date):
        return {_TYPE: "date", _VALUE: obj.isoformat()}
    if isinstance(obj, dt.time):
        return {_TYPE: "time", _VALUE: obj.isoformat()}
    if isinstance(obj, dt.timedelta):
        return {_TYPE: "timedelta", _VALUE: obj.total_seconds()}
    if isinstance(obj, uuid.UUID):
        return {_TYPE: "uuid", _VALUE: str(obj)}
    if isinstance(obj, decimal.Decimal):
        return {_TYPE: "decimal", _VALUE: str(obj)}
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return {_TYPE: "bytes", _VALUE: base64.b64encode(bytes(obj)).decode("ascii")}
    if isinstance(obj, (set, frozenset)):
        return {_TYPE: "set", _VALUE: list(obj)}
    if isinstance(obj, enum.Enum):
        return obj.value
    if isinstance(obj, os.PathLike):
        return os.fspath(obj)
    if hasattr(obj, "__json__"):
        return obj.__json__()
    # Django's lazy translation strings.
    if type(obj).__name__ == "__proxy__":
        return str(obj)
    raise TypeError(
        f"Object of type {type(obj).__name__} is not JSON serializable. "
        "Pass primitive values (ids, strings, numbers) to tasks instead of objects."
    )


_DECODERS: dict[str, Any] = {
    "datetime": dt.datetime.fromisoformat,
    "date": dt.date.fromisoformat,
    "time": dt.time.fromisoformat,
    "timedelta": lambda v: dt.timedelta(seconds=v),
    "uuid": uuid.UUID,
    "decimal": decimal.Decimal,
    "bytes": base64.b64decode,
    "set": set,
}


def _object_hook(obj: dict[str, Any]) -> Any:
    if len(obj) == 2 and _TYPE in obj and _VALUE in obj:
        decoder = _DECODERS.get(obj[_TYPE])
        if decoder is not None:
            return decoder(obj[_VALUE])
    return obj


_encoder = json.JSONEncoder(default=_default, separators=(",", ":"), ensure_ascii=False)
_decoder = json.JSONDecoder(object_hook=_object_hook)


def dumps(obj: Any) -> str:
    return _encoder.encode(obj)


def loads(data: str | bytes | bytearray | memoryview) -> Any:
    if isinstance(data, memoryview):
        data = bytes(data)
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8")
    return _decoder.decode(data)


def check_serializable(obj: Any) -> None:
    """Raise TypeError early, at ``.delay()`` time, if ``obj`` can't be encoded."""
    _encoder.encode(obj)


# --- exceptions ---------------------------------------------------------------


def exception_to_dict(exc: BaseException) -> dict[str, Any]:
    args: list[Any] = []
    for arg in exc.args:
        try:
            _encoder.encode(arg)
            args.append(arg)
        except (TypeError, ValueError):
            args.append(repr(arg))
    return {
        "exc_type": type(exc).__qualname__,
        "exc_module": type(exc).__module__,
        "exc_message": args,
    }


def exception_from_dict(data: dict[str, Any] | None) -> BaseException | None:
    """Rebuild an exception without importing anything.

    Only classes from modules that are already imported in this process are
    reconstructed; anything else becomes a :class:`RemoteError`. Results come from
    the broker, so they must never be able to trigger arbitrary imports.
    """
    if not data:
        return None
    type_name = data.get("exc_type", "Exception")
    module_name = data.get("exc_module") or "builtins"
    args = data.get("exc_message", [])
    if not isinstance(args, (list, tuple)):
        args = [args]
    module = sys.modules.get(module_name)
    cls: Any = module
    for part in type_name.split("."):
        cls = getattr(cls, part, None) if cls is not None else None
    if isinstance(cls, type) and issubclass(cls, BaseException):
        try:
            return cls(*args)
        except Exception:
            pass
    message = ", ".join(str(a) for a in args)
    return RemoteError(type_name, message, module_name)
