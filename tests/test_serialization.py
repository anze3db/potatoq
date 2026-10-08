import datetime as dt
import enum
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

from potatoq import serialization
from potatoq.exceptions import RemoteError
from potatoq.message import Message


class Color(enum.Enum):
    RED = "red"


def test_round_trip_types():
    value = {
        "dt": dt.datetime(2026, 1, 1, 12, 0, tzinfo=dt.UTC),
        "naive": dt.datetime(2026, 1, 1, 12, 0),
        "d": dt.date(2026, 1, 1),
        "t": dt.time(12, 30),
        "td": dt.timedelta(seconds=90),
        "u": uuid.uuid4(),
        "dec": Decimal("1.10"),
        "b": b"bytes",
        "s": {1, 2},
        "nested": [{"x": (1, 2)}],
    }
    out = serialization.loads(serialization.dumps(value))
    assert out["nested"] == [{"x": [1, 2]}]  # tuples become lists, like JSON
    del value["nested"], out["nested"]
    assert out == value


def test_enums_and_paths_are_flattened():
    assert serialization.loads(serialization.dumps([Color.RED, Path("/tmp/x")])) == ["red", "/tmp/x"]


def test_objects_are_rejected_with_a_helpful_message():
    with pytest.raises(TypeError, match="Pass primitive values"):
        serialization.dumps(object())


def test_exceptions_rebuild_only_known_classes():
    exc = serialization.exception_from_dict(serialization.exception_to_dict(ValueError("bad", 1)))
    assert isinstance(exc, ValueError) and exc.args == ("bad", 1)
    unknown = serialization.exception_from_dict(
        {"exc_type": "Evil", "exc_module": "not.imported.module", "exc_message": ["x"]}
    )
    assert isinstance(unknown, RemoteError)


def test_message_round_trip_ignores_unknown_fields():
    m = Message(task="t", args=[1], kwargs={"a": dt.date(2026, 1, 1)})
    data = m.to_dict()
    data["future_field"] = 1
    assert Message.from_dict(data) == m
    assert Message.decode(m.encode()) == m


def test_wire_format_is_compact_and_versioned():
    import json

    m = Message(task="t", args=[1])
    wire = json.loads(m.encode())
    assert set(wire) == {"task", "args", "id", "queue", "enqueued_at", "v"}
    newer = {**wire, "v": 99}
    with pytest.raises(ValueError, match="upgrade potatoq"):
        Message.from_dict(newer)


def test_user_dicts_are_never_mistaken_for_type_markers():
    # kombu-style keys are ordinary data here
    data = {"__type__": "uuid", "__value__": "not-a-uuid"}
    assert serialization.loads(serialization.dumps(data)) == data
    # an unknown or malformed marker (e.g. from a newer version) stays a plain dict
    unknown = {"__potatoq__": "from-the-future", "v": 1}
    assert serialization.loads(serialization.dumps(unknown)) == unknown
    broken = {"__potatoq__": "uuid", "v": "not-a-uuid"}
    assert serialization.loads(serialization.dumps(broken)) == broken


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ((1, 2), [1, 2]),  # tuples become lists
        ({1: "a"}, {"1": "a"}),  # dict keys become strings
        (frozenset({1}), {1}),  # frozensets come back as sets
        (Color.RED, "red"),  # enums are sent as their value
        (float("inf"), float("inf")),
        (2**80, 2**80),  # big ints are exact
    ],
)
def test_documented_json_conversions(value, expected):
    assert serialization.loads(serialization.dumps(value)) == expected


def test_non_string_tuple_keys_are_rejected():
    with pytest.raises(TypeError):
        serialization.dumps({(1, 2): "x"})
