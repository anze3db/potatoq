"""Periodic schedules: ``crontab`` and fixed intervals, compatible with ``celery.schedules``.

Every schedule computes *deterministic* fire times: an interval of 30s always fires at
:00 and :30 (aligned to the Unix epoch) rather than "30s after the scheduler happened
to start". That lets every worker run the scheduler at once and deduplicate on
``(entry name, fire time)``, so there is no single ``beat`` process to keep alive and
no risk of double-scheduling when you accidentally run two.
"""

from __future__ import annotations

import calendar
import re
from datetime import UTC, datetime, timedelta, tzinfo
from typing import Any
from zoneinfo import ZoneInfo

__all__ = ["BaseSchedule", "crontab", "maybe_schedule", "schedule"]

_DAY_NAMES = {name: i for i, name in enumerate(["sun", "mon", "tue", "wed", "thu", "fri", "sat"])}
_MONTH_NAMES = {name.lower(): i for i, name in enumerate(calendar.month_abbr) if name}


class BaseSchedule:
    def next_after(self, after: datetime) -> datetime:
        """Return the first fire time strictly after ``after`` (an aware datetime)."""
        raise NotImplementedError

    def fire_times_between(self, start: datetime, end: datetime, limit: int = 1000) -> list[datetime]:
        """All fire times in ``(start, end]``."""
        times: list[datetime] = []
        t = self.next_after(start)
        while t <= end and len(times) < limit:
            times.append(t)
            t = self.next_after(t)
        return times


class schedule(BaseSchedule):
    """Run every ``run_every`` (seconds or timedelta), aligned to the epoch."""

    def __init__(self, run_every: float | timedelta, relative: bool = False, **kwargs: Any):
        if isinstance(run_every, timedelta):
            run_every = run_every.total_seconds()
        if run_every <= 0:
            raise ValueError("schedule interval must be positive")
        self.seconds = float(run_every)
        self.run_every = timedelta(seconds=self.seconds)

    def next_after(self, after: datetime) -> datetime:
        ts = after.timestamp()
        n = int(ts // self.seconds) + 1
        return datetime.fromtimestamp(n * self.seconds, tz=UTC)

    def __repr__(self) -> str:
        return f"<schedule: every {self.seconds:g}s>"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, schedule) and other.seconds == self.seconds

    def __hash__(self) -> int:
        return hash(("schedule", self.seconds))


def _parse_field(spec: Any, lo: int, hi: int, names: dict[str, int] | None = None) -> frozenset[int]:
    if isinstance(spec, int):
        values = {spec}
    elif isinstance(spec, (list, tuple, set, frozenset, range)):
        values = set()
        for item in spec:
            values |= _parse_field(item, lo, hi, names)
    else:
        values = set()
        for part in str(spec).lower().replace(" ", "").split(","):
            if not part:
                continue
            step = 1
            if "/" in part:
                part, step_s = part.split("/", 1)
                step = int(step_s)
                if step <= 0:
                    raise ValueError(f"Invalid step in cron field {spec!r}")
            if part in ("*", ""):
                start, end = lo, hi
            elif "-" in part:
                a, b = part.split("-", 1)
                start, end = _to_int(a, names), _to_int(b, names)
            else:
                start = _to_int(part, names)
                end = hi if step != 1 else start
            if end < start:  # wrap-around ranges such as fri-mon
                values.update(range(start, hi + 1, step))
                values.update(range(lo, end + 1, step))
            else:
                values.update(range(start, end + 1, step))
    if names is _DAY_NAMES:
        values = {0 if v == 7 else v for v in values}
    for v in values:
        if not lo <= v <= hi:
            raise ValueError(f"Value {v} out of range [{lo}, {hi}] in cron field {spec!r}")
    if not values:
        raise ValueError(f"Empty cron field {spec!r}")
    return frozenset(values)


def _to_int(token: str, names: dict[str, int] | None) -> int:
    if names and token[:3] in names and not token.isdigit():
        return names[token[:3]]
    if not re.fullmatch(r"\d+", token):
        raise ValueError(f"Invalid cron token {token!r}")
    return int(token)


class crontab(BaseSchedule):
    """A cron-like schedule. Fields accept ints, iterables or cron syntax strings.

    ``day_of_week`` uses 0 (or 7) = Sunday and accepts names (``"mon-fri"``). As in
    standard cron, when both ``day_of_month`` and ``day_of_week`` are restricted a day
    matches if *either* matches.
    """

    def __init__(
        self,
        minute: Any = "*",
        hour: Any = "*",
        day_of_week: Any = "*",
        day_of_month: Any = "*",
        month_of_year: Any = "*",
        tz: str | tzinfo | None = None,
        **kwargs: Any,
    ):
        self._orig = (minute, hour, day_of_week, day_of_month, month_of_year)
        self.minute = _parse_field(minute, 0, 59)
        self.hour = _parse_field(hour, 0, 23)
        self.day_of_week = _parse_field(day_of_week, 0, 7, _DAY_NAMES)
        self.day_of_month = _parse_field(day_of_month, 1, 31)
        self.month_of_year = _parse_field(month_of_year, 1, 12, _MONTH_NAMES)
        self._dow_star = str(day_of_week).strip() == "*"
        self._dom_star = str(day_of_month).strip() == "*"
        self.tz: tzinfo | None = ZoneInfo(tz) if isinstance(tz, str) else tz

    @classmethod
    def from_string(cls, expr: str, tz: str | tzinfo | None = None) -> crontab:
        aliases = {
            "@yearly": "0 0 1 1 *",
            "@annually": "0 0 1 1 *",
            "@monthly": "0 0 1 * *",
            "@weekly": "0 0 * * 0",
            "@daily": "0 0 * * *",
            "@midnight": "0 0 * * *",
            "@hourly": "0 * * * *",
        }
        expr = aliases.get(expr.strip(), expr)
        parts = expr.split()
        if len(parts) != 5:
            raise ValueError(f"Cron expression must have 5 fields, got {expr!r}")
        minute, hour, dom, month, dow = parts
        return cls(minute, hour, dow, dom, month, tz=tz)

    def _day_matches(self, d: datetime) -> bool:
        dom_ok = d.day in self.day_of_month
        dow_ok = (d.isoweekday() % 7) in self.day_of_week
        if self._dom_star and self._dow_star:
            return True
        if self._dom_star:
            return dow_ok
        if self._dow_star:
            return dom_ok
        return dom_ok or dow_ok

    def next_after(self, after: datetime, tz: tzinfo | None = None) -> datetime:
        zone = self.tz or tz or after.tzinfo or UTC
        after_utc = after.astimezone(UTC)
        # Work in naive local wall-clock time, then localize.
        t = after.astimezone(zone).replace(tzinfo=None, second=0, microsecond=0) + timedelta(minutes=1)
        limit = t + timedelta(days=366 * 5)
        while t < limit:
            if t.month not in self.month_of_year:
                t = (t.replace(day=1) + timedelta(days=32)).replace(day=1, hour=0, minute=0)
                continue
            if not self._day_matches(t):
                t = (t + timedelta(days=1)).replace(hour=0, minute=0)
                continue
            if t.hour not in self.hour:
                t = (t + timedelta(hours=1)).replace(minute=0)
                continue
            if t.minute not in self.minute:
                t += timedelta(minutes=1)
                continue
            aware = t.replace(tzinfo=zone, fold=0)
            as_utc = aware.astimezone(UTC)
            # Skip wall times that don't exist (DST spring-forward gap) and the
            # repeated hour's second pass once we are past it.
            if as_utc.astimezone(zone).replace(tzinfo=None) != t or as_utc <= after_utc:
                t += timedelta(minutes=1)
                continue
            return as_utc
        raise ValueError(f"{self!r} never fires")

    def __repr__(self) -> str:
        m, h, dow, dom, moy = self._orig
        return f"<crontab: {m} {h} {dom} {moy} {dow} (m/h/dM/MY/d)>"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, crontab) and (
            other.minute,
            other.hour,
            other.day_of_week,
            other.day_of_month,
            other.month_of_year,
        ) == (self.minute, self.hour, self.day_of_week, self.day_of_month, self.month_of_year)

    def __hash__(self) -> int:
        return hash((self.minute, self.hour, self.day_of_week, self.day_of_month, self.month_of_year))


def maybe_schedule(s: Any) -> BaseSchedule:
    if isinstance(s, BaseSchedule):
        return s
    if isinstance(s, (int, float, timedelta)):
        return schedule(s)
    if isinstance(s, str):
        return crontab.from_string(s)
    raise TypeError(f"Unsupported schedule: {s!r}")
