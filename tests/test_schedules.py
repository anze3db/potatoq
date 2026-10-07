from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from potatoq import crontab, schedule
from potatoq.schedules import maybe_schedule


def utc(*args):
    return datetime(*args, tzinfo=UTC)


def test_interval_is_aligned_to_epoch():
    s = schedule(timedelta(seconds=30))
    assert s.next_after(utc(2026, 1, 1, 0, 0, 10)) == utc(2026, 1, 1, 0, 0, 30)
    assert s.next_after(utc(2026, 1, 1, 0, 0, 30)) == utc(2026, 1, 1, 0, 1, 0)


@pytest.mark.parametrize(
    "cron,after,expected",
    [
        (crontab(), utc(2026, 1, 1, 0, 0, 30), utc(2026, 1, 1, 0, 1)),
        (crontab(minute=0, hour=0), utc(2026, 1, 1, 0, 0), utc(2026, 1, 2, 0, 0)),
        (crontab(minute="*/15"), utc(2026, 1, 1, 10, 7), utc(2026, 1, 1, 10, 15)),
        (crontab(minute=30, hour=7, day_of_week="mon-fri"), utc(2026, 1, 2, 8, 0), utc(2026, 1, 5, 7, 30)),
        (crontab(0, 0, day_of_month=1), utc(2026, 1, 15), utc(2026, 2, 1)),
        (crontab(0, 0, day_of_month=31), utc(2026, 2, 1), utc(2026, 3, 31)),
        (crontab(0, 0, month_of_year="jan"), utc(2026, 2, 1), utc(2027, 1, 1)),
        (crontab.from_string("0 9 * * sun"), utc(2026, 1, 1), utc(2026, 1, 4, 9, 0)),
        (crontab.from_string("@hourly"), utc(2026, 1, 1, 5, 59), utc(2026, 1, 1, 6, 0)),
    ],
)
def test_crontab_next(cron, after, expected):
    assert cron.next_after(after) == expected


def test_crontab_dom_or_dow_like_cron():
    # Both restricted: matches the 13th OR any Friday (standard cron semantics).
    cron = crontab(0, 0, day_of_week="fri", day_of_month=13)
    assert cron.next_after(utc(2026, 1, 1)) == utc(2026, 1, 2)  # Friday 2 Jan
    assert cron.next_after(utc(2026, 1, 10)) == utc(2026, 1, 13)


def test_crontab_timezone_and_dst():
    tz = ZoneInfo("Europe/Ljubljana")
    cron = crontab(minute=30, hour=2, tz=tz)
    # 29 March 2026: 02:30 doesn't exist (clocks jump 02:00 -> 03:00): skip to next day.
    after = datetime(2026, 3, 28, 12, 0, tzinfo=tz)
    nxt = cron.next_after(after)
    assert nxt.astimezone(tz).date().isoformat() == "2026-03-30"
    # Winter time: 02:30 local = 01:30 UTC.
    assert cron.next_after(datetime(2026, 1, 1, tzinfo=tz)) == utc(2026, 1, 1, 1, 30)


def test_fire_times_between():
    times = crontab(minute="*/10").fire_times_between(utc(2026, 1, 1, 0, 0), utc(2026, 1, 1, 0, 35))
    assert [t.minute for t in times] == [10, 20, 30]


def test_invalid_crontab():
    with pytest.raises(ValueError):
        crontab(minute=61)
    with pytest.raises(ValueError):
        crontab.from_string("* * *")


def test_maybe_schedule():
    assert isinstance(maybe_schedule(10), schedule)
    assert isinstance(maybe_schedule(timedelta(minutes=1)), schedule)
    assert isinstance(maybe_schedule("*/5 * * * *"), crontab)
