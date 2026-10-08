"""Colors, emojis and layout of the CLI and the worker's logs."""

from __future__ import annotations

import io
import logging
import re
import sys

import pytest

from potatoq import Potatoq, cli, console
from potatoq.console import Painter, color_enabled, emoji_enabled, width
from potatoq.log import PotatoqFormatter, setup_logging

ANSI = re.compile(r"\x1b\[[0-9;]*m")


class Tty(io.StringIO):
    encoding = "utf-8"

    def isatty(self):
        return True


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for name in ("NO_COLOR", "FORCE_COLOR", "POTATOQ_NO_EMOJI", "TERM"):
        monkeypatch.delenv(name, raising=False)
    yield
    console.configure()


def test_colors_follow_the_terminal_and_the_conventions(monkeypatch):
    assert color_enabled(Tty()) and not color_enabled(io.StringIO())
    monkeypatch.setenv("TERM", "dumb")
    assert not color_enabled(Tty())
    monkeypatch.delenv("TERM")
    monkeypatch.setenv("FORCE_COLOR", "1")
    assert color_enabled(io.StringIO())
    monkeypatch.setenv("NO_COLOR", "1")
    assert not color_enabled(Tty())  # NO_COLOR wins
    console.configure(color=True)
    assert color_enabled(io.StringIO())  # --color wins over everything


def test_emojis_unless_asked_not_to_or_the_stream_cant(monkeypatch):
    class Ascii(io.StringIO):
        encoding = "ascii"

    class Closed:
        def isatty(self):
            raise ValueError("I/O operation on closed file")

    assert emoji_enabled(io.StringIO())
    assert not emoji_enabled(Ascii())
    assert not color_enabled(Closed())
    monkeypatch.setenv("POTATOQ_NO_EMOJI", "1")
    assert not emoji_enabled(io.StringIO())
    console.configure(emoji=True)
    assert emoji_enabled(Ascii())


def test_width_counts_terminal_cells():
    assert width("abc") == 3
    assert width("✅ ok") == 5
    assert width("\x1b[32mgreen\x1b[0m") == 5
    assert width("⏱️") == 2  # the variation selector takes no cell
    assert width("é") == 1  # e + combining accent
    assert width("👩\u200d💻") == 4  # joined emoji: counted per part (terminals differ)


def test_painter_in_color_and_plain():
    colored = Painter(color=True, emoji=True)
    plain = Painter(color=False, emoji=False)
    assert colored.style("hi", "green") == "\x1b[32mhi\x1b[0m"
    assert colored.style("hi") == "hi" and plain.style("hi", "red") == "hi"
    assert colored.icon("🥔") == "🥔 " and plain.icon("🥔") == ""
    assert colored.dot(True) == "🟢 " and colored.dot(False) == "🟡 "
    assert Painter(color=True, emoji=False).dot(True) == "\x1b[32m●\x1b[0m "
    assert plain.dot(True) == ""
    assert plain.line("broker", "redis://x") == "    broker   redis://x"
    assert plain.line("", "more") == " " * 13 + "more"
    badge = colored.tag("potatoq", "on_potato")
    assert ANSI.sub("", badge) == "   potatoq " and width(badge) == console.GUTTER
    assert plain.tag("potatoq", "on_potato") == "   potatoq "  # no badge without colors
    assert plain.table(["A", "COUNT"], [["x", "1"], ["long", "22"]], "<>") == [
        " " * 13 + "A      COUNT",
        " " * 13 + "x          1",
        " " * 13 + "long      22",
    ]


def _record(msg, *args, level=logging.INFO, **extra):
    record = logging.LogRecord("potatoq.worker", level, __file__, 1, msg, args, None)
    record.processName = "ForkPoolWorker-1"
    record.__dict__.update(extra)
    return record


def test_formatter_on_a_terminal():
    fmt = PotatoqFormatter(Tty())
    event = {"potatoq_event": "success", "task_name": "shop.add", "task_id": "abc", "runtime": 0.01}
    line = fmt.format(_record("Task %s succeeded in %s", "shop.add[abc]", "10ms", **event))
    assert re.fullmatch(r"\d\d:\d\d:\d\d +INFO +✅ shop\.add\[abc\] succeeded in 10ms", ANSI.sub("", line))
    assert "\x1b[1mshop.add\x1b[0m" in line and "\x1b[32msucceeded in 10ms\x1b[0m" in line

    failure = {**event, "potatoq_event": "failure"}
    failed = _record("Task shop.add[abc] failed in 1ms: ValueError('x')", level=logging.ERROR, **failure)
    line = ANSI.sub("", fmt.format(failed))
    assert line.endswith("ERROR   ❌ shop.add[abc] failed in 1ms: ValueError('x')")

    tagged = ANSI.sub("", fmt.format(_record("redis://x", potatoq_tag="broker")))
    assert tagged.endswith("   broker   redis://x")
    ready = fmt.format(_record("Worker w is ready", potatoq_tag="potatoq", potatoq_icon="🥔"))
    assert "\x1b[30;48;5;214;1m potatoq \x1b[0m" in ready and ready.endswith("🥔 Worker w is ready")

    in_task = ANSI.sub("", fmt.format(_record("charging", task_name="shop.charge", task_id="t1")))
    assert in_task.endswith("INFO   shop.charge[t1] charging")


def test_formatter_for_files_and_log_collectors():
    fmt = PotatoqFormatter(io.StringIO())
    event = {"potatoq_event": "retry", "task_name": "shop.add", "task_id": "abc"}
    line = fmt.format(_record("Task shop.add[abc] failed in 1ms, will retry in 8s: E()", **event))
    assert re.fullmatch(
        r"\[\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d+: INFO/ForkPoolWorker-1\] 🔁 Task shop\.add\[abc\] failed in 1ms, will retry in 8s: E\(\)",
        line,
    )
    assert fmt.format(_record("redis://x", potatoq_tag="broker")).endswith("] broker: redis://x")
    assert fmt.format(_record("Worker ready", potatoq_tag="potatoq")).endswith("] Worker ready")
    assert fmt.format(_record("charging", task_name="shop.charge", task_id="t1")).endswith(
        "] shop.charge[t1]: charging"
    )
    plain = PotatoqFormatter(io.StringIO(), emoji=False)
    assert plain.format(_record("x", potatoq_icon="🥔")).endswith("] x")


def test_tracebacks_start_at_the_task_and_end_where_it_raised(tmp_path):
    from potatoq.worker import executor

    app = Potatoq("tb", broker="memory://", set_as_current=False)

    @app.task(name="tb.fail", bind=True, max_retries=0)
    def fail(self):
        raise self.retry(exc=ValueError("boom"))

    from potatoq.message import Message

    outcome = executor.execute(app, Message(task="tb.fail", id="1"))
    exc = outcome.exc
    fmt = PotatoqFormatter(io.StringIO())
    text = fmt.formatException((type(exc), exc, exc.__traceback__))
    frames = re.findall(r'File "([^"]+)"', text)
    assert frames == [__file__]  # neither the worker's frames nor retry() re-raising
    assert text.endswith("ValueError: boom")
    record = _record("failed", level=logging.ERROR)
    record.exc_info = (type(exc), exc, exc.__traceback__)
    assert fmt.format(record).endswith("ValueError: boom")
    record.exc_info, record.exc_text, record.stack_info = None, None, "Stack (most recent call last):\n  here"
    assert fmt.format(record).endswith("  here")


def test_setup_logging_uses_settings_and_flags(monkeypatch):
    root = logging.getLogger()
    saved = root.handlers[:], root.level
    app = Potatoq("logs", broker="memory://", set_as_current=False)
    try:
        root.handlers = []
        app.conf.worker_log_color = True
        app.conf.worker_log_emoji = False
        setup_logging(app, "INFO")
        formatter = root.handlers[-1].formatter
        assert formatter.paint.color and not formatter.paint.emoji
        root.handlers = []
        console.configure(color=False, emoji=True)  # --no-color --emoji
        setup_logging(app, "INFO")
        formatter = root.handlers[-1].formatter
        assert not formatter.paint.color and formatter.paint.emoji
        root.handlers = []
        app.conf.worker_log_format = "%(levelname)s %(message)s"  # Celery-style: used as given
        setup_logging(app, "INFO")
        assert not isinstance(root.handlers[-1].formatter, PotatoqFormatter)
    finally:
        root.handlers, root.level = saved


def test_cli_flags_and_pretty_call(monkeypatch, capsys):
    app = Potatoq("flags", broker="memory://", set_as_current=False)
    monkeypatch.setenv("FORCE_COLOR", "1")  # pretty output, as on a terminal
    assert cli.main(["-A", app, "--no-color", "--emoji", "call", "flags.x", "--countdown", "90"]) == 0
    out = capsys.readouterr().out
    assert "\x1b[" not in out
    assert out.splitlines()[0] == "      sent   📨 flags.x to default in 1m"
    assert cli.main(["-A", app, "--color", "--no-emoji", "queues"]) == 0
    assert "\x1b[36mqueues\x1b[0m" in capsys.readouterr().out


def test_cli_reports_configuration_errors_briefly(monkeypatch, capsys):
    from potatoq.exceptions import ImproperlyConfigured

    app = Potatoq("errors", broker="memory://", set_as_current=False)

    def broken(app, args):
        raise ImproperlyConfigured("Environment variable 'X' is not set")

    monkeypatch.setattr(cli, "cmd_queues", broken)
    monkeypatch.setenv("POTATOQ_NO_EMOJI", "1")
    assert cli.main(["-A", app, "queues"]) == 1
    assert capsys.readouterr().err == "     error   Environment variable 'X' is not set\n"


@pytest.mark.skipif(sys.version_info < (3, 14), reason="argparse colors are new in 3.14")
def test_help_is_colored_on_314(monkeypatch):
    monkeypatch.setenv("FORCE_COLOR", "1")
    assert cli.build_parser().color


def test_describing_schedules_and_durations():
    from potatoq.cli import _coarse
    from potatoq.schedules import BaseSchedule, crontab, schedule
    from potatoq.worker.scheduler import describe_schedule

    class Odd(BaseSchedule):
        def next_after(self, after):
            return after

        def __repr__(self):
            return "<odd>"

    assert describe_schedule(crontab(minute="*/5", day_of_week="mon-fri")) == "*/5 * * * mon-fri"
    assert describe_schedule(schedule(2 * 86400)) == "every 2d"
    assert describe_schedule(schedule(7200)) == "every 2h"
    assert describe_schedule(schedule(90)) == "every 90s"
    assert describe_schedule(schedule(0.5)) == "every 0.5s"
    assert describe_schedule(Odd()) == "<odd>"
    assert repr(schedule(30)) == "<schedule: every 30s>"
    assert [_coarse(s) for s in (5, 300, 3600, 3720, 86400, 90000)] == ["5s", "5m", "1h", "1h02m", "1d", "1d1h"]


def test_schedule_command_survives_a_broken_entry(monkeypatch, capsys):
    from potatoq.schedules import BaseSchedule

    class Broken(BaseSchedule):
        def next_after(self, after):
            raise RuntimeError("nope")

    app = Potatoq("sched", broker="memory://", set_as_current=False)
    app.conf.beat_schedule = {"bad": {"task": "x", "schedule": Broken()}}
    monkeypatch.setenv("POTATOQ_NO_EMOJI", "1")
    assert cli.main(["-A", app, "schedule"]) == 0
    assert "unknown (nope)" in capsys.readouterr().out
