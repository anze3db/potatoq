"""Terminal output for the CLI and the worker's logs: colors, emojis, tags and tables.

The layout follows FastAPI's CLI: a gutter of right-aligned tags, then the text.

* **Colors** only go to terminals, and follow the `NO_COLOR` and `FORCE_COLOR`
  conventions (https://no-color.org).
* **Emojis** are on unless `POTATOQ_NO_EMOJI` is set or the stream can't encode them.
* `potatoq --color/--no-color` and `--emoji/--no-emoji` override both.

No dependencies: plain ANSI escape codes.
"""

from __future__ import annotations

import os
import re
import sys
import unicodedata
from collections.abc import Iterable, Sequence
from typing import IO, Any

__all__ = ["GUTTER", "Painter", "configure", "painter", "width"]

#: Width of the tag column.
GUTTER = 11

_overrides: dict[str, bool | None] = {"color": None, "emoji": None}

_CODES = {
    "bold": "1",
    "dim": "2",
    "italic": "3",
    "red": "31",
    "green": "32",
    "yellow": "33",
    "blue": "34",
    "magenta": "35",
    "cyan": "36",
    "white": "37",
    "grey": "90",
    "potato": "38;5;214",  # amber, like the logo
    "on_red": "30;41",
    "on_green": "30;42",
    "on_yellow": "30;43",
    "on_blue": "30;44",
    "on_magenta": "30;45",
    "on_cyan": "30;46",
    "on_potato": "30;48;5;214",
}
_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def configure(color: bool | None = None, emoji: bool | None = None) -> None:
    """Force colors or emojis on or off (``None``: decide per stream)."""
    _overrides["color"] = color
    _overrides["emoji"] = emoji


def color_enabled(stream: IO[str] | Any) -> bool:
    if _overrides["color"] is not None:
        return bool(_overrides["color"])
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    return _isatty(stream) and os.environ.get("TERM") != "dumb"


def emoji_enabled(stream: IO[str] | Any) -> bool:
    if _overrides["emoji"] is not None:
        return bool(_overrides["emoji"])
    if os.environ.get("POTATOQ_NO_EMOJI"):
        return False
    try:
        "🥔".encode(getattr(stream, "encoding", None) or "utf-8")
    except (UnicodeEncodeError, LookupError):
        return False
    return True


def _isatty(stream: Any) -> bool:
    try:
        return bool(stream.isatty())
    except (AttributeError, ValueError):  # closed, or not a file
        return False


def width(text: str) -> int:
    """How many terminal cells ``text`` takes (ANSI codes and emoji modifiers excluded)."""
    total = 0
    last = 0
    for char in _ANSI.sub("", text):
        if char == "\N{VARIATION SELECTOR-16}":  # emoji presentation: the previous character is wide
            total += 2 - last
            last = 2
            continue
        if unicodedata.combining(char) or char in ("\N{ZERO WIDTH JOINER}", "\N{VARIATION SELECTOR-15}"):
            continue
        last = 2 if unicodedata.east_asian_width(char) in ("W", "F") else 1
        total += last
    return total


class Painter:
    """Styles text for one stream (stdout for the CLI, the log handler's stream)."""

    def __init__(self, stream: IO[str] | Any = None, *, color: bool | None = None, emoji: bool | None = None):
        stream = sys.stdout if stream is None else stream
        self.color = color_enabled(stream) if color is None else color
        self.emoji = emoji_enabled(stream) if emoji is None else emoji
        #: A person is reading (a terminal, or FORCE_COLOR): not a script or a log file.
        self.pretty = _isatty(stream) or bool(os.environ.get("FORCE_COLOR"))

    def style(self, text: str, *styles: str) -> str:
        if not self.color or not styles or not text:
            return text
        codes = ";".join(_CODES[s] for s in styles)
        return f"\x1b[{codes}m{text}\x1b[0m"

    def icon(self, emoji: str) -> str:
        """``emoji`` and a space, or nothing with emojis off."""
        return f"{emoji} " if self.emoji else ""

    def dot(self, ok: bool) -> str:
        """A status light: 🟢/🟡, a colored ● without emojis, nothing in plain text."""
        if self.emoji:
            return "🟢 " if ok else "🟡 "
        return self.style("●", "green" if ok else "yellow") + " " if self.color else ""

    def tag(self, label: str, style: str = "cyan") -> str:
        """A right-aligned label for the gutter. ``on_*`` styles become a badge."""
        if style.startswith("on_") and self.color:
            badge = f" {label} "
            return " " * max(0, GUTTER - width(badge)) + self.style(badge, style, "bold")
        return " " * max(0, GUTTER - 1 - width(label)) + self.style(label, style) + " "

    def line(self, label: str, text: str, style: str = "cyan") -> str:
        """``   broker  redis://…``: a gutter tag, then the text."""
        return f"{self.tag(label, style) if label else ' ' * GUTTER}  {text}"

    def table(self, headers: Sequence[str], rows: Iterable[Sequence[str]], align: str | None = None) -> list[str]:
        """Rows as aligned columns under a dim header, in the text column.
        ``align``: one ``<`` or ``>`` per column."""
        rows = [list(r) for r in rows]
        align = align or "<" * len(headers)
        widths = [max([width(h), *(width(r[i]) for r in rows)]) for i, h in enumerate(headers)]

        def fmt(cells: Sequence[str]) -> str:
            out = []
            for cell, w, a in zip(cells, widths, align, strict=True):
                pad = " " * (w - width(cell))
                out.append(pad + cell if a == ">" else cell + pad)
            return (" " * (GUTTER + 2) + "   ".join(out)).rstrip()

        return [self.style(fmt(headers), "dim", "bold"), *(fmt(r) for r in rows)]


def painter(stream: IO[str] | Any = None) -> Painter:
    return Painter(stream)
