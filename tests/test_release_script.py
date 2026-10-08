"""scripts/release.py: CalVer, changelog generation and extraction (no network)."""

from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("release", Path(__file__).parent.parent / "scripts" / "release.py")
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)

D = dt.date(2026, 10, 8)


@pytest.mark.parametrize(
    ("tags", "today", "expected"),
    [
        ([], D, "26.1"),
        (["26.1", "26.2"], D, "26.3"),
        (["26.9", "26.10"], D, "26.11"),  # numeric, not lexical
        (["25.7", "26.1"], dt.date(2027, 1, 2), "27.1"),  # new year starts over
        (["v1.0", "0.1.0", "26.1"], D, "26.2"),  # unrelated tags are ignored
    ],
)
def test_next_version(tags, today, expected):
    assert release.next_version(tags, today) == expected


def test_previous_version():
    tags = ["25.9", "26.1", "26.2", "26.10"]
    assert release.previous_version(tags, "26.11") == "26.10"
    assert release.previous_version(tags, "26.1") == "25.9"
    assert release.previous_version([], "26.1") is None


GENERATED = """## What's Changed
### ✨ Features
* Add threads by @alice in https://github.com/o/r/pull/1
### 🐛 Fixes
* Fix chords by @bob in https://github.com/o/r/pull/2

## New Contributors
* @bob made their first contribution in https://github.com/o/r/pull/2

**Full Changelog**: https://github.com/o/r/compare/26.1...26.2"""

CHANGELOG = """# Changelog

Intro.

## Unreleased

- A hand-written highlight.

## [26.1](https://github.com/o/r/releases/tag/26.1) - 2026-01-05

Old notes.
"""


def test_render_and_insert_section():
    highlights, emptied = release.split_unreleased(CHANGELOG)
    assert highlights == "- A hand-written highlight."
    section = release.render_section("26.2", D, "o/r", GENERATED, ["alice", "bob"], highlights)
    assert section.startswith("## [26.2](https://github.com/o/r/releases/tag/26.2) - 2026-10-08")
    assert "### Highlights\n\n- A hand-written highlight." in section
    assert "### What's Changed" in section and "#### ✨ Features" in section  # demoted one level
    assert "Thank you to everyone who contributed to this release: @alice, @bob" in section

    updated = release.insert_section(emptied, section)
    assert updated.index("## Unreleased") < updated.index("## [26.2]") < updated.index("## [26.1]")
    assert "A hand-written highlight" not in updated.split("## [26.2]")[0]  # moved, not duplicated
    assert release.extract_section(updated, "26.2").startswith("### Highlights")
    assert release.extract_section(updated, "26.1") == "Old notes.\n"
    with pytest.raises(SystemExit):
        release.extract_section(updated, "26.9")


def test_contributors_from_commits_dedupes_and_skips_bots():
    commits = [
        {"author": {"login": "alice"}, "commit": {"message": "x"}},
        {"author": {"login": "dependabot[bot]"}, "commit": {"message": "bump"}},
        {"author": None, "committer": {"login": "bob"}, "commit": {"message": "y"}},
        {
            "author": {"login": "alice"},
            "commit": {"message": "pair\n\nCo-authored-by: Carol <123+carol@users.noreply.github.com>"},
        },
    ]
    assert release.contributors_from_commits(commits) == ["alice", "bob", "carol"]


def test_set_version():
    assert 'version = "26.2"' in release.set_version('[project]\nname = "x"\nversion = "26.1"\n', "26.2")


def test_repository_changelog_and_version_agree():
    """CHANGELOG.md parses and pyproject's version is a valid CalVer version."""
    root = Path(__file__).parent.parent
    version = release.re.search(r'^version = "([^"]+)"', (root / "pyproject.toml").read_text(), release.re.M)[1]
    assert release.VERSION_RE.match(version)
    highlights, _ = release.split_unreleased((root / "CHANGELOG.md").read_text())
    assert highlights or release.extract_section((root / "CHANGELOG.md").read_text(), version)
