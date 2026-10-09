# potatoq's development tasks: `just` lists them. Everything runs through uv.

set shell := ["bash", "-euo", "pipefail", "-c"]
set positional-arguments

paths := "src tests benchmarks scripts"

# List the recipes
[private]
default:
    @just --list --unsorted

# Install Python and the dev dependencies
sync:
    uv sync

# Run the tests in parallel; arguments go to pytest (`just test tests/test_cli.py -x`)
[group("test")]
test *args:
    uv run pytest -n auto --dist loadgroup "$@"

# Run the tests on another Python, in .venv-<python> (`just test-on 3.11`, `just test-on 3.14t`)
[group("test")]
test-on python *args:
    UV_PROJECT_ENVIRONMENT=".venv-{{ python }}" uv sync -q --python "{{ python }}" {{ if python =~ 't$' { "--no-group binary" } else { "" } }}
    UV_PROJECT_ENVIRONMENT=".venv-{{ python }}" uv run --no-sync pytest -n auto --dist loadgroup "${@:2}"

# Coverage on the current Python (some lines only run on other versions: see `cov-all`)
[group("test")]
cov:
    rm -f .coverage .coverage.*
    uv run coverage run -m pytest -q -n auto --dist loadgroup -m "not serial"
    uv run coverage run -m pytest -q -m serial
    uv run coverage combine -q
    uv run coverage report --fail-under=0
    uv run coverage html -q --skip-covered --fail-under=0
    @echo "HTML report: htmlcov/index.html"

# Coverage on several Pythons, combined like CI's (must be 100%)
[group("test")]
cov-all *pythons="3.11 3.13":
    #!/usr/bin/env bash
    set -euo pipefail
    rm -rf .coverage-data && mkdir .coverage-data
    for py in {{ pythons }}; do
        echo "── Python $py"
        rm -f .coverage .coverage.*
        export UV_PROJECT_ENVIRONMENT=".venv-$py"
        uv sync -q --python "$py" $([[ $py == *t ]] && echo --no-group binary)
        uv run --no-sync coverage run -m pytest -q -n auto --dist loadgroup -m "not serial"
        uv run --no-sync coverage run -m pytest -q -m serial
        uv run --no-sync coverage combine -q
        mv .coverage ".coverage-data/.coverage.python-$py"
    done
    unset UV_PROJECT_ENVIRONMENT
    uv run coverage combine -q .coverage-data
    rm -rf .coverage-data
    uv run coverage html -q --skip-covered --fail-under=0
    uv run coverage report

# Lint, format check and type check, as CI does
[group("check")]
lint:
    uv run ruff check {{ paths }}
    uv run ruff format --check {{ paths }}
    uv run mypy

# Format the code and apply ruff's safe fixes
[group("check")]
fmt:
    uv run ruff format {{ paths }}
    uv run ruff check --fix {{ paths }}

# Audit the GitHub workflows (zizmor, actionlint)
[group("check")]
lint-actions:
    uvx zizmor .github/workflows
    uvx --from actionlint-py actionlint

# Everything before a push: lint, then the tests
[group("check")]
check: lint test

# Serve the docs at http://localhost:8000, rebuilding on change
[group("docs")]
docs:
    uv run --group docs zensical serve

# Build the docs into site/, as the Docs workflow does
[group("docs")]
docs-build:
    uv run --group docs zensical build --clean

# Throughput against Celery (needs Redis); BENCH_N and BENCH_C tune it
[group("bench")]
bench:
    uv run --with 'celery[redis]' python benchmarks/throughput.py

# CPU-bound tasks: processes vs threads, GIL vs free-threaded (`just bench-cpu 3.14,3.14t`)
[group("bench")]
bench-cpu pythons="3.14,3.14t":
    BENCH_PYTHONS="{{ pythons }}" uv run python benchmarks/cpu.py

# Preview the next release's changelog section; changes nothing (`--pre` for the next alpha)
[group("release")]
release-preview *args:
    uv run scripts/release.py prepare --dry-run "$@"

# Commit "Release YY.N" on main; `git push` publishes it once CI passes (`--push` pushes too)
[group("release")]
release *args:
    uv run scripts/release.py prepare "$@"

# Commit the next alpha, e.g. "Release 26.1a2" (`--push` pushes too)
[group("release")]
release-alpha *args:
    uv run scripts/release.py prepare --pre "$@"

# Remove build, docs, coverage and cache output
clean:
    rm -rf dist site htmlcov .coverage .coverage.* .coverage-data .pytest_cache .ruff_cache .mypy_cache
