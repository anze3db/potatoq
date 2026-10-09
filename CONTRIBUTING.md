# Contributing to potatoq

Thanks for helping! Bug reports, docs fixes and pull requests are all welcome.

## Setup

potatoq uses [uv](https://docs.astral.sh/uv/) for everything:

```console
$ git clone https://github.com/anze3db/potatoq && cd potatoq
$ uv sync                       # Python + all dev dependencies
```

The test suite runs against real brokers. Start whichever you have; tests for missing
services are skipped:

| Service | Default URL | Override with |
|---|---|---|
| Redis / Valkey | `redis://localhost:6379/15` | `POTATOQ_TEST_REDIS` |
| PostgreSQL | `postgresql://localhost/potatoq_test` (create the database first) | `POTATOQ_TEST_POSTGRES` |
| RabbitMQ 4.x | `amqp://guest:guest@localhost:5672//` | `POTATOQ_TEST_RABBITMQ` |

Tests isolate themselves (unique key prefixes, schemas and queue names) and clean up
after themselves, so they're safe to run against services you use for other things.

## Checks

Everything CI runs, locally:

```console
$ uv run pytest -n auto --dist loadgroup          # tests, in parallel (about a minute;
                                                  # CPU-timing tests marked `serial` are only reliable alone)
$ uv run coverage run -m pytest -n auto --dist loadgroup && uv run coverage combine && uv run coverage report   # coverage
$ uv run mypy                                      # types
$ uv run ruff check . && uv run ruff format --check src tests benchmarks scripts
$ uv run --group docs zensical serve               # docs at http://localhost:8000
```

Coverage must stay at 100%, measured across **all** CI jobs combined: every Python
version (3.11–3.15, free-threaded included) and Django 6.0. A few lines only run on
some of them (for example the Django 5.2 fallback on Python 3.11), so a local run on a
single version can show them as missing. CI's `coverage` job combines everything; its
summary and an HTML report are on the workflow run page.

- **Coverage is 100% and stays there.** Add tests with your change. Use
  `# pragma: no cover - <reason>` only for lines that genuinely can't be exercised.
- **Free-threaded Python** is supported. To test it:
  `UV_PROJECT_ENVIRONMENT=.venv-3.14t uv sync --python 3.14t --no-group binary`,
  then `uv run --no-sync --python 3.14t pytest`.
- Write tests that run against every broker where it makes sense: see the `broker_app`
  fixture in `tests/conftest.py` and `tests/test_brokers.py`.

## Pull requests

The changelog is generated from pull requests, so:

- **The title is the changelog entry.** Write it for users ("Retry Redis connections on
  failover"), in the imperative, without a trailing period.
- **Add one label:** `feature`, `bug`, `breaking`, `performance`, `documentation` or
  `maintenance`. Use `skip-changelog` for changes users don't need to hear about.
- Bigger changes deserve a line under **Unreleased** in `CHANGELOG.md`.
- Behaviour that users can observe should come with docs in `docs/`.

Looking for something to work on? See the
[wishlist](https://anze3db.github.io/potatoq/wishlist/) ([source](docs/wishlist.md)).

For substantial changes (a new broker feature, a change to a default), please open an
issue first to discuss the design. The defaults are deliberate, and
[docs/design/defaults.md](docs/design/defaults.md) explains the reasoning behind each one.

## Releases

Maintainers: see [RELEASING.md](RELEASING.md).
