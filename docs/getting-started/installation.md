# Installation

potatoq needs Python 3.11 or newer. 3.11 – 3.15 are tested, including the free-threaded 3.14t and 3.15t builds.

!!! warning "Alpha"
    potatoq is alpha software: use it in production at your own risk for now. APIs and
    defaults may still change between releases.

!!! warning "Linux and macOS only"
    Windows isn't supported: workers rely on `fork()`, POSIX signals and `setitimer`, the
    same as Celery's default pool. On Windows, run potatoq under
    [WSL](https://learn.microsoft.com/windows/wsl/) or in a Linux container.
 The core has no
dependencies; each broker's client library is an optional extra.

=== "uv"

    ```console
    $ uv add potatoq                # SQLite broker only
    $ uv add "potatoq[postgres]"    # + PostgreSQL (psycopg 3)
    $ uv add "potatoq[redis]"       # + Redis / Valkey
    $ uv add "potatoq[rabbitmq]"    # + RabbitMQ (pika)
    $ uv add "potatoq[all]"
    ```

=== "pip"

    ```console
    $ pip install potatoq
    $ pip install "potatoq[postgres]"
    $ pip install "potatoq[redis]"
    $ pip install "potatoq[rabbitmq]"
    ```

| Extra | Installs | Broker URLs |
|---|---|---|
| *(none)* | nothing | `sqlite:///path.db`, `memory://` |
| `postgres` | `psycopg[binary]>=3.2` | `postgresql://…`, `postgres://…` |
| `redis` | `redis>=5` | `redis://…`, `rediss://…`, `valkey://…`, `unix://…` |
| `rabbitmq` | `pika>=1.3` | `amqp://…`, `amqps://…` |

!!! note "Free-threaded Python (3.13t, 3.14t, 3.15t)"
    potatoq supports free-threaded builds. `psycopg-binary` doesn't publish wheels for
    them yet, so for Postgres install `psycopg` (pure Python) and make sure the system
    `libpq` is available (`apt install libpq5`, `brew install libpq`, or Postgres.app).

Next: [the quickstart](quickstart.md).
