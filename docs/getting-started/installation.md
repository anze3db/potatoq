# Installation

potatoq needs Python 3.11 or newer (3.11 – 3.15 are tested). The core has no
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

Next: [the quickstart](quickstart.md).
