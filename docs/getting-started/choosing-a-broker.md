# Choosing a broker

Every broker supports the full feature set (delays, priorities, retries, dead letters,
chords and periodic tasks), each implemented with its own native primitives. Choose
based on what you already run.

| | Postgres | Redis / Valkey | RabbitMQ | SQLite |
|---|---|---|---|---|
| **Best for** | most apps | very high throughput | existing RabbitMQ shops | development, single host |
| Extra infrastructure | none if you already use Postgres | Redis | RabbitMQ 4.x | none |
| Transactional enqueue | :lucide-check: | — (on commit) | — (on commit) | :lucide-check: |
| Results | built in, free | opt-in | needs a `result_backend` | built in, free |
| Wake-up | `LISTEN/NOTIFY` | `BZPOPMIN` | push (consume) | `data_version` |
| Throughput (local, 4 procs) | ~7k tasks/s | ~11k tasks/s | ~6k tasks/s | ~5k tasks/s |

## Recommendations

**Already on Postgres? Use it.** You get no new infrastructure, tasks that commit and
roll back with your data, and results stored in the same transaction as the ack. A
single Postgres instance handles thousands of tasks per second, more than most
applications ever enqueue.
:lucide-arrow-right: [Postgres broker](../brokers/postgres.md)

**Need tens of thousands of tasks per second, or already run Redis?** Use Redis, with
`maxmemory-policy noeviction` and AOF persistence.
:lucide-arrow-right: [Redis broker](../brokers/redis.md)

**Already operate RabbitMQ?** It works well: quorum queues, publisher confirms, and
broker-side delays. Pair it with a Redis or Postgres `result_backend` if you need
results.
:lucide-arrow-right: [RabbitMQ broker](../brokers/rabbitmq.md)

**Development, CI, a single small server?** SQLite is the default because it needs
nothing at all.
:lucide-arrow-right: [SQLite broker](../brokers/sqlite.md)

!!! tip "Tests"
    For unit tests use `memory://` with [`potatoq.testing.drain`](../guide/testing.md).
