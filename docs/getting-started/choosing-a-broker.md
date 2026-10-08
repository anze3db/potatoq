---
hide:
  - toc
---

# Choosing a broker

Every broker supports the full feature set (delays, priorities, retries, dead letters,
chords and periodic tasks), each implemented with its own native primitives. Choose
based on what you already run.

| Broker | Best for | Extra infrastructure | Transactional enqueue | Results | Throughput¹ |
|---|---|---|---|---|---:|
| [**Postgres**](../brokers/postgres.md) | most apps | none (your database) | :lucide-check: | built in | ~7.5k/s |
| [**Redis / Valkey**](../brokers/redis.md) | high throughput | Redis | on commit | opt-in | ~12k/s |
| [**RabbitMQ**](../brokers/rabbitmq.md) | RabbitMQ shops | RabbitMQ 4.x | on commit | separate backend | ~7k/s |
| [**SQLite**](../brokers/sqlite.md) | development, single host | none (a file) | :lucide-check: | built in | ~6k/s |

"On commit": a transactional enqueue needs the queue in your database, so these brokers
send tasks right after `COMMIT` instead.

¹ No-op tasks per second, 4 worker processes,
broker on localhost ([benchmark](https://github.com/anze3db/potatoq/blob/main/benchmarks/throughput.py)).

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
