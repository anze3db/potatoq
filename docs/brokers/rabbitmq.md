# RabbitMQ

```python
app = Potatoq(
    "proj",
    broker="amqp://user:pass@rabbit:5672//",
    backend="redis://localhost:6379/0",       # RabbitMQ can't store results
)
```

Requires `potatoq[rabbitmq]` and RabbitMQ 4.0 or newer (4.3 recommended).

## Why it's good

- **Quorum queues**: replicated, with a delivery limit (poison-message guard) and an
  at-least-once dead-letter queue per queue (`<queue>.dlq`).
- **Publisher confirms** on every publish: `delay()` returns only once the broker has
  the task.
- **Delays without plugins** (the delayed-message plugin is archived and doesn't work on
  4.3): a 28-level TTL cascade carries countdowns and retries for up to ~8.5 years at
  1-second precision.
- **One message per idle process**, and a consumer timeout set above your task time
  limit, so long tasks aren't interrupted by RabbitMQ's 30-minute default.

## Things to know

- Queue names can't contain `.` (they become routing-key words for delays).
- Queues are created on first use as quorum queues. If a queue of the same name already
  exists with different arguments (for example a classic queue created by Celery),
  potatoq refuses to use it and tells you so. Pick a new queue name.
- Results, revocation, chords and `potatoq status` need a `result_backend`
  (Redis, Postgres or SQLite).
- Without a result backend, a task killed for exceeding its hard time limit is
  redelivered until the delivery limit. The worker warns about this at startup.

## Options

```python
app.conf.broker_transport_options = {
    "delivery_limit": 5,     # x-delivery-limit for new queues (default: task_max_deliveries)
}
```

[How it works in detail :octicons-arrow-right-24:](../design/internals.md#rabbitmq)
