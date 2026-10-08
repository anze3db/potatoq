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
- **One message per idle process**, and a consumer timeout set above your longest task
  time limit, so long tasks aren't interrupted by RabbitMQ's 30-minute default
  (RabbitMQ 4.3+, see below).

## Things to know

- Queue names can't contain `.` (they become routing-key words for delays).
- Queues are created on first use as quorum queues. If a queue of the same name already
  exists with different arguments (for example a classic queue created by Celery),
  potatoq refuses to use it and tells you so. Pick a new queue name.
- Results, revocation, chords and `potatoq status` need a `result_backend`
  (Redis, Postgres or SQLite).
- Without a result backend, a task killed for exceeding its hard time limit is
  redelivered until the delivery limit. The worker warns about this at startup.
- Periodic tasks are sent by one elected worker. If it dies, a new one takes over within
  about 1 s, or about 60 s if its host or network went down. Runs due in that window can
  be skipped, or occasionally sent twice
  ([details](../guide/periodic-tasks.md#rabbitmq-leader-failover)).

### Long tasks and the consumer timeout

RabbitMQ gives up on a delivery that stays unacknowledged longer than its *consumer
timeout* (30 minutes by default) and hands the message to another worker, while the
first one is still running it. potatoq asks for a timeout 5 minutes above the longest
time limit of the tasks the worker has registered, but RabbitMQ only honours that from
**4.3**. On older servers, the server-wide `consumer_timeout` applies: raise it in
`rabbitmq.conf`, or set a policy on potatoq's queues (merge it with any policy they
already have, only one applies per queue):

```console
$ rabbitmqctl set_policy potatoq-timeout "^(?!potatoq\.)" '{"consumer-timeout": 18300000}' --apply-to quorum_queues
```

A time limit passed per call (`apply_async(time_limit=...)`) can't be known in advance:
keep those below the longest limit set on a task. If RabbitMQ does cancel a worker's
consumer, the worker logs it and reconnects.

### One app per vhost

The scheduler's leader token (`potatoq.scheduler.leader`), the delay exchanges
(`potatoq.delay.*`) and the dead-letter exchange are shared by every potatoq app on a
vhost. Two apps on one vhost means only one of them sends its periodic tasks, so give
each app (and each environment, like staging and production) its own vhost:
`amqp://user:pass@rabbit:5672/myapp-production`.

## Options

```python
app.conf.broker_transport_options = {
    "delivery_limit": 5,     # x-delivery-limit for new queues (default: task_max_deliveries)
}
```

[How it works in detail :octicons-arrow-right-24:](../design/internals.md#rabbitmq)
