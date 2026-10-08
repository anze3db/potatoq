---
title: potatoq
hide:
  - navigation
  - toc
---

<div class="pq-hero" markdown>

<img class="pq-logo" src="assets/images/logo.svg" alt="">

# potatoq

<p class="pq-tagline">The Python task queue with the defaults Celery should have had. Same API, every hard production lesson already applied, and each broker used the way it was meant to be.</p>

[Get started](getting-started/quickstart.md){ .md-button .md-button--primary }
[Migrating from Celery](migrating-from-celery.md){ .md-button }

<div class="pq-install">uv add potatoq</div>

</div>

!!! warning "Alpha software"
    potatoq is new and hasn't run real production workloads yet. Use it in production at
    your own risk for now; APIs and defaults may still change between releases.

```python
from potatoq import Potatoq          # or: from potatoq import Celery

app = Potatoq("proj")                # broker: $POTATOQ_BROKER_URL, your Django DB, or ./potatoq.sqlite3

@app.task
def send_receipt(order_id):
    ...

send_receipt.delay(42)
```

```console
$ potatoq -A proj worker
```

<div class="grid cards" markdown>

-   :lucide-shield-check:{ .lg .middle } **Tasks don't get lost**

    ---

    Acks happen after the task finishes. A crashed process's task is requeued, and
    poison tasks are dead-lettered after 5 crashes. Failures land in a dead-letter
    store you can replay.

-   :lucide-zap:{ .lg .middle } **No head-of-line blocking**

    ---

    Each idle process takes exactly one task. ETAs and retries wait in the broker,
    not in worker memory, so long tasks are never redelivered.

-   :lucide-database:{ .lg .middle } **Your database is the queue**

    ---

    Postgres and SQLite are first-class brokers. Tasks are written inside your
    transaction: no `DoesNotExist` races, and no lost tasks on rollback.

-   :lucide-layers:{ .lg .middle } **Native on every broker**

    ---

    `SKIP LOCKED` and `LISTEN/NOTIFY` on Postgres, Lua leases on Redis, quorum queues
    and confirms on RabbitMQ, WAL and `data_version` on SQLite.

-   :lucide-clock:{ .lg .middle } **No beat to babysit**

    ---

    Every worker runs the scheduler. Each run is claimed exactly once through the
    broker, so you can't run zero schedulers or two.

-   :lucide-arrow-right-left:{ .lg .middle } **A find-and-replace migration**

    ---

    `@shared_task`, `delay`, `apply_async`, `chain`/`group`/`chord`, `beat_schedule`,
    signals and `CELERY_*` settings all keep working.

</div>

## Defaults, side by side

| | Celery | potatoq |
|---|---|---|
| Acknowledgement | before running: <span class="pq-bad">a crash loses the task</span> | <span class="pq-good">after it finishes</span> |
| Worker process killed (OOM) | <span class="pq-bad">task lost</span> | <span class="pq-good">requeued; dead-lettered after 5 crashes</span> |
| Prefetch | 4 × concurrency | <span class="pq-good">one task per idle process</span> |
| ETA / countdown | held in worker RAM | <span class="pq-good">stored by the broker</span> |
| Long tasks on Redis | <span class="pq-bad">re-run every hour</span> | <span class="pq-good">leases renewed while running</span> |
| Time limits | none | <span class="pq-good">30 min, soft 30 s earlier</span> |
| Failed tasks | discarded | <span class="pq-good">dead-letter store, replayable</span> |
| `.delay()` inside `atomic()` | sent immediately | <span class="pq-good">sent on commit</span> |
| Scheduler | separate `beat` process | <span class="pq-good">built into every worker, deduplicated</span> |
| `async def` tasks | unsupported | <span class="pq-good">supported</span> |

[Every default, with the reasoning and sources :octicons-arrow-right-24:](design/defaults.md)

## Fast where it counts

5000 no-op tasks, 4 worker processes, every broker on localhost:

| Broker | potatoq | Celery 5.6 |
|---|---:|---:|
| Redis | **11,400 tasks/s** | 2,700 tasks/s |
| Postgres | **7,000 tasks/s** | — |
| RabbitMQ | 6,300 tasks/s ¹ | 8,200 tasks/s ¹ |
| SQLite | **5,700 tasks/s** | — |

¹ On RabbitMQ, Celery acks before running the task and publishes without waiting for
confirms. potatoq acks after the task finishes and waits for every publish to be
confirmed.

---

<small>potatoq was generated with **Claude Opus 5.5** (high effort): the research, design, code, tests and docs.
Linux and macOS only. MIT licensed.</small>
