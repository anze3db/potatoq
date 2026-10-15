# Migrating to potatoq

Step-by-step guides:

- **[From Celery](migrating-from-celery.md)**: mostly a find-and-replace. The API,
  settings and Django integration are the same; the defaults are safer.
- **[From Dramatiq](migrating-from-dramatiq.md)**: actors become tasks, milliseconds
  become seconds, and retries become opt-in.

## Switching over

potatoq uses its own message format and its own queues, tables and keys, so it never
reads another queue's messages and can run next to it during the switch:

1. Deploy potatoq workers next to your old workers.
2. Deploy the producers (web/app code) using potatoq. New tasks go to potatoq's queues.
   Stop the old scheduler (`celery beat`, APScheduler, cron) in the same deploy:
   potatoq's workers run the schedule.
3. When the old queues are empty, stop the old workers and remove the old library.
   "Empty" includes delayed tasks: Celery workers hold eta/countdown tasks
   unacknowledged until they're due, and Dramatiq keeps them in separate `<queue>.DQ`
   queues.

If a deploy replaces the code and restarts everything at once (a single host, say), the
old workers can't keep running: deploy, then move what's left in the old queues into
potatoq's. The Celery guide has
[the commands to check the queues and scripts to move leftover tasks](migrating-from-celery.md#4-deploy-new-queues-then-drain-the-old-ones),
for Redis and RabbitMQ.

On RabbitMQ, potatoq needs quorum queues and refuses a queue name that an existing
classic queue already uses. Pick new queue names rather than reusing the old ones.

## What's different from any other queue

These defaults catch people coming from every library:

!!! warning "Retries are opt-in"
    A task that raises is recorded as failed and dead-lettered; it's **not retried**
    unless you ask with `autoretry_for` or `self.retry()`. Celery behaves the same way,
    but Dramatiq and Sidekiq retry every exception by default: set
    `task_autoretry_for = (Exception,)` to
    [retry every task](guide/retries-and-failures.md#retrying-every-task).

- **At-least-once delivery.** A task is acknowledged after it finishes, so a crash or a
  deploy never loses it, and it occasionally runs twice: make tasks idempotent.
- **JSON only.** Pass ids and plain data, not model instances. `.delay()` names the
  argument it can't send.
- **Enqueued on commit.** `.delay()` inside a database transaction sends the task when
  the transaction commits and drops it on rollback. Remove your `on_commit` wrappers.
  Eager mode (`task_always_eager`) follows the same rule, so development with eager
  mode on runs the task after COMMIT, as production does
  ([details](guide/testing.md#eager-mode-and-transactions)).
- **A 30-minute time limit** on every task. Raise it for long tasks:
  `@shared_task(time_limit=4 * 3600)`.
- **Failed tasks are kept** in a dead-letter store: `potatoq dead list`, then
  `potatoq dead retry <id>` to replay one.
- **Tests run tasks with [`drain`](guide/testing.md)**, through the same code path as a
  worker, skipping ahead to countdowns and retries.
