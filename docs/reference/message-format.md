# Message format

Every potatoq task message is one JSON object. Use this page to send tasks from code that
isn't Python (a Go service, a Lambda in another language, a database trigger). From
Python, use potatoq itself: `app.send_task("name", args=[...])` works without importing
the task.

Celery messages aren't understood: a Celery-protocol message on a potatoq queue is
dead-lettered with a reason saying so, and `potatoq dead list` shows its task name and id.

## Fields

```json
{"v": 1, "task": "scans.finished", "id": "5b1f7c1e-0a2d-4c47-9a3e-1f0c2b7e9d10",
 "queue": "default", "args": [42], "kwargs": {"status": "ok"}, "enqueued_at": 1791459612.5}
```

| Field | Type | Required | Meaning |
|---|---|---|---|
| `task` | string | yes | The registered task name, e.g. `scans.tasks.finished` |
| `id` | string | yes | Unique per task (a UUID). Workers use it to recognise redeliveries and revocations |
| `v` | integer | | Protocol version, `1`. A worker rejects versions newer than it understands |
| `args` | array | | Positional arguments (default `[]`) |
| `kwargs` | object | | Keyword arguments (default `{}`) |
| `queue` | string | | The queue it's on (default `"default"`) |
| `priority` | integer | | Higher runs first (default `0`) |
| `expires` | number | | Unix timestamp after which the task is discarded instead of run |
| `headers` | object | | Free-form; available as `self.request.headers` |
| `enqueued_at` | number | | Unix timestamp |

Fields you don't know about can be left out: workers fill in defaults, and they ignore
fields they don't know, so future versions can add optional fields without breaking
your producers. `retries`, `eta`, `root_id`, `parent_id`, `group_id`, `group_index`,
`options`, `link`, `link_error`, `chord` and `ignore_result` are used by potatoq's own
retries and workflows; leave them out.

### Argument values

Arguments are plain JSON. A few Python types use a tagged object, which you can send
too:

| Python type | JSON |
|---|---|
| `datetime`, `date`, `time` | `{"__potatoq__": "datetime", "v": "2026-10-09T10:00:00+00:00"}` (ISO 8601) |
| `timedelta` | `{"__potatoq__": "timedelta", "v": 90.0}` (seconds) |
| `UUID` | `{"__potatoq__": "uuid", "v": "5b1f7c1e-…"}` |
| `Decimal` | `{"__potatoq__": "decimal", "v": "19.99"}` |
| `bytes` | `{"__potatoq__": "bytes", "v": "aGVsbG8="}` (base64) |
| `set` | `{"__potatoq__": "set", "v": [1, 2]}` |

## Sending it

=== "RabbitMQ"

    Publish the JSON to the default exchange (`""`) with the queue name as routing key:

    | Property | Value |
    |---|---|
    | `content_type` | `application/json` |
    | `delivery_mode` | `2` (persistent) |
    | `message_id` | the message's `id` |
    | `priority` | the message's `priority`, 0–31 |

    Turn on publisher confirms and publish with `mandatory`, so a missing queue is an
    error rather than a lost task. potatoq workers declare the queues they consume
    (quorum queues with a dead-letter queue) when they start, and `potatoq migrate`
    declares the default queue: do either before the first publish.

    Delays (`eta`) aren't supported from other producers on RabbitMQ: send the task
    when it's due.

=== "Postgres"

    Insert a row, in your own transaction if you like, then wake the workers:

    ```sql
    INSERT INTO potatoq_jobs (id, queue, task, state, priority, run_at, payload)
    VALUES ('5b1f7c1e-…', 'default', 'scans.finished', 1, 0, now(), '{"v": 1, "task": …}');
    SELECT pg_notify('potatoq:default', '');
    ```

    `state` is `1` (ready), or `0` (scheduled) with a future `run_at` for a delayed task.
    Without the `pg_notify`, workers still find the task within their poll interval
    (1 s). The table name follows the `schema` and `table_prefix` options.

=== "SQLite"

    ```sql
    INSERT INTO potatoq_jobs (id, queue, task, state, priority, run_at, created_at, payload)
    VALUES ('5b1f7c1e-…', 'default', 'scans.finished', 1, 0, unixepoch('subsec'), unixepoch('subsec'), '{…}');
    ```

    `run_at` and `created_at` are Unix timestamps. Use `BEGIN IMMEDIATE` and a busy
    timeout, like potatoq does. Workers notice the write by themselves.

=== "Redis"

    Redis queues are sorted sets and hashes maintained by Lua scripts, and they aren't a
    stable interface yet. Send from Python, or use another broker for producers in other
    languages.
