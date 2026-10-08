# Settings

Set them on the app or load them from an object:

```python
app.conf.task_time_limit = 600
app.conf.update(task_default_queue="main", result_expires=3600)
app.config_from_object("myproj.settings")                         # module, object or dict
app.config_from_object("django.conf:settings", namespace="CELERY")
```

Names match Celery's lowercase settings. Uppercase Celery 3 names (`BROKER_URL`,
`CELERYD_CONCURRENCY`, …) and the `CELERY_` / `POTATOQ_` prefixes are translated
automatically. The [design notes](../design/defaults.md) explain each default.

## Broker and results

| Setting | Default | |
|---|---|---|
| `broker_url` | `$POTATOQ_BROKER_URL`, Django DB, then `sqlite:///potatoq.sqlite3` | |
| `broker_transport_options` | `{}` | Per-broker options ([Postgres](../brokers/postgres.md#options), [Redis](../brokers/redis.md#options), [RabbitMQ](../brokers/rabbitmq.md#options), [SQLite](../brokers/sqlite.md#options)) |
| `broker_connection_timeout` | `10` | Seconds; `delay()` fails after this instead of hanging |
| `result_backend` | `None` | `None` = the broker, when it's a database. `"broker"`, or any broker URL |
| `result_expires` | `86400` | Seconds results are kept |
| `database_auto_create_schema` | `True` | Create tables/queues on first use (`potatoq migrate` does it explicitly) |

## Tasks

| Setting | Default | |
|---|---|---|
| `task_default_queue` | `"default"` | |
| `task_default_priority` | `0` | Higher runs first |
| `task_routes` | `{}` | `{"billing.*": {"queue": "billing"}}`, a list of those, or a callable |
| `task_time_limit` | `1800` | Hard limit in seconds; `None` disables it |
| `task_soft_time_limit` | `None` | `None` = 30 s (or 10%) before the hard limit |
| `task_max_retries` | `3` | |
| `task_retry_backoff` | `10` | Exponential backoff base in seconds; `False` = `task_default_retry_delay` |
| `task_retry_backoff_max` | `600` | |
| `task_retry_jitter` | `True` | |
| `task_default_retry_delay` | `180` | Used when backoff is off |
| `task_max_deliveries` | `5` | Crashed deliveries before a task is dead-lettered |
| `task_reject_on_worker_lost` | `True` | Requeue tasks whose process died |
| `task_ignore_result` | `None` | `None` = store when free (database brokers) or when `result_backend` is set |
| `task_enqueue_on_commit` | `True` | Send `.delay()` on commit inside transactions |
| `task_dead_letter_failures` | `True` | Keep failed tasks in the dead-letter store |
| `dead_letter_max` | `10000` | |
| `dead_letter_ttl` | `2592000` | 30 days |
| `task_always_eager` | `False` | Run tasks in-process at `.delay()` (tests) |
| `task_eager_propagates` | `True` | |
| `task_store_eager_result` | `False` | |

## Worker

| Setting | Default | |
|---|---|---|
| `worker_concurrency` | `None` | Processes; `None` = CPUs available to the process |
| `worker_threads` | `1` | Task threads per process ([time-limit caveats](../guide/workers.md#threads)) |
| `worker_max_tasks_per_child` | `1000` | `None` disables recycling |
| `worker_max_memory_per_child` | `None` | `"512MB"`, or KiB as an int |
| `worker_import_urlconf` | `False` | Django: import `ROOT_URLCONF` when a worker starts, so tasks defined in views are registered ([details](../integrations/django.md#where-tasks-live)) |
| `imports` | `()` | Extra modules workers import at startup (`CELERY_IMPORTS`) |
| `worker_shutdown_timeout` | `25` | Seconds running tasks get on SIGTERM |
| `worker_heartbeat_interval` | `5` | |
| `worker_dead_after` | `60` | A silent node's tasks are recovered after this |
| `worker_enable_scheduler` | `True` | Run `beat_schedule` in this worker |
| `worker_hijack_root_logger` | `False` | |
| `worker_log_format`, `worker_task_log_format` | Celery's formats | Setting either replaces potatoq's [log format](../guide/workers.md#logging) with yours |
| `worker_log_color` | `None` | `None`: on for terminals; `NO_COLOR` and `FORCE_COLOR` are respected |
| `worker_log_emoji` | `None` | `None`: on unless `POTATOQ_NO_EMOJI` is set |

## Scheduling

| Setting | Default | |
|---|---|---|
| `beat_schedule` | `{}` | See [periodic tasks](../guide/periodic-tasks.md) |
| `timezone` | `"UTC"` | Django's `TIME_ZONE` when using the Django integration |

## Accepted for compatibility, not needed

`task_acks_late`, `worker_prefetch_multiplier`, `task_serializer`, `accept_content`,
`result_serializer`, `broker_connection_retry_on_startup`, `broker_pool_limit`,
`broker_heartbeat`, `worker_cancel_long_running_tasks_on_connection_loss`,
`task_acks_on_failure_or_timeout`, `beat_scheduler`, `task_queues`, `task_default_exchange`,
`task_default_routing_key`, `result_extended`, `worker_send_task_events` and the rest of
Celery's transport tuning. They are read without error and change nothing.
