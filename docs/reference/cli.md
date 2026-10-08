# Command line

```console
$ potatoq [-A APP] [-b BROKER_URL] [--result-backend URL] [--workdir DIR] COMMAND
```

`-A` accepts `proj`, `proj.module` or `proj.module:attribute`. It finds an `app`,
`potatoq` or `celery` attribute, or a `potatoq.py`/`celery.py` submodule. With
`DJANGO_SETTINGS_MODULE` set, `-A` is optional. Every command is also available as
`python manage.py potatoq …` and `python -m potatoq …`.

| Command | |
|---|---|
| `worker` | Start a worker ([flags](../guide/workers.md)) |
| `beat` | Run only the scheduler (optional: workers already schedule) |
| `schedule` | Periodic tasks and their next run, in the app's timezone |
| `status [--json]` | Live workers, their queues and running tasks |
| `inspect active\|registered\|stats\|ping\|active_queues` | Celery-style inspect, from heartbeats. `scheduled` and `reserved` exist for Celery scripts and are always empty: workers don't hold tasks in memory |
| `queues [--json]` | Waiting tasks per queue |
| `dead list [--limit N] [--json]` | Dead-lettered tasks and why they died |
| `dead retry TASK_ID…` | Put dead-lettered tasks back on their queue |
| `call NAME [-a JSON_ARGS] [-k JSON_KWARGS] [--countdown S] [-Q QUEUE]` | Enqueue by name |
| `result TASK_ID [--wait SECONDS]` | Show a task's state and result |
| `revoke TASK_ID…` | Stop waiting tasks from running |
| `purge [-Q QUEUES] [-f]` | Delete waiting tasks |
| `migrate` | Create tables / queues now |
| `shell` | Python shell with the app and its tasks loaded |
