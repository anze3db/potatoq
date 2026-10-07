# Celery: bad defaults, community pain points, and what Potatoq should do instead

Research date: 2026-10-07. Reference version: **Celery 5.6.3 / Kombu 5.6.2 / Billiard 4.3.1** (latest on PyPI at time of writing). All defaults below were verified against the source (`celery/app/defaults.py`, `kombu/transport/redis.py`, `celery/app/task.py`, `celery/app/autoretry.py`, `billiard/context.py`) and the docs shipped in the sdist, not just blog posts.

Conventions:
- **Celery default** = what you get with zero config.
- **Why it's bad** = evidence with sources.
- **Potatoq default** = recommended default for a new library that aims to be drop-in compatible at the API level.

---

## 0. TL;DR: recommended defaults

| # | Area | Celery default | Potatoq recommended default |
|---|------|----------------|-----------------------------|
| 1 | Ack timing | `task_acks_late=False` (ack *before* run, so a crash loses the task) | **Ack after completion** (at-least-once). Tasks are documented as needing to be idempotent. |
| 2 | Worker/child killed mid-task | `task_reject_on_worker_lost=False`, so the message is acked even with acks_late | **Requeue on worker loss**, with a **redelivery cap** (`max_deliveries=5`). After that the task goes to the dead-letter store, so you don't get poison-message loops. |
| 3 | Redelivery of unacked work (Redis/SQL) | `visibility_timeout=3600` fixed; ETA and long tasks redeliver in a loop | **Lease plus heartbeat**: short lease (30s) that the worker renews while the task runs. A dead worker's tasks come back in about 30s, and a running task is never redelivered. |
| 4 | Prefetch | `worker_prefetch_multiplier=4` | **1** (reserve only what you can run). Throughput users can opt into more. |
| 5 | Child scheduling | `-Ofair` (default since 4.0; the "use -O fair" advice is stale) | Fair (only dispatch to idle children). |
| 6 | ETA/countdown | Held in **worker RAM**, unacked, with prefetch bumped without limit; `worker_eta_task_limit=None` | **Stored on the broker side** (Redis ZSET, SQL `run_at` column, RabbitMQ delayed queues). Workers never hold future tasks. |
| 7 | Time limits | `task_time_limit=None`, `task_soft_time_limit=None` | **Hard 30 min, soft = hard − 30s**, overridable per task. Log loudly when a limit fires. |
| 8 | Concurrency | `os.cpu_count()` (host cores, ignores cgroup quota) | **cgroup/affinity-aware CPU count** (`os.process_cpu_count()` + `cpu.max`), logged at startup. |
| 9 | Child recycling | `max_tasks_per_child=None`, `max_memory_per_child=None` | `max_tasks_per_child=1000`; `max_memory_per_child` opt-in with human units (`"512MB"`). **Always log recycles.** |
| 10 | Results | No backend; once one is configured, `ignore_result=False` stores everything for 1 day | **`ignore_result=True` by default.** Opt in per task. Store automatically when a canvas needs it (chord header). `.get()` on an ignored task **raises immediately** instead of hanging forever in PENDING. |
| 11 | Unknown task id | `AsyncResult("anything").state == "PENDING"` | Separate **`UNKNOWN`** state, plus `QUEUED`/`STARTED` (track_started on when results are on). |
| 12 | Result TTL | `result_expires=1 day` (DB backends need beat to clean up) | 1 day, enforced natively (Redis TTL, SQL sweeper inside the worker, no beat needed). |
| 13 | Retries | `max_retries=3`, `default_retry_delay=180s`, `retry_backoff=False`, `retry_jitter=True`, `retry_backoff_max=600` | Keep `max_retries=3`. **Exponential backoff with full jitter on by default** for both `autoretry_for` and bare `self.retry()` (base 1s → cap 10 min). After the final failure the task goes to the **dead-letter store**. |
| 14 | Failure acking | `task_acks_on_failure_or_timeout=True` (failed tasks just vanish) | Ack, **and record in a DLQ/failed-task table** that can be inspected and replayed. |
| 15 | Connection loss with acks_late | `worker_cancel_long_running_tasks_on_connection_loss=False` (Celery 6 plans to flip it) | **Cancel** (or lease-fence) tasks whose lease or channel is gone, so they don't run twice. |
| 16 | Shutdown | Warm shutdown waits forever; `worker_soft_shutdown_timeout=0` | On SIGTERM: stop fetching, wait `shutdown_timeout=25s` (fits K8s' 30s grace), then **nack/requeue** unfinished tasks immediately and exit. |
| 17 | Publish reliability (RabbitMQ) | `confirm_publish=False`, so publishes can be lost silently | **Publisher confirms on.** |
| 18 | Publish when broker is down | Retry policy of 3 tries, but `.delay()` has been reported to block forever (#4296) | **Bounded publish timeout** (5–10s total), then raise `PublishError`. |
| 19 | Startup connection | `broker_connection_retry_on_startup=None` → deprecation-warning dance | Retry on startup with exponential backoff, log every attempt, no deprecation noise. |
| 20 | Gossip/mingle/heartbeat | All on | **Off** (no gossip, no mingle, no event heartbeats unless events are enabled). Liveness via a health file or endpoint. |
| 21 | Events | `worker_send_task_events=False`, `task_send_sent_event=False` | Off by default. One `events=True` switch turns on both (and works with Flower-compatible tooling). |
| 22 | Logging | `worker_hijack_root_logger=True` | **Don't touch the root logger** if it already has handlers. Add a handler only if none is configured. |
| 23 | Serializer | JSON (since 4.0; pickle before) | JSON only, with datetime/UUID/Decimal/Pydantic support. **Eager/test mode still round-trips through the serializer.** |
| 24 | Task names | Auto-generated from `module.qualname` | Same algorithm (for compatibility), plus **warnings when a name changes between deploys**, plus `potatoq check` to detect unregistered/duplicate names. Encourage explicit `name=`. |
| 25 | Default queue | `"celery"` | `"celery"` if wire compatibility with Celery workers is a goal, otherwise `"default"` (matches Django 6 Tasks). |
| 26 | Priority | Redis: 0 = highest (emulated, 4 buckets, needs `queue_order_strategy`). RabbitMQ: higher = higher | **One semantic on every broker: higher number = higher priority** (Django Tasks: −100..100). Works without extra config. |
| 27 | Rate limits | Per worker only | **Global (broker-backed) token bucket** by default. Per-worker available as an option. |
| 28 | Beat | Single instance, no lock; state in a local `shelve` file | **Built-in leader lock** (Redis lock, PG advisory lock, SQLite file lock, RabbitMQ single-active-consumer). Schedule state lives in the broker/DB. Safe to run N replicas or `worker --beat`. |
| 29 | Timezone | `enable_utc=True`, `timezone=UTC`; Django `TIME_ZONE` ignored | UTC internally. Crontab evaluated in `timezone` (default UTC); per-schedule tz allowed. Warn if Django `TIME_ZONE` ≠ configured tz. |
| 30 | Django transactions | `.delay()` publishes immediately; `delay_on_commit` is opt-in (5.4+) | **Enqueue on commit by default inside `atomic()` blocks** (configurable), with `.delay_on_commit` kept as an alias. |
| 31 | Redis key namespace | No prefix (`global_keyprefix=''`); visibility settings shared across apps | **Prefix every key** with the app name by default. Warn at startup if `maxmemory-policy` isn't `noeviction`. |
| 32 | RabbitMQ queue type | Classic | **Quorum** (durable, replicated, built-in delivery-limit=20 → DLX). Classic is opt-in. |
| 33 | Pool limit / heartbeats | `broker_pool_limit=10`, `broker_heartbeat=120` | Keep 10, but fork-safe (reset pools in children). AMQP heartbeat 60s. |
| 34 | asyncio | Not supported (most-upvoted issue, #6552) | **Native `async def` tasks** and `await task.aenqueue()` / `await result.aget()`. |

---

## 1. Methodology and primary sources

- Celery 5.6.3 source and docs (downloaded from PyPI and grepped).
- Celery docs: [Configuration](https://docs.celeryq.dev/en/stable/userguide/configuration.html), [Tasks](https://docs.celeryq.dev/en/stable/userguide/tasks.html), [Calling](https://docs.celeryq.dev/en/stable/userguide/calling.html), [Optimizing](https://docs.celeryq.dev/en/stable/userguide/optimizing.html), [Redis broker](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html), [RabbitMQ broker](https://docs.celeryq.dev/en/main/getting-started/backends-and-brokers/rabbitmq.html), [Routing](https://docs.celeryq.dev/en/stable/userguide/routing.html), [FAQ](https://docs.celeryq.dev/en/stable/faq.html), [What's new 4.0](https://docs.celeryq.dev/en/stable/history/whatsnew-4.0.html), [What's new 5.5](https://docs.celeryq.dev/en/stable/history/whatsnew-5.5.html).
- Practitioner posts: Adam Johnson, [Common Issues Using Celery](https://adamj.eu/tech/2020/02/03/common-celery-issues-on-django-projects/) and [Working around memory leaks](https://adamj.eu/tech/2019/09/19/working-around-memory-leaks-in-your-django-app/). Deni Bertovic, [Celery best practices](https://denibertovic.com/posts/celery-best-practices/) ([HN, 174 pts](https://news.ycombinator.com/item?id=7909201)). Vinta, [Celery in the wild](https://www.vintasoftware.com/blog/2018/celery-wild-tips-and-tricks-run-async-tasks-real-world) and [celery-tasks-checklist](https://github.com/vintasoftware/celery-tasks-checklist). Steve Dignam, [The Many Problems with Celery](https://steve.dignam.xyz/2023/05/20/many-problems-with-celery/) ([HN](https://news.ycombinator.com/item?id=36021877)). Ayush Shanker, [Celery in production: three more years of fixing bugs](https://ayushshanker.com/posts/celery-in-production-bugfixes) ([HN, 109 pts, 62 comments](https://news.ycombinator.com/item?id=30567986)). Hatchet, [The problems with Celery](https://hatchet.run/blog/problems-with-celery). CloudAMQP, [Celery & RabbitMQ: mingling, gossip, heartbeats](https://www.cloudamqp.com/blog/python-celery-and-rabbitmq.html) and [CloudAMQP Celery docs](https://www.cloudamqp.com/docs/celery.html). Caktus, [Celery in production](https://www.caktusgroup.com/blog/2014/09/29/celery-production/). Wiredcraft, [3 gotchas for Celery](https://wiredcraft.com/blog/3-gotchas-for-celery/). Instawork, [Celery ETA tasks demystified](https://engineering.instawork.com/celery-eta-tasks-demystified-424b836e4e94). Merge.dev, [Long-running tasks with Celery and Kubernetes](https://www.merge.dev/blog/managing-long-running-tasks-with-celery-and-kubernetes-or-keeping-your-sanity-during-deploys). Lycore, [Running Celery in production](https://dev.to/lycore/running-celery-in-production-what-we-do-differently-after-years-of-real-projects-3en5). [Celery loses 8% of your tasks by default](https://dev.to/akoladefaj/celery-loses-8-of-your-tasks-by-default-heres-the-reliability-layer-i-built-to-fix-that-40mc). Instagram, [PyCon 2012 "Messaging at scale" notes](https://mark-ransom-pycon-2012-notes.readthedocs.io/en/latest/friday/session_1.html). GOV.UK Notify, [Upgrading Celery](https://technology.blog.gov.uk/2022/02/01/upgrading-celery-on-gov-uk-notify). Migration stories: [Cloudraft → Argo](https://www.cloudraft.io/blog/migrating-celery-argo-workflows), [AmpUp → Inngest](https://www.ampup.ai/blog/eng-stack-inngest), [Celery → RQ (Sylvain Zimmer)](https://talks.sylvainzimmer.com/2013-parispy/slides.pdf), [Dramatiq motivation](https://dramatiq.io/motivation.html).
- Community: HN threads (via Algolia), the [Dramatiq Show HN](https://news.ycombinator.com/item?id=15681066), and the [Hatchet Launch HN](https://hn.svelte.dev/item/40810986). Reddit and StackOverflow block automated fetches from this environment, so their content is cited only indirectly through search snippets.
- GitHub: the most-reacted [celery/celery issues](https://github.com/celery/celery/issues?q=is%3Aissue+sort%3Areactions-%2B1-desc) (list in §5).

---

## 2. Pain points in detail

### A. Delivery semantics and reliability

#### A1. Early acknowledgement (`task_acks_late=False`)
- **Celery default:** the message is acked when the worker *receives/starts* it, before the task body runs. A crash, OOM, deploy, or `kill -9` loses the task permanently.
- **Why it's bad:**
  - Adam Johnson: the default "acks early" is counter-intuitive and the *opposite* of what most queues do, such as SQS. If interrupted, "Celery won't retry the task". He recommends `acks_late = True` globally. ([adamj.eu](https://adamj.eu/tech/2020/02/03/common-celery-issues-on-django-projects/))
  - Hatchet lists it under "questionable defaults": "if the worker crashes halfway through a task execution… the task won't be retried". ([hatchet.run](https://hatchet.run/blog/problems-with-celery))
  - Steve Dignam: "If a task raises an exception, or a worker process dies, Celery will by default lose the job". Fix: `task_acks_late` + `task_reject_on_worker_lost`. ([steve.dignam.xyz](https://steve.dignam.xyz/2023/05/20/many-problems-with-celery/))
  - Vinta: "enabling the `task_acks_late` setting won't harm your application. It will enhance its robustness." ([vintasoftware.com](https://www.vintasoftware.com/blog/2018/celery-wild-tips-and-tricks-run-async-tasks-real-world))
  - A SIGKILL benchmark (5 workers × prefork 4, Redis) measured **92% delivery** with defaults and 96% with `acks_late` (the rest was stuck behind the 1h visibility timeout). ([dev.to](https://dev.to/akoladefaj/celery-loses-8-of-your-tasks-by-default-heres-the-reliability-layer-i-built-to-fix-that-40mc))
  - AmpUp: "if a worker process dies mid-task, that unit of work is gone unless you built your own idempotency layer". This was a reason they migrated off Celery. ([ampup.ai](https://www.ampup.ai/blog/eng-stack-inngest))
  - Lycore's production config sets `task_acks_late=True` + `task_reject_on_worker_lost=True`. ([dev.to/lycore](https://dev.to/lycore/running-celery-in-production-what-we-do-differently-after-years-of-real-projects-3en5))
- **Potatoq default:** **late ack (at-least-once)**. Document idempotency prominently. Allow `acks_late=False` per task for non-idempotent side effects ("at-most-once"), as a deliberate choice.

#### A2. Acks even with `acks_late` when the child dies (`task_reject_on_worker_lost=False`)
- **Celery default:** "Even if `task_acks_late` is enabled, the worker will acknowledge tasks when the worker process executing them abruptly exits or is signaled." Turning on `task_reject_on_worker_lost` "can cause message loops; make sure you know what you're doing." (config docs). The Tasks guide explains that this is deliberate, so that segfaulting or OOM-ing tasks don't loop forever.
- **Why it's bad:** most users believe `acks_late` protects against OOM-kill. It does not ([celery#6178](https://github.com/celery/celery/issues/6178) documents the contradiction between the FAQ and the docs). OOM is the *most common* way a prefork child dies, so the protection is missing in exactly the case it's needed.
- **Potatoq default:** **requeue on worker loss**, with a **per-message delivery counter** (`max_deliveries`, default 5). When exceeded, move the task to the dead-letter store with reason `worker_lost`. This removes the "message loop" excuse. RabbitMQ quorum queues already implement this natively (delivery-limit default 20 in RabbitMQ 4.0, [rabbitmq.com](https://www.rabbitmq.com/blog/2024/08/28/quorum-queues-in-4.0)).

#### A3. Failed tasks are acked and disappear (`task_acks_on_failure_or_timeout=True`); no dead-lettering
- **Celery default:** failures and timeouts are acked. Celery has "no first-class support for dead-lettering" ([Hatchet](https://hatchet.run/blog/problems-with-celery)), so you must use broker-specific DLX features.
- **Potatoq default:** ack failures (don't redeliver plain Python exceptions; that's what retries are for), and **always write terminal failures to a failed-task store**. Store task name, args, exception, traceback, attempts, and timestamps, and provide `potatoq failed list/retry/purge`.

#### A4. Redis `visibility_timeout = 3600` (1 hour)
- **Celery/Kombu default:** `visibility_timeout = 3600` (`kombu/transport/redis.py`).
- **Why it's bad:**
  - "If a task isn't acknowledged within the visibility timeout the task will be redelivered… This causes problems with ETA/countdown/retry tasks where the time to execute exceeds the visibility timeout; **in fact if that happens it will be executed again, and again in a loop**." ([Celery Redis docs](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html#caveats))
  - Long-running tasks with acks_late on Redis get duplicated every hour: [celery#5935](https://github.com/celery/celery/issues/5935) (open), [#3430 "Long tasks are executed multiple times"](https://github.com/celery/celery/issues/3430) (open), [#4400 "Same task runs multiple times at once?"](https://github.com/celery/celery/issues/4400) (open, ETA tasks), [#6229](https://github.com/celery/celery/issues/6229).
  - The fix requires setting **three** settings to the same value (`broker_transport_options`, `result_backend_transport_options`, `visibility_timeout`). And "if multiple applications are sharing the same Broker, with different settings, the *shortest* value will be used." (docs)
  - The opposite problem: raising it delays recovery of genuinely lost tasks by hours. "having a long visibility timeout will only delay the redelivery of 'lost' tasks in the event of a power failure or forcefully terminated workers." (docs; also the [8% benchmark](https://dev.to/akoladefaj/celery-loses-8-of-your-tasks-by-default-heres-the-reliability-layer-i-built-to-fix-that-40mc), where acks_late plus a 1h timeout "traded silent loss for hour-long redelivery latency").
  - The same class of problem exists on SQS ([celery#8875](https://github.com/celery/celery/issues/8875)).
- **Potatoq default:** **lease + heartbeat**, never a fixed visibility timeout.
  - Redis: Streams consumer groups (`XAUTOCLAIM` with min-idle ≈ lease), or a ZSET of `lease_expires_at` that the worker renews every `lease/3` seconds.
  - Postgres: `SELECT … FOR UPDATE SKIP LOCKED` + `locked_until` column renewed by heartbeat.
  - SQLite: same with `BEGIN IMMEDIATE`.
  - Default lease 30s. A running task is never redelivered. A dead worker's task is recovered in about 30s. ETA tasks never sit in the unacked set (see C1).

#### A5. RabbitMQ `consumer_timeout` (30 min) vs ETA and long acks_late tasks
- **Behavior:** since RabbitMQ 3.8.15, unacked deliveries older than `consumer_timeout` (15 min, then 30 min from 3.8.17) close the channel with `PRECONDITION_FAILED`. Celery holds ETA tasks unacked in the worker, so any countdown over 30 min kills the channel. Celery's own docs suggest setting `consumer_timeout = 31622400000` (1 year) in `rabbitmq.conf`. ([Celery calling docs](https://docs.celeryq.dev/en/stable/userguide/calling.html#eta-and-countdown); [Django forum](https://forum.djangoproject.com/t/preconditionfailed-with-scheduled-celery-task/19879); [OpenedX](https://discuss.openedx.org/t/rabbitmq-config-compute-all-grades-for-course-eta-are-unable-to-work-together-causing-unrecoverable-error-on-celery/10774)). Long-running acks_late tasks (over 30 min) hit the same wall.
- **Potatoq default:** never hold ETA messages on a consumer. Use delayed queues: a TTL+DLX ladder like Celery 5.5's "native delayed delivery", or the delayed-message exchange. For tasks longer than `consumer_timeout`, document it, set the per-queue `x-consumer-timeout` argument (RabbitMQ ≥3.12), and warn at startup if `time_limit > consumer_timeout`.

#### A6. Connection loss while running acks_late tasks
- **Celery default:** `worker_cancel_long_running_tasks_on_connection_loss=False`. Docs: tasks that can no longer be acked are redelivered, so "the task is being executed twice per connection loss (and sometimes in parallel in other workers)". "In Celery 6.0 [it] will be set to True by default as the current behavior leads to more problems than it solves."
- **Potatoq default:** fencing. If the lease or channel is lost, the task is cancelled (cooperatively, then hard) and its result is discarded. On by default.

#### A7. Silent publish loss on RabbitMQ (`confirm_publish=False`)
- [celery#5410](https://github.com/celery/celery/issues/5410) (35 upvotes): when RabbitMQ hits a memory or disk alarm, "this results in silent message loss when invoking Celery tasks… Calls to `apply_async()` or `delay()` return immediately and without error even though the message was never published." Quorum queue docs in Celery now tell you to set `{"confirm_publish": True}`.
- **Potatoq default:** publisher confirms on (all brokers: Redis/SQL publishes are synchronous anyway).

#### A8. Publishing when the broker is down
- `task_publish_retry_policy = {max_retries: 3, interval_start: 0, interval_step: 0.2, interval_max: 1}`, but [celery#4296 "Delay and apply_async waiting forever when the broker is down"](https://github.com/celery/celery/issues/4296) (21 upvotes) shows the web request can hang anyway.
- **Potatoq default:** a hard total publish deadline (default 10s), then raise `PublishError`. Never block a web request indefinitely.

#### A9. Shutdown and deploys (SIGTERM in Kubernetes)
- K8s sends SIGTERM, waits `terminationGracePeriodSeconds` (30s), then SIGKILL. Celery's warm shutdown waits for running tasks with no timeout, so it gets SIGKILLed. Tasks in flight are then lost (early ack) or stuck until the visibility timeout (late ack). ([merge.dev](https://www.merge.dev/blog/managing-long-running-tasks-with-celery-and-kubernetes-or-keeping-your-sanity-during-deploys)) Celery 5.5 added "soft shutdown", but `worker_soft_shutdown_timeout=0.0`, i.e. **off**. Orphaned prefork children after parent death are a separate known problem ([Ayush Shanker](https://ayushshanker.com/posts/celery-in-production-bugfixes)). [celery#4079](https://github.com/celery/celery/issues/4079) (43 upvotes) asks how to do liveness/readiness probes at all.
- **Potatoq default:** SIGTERM → stop fetching → wait `shutdown_timeout=25s` → requeue (nack) unfinished late-ack tasks immediately → kill children (process group) → exit 0. Children die with the parent (`PR_SET_PDEATHSIG` on Linux). Ship a `potatoq worker --health-file` / HTTP `/healthz` for probes.

### B. Scheduling, fairness, concurrency

#### B1. `worker_prefetch_multiplier = 4`
- **Celery default:** each worker reserves `4 × concurrency` messages.
- **Why it's bad:**
  - Head-of-line blocking: short tasks wait behind long ones reserved by a busy worker while other workers sit idle. "the first worker which comes online might pull in all active tasks" ([Hatchet](https://hatchet.run/blog/problems-with-celery)). "By default, each Celery worker prefetches 4 jobs from the queue" ([Dignam](https://steve.dignam.xyz/2023/05/20/many-problems-with-celery/)). Wiredcraft gotcha #3, "Tasks queuing despite available workers" ([wiredcraft](https://wiredcraft.com/blog/3-gotchas-for-celery/)).
  - Celery's own docs: "the first worker to start will receive four times the number of messages initially. Thus the tasks may not be fairly distributed."
  - CloudAMQP: set `worker_prefetch_multiplier = 1`, "as it causes problems and doesn't help performance" ([CloudAMQP docs](https://www.cloudamqp.com/docs/celery.html)).
  - Prefetch also breaks priorities: "due to worker prefetching, if a bunch of tasks submitted at the same time they may be out of priority order" ([routing docs](https://docs.celeryq.dev/en/stable/userguide/routing.html)).
  - Even multiplier=1 with early ack reserves 2× concurrency ("10 acknowledged tasks executing, and 10 unacknowledged reserved tasks", [optimizing docs](https://docs.celeryq.dev/en/stable/userguide/optimizing.html)). Celery 5.6 added `worker_disable_prefetch` (Redis only) to fix this.
  - Footgun: `0` means *unlimited*, not disabled.
- **Potatoq default:** fetch only when a slot is free (effective multiplier 1 with late ack, so a worker holds exactly `concurrency` messages). Expose `prefetch=N` for high-throughput tiny tasks, and reject 0 as "unlimited" (use `None`).

#### B2. `-O fair` advice is stale
- Celery 4.0 made fair scheduling the default: "`-Ofair` is now the default scheduling strategy" ([whatsnew-4.0](https://docs.celeryq.dev/en/stable/history/whatsnew-4.0.html)). In 5.6.3 `SCHED_STRATEGIES = {None: FAIR, 'default': FAIR, 'fair': FAIR, 'fast'/'fcfs': FCFS}`. Adam Johnson's 2020 post still recommends `-O fair`. Today the remaining issue is *broker-level* prefetch (B1), not child dispatch.
- **Potatoq default:** fair. Accept `-O fair` as a no-op for CLI compatibility.

#### B3. Concurrency defaults to host CPU count
- **Celery default:** `worker_concurrency = billiard.cpu_count()` → `os.cpu_count()`, which reports **host** cores and ignores cgroup CPU quotas or affinity.
- **Why it's bad:** Ayush Shanker: instances stayed at 90–100% RAM after doubling resources, because "if you don't specify `--concurrency`… it defaults to using the number of cores available", so scaling up spawned more children ([post](https://ayushshanker.com/posts/celery-in-production-bugfixes)). In Kubernetes a 1-CPU pod on a 64-core node forks 64 children and OOMs. Defaults are per-queue/worker, so several workers multiply it.
- **Potatoq default:** `min(os.process_cpu_count(), ceil(cgroup cpu.max quota))`, at least 1. Print the chosen value and how it was derived at startup.

#### B4. One default queue named `celery`
- Everything goes to one queue unless you configure routing ([Deni Bertovic](https://denibertovic.com/posts/celery-best-practices/); [celery.school](https://celery.school/celery-task-routing); Instagram split into Fast/Feed/Default ([PyCon 2012 notes](https://mark-ransom-pycon-2012-notes.readthedocs.io/en/latest/friday/session_1.html)); Lycore uses critical/default/slow). No per-queue concurrency in one worker ([celery#1599](https://github.com/celery/celery/issues/1599), 50 upvotes, open).
- **Potatoq default:** keep a single default queue (simple), but make routing ergonomic: `@task(queue="emails")` is enough, with no `task_routes` boilerplate. Support `worker -Q high=4,default=8` (per-queue concurrency in one process). `task_create_missing_queues=True` stays.

#### B5. Priorities are inconsistent across brokers
- Redis: emulated with 4 buckets (`priority_steps`), "will never be as good as priorities implemented at the broker server level, and may be approximate at best", and only works if you set `queue_order_strategy='priority'` (Kombu default is `round_robin`). The highest priority is **0**. RabbitMQ classic queues: `x-max-priority`, where a **higher** number is higher priority. Quorum queues (RabbitMQ 4): only 2 levels (≥5 = high). [celery#4028 "Clarify support for task priority with redis"](https://github.com/celery/celery/issues/4028) is open.
- **Potatoq default:** one semantic everywhere, where **a higher number runs sooner**. Use Django Tasks' −100..100 range ([Django docs](https://docs.djangoproject.com/en/6.0/ref/tasks/)) and map it per broker. Priorities work with zero config on Redis/SQL (`ORDER BY priority DESC, id`). Provide a compatibility shim for Celery's Redis "0 = highest" if needed.

#### B6. Rate limits are per worker, not global
- "Note that this is a per worker instance rate limit, and not a global rate limit" (Celery docs). [celery#5732](https://github.com/celery/celery/issues/5732) is open. Hatchet: "impossible to set a global rate limit". HN: "Task throttling in Celery is really bad to the point of being mostly useless" ([HN](https://news.ycombinator.com/item?id=15681066)). Third-party fixes such as [celery-heimdall](https://pypi.org/project/celery-heimdall).
- **Potatoq default:** `rate_limit="10/m"` is **global**, implemented as a broker-backed token bucket (Redis Lua, SQL row). Per-worker is available via `rate_limit_scope="worker"`. Rate-limited tasks should *not* consume prefetch slots (see [discussion #7803](https://github.com/celery/celery/discussions/7803)).

### C. Delayed tasks (ETA/countdown) and retries

#### C1. ETA/countdown tasks live in worker memory
- **Celery default:** "Tasks with `eta` or `countdown` are immediately fetched by the worker and until the scheduled time passes, they reside in the worker's memory… tasks are not acknowledged until the worker starts executing them… using `eta` and `countdown` **is not recommended** for scheduling tasks for a distant future. Ideally, use values no longer than several minutes." ([calling docs](https://docs.celeryq.dev/en/stable/userguide/calling.html#eta-and-countdown))
- **Mechanics:** to keep consuming, the worker *increments the QoS prefetch count* for each ETA task it holds. With many far-future tasks it "will keep taking in more of these until it runs out of memory" ([Instawork](https://engineering.instawork.com/celery-eta-tasks-demystified-424b836e4e94)). `worker_eta_task_limit` (5.6) defaults to **None**. Adam Johnson: "With many such tasks, the Celery worker process will use a lot of memory", and restarts get slow ([adamj.eu](https://adamj.eu/tech/2020/02/03/common-celery-issues-on-django-projects/)). [celery#4522 "Celery not useful for long term future tasks"](https://github.com/celery/celery/issues/4522) is open. Interacts with A4 (Redis loop) and A5 (RabbitMQ channel kill). With quorum queues ETA "will block the worker" unless native delayed delivery is used ([RabbitMQ docs](https://docs.celeryq.dev/en/main/getting-started/backends-and-brokers/rabbitmq.html)). Cancelling scheduled tasks is unreliable ("reliably cancelling scheduled (future) tasks… persistent revokes has been unreliable", [HN](https://news.ycombinator.com/item?id=30567986)).
- **Potatoq default:** scheduled tasks are **stored on the broker side** until due: a Redis ZSET scored by `eta` and moved atomically by a Lua script, SQL `run_at` with an index, or RabbitMQ delayed queues. Workers only see due tasks. Cancellation (`revoke`) of a scheduled task just deletes the row or ZSET member, so it is reliable. No practical limit on horizon (months).

#### C2. Retry defaults
- **Celery defaults** (`celery/app/task.py`, `autoretry.py`): `max_retries=3`, `default_retry_delay=180` (3 min), `retry_backoff=False`, `retry_backoff_max=600`, `retry_jitter=True` (full jitter: delay is uniform in `[0, backoff]`, so it can be 0), `autoretry_for=()`.
- **Gotchas:**
  - Docs say that with `retry_backoff=False` "autoretries will not be delayed". The code actually falls through to `default_retry_delay=180s`.
  - Each retry is a *new ETA message*, so it inherits all of C1/A4/A5.
  - `retry()` raises, and code after it doesn't run (surprises newcomers).
  - Eager mode + `task_eager_propagates` → `retry()` always raises `RuntimeError` ([#4661](https://github.com/celery/celery/issues/4661)).
  - [#7091 autoretry fails when `expires` is set](https://github.com/celery/celery/issues/7091).
  - Dignam lists "poor retry defaults: no exponential backoff by default"; Vinta's checklist says to always use `retry_backoff` + jitter and set retry limits.
- **Potatoq default:** keep `max_retries=3` (compatible). Exponential backoff **on** for both `autoretry_for` and bare `self.retry()` without countdown: `delay = random(0.5, 1.0) × min(cap, base × 2^n)` with `base=1s`, `cap=600s` ("equal jitter", so the delay is never zero). An explicit `countdown=` always wins. After the final attempt the task goes to the DLQ (A3). Retries reuse the broker-side scheduler (C1).

#### C3. Idempotency, de-duplication, expiry
- `worker_deduplicate_successful_tasks=False` (and it only works with a persistent result backend). There is no unique/singleton task feature. `expires` has no default.
- **Potatoq default:** provide `@task(unique_for=…)` / `idempotency_key=` (broker-backed SETNX/unique index), and dedup of redelivered-but-already-SUCCEEDED tasks when results are stored. Keep `expires=None` globally, but support `Task.expires` per task (Celery-compatible) and the `expires=` arg.

### D. Time limits and resource hygiene

#### D1. No time limits by default
- **Celery default:** `task_time_limit=None`, `task_soft_time_limit=None`.
- **Why it's bad:** one hung HTTP call permanently occupies a slot. With Redis + acks_late, it also gets redelivered hourly (A4). Hatchet: "Tasks executing indefinitely cause queue overflow… Recommendation: implement 1-minute default timeout". Instagram used soft 20s / hard 30s ([PyCon 2012](https://mark-ransom-pycon-2012-notes.readthedocs.io/en/latest/friday/session_1.html)). The Vinta checklist says "Set hard and soft time limits". An HN commenter: "when you are using solo there should be a default hard task timeout smaller than effective heartbeat value otherwise it will fail and retry the same task forever" ([HN](https://news.ycombinator.com/item?id=30567986)). Workers silently hanging on IPC: [#4185](https://github.com/celery/celery/issues/4185) (28 upvotes, 123 comments).
- **Potatoq default:** `time_limit=1800s`, `soft_time_limit=time_limit−30s`. Both are per-task and global overridable, and `None` is allowed explicitly. Time-limit kills count as a delivery and go to retry/DLQ with a clear reason. Threads/async pools get cooperative cancellation for soft limits.

#### D2. Memory leaks / child recycling
- **Celery default:** `worker_max_tasks_per_child=None`, `worker_max_memory_per_child=None`.
- **Why it's bad:** Python's high-water-mark memory behavior plus leaky libraries. [celery#4843 "Continuous memory leak"](https://github.com/celery/celery/issues/4843) is open since 2018 with 178 comments and 82 upvotes (parent-process leak). Ayush Shanker saw 4–5× RAM growth after removing `--max-memory-per-child`. Adam Johnson recommends ~100 tasks per child for Celery ([adamj.eu](https://adamj.eu/tech/2019/09/19/working-around-memory-leaks-in-your-django-app/)). Lycore uses 1000 tasks / 200 MB. Recycling is silent ([#8916 "Add log when worker is rebooted with --max-tasks-per-child"](https://github.com/celery/celery/issues/8916), open). Recycling can cause duplicate execution ([#5120](https://github.com/celery/celery/issues/5120)).
- **Potatoq default:** `max_tasks_per_child=1000`, and `max_memory_per_child=None` but accepting `"512MB"`/`"80%"` (of cgroup memory ÷ concurrency). Log every recycle with the reason. Recycle only *between* tasks, and the result is reported before exit.

### E. Results

#### E1. Result storage defaults
- **Celery defaults:** no result backend (`DisabledBackend`). Once you configure one, `task_ignore_result=False` stores every task's result, `result_expires=1 day`, `result_extended=False`, `task_track_started=False`, `task_store_errors_even_if_ignored=False`.
- **Why it's bad:**
  - Most tasks are fire-and-forget. Storing results wastes Redis memory and DB writes: Deni Bertovic "Keep track of results only when necessary"; Caktus "Results that are not needed should be ignored"; Redis key growth of `celery-task-meta-*`.
  - With the DB backend, expiry needs `celery beat` running `celery.backend_cleanup`.
  - Calling `.get()` on a task that ignores results **hangs forever** in PENDING.
  - `AsyncResult('ANYTHING').state == 'PENDING'`: there is no way to tell unknown from queued ([celery#3596](https://github.com/celery/celery/issues/3596), 43 upvotes).
  - A running task shows PENDING unless `track_started` is enabled.
  - Chords break if header tasks ignore results ([gwcelery#511](https://git.ligo.org/emfollow/gwcelery/-/issues/511)).
- **Potatoq default:** `ignore_result=True` globally. Tasks opt in with `@task(ignore_result=False)` (or `store_result=True`). Canvas automatically forces storage for chord headers and chain links whose results are consumed. `.get()` on an ignored task raises `ResultsDisabled` immediately. States: `UNKNOWN`, `QUEUED`, `STARTED` (always tracked when storing), `RETRY`, `SUCCESS`, `FAILURE`, `REVOKED`. TTL of 1 day enforced natively (Redis `EXPIRE`, SQL sweeper run by any worker, no beat dependency). Extended metadata (name, args, queue, worker) on.

#### E2. Blocking on results inside tasks
- `result.get()` inside a task raises `RuntimeError("Never call result.get() within a task!")` by default (`disable_sync_subtasks=True`). This is a good default ("may even cause a deadlock if the worker pool is exhausted", Tasks docs; Vinta "Do not wait for other tasks inside a task").
- **Potatoq default:** keep this behavior and the error message, and point to chains/chords in the message.

### F. Serialization
- **History:** pickle was the default until 4.0. "The time has finally come to end the reign of pickle as the default serialization mechanism" ([whatsnew-4.0](https://docs.celeryq.dev/en/stable/history/whatsnew-4.0.html)). Current defaults: `task_serializer='json'`, `accept_content={'json'}`. Kombu's JSON handles datetime/UUID/Decimal. Pydantic arg validation arrived in 5.5.
- **Pain:**
  - Eager mode skips serialization, which hides bugs: "This hid a bunch of bugs in our application code" ([celery#4008](https://github.com/celery/celery/issues/4008)). Dignam: `task_always_eager` "skips serialization validation".
  - Passing ORM objects leads to stale data ([Deni Bertovic](https://denibertovic.com/posts/celery-best-practices/), [Adam Johnson](https://adamj.eu/tech/2020/02/03/common-celery-issues-on-django-projects/)).
  - Changing task signatures breaks queued messages: "treat task function signatures with the same consideration as database migrations" (Adam Johnson).
- **Potatoq default:** JSON only (no pickle without an explicit opt-in that also needs signing). **Validate serializability at enqueue time**, including in eager/test mode, which must round-trip through the serializer. Optional typed args via annotations/Pydantic. Raise a helpful error when a Django model instance is passed ("pass `obj.pk` instead").

### G. Periodic tasks (beat)
- **Celery defaults:** `beat_scheduler='celery.beat:PersistentScheduler'` with a local shelve file `celerybeat-schedule`, single instance, no lock.
- **Why it's bad:**
  - Running two beats (or `worker -B` on several replicas) duplicates every periodic task. "Requiring the user to ensure that only one instance of celerybeat exists… creates a substantial implementation burden (either creating a single point-of-failure or encouraging users to roll their own distributed mutex)" ([celery#251](https://github.com/celery/celery/issues/251)). Third-party lock-based schedulers exist because of this: [RedBeat](https://pypi.org/project/celery-redbeat/0.9.3rc5), [celery-redundant-scheduler](https://pypi.org/project/celery-redundant-scheduler).
  - The shelve file corrupts or vanishes in containers ("Bad magic number", dbm errors; [fixdevs](https://fixdevs.com/blog/celery-beat-not-working/), [Red Hat](https://access.redhat.com/discussions/4638681)).
  - django-celery-beat: changing `TIME_ZONE` requires resetting `last_run_at` manually ([docs](https://django-celery-beat.readthedocs.io/en/stable/reference/django-celery-beat.tzcrontab.html)).
  - There is no first-class way to disable a misbehaving cron job (Dignam).
  - Beat systemd and K8s probe questions: [#4304](https://github.com/celery/celery/issues/4304), [#4079](https://github.com/celery/celery/issues/4079).
- **Potatoq default:**
  - Built-in **leader election** via the broker: Redis `SET NX PX` lease, Postgres `pg_try_advisory_lock`, SQLite file lock, RabbitMQ single-active-consumer. Running N beat replicas or `worker --beat` everywhere is safe.
  - Schedule state (last run) stored in the broker/DB, with **deterministic dedup keys per (schedule, fire time)** so that even a split-brain doesn't double-fire.
  - Missed runs: run once at most on catch-up (configurable).
  - `beat_schedule` dict and `crontab()`/`solar`/timedelta are Celery-compatible. Schedules can be disabled through CLI/admin.

### H. Canvas (chain/group/chord)
- **Pain:**
  - Bugs and brittleness: "a bug with Celery not resolving 'chord' callbacks when all the parallel tasks had completed" ([HN](https://news.ycombinator.com/item?id=15681066)).
  - "many bugs in advanced features like Canvas" (HN).
  - Errors in chains of groups hang `.get()` ([discussion #8782](https://github.com/celery/celery/discussions/8782)).
  - [#4834 group nested in double chain raising internal error](https://github.com/celery/celery/issues/4834), present in 4.2.0–5.6.0.
  - AmpUp: "one timeout in step three produces an error that surfaces at step five with a cryptic message".
  - Chords need a result backend. Non-Redis backends use `chord_unlock` polling every 1s (`result_chord_retry_interval=1.0`).
  - Dignam: canvas "encourages brittle pipelines".
  - Wiredcraft: chain arguments are passed as tuples, which is awkward.
- **Potatoq default:** support the common subset faithfully (`chain`, `group`, `chord`, `.s()/.si()/signature`, `link`, `link_error`). Implement chords with atomic counters on every backend (Redis INCR, SQL `UPDATE … RETURNING`), so there is no polling. Errors propagate deterministically: the chord fails fast, and `link_error` is called once with the original exception. Results are stored automatically for canvas members (E1).

### I. Framework integration (Django, Flask, FastAPI)

#### I1. Django transactions
- Enqueuing inside a transaction means the worker may run before COMMIT, which leads to `DoesNotExist` ([Adam Johnson](https://adamj.eu/tech/2020/02/03/common-celery-issues-on-django-projects/); [testdriven.io](https://testdriven.io/blog/celery-database-transactions/)). Celery 5.4 added `delay_on_commit()`, but it's opt-in and returns no task id. Django 6 Tasks also requires manual `transaction.on_commit(partial(task.enqueue, …))`. Dignam asks for a transactional outbox.
- **Potatoq default:** when `potatoq.contrib.django` is installed, `.delay()`/`.apply_async()` called inside an `atomic()` block **defer until commit** by default (`enqueue_on_commit=True`; still return a pre-generated task id). Outside a transaction, publish immediately. On rollback, nothing is sent. With the Postgres/SQLite broker in the same DB, the enqueue *is* part of the transaction (true outbox semantics for free). Keep `delay_on_commit` as an alias.

#### I2. Configuration naming / `CELERY_` namespace
- Settings were renamed to lowercase in 4.0. Django uses `config_from_object('django.conf:settings', namespace='CELERY')` → `CELERY_TASK_ACKS_LATE`, etc. Old and new names coexist (`CELERYD_PREFETCH_MULTIPLIER`), so tutorials disagree. Env var `BROKER_URL` has priority over app settings ([#4284](https://github.com/celery/celery/issues/4284)). [#6285 "Basic Django config files break upgrading from 4.4.6 to 4.4.7"](https://github.com/celery/celery/issues/6285). Dignam: config "isn't type safe".
- **Potatoq default:** accept Celery's lowercase names and the `CELERY_` namespace for drop-in compatibility, plus a typed `POTATOQ = {...}` dict. **Warn on unknown and misspelled keys** (Celery silently ignores them) and on deprecated old-style names.

#### I3. Autodiscovery and task names
- `autodiscover_tasks()` only imports `<app>.tasks`. Tasks elsewhere cause "Received unregistered task". Auto-names derive from `module.__name__`, so relative/absolute import differences or moving a function **rename the task**, and in-flight messages then fail with `NotRegistered` ([Celery task names docs](https://docs.celeryq.dev/en/stable/userguide/tasks.html#names); Dignam: "always explicitly name tasks"). [#3642 pytest plugin `NotRegistered`](https://github.com/celery/celery/issues/3642).
- **Potatoq default:** same naming algorithm (compatibility), plus `potatoq check`, which imports the app and lists tasks, duplicates, and names that differ from the previous deploy (persist a registry snapshot in the broker). Optional `strict_names=True` requires explicit names. When a worker gets an unknown task, it goes to the DLQ (not dropped) with a clear message listing close matches.

#### I4. Logging hijack
- `worker_hijack_root_logger=True`: "Celery has a default configuration that removes every customized logs configuration" ([Vinta lesson](https://www.vintasoftware.com/lessons-learned/celery-has-a-default-configuration-that)). Disabling it still doesn't stop log-level changes; you need the `setup_logging` signal. `worker_redirect_stdouts=True` (WARNING level).
- **Potatoq default:** if the root logger has handlers (Django `LOGGING`, structlog, etc.), leave it alone and only add the task-context filter. Otherwise install a sensible stderr handler. Include task id and name in log records via contextvars. Keep the `setup_logging`/`after_setup_logger` signals for compatibility.

#### I5. Timezone
- `enable_utc=True`, `timezone=None→UTC`. Django's `TIME_ZONE` is *not* used unless `CELERY_TIMEZONE` is set, which is a common source of crontab-fires-at-wrong-hour confusion ([stackharbor](https://stackharbor.com/en/knowledge-base/python-celery-beat-scheduling/)).
- **Potatoq default:** UTC everywhere internally, and aware datetimes required for `eta=`. Crontab is evaluated in `timezone` (default UTC), with optional per-schedule `tz=`. Log a startup warning when Django's `TIME_ZONE` differs from Potatoq's `timezone`.

### J. Operational overhead

#### J1. Gossip, mingle, heartbeat
- On by default. CloudAMQP: they cause "excessive message traffic that provides little utility and can cause severe stress on the RabbitMQ cluster", approaching "250,000 messages per second ((1000 workers × 0.5 msg/s)²)". Disabling them gives "1 AMQP connection with 1 channel per worker instead of 3 AMQP connections and 4 channels" ([CloudAMQP blog](https://www.cloudamqp.com/blog/python-celery-and-rabbitmq.html)). Hatchet: gossip scales n² and is mainly for clock sync. Standard advice is `--without-gossip --without-mingle --without-heartbeat`.
- **Potatoq default:** no gossip, no mingle. Worker heartbeats only feed the lease mechanism (A4) and, if `events=True`, the events stream. Revokes are persisted in the broker (so new workers don't need mingle to learn them).

#### J2. Broker connection settings
- `broker_pool_limit=10`, `broker_heartbeat=120`, `broker_connection_timeout=4`, `broker_connection_max_retries=100`. CloudAMQP recommends `broker_pool_limit=1` and `broker_heartbeat=None` on hosted plans ([CloudAMQP docs](https://www.cloudamqp.com/docs/celery.html)). Since 5.3, users see `CPendingDeprecationWarning` about `broker_connection_retry_on_startup` (still a TODO for 6.0 in `consumer.py`). Heartbeat failures cause roughly 15-minute hangs on failover ([#4075](https://github.com/celery/celery/issues/4075)). Workers stop consuming after Redis reconnects ([gwcelery#491](https://git.ligo.org/emfollow/gwcelery/-/issues/491)); "Long-standing disconnection issues with the Redis broker have been identified and resolved in Kombu 5.5.0" ([whatsnew-5.5](https://docs.celeryq.dev/en/stable/history/whatsnew-5.5.html)).
- **Potatoq default:** pool 10 per process, fork-safe. Infinite reconnect with capped exponential backoff and a log line for each attempt. Readiness reports "not ready" while disconnected. AMQP heartbeat 60s. TCP keepalive on. A watchdog restarts the consumer if no broker I/O happens for 2× heartbeat.

#### J3. Redis specifics
- `global_keyprefix=''`. Keys like `_kombu.binding.*`, `unacked`, and `celery-task-meta-*` collide when apps share a DB. Redis key eviction causes `InconsistencyError` unless `maxmemory-policy` is `noeviction` (docs). `fanout_prefix`/`fanout_patterns` are True. Redis as a broker has no persistence by default (HN: "Anything using redis will be riskier than AMQP by default"). "Proposal to deprecate Redis as a broker" was rejected ([#3274](https://github.com/celery/celery/issues/3274)).
- **Potatoq default:** prefix all keys with `potatoq:{app}:`. On startup, `CONFIG GET maxmemory-policy` (if permitted) and warn unless `noeviction`, and warn if `appendonly no`. Support Redis/Valkey/Dragonfly and Sentinel/Cluster (hash-tagged keys).

#### J4. RabbitMQ queue type
- `task_default_queue_type='classic'`. Quorum queues are supported only from 5.5 and disable global QoS, so autoscale breaks ([RabbitMQ docs in Celery](https://docs.celeryq.dev/en/main/getting-started/backends-and-brokers/rabbitmq.html)). Classic mirrored queues are removed in RabbitMQ 4.
- **Potatoq default:** quorum queues + publisher confirms + per-consumer QoS. Use delayed delivery for ETA. Rely on the built-in delivery-limit/DLX for poison messages.

#### J5. Observability
- Events are off by default, and Flower needs `-E`. Flower "doesn't support reading tasks from a Celery backend and you'll miss Celery tasks when Flower goes down or restarts" ([Hatchet](https://hatchet.run/blog/problems-with-celery)). "observability of Celery tasks is not great" ([HN](https://hn.svelte.dev/item/40810986)). AmpUp: "A task would get dispatched into Redis and then… something would happen." Dignam: monitoring is "pretty limited".
- **Potatoq default:** built-in Prometheus/OpenTelemetry metrics (queue depth, latency, runtime, failures, retries, DLQ size) and trace propagation from publisher to worker. A `potatoq status` CLI shows queue depth per queue. Events stream is optional.

### K. Missing features that drive people away
| Ask | Evidence | Potatoq |
|-----|----------|---------|
| asyncio tasks | [#6552 "Support async function"](https://github.com/celery/celery/issues/6552) (**#1 most-upvoted issue**, 99), [#3884](https://github.com/celery/celery/issues/3884), [#7874](https://github.com/celery/celery/issues/7874), [#6603 awaitable result](https://github.com/celery/celery/issues/6603); Hatchet, Dignam | `async def` tasks on an asyncio pool; `aenqueue`/`aget` |
| Postgres broker | [#5149](https://github.com/celery/celery/issues/5149) (96 upvotes, closed); SQLAlchemy transport isn't even listed among supported brokers | First-class Postgres + SQLite brokers (SKIP LOCKED, LISTEN/NOTIFY) |
| Per-queue concurrency | [#1599](https://github.com/celery/celery/issues/1599) (50) | `-Q a=4,b=8` |
| Global rate limit | [#5732](https://github.com/celery/celery/issues/5732), Hatchet | Default (B6) |
| Dead-letter queue | Hatchet | Built-in (A3) |
| spawn instead of fork | [#6036](https://github.com/celery/celery/issues/6036) | `pool=prefork` default with `start_method` option |
| Serverless / one-shot worker | [#6687](https://github.com/celery/celery/issues/6687) | `potatoq worker --burst` |
| Redis Cluster / Valkey | [#2852](https://github.com/celery/celery/issues/2852), [#9092](https://github.com/celery/celery/issues/9092) | Supported |
| Type hints | [#7394](https://github.com/celery/celery/issues/7394), Dignam | Fully typed public API; `Task[P, R]` generics so `.delay()` is type-checked |
| Liveness probes | [#4079](https://github.com/celery/celery/issues/4079) | Health file/endpoint |

### L. "Celery is overkill" / complexity
- "In many projects Celery is overkill. Common scenario: 1. We have problem, lets use Celery 2. Now we have one more problem." "Celery is an overkill most of the times and will force you to spend more time doing ops" ([HN 7909201](https://news.ycombinator.com/item?id=7909201)).
- "I ran Celery in many projects in production over the past 10 years; I would not recommend it. It is mostly a constant fight." "its configuration is really complicated and many default values do not make sense." ([HN 30567986](https://news.ycombinator.com/item?id=30567986))
- "Celery is doing itself a disservice by supporting so many brokers and result stores with different behaviors about delivery guarantee, leading to leaky abstraction everywhere." ([HN 36021877](https://news.ycombinator.com/item?id=36021877))
- "Celery's source code is spread across 3 different projects (celery, billiard and kombu) and it's impenetrable" ([Dramatiq motivation](https://dramatiq.io/motivation.html)).
- RQ migration: ~27k LOC vs ~2k; "understanding 100% of their application's code" mattered more ([Zimmer slides](https://talks.sylvainzimmer.com/2013-parispy/slides.pdf)).
- Defenders note that most complaints are about defaults, not capability ("complaining that celery doesn't have defaults that better suit you", HN). **That is exactly Potatoq's opportunity.**
- **Implication:** **identical semantics across brokers** (lease-based at-least-once everywhere), a small codebase, and SQLite/Postgres brokers so small projects need no extra infrastructure.

---

## 3. API surface that must be drop-in compatible

Rough usage signal from GitHub code search (`language:python`, Oct 2026; counts are approximate and noisy, so read them as relative popularity only): `.apply_async(` ~134k, `@shared_task` ~67k, `beat_schedule` ~49k, `@app.task` ~44k, `ignore_result` ~39k, `.delay(` + `from celery` ~34k, `bind=True` + `self.retry(` ~29k, `crontab(` + celery ~25k, `acks_late` ~24k, `soft_time_limit` ~24k, `send_task(` ~20k, `self.request.id` ~13k, `autoretry_for` ~13k, `countdown=` + apply_async ~11k, `chain(` + `from celery` ~5.7k, `chord(` + `from celery` ~5.3k, `delay_on_commit` ~0.7k (new in 5.4). Additional counts are appended in §6 if available.

### Tier 1: must work unchanged (used by almost every project)
- **App:** `Celery("proj", broker=..., backend=..., include=[...])`, `app.config_from_object("django.conf:settings", namespace="CELERY")`, `app.config_from_object(obj_or_module)`, `app.conf.update(...)`, `app.conf.<setting> = ...`, `app.autodiscover_tasks()`, the `proj/celery.py` + `__init__.py` `from .celery import app as celery_app` pattern, `celery -A proj worker|beat|inspect|purge|call|shell` CLI.
- **Decorators:** `@app.task`, `@shared_task`, with or without parentheses. Options: `name`, `bind=True`, `base=`, `queue`, `ignore_result`, `max_retries`, `default_retry_delay`, `autoretry_for`, `dont_autoretry_for`, `retry_kwargs`, `retry_backoff`, `retry_backoff_max`, `retry_jitter`, `acks_late`, `reject_on_worker_lost`, `time_limit`, `soft_time_limit`, `rate_limit`, `expires`, `priority`, `serializer`, `track_started`, `typing`, `pydantic`.
- **Calling:** `task.delay(*args, **kwargs)`; `task.apply_async(args=, kwargs=, countdown=, eta=, expires=, queue=, routing_key=, priority=, task_id=, link=, link_error=, headers=, retry=, retry_policy=, serializer=, ignore_result=, shadow=)`; `task(*args)` (direct call, runs inline); `task.apply()` (eager); `app.send_task("name", args, kwargs, ...)`; `.delay_on_commit()` (5.4+).
- **Results:** `AsyncResult(id)` / `app.AsyncResult(id)`, `.get(timeout=, propagate=, disable_sync_subtasks=)`, `.ready()`, `.successful()`, `.failed()`, `.state`/`.status`, `.result`, `.info`, `.traceback`, `.id`, `.revoke(terminate=)`, `.forget()`; `GroupResult` (`.join()`, `.completed_count()`); `states.*` constants.
- **Bound task:** `self.request` (`id`, `args`, `kwargs`, `retries`, `delivery_info`, `hostname`, `eta`, `expires`, `is_eager`, `called_directly`, `parent_id`, `root_id`, `correlation_id`, `headers`, `timelimit`, `group`, `chord`), `self.retry(exc=, countdown=, eta=, max_retries=, args=, kwargs=, throw=)`, `self.update_state(state=, meta=)`, `self.name`, `self.max_retries`, `self.app`, `self.backend`, `self.replace(sig)`.
- **Exceptions:** `celery.exceptions.SoftTimeLimitExceeded`, `TimeLimitExceeded`, `MaxRetriesExceededError`, `Retry`, `Ignore`, `Reject`, `TaskRevokedError`, `NotRegistered`, `TimeoutError`.
- **Beat:** `app.conf.beat_schedule = {"name": {"task": "...", "schedule": crontab(...)|timedelta|float|solar, "args": (...), "kwargs": {...}, "options": {...}}}`; `from celery.schedules import crontab` (`minute`, `hour`, `day_of_week`, `day_of_month`, `month_of_year`); `@app.on_after_configure.connect` / `on_after_finalize` + `sender.add_periodic_task(...)` (the `setup_periodic_tasks` idiom); django-celery-beat's `DatabaseScheduler` (a compatibility target or migration path).
- **Routing/config:** `task_routes` (dict or callable, glob patterns), `task_queues` / `Queue`/`Exchange` (kombu), `task_default_queue`, `-Q` on the worker.
- **Testing:** `task_always_eager`, `task_eager_propagates`, the `celery.contrib.testing` / pytest `celery_app`, `celery_worker` fixtures.

### Tier 2: widely used, implement early
- **Canvas:** `signature()/.s()/.si()`, `chain` (and `|`), `group`, `chord` (`chord(header)(callback)`), `link`/`link_error`, `starmap`/`chunks` (less common), `GroupResult.save/restore`.
- **Signals:** `task_prerun`, `task_postrun`, `task_success`, `task_failure`, `task_retry`, `task_revoked`, `task_received`, `before_task_publish`, `after_task_publish`, `worker_process_init` (DB connection reset after fork, very common), `worker_process_shutdown`, `worker_ready`, `worker_shutdown`, `worker_init`, `celeryd_init`, `celeryd_after_setup`, `setup_logging`, `after_setup_logger`, `after_setup_task_logger`, `beat_init`, `heartbeat_sent`.
- **Custom base classes:** `class MyTask(Task)` overriding `on_failure(self, exc, task_id, args, kwargs, einfo)`, `on_success`, `on_retry`, `after_return`, `before_start`, `__call__`. `app.Task = ...`; `DjangoTask`.
- **Control/inspect:** `app.control.revoke(id, terminate=True)`, `app.control.inspect().active()/reserved()/scheduled()/registered()/stats()`, `app.control.purge()`, `app.control.rate_limit()`, `ping`.
- **Logging:** `from celery.utils.log import get_task_logger`.
- **Worker CLI flags:** `--concurrency/-c`, `--pool/-P (prefork|threads|solo|gevent|eventlet)`, `-Q`, `--loglevel/-l`, `-n/--hostname`, `-E`, `-B`, `--max-tasks-per-child`, `--max-memory-per-child`, `--prefetch-multiplier`, `-O fair`, `--without-gossip/--without-mingle/--without-heartbeat` (accepted as no-ops), `--autoscale`, `--time-limit`, `--soft-time-limit`, `--detach`, `--pidfile`.

### Tier 3: rare, compatibility shim or explicit "not supported"
`app.Task` metaclass tricks, `celery multi`, `worker_state_db`, custom `Strategy`, custom bootsteps, `celery.contrib.abortable`, `celery.contrib.rdb`, `group.skew`, `chunks`/`xmap`, message protocol v1, remote-control broadcast custom commands, `app.events.Receiver` (needed for Flower compatibility if pursued).

### Wire compatibility decision
If Potatoq workers must consume messages produced by Celery (or vice versa) during migration, implement Celery **message protocol v2** (headers `task`, `id`, `eta`, `expires`, `retries`, `timelimit`, `root_id`, `parent_id`, `argsrepr`, `kwargsrepr`, `origin`; body `[args, kwargs, embed{callbacks, errbacks, chain, chord}]`) and keep the `celery` default queue name. This enables a rolling migration story ("swap the import, deploy workers first"), which is a strong selling point.

---

## 4. Additional notes for the SQLite/Postgres brokers (Potatoq-specific)
- Celery effectively doesn't support SQL brokers. The Kombu SQLAlchemy transport isn't in the supported table, and Deni Bertovic/Vinta warn "never use a relational database as production broker" because of polling I/O. Demand exists ([#5149](https://github.com/celery/celery/issues/5149), 96 upvotes; HN: "For under 1000 messages per second, PostgreSQL is great at queues").
- To avoid the classic DB-queue problems:
  - Postgres uses `FOR UPDATE SKIP LOCKED` + `LISTEN/NOTIFY` wakeups (no tight polling).
  - Use a partial index on `(queue, priority DESC, run_at) WHERE state='queued'`.
  - Delete or move finished rows promptly (avoid table bloat), and set autovacuum hints.
  - SQLite uses WAL mode, `BEGIN IMMEDIATE`, a short busy timeout, and a single-host warning.
  - Same-DB enqueue gives transactional enqueue (I1) for free. This is a headline feature.

---

## 5. Most-upvoted celery/celery GitHub issues (pain signal)
Sorted by reactions, fetched via the GitHub API on 2026-10-07:

| Issue | 👍 | State | Title |
|---|---|---|---|
| [#6552](https://github.com/celery/celery/issues/6552) | 99 | open | Support async function |
| [#5149](https://github.com/celery/celery/issues/5149) | 96 | closed | Support for PostgreSQL as a broker – gauge of interest |
| [#4843](https://github.com/celery/celery/issues/4843) | 82 | open | Continuous memory leak (178 comments) |
| [#3759](https://github.com/celery/celery/issues/3759) | 53 | closed | Tasks received but not executing |
| [#1599](https://github.com/celery/celery/issues/1599) | 50 | open | Specify concurrency level per queue |
| [#4400](https://github.com/celery/celery/issues/4400) | 48 | open | Same task runs multiple times at once? |
| [#3773](https://github.com/celery/celery/issues/3773) | 47 | open | Couldn't ack, reason: BrokenPipeError |
| [#3596](https://github.com/celery/celery/issues/3596) | 43 | closed | AsyncResult task existence check with arbitrary task id |
| [#4079](https://github.com/celery/celery/issues/4079) | 43 | closed | [K8S] liveness/readiness probes for beat and workers |
| [#5410](https://github.com/celery/celery/issues/5410) | 35 | closed | confirm_publish defaults to False and insufficiently documented |
| [#4185](https://github.com/celery/celery/issues/4185) | 28 | closed | Workers randomly hang on IPC (123 comments) |
| [#3430](https://github.com/celery/celery/issues/3430) | 26 | open | Long tasks are executed multiple times |
| [#251](https://github.com/celery/celery/issues/251) | 26 | closed | Beat should avoid concurrent invocations |
| [#6036](https://github.com/celery/celery/issues/6036) | 26 | closed | Allow celery to spawn rather than fork |
| [#4008](https://github.com/celery/celery/issues/4008) | 25 | closed | Eager mode hides serialization side-effects |
| [#4522](https://github.com/celery/celery/issues/4522) | 23 | open | Celery not useful for long term future tasks |
| [#5732](https://github.com/celery/celery/issues/5732) | 23 | open | Rate limiting with multiple workers |
| [#6067](https://github.com/celery/celery/issues/6067) | 23 | closed | Support for quorum queues |
| [#8916](https://github.com/celery/celery/issues/8916) | 22 | open | Log when worker is recycled by max-tasks/memory-per-child |
| [#4296](https://github.com/celery/celery/issues/4296) | 21 | closed | delay/apply_async waiting forever when broker is down |
| [#7007](https://github.com/celery/celery/issues/7007) | 21 | open | ForkPoolWorker exited with signal 11 |
| [#4028](https://github.com/celery/celery/issues/4028) | 19 | open | Clarify support for task priority with redis |
| [#5120](https://github.com/celery/celery/issues/5120) | 19 | closed | Duplicate execution on worker recycling (exitcode 155) |
| [#5935](https://github.com/celery/celery/issues/5935) | 19 | open | Long-running jobs redelivered after visibility timeout (Redis) |
| [#4284](https://github.com/celery/celery/issues/4284) | 18 | open | Env var broker URL prioritized over app settings |
| [#7091](https://github.com/celery/celery/issues/7091) | 16 | closed | Autoretry fails when expires is set |
| [#6229](https://github.com/celery/celery/issues/6229) | 15 | closed | Tasks longer than visibility timeout not re-queued (acks_late + redis) |
| [#6687](https://github.com/celery/celery/issues/6687) | 15 | open | Process one task and quit (serverless) |

Clusters: **duplicate/lost execution** (visibility timeout, acks, recycling), **async**, **SQL broker**, **memory/hangs**, **ETA far future**, **rate limits/concurrency per queue**, **beat HA**, **ops (probes, logging)**.

---

## 6. Appendix: raw GitHub code-search counts

GitHub code search `total_count` for each query (approximate; generic identifiers such as `update_state(` and `retry_backoff` are inflated by non-Celery code).

| Count | Query |
|---:|---|
| 172544 | `"update_state(" language:python` |
| 134144 | `".apply_async(" language:python` |
| 100096 | `"retry_backoff" language:python` |
| 67328 | `"@shared_task" language:python` |
| 48640 | `"beat_schedule" language:python` |
| 43648 | `"@app.task" language:python` |
| 43648 | `"autodiscover_tasks" language:python` |
| 42240 | `"max_retries=" "celery" language:python` |
| 38720 | `"ignore_result" language:python` |
| 34240 | `".delay(" "from celery" language:python` |
| 28960 | `"bind=True" "self.retry(" language:python` |
| 25152 | `"crontab(" "celery" language:python` |
| 24320 | `"acks_late" language:python` |
| 24256 | `"soft_time_limit" language:python` |
| 20128 | `"send_task(" language:python` |
| 20096 | `"link_error" language:python` |
| 19648 | `"task_routes" language:python` |
| 18944 | `"visibility_timeout" language:python` |
| 18048 | `"time_limit=" "celery" language:python` |
| 17920 | `"worker_prefetch_multiplier" language:python` |
| 14784 | `"queue=" "apply_async" language:python` |
| 12992 | `"priority=" "apply_async" language:python` |
| 12960 | `"self.request.id" language:python` |
| 12640 | `"autoretry_for" language:python` |
| 11312 | `"task_failure" "connect" language:python` |
| 11072 | `"countdown=" "apply_async" language:python` |
| 8704 | `"SoftTimeLimitExceeded" language:python` |
| 7664 | `"revoke(" "celery" language:python` |
| 7584 | `"app.control.inspect" language:python` |
| 6800 | `"task_reject_on_worker_lost" language:python` |
| 6640 | `"eta=" "apply_async" language:python` |
| 5720 | `"chain(" "from celery" language:python` |
| 5280 | `"chord(" "from celery" language:python` |
| 4840 | `"worker_process_init" language:python` |
| 4120 | `"task_prerun" language:python` |
| 2960 | `"expires=" "apply_async" language:python` |
| 2460 | `"on_failure(self, exc, task_id" language:python` |
| 2180 | `".si(" "celery" language:python` |
| 1932 | `"rate_limit=" "celery" language:python` |
| 684 | `"delay_on_commit" language:python` |
| rate-limited | `"AsyncResult(" language:python` |
| rate-limited | `"group(" "from celery" language:python` |
