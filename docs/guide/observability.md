# Tracing, APM and error tracking

potatoq ships integrations for **OpenTelemetry**, **Sentry** and **New Relic**. Through
OpenTelemetry it also works with **Logfire**, **Datadog**, Honeycomb, Grafana, Jaeger
and every other backend that accepts OTLP. All of them give you:

- **One trace from the request to the task.** `.delay()` puts the current trace context
  into the task message's headers, and the worker continues that trace. A task deferred
  to the end of a transaction carries the context of the `.delay()` call.
- **Whole workflows in one trace.** A retry, a chain's next step and a chord's callback
  are sent from inside the running task, so each attempt is a child of the one before.
- **A span or transaction per attempt**, with the task's name, id, queue, retry count and
  final state, and how long it waited in the queue after it was due.
- **Errors reported once.** An attempt that will be retried isn't an error; the last one
  is. A task whose process died (hard time limit, out of memory) is reported by the
  worker's supervisor, because the process that ran it can't.
- **Nothing lost when a worker process exits.** Worker processes end with `os._exit()`
  (as in Celery), which skips the SDKs' own exit hooks; potatoq flushes them first.

Task arguments and results are never recorded (Sentry: only with `send_default_pii`).

!!! tip "Set it up in every process"
    Configure tracing in the code that both your web processes and your workers load:
    your `celery.py`, your Django settings or your app factory. The web side injects the
    trace context; without it, each task starts a new trace.

## OpenTelemetry

```console
$ uv add "potatoq[opentelemetry]" opentelemetry-sdk opentelemetry-exporter-otlp-proto-http
```

```python title="proj/telemetry.py"
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from potatoq.contrib.opentelemetry import PotatoqInstrumentor

# Service name, endpoint and headers come from OTEL_* environment variables.
tracer_provider = TracerProvider()
tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(tracer_provider)
metrics.set_meter_provider(MeterProvider(metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())]))

PotatoqInstrumentor().instrument()
```

Import it from your `celery.py` or settings, next to your web framework's
instrumentation (`DjangoInstrumentor`, `FlaskInstrumentor`, `FastAPIInstrumentor`) and
your database client's, so the task's queries show up inside its span.

Or skip the code: potatoq registers itself with OpenTelemetry's auto-instrumentation.

```console
$ uv add opentelemetry-distro opentelemetry-exporter-otlp-proto-http
$ export OTEL_SERVICE_NAME=shop-worker OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
$ opentelemetry-instrument potatoq worker
```

### What you get

Each task message gets a `send <task>` span (kind `PRODUCER`) where it's sent, and each
attempt a `process <task>` span (kind `CONSUMER`) as its child, with the task's own logs,
queries and HTTP calls inside. Both follow OpenTelemetry's
[messaging conventions](https://opentelemetry.io/docs/specs/semconv/messaging/):

| Attribute | Example | |
|---|---|---|
| `messaging.system` | `potatoq` | |
| `messaging.destination.name` | `default` | the queue |
| `messaging.message.id` | `9f1c…` | the task id |
| `messaging.message.conversation_id` | `4b2a…` | the workflow's root task id |
| `potatoq.task.name` | `shop.tasks.send_receipt` | |
| `potatoq.task.state` | `SUCCESS` | also `FAILURE`, `RETRY`, `REJECTED`, `REVOKED`, `IGNORED` |
| `potatoq.task.retries`, `potatoq.task.delivery_count` | `1`, `1` | `delivery_count > 1`: redelivered after a crash |
| `potatoq.task.periodic` | `nightly-report` | the `beat_schedule` entry |
| `potatoq.task.retry_eta`, `potatoq.task.reason` | | when it retries; why it was rejected or revoked |
| `error.type` | `ConnectionError` | on failures, with the exception recorded |

Failed and rejected attempts have status `ERROR`. Retried ones don't, but the exception
is recorded as an event.

Metrics, with the task name, queue and (for durations) final state as attributes:

| Metric | Unit | |
|---|---|---|
| `messaging.client.sent.messages` | messages | tasks sent |
| `messaging.process.duration` | s | how long attempts took |
| `potatoq.task.wait.duration` | s | how long tasks waited in the queue after they were due: alert on this one |

### Forking and gRPC

The worker forks its processes after your code configured OpenTelemetry. The SDK
restarts its export threads in each process, so this works with the HTTP exporter, as
above. gRPC connections don't survive a fork: with the gRPC exporter (or
`OTEL_EXPORTER_OTLP_PROTOCOL=grpc`), configure OpenTelemetry in each worker process
instead.

```python
from potatoq import signals


@signals.worker_process_init.connect
def init_telemetry(**kwargs):
    ...  # the provider setup from above
```

### Where to send it

Any OTLP backend works: set `OTEL_EXPORTER_OTLP_ENDPOINT` and
`OTEL_EXPORTER_OTLP_HEADERS` as your vendor documents. For example:

| Backend | Settings |
|---|---|
| [Honeycomb](https://docs.honeycomb.io/send-data/python/opentelemetry-sdk/) | `OTEL_EXPORTER_OTLP_ENDPOINT=https://api.honeycomb.io`, `OTEL_EXPORTER_OTLP_HEADERS=x-honeycomb-team=<api key>` |
| Grafana Cloud, Jaeger, Tempo, SigNoz, Uptrace | the endpoint of your collector or of the service's OTLP gateway |
| New Relic, Dynatrace, Elastic, Splunk | their OTLP endpoints (New Relic also has [an agent integration](#new-relic)) |
| Azure Monitor | `configure_azure_monitor()` from `azure-monitor-opentelemetry` instead of the provider setup |
| AWS X-Ray, Google Cloud Trace | through the AWS Distro for OpenTelemetry or the OpenTelemetry Collector |

Use a different `OTEL_SERVICE_NAME` for web and worker processes (`shop-web`,
`shop-worker`), so you can tell them apart.

## Logfire

[Logfire](https://logfire.pydantic.dev/) is built on OpenTelemetry, so the same
instrumentor works:

```python
import logfire

from potatoq.contrib.opentelemetry import PotatoqInstrumentor

logfire.configure(service_name="shop-worker", distributed_tracing=True)
PotatoqInstrumentor().instrument()
```

Set `distributed_tracing=True` in both the web and the worker processes. Without it,
Logfire warns about "unintentional distributed tracing" when a worker continues a trace
that started elsewhere.

## Sentry

```python
import sentry_sdk
from potatoq.contrib.sentry import PotatoqIntegration

sentry_sdk.init(
    dsn="https://…",
    traces_sample_rate=0.1,
    integrations=[PotatoqIntegration(monitor_periodic_tasks=True)],
)
```

- **Errors**: a task that fails for good is an issue tagged with the task's name, with
  its id, queue and retry count. Breadcrumbs are per task: each attempt gets its own
  scope. `SoftTimeLimitExceeded` is grouped per task.
- **Tracing**: each attempt is a transaction (`queue.task.potatoq`) in the trace of the
  request that sent it, with a `queue.process` span; the sender gets a `queue.publish`
  span. Sentry's **Queues** page shows each queue's latency and error rate from these.
- **Crons**: with `monitor_periodic_tasks=True`, every `beat_schedule` run sends
  check-ins, and the monitors are created from your schedules (crontabs, and intervals
  of a minute or more), so a run that's missed, fails or hangs alerts you.

| Option | Default | |
|---|---|---|
| `propagate_traces` | `True` | carry the trace into tasks (`sentry-trace` and `baggage` headers) |
| `monitor_periodic_tasks` | `False` | send cron check-ins for periodic tasks |
| `exclude_periodic_tasks` | `None` | regexes of `beat_schedule` entry names to leave out |

Once Celery is uninstalled, Sentry's own `CeleryIntegration` no longer turns itself on;
until then, both can be active without interfering.

## New Relic

```python
import newrelic.agent

newrelic.agent.initialize()  # newrelic.ini, or NEW_RELIC_LICENSE_KEY and NEW_RELIC_APP_NAME

from potatoq.contrib import newrelic as potatoq_newrelic

potatoq_newrelic.instrument()
```

Or start each process with `newrelic-admin run-program potatoq worker` (which calls
`initialize()` for you) and call `instrument()` in your app's code.

Each attempt is a background transaction in the `potatoq` group, named after the task,
in the distributed trace of the request that sent it. Sending is a
`MessageBroker/potatoq/Queue/Produce` segment of the request. A task run eagerly inside
a request is part of the request's transaction instead. Transactions carry
`potatoq.task_id`, `potatoq.queue`, `potatoq.retries`, `potatoq.delivery_count`,
`potatoq.state` and `potatoq.wait_time`, for queries and alerts. The agent connects in
each worker process when it starts and sends its data before the process exits.

## Datadog

Datadog's tracer accepts OpenTelemetry's API, so use the
[OpenTelemetry integration](#opentelemetry) with `ddtrace` as the provider:

```python
from ddtrace.trace import tracer

from potatoq import signals
from potatoq.contrib.opentelemetry import PotatoqInstrumentor

PotatoqInstrumentor().instrument()


@signals.worker_process_shutdown.connect
def flush_datadog(**kwargs):
    tracer.flush()  # ddtrace's provider has no force_flush
```

```console
$ DD_TRACE_OTEL_ENABLED=true DD_SERVICE=shop-worker ddtrace-run potatoq worker
```

Tasks then appear as `process <task>` spans in the same trace as the request that sent
them, alongside everything else `ddtrace` instruments.

## Other tools

- **Error trackers without tracing** (Rollbar, Bugsnag, Honeybadger, Airbrake): report
  from the `task_failure` signal, which fires once per task that fails for good.
- **Prometheus, StatsD**: export the OpenTelemetry metrics above, or record your own
  from an instrumentation (below).
- **Logs**: potatoq's worker log records carry `task_id` and `task_name` attributes.
  Sentry, New Relic and Logfire attach logs to the running task automatically. With
  OpenTelemetry, [`opentelemetry-instrumentation-logging`](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/logging/logging.html)
  adds trace ids to log records; put `%(otelTraceID)s` in `worker_task_log_format` to
  print them.

## Writing your own

The integrations are built on two hooks in `potatoq.instrumentation`, which you can use
too:

```python
import time
from contextlib import contextmanager

from potatoq import instrumentation


class Timings(instrumentation.Instrumentation):
    def message_created(self, message):
        """Every task message, where it's sent: add your trace headers to message.headers."""

    @contextmanager
    def task_run(self, run):
        """Around every attempt (in workers and in eager mode)."""
        yield
        statsd.timing(f"tasks.{run.name}.{run.state}", run.runtime)
        statsd.timing(f"queues.{run.queue}.wait", run.wait_time)


instrumentation.register(Timings())
```

`run` is a `TaskRun`: the task's `message` (with its `headers`), `name`, `queue`,
`retries`, `delivery_count`, `periodic`, `is_eager` and `wait_time`, and once the
attempt is over its `state`, `exception`, `runtime` and `retry_eta`. `process_died` is
true when the supervisor reports a task whose process died. An instrumentation that
raises is logged and skipped; it never breaks a task.

## Good practices

- **Sample at the start of the trace.** Workers follow the sampling decision made where
  the trace started (OpenTelemetry's default `parentbased_*` samplers, Sentry's
  `traces_sample_rate`), so a request and its tasks are kept or dropped together. Set
  the rate on the web side (`OTEL_TRACES_SAMPLER=parentbased_traceidratio`,
  `OTEL_TRACES_SAMPLER_ARG=0.1`); periodic tasks start their own traces and are sampled
  by the worker's settings.
- **Alert on wait time, not only on failures**: `potatoq.task.wait.duration`, Sentry's
  queue latency or New Relic's `potatoq.wait_time` show a backlog building up before
  users notice. Set up cron monitors for periodic tasks, so a run that never happens
  alerts you too.
- **One tracer per process.** OpenTelemetry (with Logfire or Datadog) and New Relic both
  use W3C `traceparent` headers: run one of them. Sentry can run next to either one;
  it keeps the other's `baggage` entries.
- **Delayed tasks** (a long `countdown`, a retry with backoff) start long after the span
  that sent them ended. That's expected: the trace stays connected, it's just long.
