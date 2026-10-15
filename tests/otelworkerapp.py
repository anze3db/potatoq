"""A worker app traced with OpenTelemetry, configured at import like most projects do
(before the worker forks), exporting spans to a file only when flushed."""

import json
import os

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult

from potatoq import Potatoq
from potatoq.contrib.opentelemetry import PotatoqInstrumentor


class FileExporter(SpanExporter):
    def export(self, spans):
        with open(os.environ["TEST_SPANS"], "a") as f:
            for span in spans:
                parent = span.parent.span_id if span.parent else None
                row = {"name": span.name, "trace": span.context.trace_id, "parent": parent, "pid": os.getpid()}
                f.write(json.dumps(row) + "\n")
        return SpanExportResult.SUCCESS


provider = TracerProvider()
# Exports only when flushed (or full): spans of a child that exits unflushed are lost.
provider.add_span_processor(BatchSpanProcessor(FileExporter(), schedule_delay_millis=600_000))
trace.set_tracer_provider(provider)
PotatoqInstrumentor().instrument()

app = Potatoq("otelworkerapp", broker=os.environ["TEST_BROKER"], worker_max_tasks_per_child=2)


@app.task
def add(x, y):
    return x + y
