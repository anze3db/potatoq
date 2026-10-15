"""Run by test_newrelic.py in its own process: the agent hooks imports process-wide."""

import json
import os
import sys

import newrelic.agent

newrelic.agent.initialize(sys.argv[1])
assert newrelic.agent.register_application(timeout=10).active

from potatoq import Potatoq, signals  # noqa: E402
from potatoq.contrib import newrelic as potatoq_newrelic  # noqa: E402
from potatoq.testing import drain  # noqa: E402
from potatoq.worker import executor  # noqa: E402

potatoq_newrelic.instrument()
app = Potatoq("nr", broker="memory://", set_as_current=True)
seen = {}
out = {}


@app.task(name="shop.charge", autoretry_for=(ConnectionError,), max_retries=1, retry_backoff=False)
def charge(fail):
    transaction = newrelic.agent.current_transaction()
    seen.setdefault("transactions", []).append(transaction)
    if fail:
        raise ConnectionError("down")
    return 1


with newrelic.agent.BackgroundTask(newrelic.agent.application(), "checkout", group="Web") as web:
    message = charge.build_message([False], {})
    out["headers"] = sorted(k for k in message.headers if k in ("traceparent", "tracestate"))
    charge.apply_async((False,))
    charge.apply_async((True,))
    app.conf.task_always_eager = True
    charge.delay(False)  # eager inside the request: part of its transaction
    app.conf.task_always_eager = False
    out["eager_in_request"] = seen["transactions"][-1] is web
drain(app)

ok, retried, failed = seen["transactions"][1:]
out["name"] = ok.name
out["group"] = ok.group_for_metric if hasattr(ok, "group_for_metric") else ok._group
out["same_trace"] = ok.trace_id == web.trace_id
out["parent_type"] = ok.parent_type
out["attributes"] = {k: v for k, v in ok._custom_params.items() if k != "potatoq.wait_time"}
out["has_wait_time"] = "potatoq.wait_time" in ok._custom_params
out["retry_errors"] = len(retried._errors)
out["failed_errors"] = [e.type for e in failed._errors]
out["failed_state"] = failed._custom_params["potatoq.state"]

noticed = []
real_notice = newrelic.agent.notice_error
newrelic.agent.notice_error = lambda error=None, **kw: noticed.append(error[0].__name__)
executor.failure_outcome(app, charge.build_message([False], {}, headers={"periodic": "nightly"}), MemoryError("x"))
out["process_died"] = noticed
newrelic.agent.notice_error = real_notice

calls = []
newrelic.agent.shutdown_agent = lambda *a, **k: calls.append("shutdown")
newrelic.agent.application = lambda *a, **k: type("App", (), {"activate": lambda self: calls.append("activate")})()
signals.worker_init.send(sender=None)
signals.worker_process_init.send(sender=None)
signals.worker_process_shutdown.send(sender=None, pid=os.getpid(), exitcode=0)
out["lifecycle"] = list(calls)

potatoq_newrelic.uninstrument()
calls.clear()
signals.worker_process_shutdown.send(sender=None, pid=os.getpid(), exitcode=0)
message = charge.build_message([False], {})
out["after_uninstrument"] = [calls, "traceparent" in message.headers]
print(json.dumps(out))
