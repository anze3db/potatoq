"""New Relic: background transactions and distributed tracing (agent in developer mode)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

TESTS = Path(__file__).parent


def test_tasks_are_background_transactions_in_the_senders_trace(tmp_path):
    config = tmp_path / "newrelic.ini"
    config.write_text(
        "[newrelic]\napp_name = potatoq-test\ndeveloper_mode = true\ndistributed_tracing.enabled = true\n"
    )
    env = {**os.environ, "PYTHONPATH": str(TESTS)}
    env.pop("DJANGO_SETTINGS_MODULE", None)
    proc = subprocess.run(
        [sys.executable, str(TESTS / "newrelic_scenario.py"), str(config)],
        capture_output=True, text=True, env=env, timeout=120, check=False,
    )  # fmt: skip
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout.strip().splitlines()[-1])
    assert out["headers"] == ["traceparent", "tracestate"]
    assert out["eager_in_request"] is True
    assert out["name"] == "shop.charge"
    assert out["same_trace"] is True
    assert out["parent_type"] == "App"
    assert out["attributes"]["potatoq.queue"] == "default"
    assert out["attributes"]["potatoq.retries"] == 0
    assert out["attributes"]["potatoq.state"] == "SUCCESS"
    assert out["has_wait_time"] is True
    assert out["retry_errors"] == 0  # only the last attempt is an error
    assert out["failed_errors"] == ["builtins:ConnectionError"]
    assert out["failed_state"] == "FAILURE"
    assert out["process_died"] == ["MemoryError"]
    assert out["lifecycle"] == ["activate", "activate", "shutdown"]
    assert out["after_uninstrument"] == [[], False]
