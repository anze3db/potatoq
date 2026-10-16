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
    assert out["process_died"] == []  # reported by the supervisor: kept away from New Relic
    assert out["lifecycle"] == ["activate", "shutdown"]  # worker processes only
    assert out["after_uninstrument"] == [[], False]


def test_worker_processes_report_to_new_relic(tmp_path):
    """The agent must be activated in each worker process, never before the fork: New
    Relic can't report from a process forked after it was registered."""
    import signal

    from potatoq import Potatoq

    agent_log = tmp_path / "newrelic.log"
    config = tmp_path / "newrelic.ini"
    config.write_text(
        f"[newrelic]\napp_name = potatoq-test\ndeveloper_mode = true\nlog_file = {agent_log}\nlog_level = info\n"
    )
    broker = f"sqlite:///{tmp_path}/broker.db"
    env = {**os.environ, "PYTHONPATH": str(TESTS), "TEST_BROKER": broker, "TEST_NEWRELIC_INI": str(config)}
    env.pop("DJANGO_SETTINGS_MODULE", None)
    producer = Potatoq("nrworkerapp", broker=broker, set_as_current=False)
    add = producer.task(name="nrworkerapp.add")(lambda x, y: x + y)
    hang = producer.task(name="nrworkerapp.hang")(lambda: None)
    # Killed by its time limit, which the main process reports; then one task per new process.
    hung = hang.apply_async(queue="default")
    results = [add.delay(i, i) for i in range(3)]
    with open(tmp_path / "worker.log", "w") as log:
        worker = subprocess.Popen(
            [sys.executable, "-m", "potatoq.cli", "-A", "nrworkerapp", "worker", "-c", "1"],
            env=env, cwd=TESTS, stdout=log, stderr=subprocess.STDOUT,
        )  # fmt: skip
        try:
            assert [r.get(timeout=30) for r in results] == [0, 2, 4]
            assert hung.state == "FAILURE"
        finally:
            worker.send_signal(signal.SIGTERM)
            assert worker.wait(timeout=30) == 0, (tmp_path / "worker.log").read_text()
            producer.close()
    text = agent_log.read_text()
    assert "process different to where the agent was already registered" not in text
    assert "No data will be reported" not in text
