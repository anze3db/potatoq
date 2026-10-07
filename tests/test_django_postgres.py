"""Django on Postgres: the broker is the Django database and tasks join transactions."""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from conftest import POSTGRES_URL, postgres_available

SCRIPT = r"""
import django
from django.db import transaction
django.setup()
from potatoq import current_app
from potatoq.testing import drain
from djangoproj.shop.tasks import audit

app = current_app()
assert app.conf.broker_url.startswith("postgresql://"), app.conf.broker_url
app.broker.setup()
with transaction.atomic():
    audit.apply_async(("in-txn",), enqueue_on_commit=True)
    assert app.broker.queue_sizes() == {}, "visible before commit"
assert app.broker.queue_sizes() == {"shop": 1}
try:
    with transaction.atomic():
        audit.apply_async(("rolled-back",), enqueue_on_commit=True)
        raise RuntimeError
except RuntimeError:
    pass
assert app.broker.queue_sizes() == {"shop": 1}
[task] = drain(app, ["shop"])
assert task.result == "in-txn", task
print("OK")
"""


def test_django_postgres_transactional_enqueue():
    if not postgres_available() or "localhost" not in POSTGRES_URL:
        pytest.skip("local Postgres not available")
    schema = f"test_dj_{uuid.uuid4().hex[:8]}"
    env = {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "djangoproj.settings_pg",
        "TEST_PG_SCHEMA": schema,
        "PYTHONPATH": str(Path(__file__).parent),
    }
    try:
        out = subprocess.run([sys.executable, "-c", SCRIPT], env=env, capture_output=True, text=True, timeout=60)
        assert out.returncode == 0 and "OK" in out.stdout, out.stderr[-3000:]
    finally:
        import psycopg

        with psycopg.connect(POSTGRES_URL, autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
