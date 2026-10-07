"""Throughput of no-op tasks: Potatoq vs Celery (defaults), concurrency 4.

    uv run --with 'celery[redis]' python benchmarks/throughput.py

Enqueues N tasks, starts a worker, and measures tasks/s between the first and the
last task finishing (worker start-up excluded). Each task INCRs a Redis counter.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import redis

HERE = Path(__file__).parent
N = int(os.environ.get("BENCH_N", "5000"))
CONCURRENCY = os.environ.get("BENCH_C", "4")
counter = redis.Redis.from_url("redis://localhost:6379/13")

TARGETS = [
    ("potatoq", "sqlite", f"sqlite:///{HERE}/bench.db"),
    ("potatoq", "postgres", "postgresql://localhost/potatoq_test"),
    ("potatoq", "redis", "redis://localhost:6379/13"),
    ("potatoq", "rabbitmq", "amqp://guest:guest@localhost:5672//"),
    ("celery", "redis", "redis://localhost:6379/13"),
    ("celery", "rabbitmq", "amqp://guest:guest@localhost:5672//"),
]


def reset(kind: str, url: str) -> None:
    if kind == "sqlite":
        for suffix in ("", "-wal", "-shm"):
            Path(f"{HERE}/bench.db{suffix}").unlink(missing_ok=True)
    elif kind == "redis":
        client = redis.Redis.from_url(url)
        keys = [*client.scan_iter("bench*", count=1000)]
        if keys:
            client.delete(*keys)
    elif kind == "postgres":
        import psycopg

        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute("DROP SCHEMA IF EXISTS bench CASCADE")
    elif kind == "rabbitmq":
        import pika

        conn = pika.BlockingConnection(pika.URLParameters("amqp://guest:guest@localhost:5672/%2F"))
        ch = conn.channel()
        for q in ("bench_potatoq", "bench_potatoq.dlq", "bench_celery"):
            try:
                ch.queue_delete(q)
            except Exception:
                ch = conn.channel()
        conn.close()


def run(lib: str, kind: str, url: str) -> tuple[float, float]:
    reset(kind, url)
    counter.delete("bench:count")
    env = {**os.environ, "BENCH_BROKER": url, "PYTHONPATH": str(HERE)}
    if lib == "celery":
        # Celery 5.6's prefork pool fails on macOS + Python 3.13 without this
        # ("not enough values to unpack (expected 3, got 0)").
        env["FORKED_BY_MULTIPROCESSING"] = "1"
    module = f"tasks_{lib}"
    enqueue = (
        f"import time; from {module} import noop, app; t=time.perf_counter(); "
        f"[noop.delay(i) for i in range({N})]; print(time.perf_counter()-t)"
    )
    enqueue_s = float(subprocess.check_output([sys.executable, "-c", enqueue], env=env, cwd=HERE).decode().split()[-1])
    if lib == "potatoq":
        cmd = [sys.executable, "-m", "potatoq.cli", "-A", module, "worker", "-c", CONCURRENCY, "-l", "warning"]
    else:
        cmd = [
            sys.executable,
            "-m",
            "celery",
            "-A",
            module,
            "worker",
            "-c",
            CONCURRENCY,
            "-l",
            "warning",
            "--without-gossip",
            "--without-mingle",
            "--without-heartbeat",
        ]
    worker = subprocess.Popen(cmd, env=env, cwd=HERE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.time() + 300
        while int(counter.get("bench:count") or 0) < 1 and time.time() < deadline:
            time.sleep(0.001)
        first = time.perf_counter()
        while int(counter.get("bench:count") or 0) < N and time.time() < deadline:
            time.sleep(0.005)
        elapsed = time.perf_counter() - first
        if int(counter.get("bench:count") or 0) < N:
            raise RuntimeError(f"{lib}/{kind}: worker didn't finish within 300s")
    finally:
        worker.terminate()
        worker.wait(30)
    return N / enqueue_s, (N - 1) / elapsed


def cleanup() -> None:
    for kind, url in (("redis", "redis://localhost:6379/13"), ("rabbitmq", None), ("sqlite", None)):
        reset(kind, url)
    counter.delete("bench:count")


def main() -> None:
    print(f"N={N} no-op tasks, concurrency={CONCURRENCY}, Python {sys.version.split()[0]}")
    print(f"{'library':<10}{'broker':<10}{'enqueue/s':>12}{'process/s':>12}")
    only = os.environ.get("BENCH_ONLY")
    for lib, kind, url in TARGETS:
        if only and lib != only:
            continue
        if lib == "celery":
            try:
                import celery  # noqa: F401
            except ImportError:
                continue
        enq, proc = run(lib, kind, url)
        print(f"{lib:<10}{kind:<10}{enq:>12,.0f}{proc:>12,.0f}", flush=True)
    cleanup()


if __name__ == "__main__":
    main()
