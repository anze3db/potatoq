"""CPU-bound tasks: processes vs threads, with and without the GIL.

    uv run python benchmarks/cpu.py
    BENCH_PYTHONS=3.15,3.15t uv run python benchmarks/cpu.py

Runs a pure-Python CPU-bound task (about 40 ms each on 3.14) on a Redis broker, for each
interpreter, as 1 process, 4 processes (`-c 4`) and 1 process with 4 threads
(`-c 1 -t 4`). Reports tasks/s, the speedup over 1 process, and the memory (RSS) of
the whole worker: supervisor plus children. Each interpreter gets a temporary
virtualenv (`uv venv`) with this checkout installed.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import redis

HERE = Path(__file__).parent
REPO = HERE.parent
URL = "redis://localhost:6379/13"
N = int(os.environ.get("BENCH_N", "400"))
WORK = int(os.environ.get("BENCH_WORK", "1500000"))
PYTHONS = os.environ.get("BENCH_PYTHONS", "3.14,3.14t").split(",")
CONFIGS = [
    ("1 process", ["-c", "1"]),
    ("4 processes", ["-c", "4"]),
    ("4 threads", ["-c", "1", "-t", "4"]),
]
counter = redis.Redis.from_url(URL)


def python(version: str, root: Path) -> str:
    """The interpreter of a fresh environment with this checkout and redis."""
    venv = root / version
    subprocess.run(["uv", "venv", "--quiet", "--python", version, str(venv)], check=True)
    executable = str(venv / "bin" / "python")
    subprocess.run(["uv", "pip", "install", "--quiet", "--python", executable, "-e", str(REPO), "redis"], check=True)
    return executable


def reset() -> None:
    keys = [*counter.scan_iter("potatoq-bench*", count=1000)]
    if keys:
        counter.delete(*keys)


def tree_rss_mb(pid: int) -> float:
    pids, todo = [], [pid]
    while todo:
        p = todo.pop()
        pids.append(p)
        out = subprocess.run(["pgrep", "-P", str(p)], capture_output=True, text=True).stdout
        todo += [int(c) for c in out.split()]
    out = subprocess.run(["ps", "-o", "rss=", "-p", ",".join(map(str, pids))], capture_output=True, text=True)
    return sum(int(x) for x in out.stdout.split()) / 1024


def run(executable: str, args: list[str]) -> tuple[float, float, bool]:
    reset()
    env = {**os.environ, "BENCH_BROKER": URL, "PYTHONPATH": str(HERE)}
    enqueue = f"from tasks_cpu import burn\nfor _ in range({N}): burn.delay({WORK})"
    subprocess.run([sys.executable, "-c", enqueue], env=env, cwd=HERE, check=True)
    cmd = [executable, "-m", "potatoq.cli", "-A", "tasks_cpu", "worker", *args, "-l", "warning", "--no-scheduler"]
    worker = subprocess.Popen(cmd, env=env, cwd=HERE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.time() + 600
        while int(counter.get("potatoq-bench:count") or 0) < 1 and time.time() < deadline:
            time.sleep(0.001)
        first = time.perf_counter()
        rss = 0.0
        while (done := int(counter.get("potatoq-bench:count") or 0)) < N and time.time() < deadline:
            if not rss and done > N // 2:
                rss = tree_rss_mb(worker.pid)
            time.sleep(0.005)
        elapsed = time.perf_counter() - first
        if done < N:
            raise RuntimeError(f"{executable} {args}: worker didn't finish within 600s")
    finally:
        worker.terminate()
        worker.wait(30)
    return (N - 1) / elapsed, rss, counter.get("potatoq-bench:gil") == b"1"


def main() -> None:
    print(f"N={N} CPU-bound tasks (range({WORK:,}) loop), Redis broker")
    print(f"{'python':<8}{'GIL':<5}{'workers':<14}{'tasks/s':>9}{'speedup':>9}{'RSS MB':>8}")
    with tempfile.TemporaryDirectory() as root:
        for version in PYTHONS:
            report(version, python(version, Path(root)))
    reset()


def report(version: str, executable: str) -> None:
    baseline = None
    for name, args in CONFIGS:
        rate, rss, gil = run(executable, args)
        baseline = baseline or rate
        gil_s = "on" if gil else "off"
        print(f"{version:<8}{gil_s:<5}{name:<14}{rate:>9.1f}{rate / baseline:>8.1f}x{rss:>8.0f}", flush=True)


if __name__ == "__main__":
    main()
