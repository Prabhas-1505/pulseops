"""Runs a real Locust load test against the live gateway and records actual
measured results: throughput, p50/p95/p99 latency (from Locust's own stats),
Kafka consumer lag before/during/after, and per-container CPU/memory
(`docker stats`) sampled throughout the run.

This does not invent any numbers -- every figure in its output comes from
a tool (Locust, `docker stats`, `kafka-consumer-groups.sh`) actually
observing the live system during a real run. Intended to be executed once
and the output transcribed into docs/BENCHMARK_RESULTS.md, not run in CI.

Usage:
    python load/run_benchmark.py --users 30 --spawn-rate 10 --run-time 60
"""

from __future__ import annotations

import argparse
import csv
import json
import platform
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = REPO_ROOT / "load" / "results"
CONTAINERS = [
    "pulseops-gateway",
    "pulseops-orders",
    "pulseops-payments",
    "pulseops-kafka",
    "pulseops-postgres",
    "pulseops-processor",
    "pulseops-detector",
]
CONSUMER_GROUP = "pulseops-event-normalizer"


def hardware_info() -> dict:
    info = {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
        "cpu_count_logical": None,
        "cpu_brand": None,
        "memory_bytes": None,
    }
    import os

    info["cpu_count_logical"] = os.cpu_count()
    try:
        info["cpu_brand"] = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True, check=True
        ).stdout.strip()
        info["memory_bytes"] = int(
            subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True, check=True).stdout.strip()
        )
    except Exception:
        pass  # non-macOS host; fields stay None rather than guessed
    return info


def consumer_group_lag(group: str) -> int | None:
    result = subprocess.run(
        [
            "docker", "exec", "pulseops-kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "localhost:9092",
            "--describe", "--group", group,
        ],
        capture_output=True, text=True,
    )
    lines = [
        l for l in result.stdout.splitlines()
        if l.strip() and not l.startswith("GROUP") and "Warning" not in l and "rebalancing" not in l
    ]
    if not lines:
        return None
    total = 0
    for line in lines:
        parts = line.split()
        lag = parts[5] if len(parts) > 5 else "-"
        if lag == "-":
            continue
        total += int(lag)
    return total


def docker_stats_snapshot() -> dict[str, dict]:
    result = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}},{{.CPUPerc}},{{.MemUsage}}", *CONTAINERS],
        capture_output=True, text=True,
    )
    snapshot = {}
    for line in result.stdout.strip().splitlines():
        parts = line.split(",")
        if len(parts) == 3:
            snapshot[parts[0]] = {"cpu_pct": parts[1], "mem_usage": parts[2]}
    return snapshot


def sample_docker_stats_loop(samples: list, stop_event: threading.Event, interval_s: float) -> None:
    while not stop_event.is_set():
        samples.append({"t": time.monotonic(), "stats": docker_stats_snapshot()})
        stop_event.wait(interval_s)


def run_locust(users: int, spawn_rate: int, run_time_s: int, csv_prefix: Path) -> None:
    cmd = [
        "locust", "-f", str(REPO_ROOT / "load" / "locustfile.py"),
        "--host", "http://localhost:8000",
        "--users", str(users),
        "--spawn-rate", str(spawn_rate),
        "--run-time", f"{run_time_s}s",
        "--headless",
        "--csv", str(csv_prefix),
        "--csv-full-history",
    ]
    subprocess.run(cmd, check=True, cwd=REPO_ROOT)


def parse_locust_stats(csv_prefix: Path) -> dict:
    stats_file = csv_prefix.with_name(csv_prefix.name + "_stats.csv")
    rows = list(csv.DictReader(stats_file.open()))
    aggregated = next((r for r in rows if r["Name"] == "Aggregated"), None)
    return aggregated or {}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--users", type=int, default=30)
    parser.add_argument("--spawn-rate", type=int, default=10)
    parser.add_argument("--run-time", type=int, default=60, help="seconds")
    parser.add_argument("--stats-interval", type=float, default=5.0, help="docker stats sampling interval, seconds")
    args = parser.parse_args()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    csv_prefix = RESULTS_DIR / f"run_{run_id}"

    print("=== Hardware ===")
    hw = hardware_info()
    print(json.dumps(hw, indent=2))

    print("\n=== Pre-test state ===")
    lag_before = consumer_group_lag(CONSUMER_GROUP)
    print(f"consumer group lag before: {lag_before}")
    stats_before = docker_stats_snapshot()
    print(f"docker stats before: {json.dumps(stats_before, indent=2)}")

    stats_samples: list = []
    stop_event = threading.Event()
    sampler = threading.Thread(target=sample_docker_stats_loop, args=(stats_samples, stop_event, args.stats_interval))
    sampler.start()

    print(f"\n=== Running Locust: {args.users} users, spawn-rate {args.spawn_rate}, {args.run_time}s ===")
    start = time.monotonic()
    run_locust(args.users, args.spawn_rate, args.run_time, csv_prefix)
    wall_time_s = time.monotonic() - start

    stop_event.set()
    sampler.join()

    print("\n=== Post-test state ===")
    lag_immediately_after = consumer_group_lag(CONSUMER_GROUP)
    print(f"consumer group lag immediately after: {lag_immediately_after}")

    drain_start = time.monotonic()
    while True:
        lag = consumer_group_lag(CONSUMER_GROUP)
        if lag == 0 or lag is None:
            break
        if time.monotonic() - drain_start > 120:
            print("drain did not reach 0 within 120s, giving up on measuring drain time")
            break
        time.sleep(2)
    drain_time_s = time.monotonic() - drain_start

    locust_stats = parse_locust_stats(csv_prefix)

    summary = {
        "run_id": run_id,
        "hardware": hw,
        "config": {"users": args.users, "spawn_rate": args.spawn_rate, "run_time_s": args.run_time},
        "wall_time_s": round(wall_time_s, 2),
        "locust_aggregated_stats": locust_stats,
        "kafka_consumer_lag": {
            "before": lag_before,
            "immediately_after": lag_immediately_after,
            "drain_time_s": round(drain_time_s, 2) if lag_immediately_after else None,
        },
        "docker_stats_samples": stats_samples,
    }

    summary_path = RESULTS_DIR / f"run_{run_id}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, default=str))

    print(f"\n=== Summary written to {summary_path} ===")
    print(json.dumps({k: v for k, v in summary.items() if k != "docker_stats_samples"}, indent=2, default=str))


if __name__ == "__main__":
    main()
