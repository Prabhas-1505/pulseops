"""Measures real consumer-lag growth and drain time when the processor is
stopped mid-load, not during idle. Starts a Locust run in the background,
stops `processor` partway through, lets lag build for a fixed window,
restarts it, and keeps sampling lag until it drains to 0 (or a timeout).

Every number here comes from actually running this against the live stack.

Usage:
    python load/failure_under_load.py --run-time 60 --outage-at 15 --outage-duration 15
"""

from __future__ import annotations

import argparse
import json
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = REPO_ROOT / "load" / "results"
CONSUMER_GROUP = "pulseops-event-normalizer"


def compose(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", *args], cwd=REPO_ROOT, capture_output=True, text=True)


def consumer_group_total_lag() -> int | None:
    result = subprocess.run(
        [
            "docker", "exec", "pulseops-kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "localhost:9092",
            "--describe", "--group", CONSUMER_GROUP,
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


def run_locust_background(run_time_s: int, csv_prefix: Path) -> subprocess.Popen:
    cmd = [
        "locust", "-f", str(REPO_ROOT / "load" / "locustfile.py"),
        "--host", "http://localhost:8000",
        "--users", "20", "--spawn-rate", "10",
        "--run-time", f"{run_time_s}s",
        "--headless", "--csv", str(csv_prefix),
    ]
    return subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-time", type=int, default=60)
    parser.add_argument("--outage-at", type=int, default=15, help="seconds into the run to stop the processor")
    parser.add_argument("--outage-duration", type=int, default=15, help="seconds the processor stays stopped")
    parser.add_argument("--lag-sample-interval", type=float, default=2.0)
    args = parser.parse_args()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    csv_prefix = RESULTS_DIR / f"failure_{run_id}"

    lag_timeline: list[dict] = []
    stop_event = threading.Event()

    def lag_sampler():
        start = time.monotonic()
        while not stop_event.is_set():
            lag_timeline.append({"t": round(time.monotonic() - start, 1), "lag": consumer_group_total_lag()})
            stop_event.wait(args.lag_sample_interval)

    sampler = threading.Thread(target=lag_sampler)
    sampler.start()

    print(f"Starting {args.run_time}s load; processor will be stopped at t={args.outage_at}s for {args.outage_duration}s")
    proc = run_locust_background(args.run_time, csv_prefix)

    time.sleep(args.outage_at)
    print(f"t={args.outage_at}s: stopping processor")
    compose("stop", "processor")

    time.sleep(args.outage_duration)
    print(f"t={args.outage_at + args.outage_duration}s: restarting processor")
    restart_time = time.monotonic()
    compose("start", "processor")

    proc.wait()
    print("load generation finished, continuing to sample lag until it drains or times out")

    drain_deadline = time.monotonic() + 120
    drained_at = None
    while time.monotonic() < drain_deadline:
        lag = consumer_group_total_lag()
        if lag == 0:
            drained_at = time.monotonic()
            break
        time.sleep(args.lag_sample_interval)

    stop_event.set()
    sampler.join()

    drain_time_after_restart_s = round(drained_at - restart_time, 1) if drained_at else None
    peak_lag = max((p["lag"] for p in lag_timeline if p["lag"] is not None), default=None)

    summary = {
        "run_id": run_id,
        "config": vars(args),
        "lag_timeline": lag_timeline,
        "peak_lag_during_outage": peak_lag,
        "drain_time_after_restart_s": drain_time_after_restart_s,
    }
    summary_path = RESULTS_DIR / f"failure_{run_id}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"\n=== Summary written to {summary_path} ===")
    print(json.dumps({k: v for k, v in summary.items() if k != "lag_timeline"}, indent=2))
    print("\nlag timeline (t_seconds, lag):")
    for point in lag_timeline:
        print(f"  t={point['t']:>6.1f}s  lag={point['lag']}")


if __name__ == "__main__":
    main()
