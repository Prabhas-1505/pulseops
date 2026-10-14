"""Phase 2 recovery tests, run against the live docker-compose stack.

Three real scenarios, each actually exercised (not simulated):

1. Replay/dedup: stop the processor, reset its consumer-group offsets back
   to earliest, restart it, and confirm the Postgres row count is UNCHANGED
   -- i.e. replaying the entire topic does not duplicate business events.
2. Malformed message -> DLQ: produce a garbage (non-JSON) message directly
   to otel.raw.v1 and confirm it shows up on otel.dlq.v1 and the consumer
   group's lag still drains to zero (the poison pill didn't block the
   partition).
3. DB outage + retry: stop Postgres, send a real checkout request while it's
   down, confirm the processor retries/crash-restarts rather than silently
   dropping the event, then restart Postgres and confirm the event
   eventually lands with no data loss and no duplicate rows.

This talks to Docker directly (via `docker compose` / `docker exec`), so it
is intentionally not a unit test -- it is meant to be run by a human or CI
job against a real running stack, the same way kafka/smoke_test.py is.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import psycopg
from confluent_kafka import Consumer, Producer

REPO_ROOT = Path(__file__).resolve().parents[2]
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:8000")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_TOPIC = os.environ.get("KAFKA_RAW_TELEMETRY_TOPIC", "otel.raw.v1")
DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "otel.dlq.v1")
CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "pulseops-event-normalizer")

DB_HOST = os.environ.get("POSTGRES_HOST_FOR_TOOLS", "localhost")
DB_PORT = os.environ.get("POSTGRES_HOST_PORT", "55432")
DB_USER = os.environ.get("POSTGRES_USER", "pulseops")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "change-me-in-local-env")
DB_NAME = os.environ.get("POSTGRES_DB", "pulseops")

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}: {name}" + (f" -- {detail}" if detail else ""))


def db_conn() -> psycopg.Connection:
    return psycopg.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME)


def db_count(where: str = "") -> int:
    sql = "SELECT count(*) FROM events" + (f" WHERE {where}" if where else "")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def compose(*args: str, check_rc: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(["docker", "compose", *args], cwd=REPO_ROOT, capture_output=True, text=True)
    if check_rc and result.returncode != 0:
        raise RuntimeError(f"docker compose {' '.join(args)} failed: {result.stderr}")
    return result


def kafka_exec(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "exec", "pulseops-kafka", *args], capture_output=True, text=True
    )


def wait_until(predicate, timeout_s: float, interval_s: float = 1.0, description: str = "condition"):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return False


def consumer_group_total_lag(group: str) -> int | None:
    result = kafka_exec(
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", "localhost:9092",
        "--describe", "--group", group,
    )
    lines = [l for l in result.stdout.splitlines() if l.strip() and not l.startswith("GROUP") and "Warning" not in l and "rebalancing" not in l]
    if not lines:
        return None
    total = 0
    for line in lines:
        # columns: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
        parts = line.split()
        lag = parts[5] if len(parts) > 5 else "-"
        if lag == "-":
            continue
        total += int(lag)
    return total


def scenario_replay_does_not_duplicate() -> None:
    print("\n=== Scenario 1: consumer replay does not duplicate events ===")

    # Must be fully caught up before snapshotting `before` -- otherwise a
    # message produced just before this scenario started (e.g. by the
    # Phase 1 smoke test immediately preceding this in CI) could still be
    # unconsumed, and replaying would then insert it as a genuinely NEW row
    # rather than a duplicate, which looks identical to a real dedup bug if
    # you don't rule it out first.
    settled = wait_until(lambda: consumer_group_total_lag(CONSUMER_GROUP) == 0, timeout_s=30, interval_s=2)
    check("processor settled (caught up) before snapshotting pre-replay row count", settled)

    before = db_count()
    print(f"rows before replay: {before}")

    compose("stop", "processor")

    def try_reset() -> subprocess.CompletedProcess:
        return subprocess.run(
            [
                "docker", "exec", "pulseops-kafka",
                "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server", "localhost:9092",
                "--group", CONSUMER_GROUP,
                "--topic", RAW_TOPIC,
                "--reset-offsets", "--to-earliest", "--execute",
            ],
            capture_output=True, text=True,
        )

    def reset_succeeded(result: subprocess.CompletedProcess) -> bool:
        # kafka-consumer-groups.sh can print "Error: ... PreparingRebalance"
        # to stdout while STILL exiting 0 -- returncode alone is not
        # trustworthy here.
        return result.returncode == 0 and "Error" not in result.stdout

    # The stopped consumer doesn't send a LeaveGroup (SIGTERM, no signal
    # handler), so Kafka only notices via a missed-heartbeat session
    # timeout (tuned to 10s in processor/consumer.py). Retry the reset
    # until the group actually leaves "Stable" rather than guessing a sleep.
    reset = try_reset()
    reset_deadline = time.monotonic() + 30
    while not reset_succeeded(reset) and time.monotonic() < reset_deadline:
        time.sleep(2)
        reset = try_reset()
    check("consumer group offsets reset to earliest", reset_succeeded(reset), reset.stdout + reset.stderr)

    restart_time = subprocess.run(["date", "-u", "+%Y-%m-%dT%H:%M:%SZ"], capture_output=True, text=True).stdout.strip()
    compose("start", "processor")
    drained = wait_until(lambda: consumer_group_total_lag(CONSUMER_GROUP) == 0, timeout_s=60, interval_s=2)
    check("processor drained the replayed backlog", drained, "lag did not reach 0 within 60s")

    # Prove the backlog was actually REPROCESSED (not just skipped): sum the
    # "duplicates=" counts the processor logged since the restart and check
    # it accounts for the full pre-replay row count. Python's logging module
    # writes to stderr by default, and `docker logs` keeps stdout/stderr as
    # separate streams -- capture both or this silently sees nothing.
    logs_result = subprocess.run(
        ["docker", "logs", "--since", restart_time, "pulseops-processor"], capture_output=True, text=True
    )
    logs = logs_result.stdout + logs_result.stderr
    total_duplicates = sum(int(part.split("=")[1]) for line in logs.splitlines() if "duplicates=" in line for part in line.split() if part.startswith("duplicates="))
    check(
        "replayed backlog was actually reprocessed and deduped (not just skipped)",
        total_duplicates == before,
        f"processor logged {total_duplicates} duplicates since replay restart, expected {before}",
    )

    after = db_count()
    check("row count unchanged after full topic replay", after == before, f"before={before} after={after}")


def scenario_malformed_message_goes_to_dlq() -> None:
    print("\n=== Scenario 2: malformed message is dead-lettered, not dropped or blocking ===")
    settled = wait_until(lambda: consumer_group_total_lag(CONSUMER_GROUP) == 0, timeout_s=30, interval_s=2)
    check("processor settled (caught up) before injecting the poison pill", settled)

    marker = f"garbage-{uuid.uuid4().hex[:8]}".encode()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    producer.produce(RAW_TOPIC, value=b"not valid json {{{ " + marker)
    producer.flush(5)

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"recovery-test-dlq-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([DLQ_TOPIC])
    found = False
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline and not found:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            if marker in (msg.value() or b""):
                found = True
    finally:
        consumer.close()
    check("malformed message landed on DLQ topic with original bytes intact", found)

    drained = wait_until(lambda: consumer_group_total_lag(CONSUMER_GROUP) == 0, timeout_s=30, interval_s=2)
    check("consumer group lag still drains after the poison pill (partition not blocked)", drained)


def scenario_db_outage_retried_not_dropped() -> None:
    print("\n=== Scenario 3: Postgres outage is retried, not silently dropped ===")
    before = db_count()

    compose("stop", "postgres")
    marker_item = f"outage-recovery-{uuid.uuid4().hex[:8]}"
    try:
        resp = httpx.post(f"{GATEWAY_URL}/checkout", json={"item": marker_item, "amount_cents": 999}, timeout=15.0)
        check("checkout still succeeds at the HTTP layer while DB is down", resp.status_code == 200, f"got {resp.status_code}")
    except httpx.RequestError as exc:
        check("checkout still succeeds at the HTTP layer while DB is down", False, str(exc))

    print("waiting ~25s for the processor to exhaust retries against the dead DB ...")
    time.sleep(25)
    restart_count = subprocess.run(
        ["docker", "inspect", "pulseops-processor", "--format", "{{.RestartCount}}"],
        capture_output=True, text=True,
    ).stdout.strip()
    check(
        "processor restarted (crash-and-retry) rather than hanging forever",
        restart_count.isdigit() and int(restart_count) >= 1,
        f"RestartCount={restart_count}",
    )

    compose("start", "postgres")
    compose("start", "processor")

    def marker_persisted() -> bool:
        try:
            return db_count(f"attributes->>'pulseops.item' = '{marker_item}'") > 0
        except psycopg.OperationalError:
            return False

    recovered = wait_until(marker_persisted, timeout_s=60, interval_s=2)
    check("event eventually persisted after Postgres recovers (no data loss)", recovered)

    if not recovered:
        return

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT trace_id FROM events WHERE attributes->>'pulseops.item' = %s LIMIT 1", (marker_item,))
        trace_id = cur.fetchone()[0]

    def trace_span_counts() -> tuple[int, int]:
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*), count(DISTINCT event_id) FROM events WHERE trace_id = %s", (trace_id,)
            )
            return cur.fetchone()

    total_1, distinct_1 = trace_span_counts()
    check(
        "every span for this request has a unique event_id (no duplicate rows at this point)",
        total_1 == distinct_1,
        f"total={total_1} distinct={distinct_1}",
    )

    # If the crash/retry cycle were re-inserting rows, waiting and
    # re-querying would show the count creeping up. It shouldn't.
    time.sleep(8)
    total_2, distinct_2 = trace_span_counts()
    check(
        "span count for this trace is stable after settling (no late duplicate inserts from the crash-retry cycle)",
        total_2 == total_1 and total_2 == distinct_2,
        f"immediately-after total={total_1}, after 8s total={total_2}",
    )

    after = db_count()
    check(
        "overall row count only grew by this request's own spans, nothing duplicated",
        after - before == total_2,
        f"before={before} after={after} this_trace_spans={total_2}",
    )


def main() -> int:
    scenario_replay_does_not_duplicate()
    scenario_malformed_message_goes_to_dlq()
    scenario_db_outage_retried_not_dropped()

    print("\n--- Summary ---")
    for name, ok, _ in results:
        print(f"{'PASS' if ok else 'FAIL'}: {name}")
    failed = [n for n, ok, _ in results if not ok]
    if failed:
        print(f"\n{len(failed)} check(s) failed.")
        return 1
    print(f"\nAll {len(results)} checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
