"""Deterministic fault-injection evaluation harness for the Phase 3 detector.

Drives labeled healthy/faulty traffic windows against the LIVE stack (the
`detector` container doing the actual detecting, not a mocked call) and
checks:

1. Healthy traffic produces no false-positive incidents.
2. A real, sustained payments error-rate fault produces an open incident
   correlated across gateway+orders+payments (via trace_id overlap) within
   a bounded wait.
3. After the fault clears and healthy traffic resumes, that incident
   transitions to resolved once nothing new has been seen for the
   configured grace period.
4. A real, sustained latency fault produces a *separate* "latency" alert
   (distinct metric_type from the error-rate case above).

The fifth required property -- correlation must NOT merge unrelated
failures -- is deliberately NOT re-tested here with live traffic: this
repo's three demo services are always causally chained
(gateway->orders->payments), so there is no way to produce a genuinely
*unrelated* failure from real traffic. That property is covered
deterministically with fabricated data in
`tests/phase3/test_correlate.py::test_unrelated_service_with_no_shared_trace_id_does_not_merge`,
which is a more reliable way to test it than hoping live timing cooperates.

Everything here reads state via `incidents.id > baseline_marker` (recorded
at harness start) rather than truncating tables, so it doesn't disturb data
from Phase 1/2 test runs.
"""

from __future__ import annotations

import os
import sys
import time
import uuid

import httpx
import psycopg

GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:8000")
PAYMENTS_URL = os.environ.get("PAYMENTS_URL", "http://localhost:8002")

DB_HOST = os.environ.get("POSTGRES_HOST_FOR_TOOLS", "localhost")
DB_PORT = os.environ.get("POSTGRES_HOST_PORT", "55432")
DB_USER = os.environ.get("POSTGRES_USER", "pulseops")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "change-me-in-local-env")
DB_NAME = os.environ.get("POSTGRES_DB", "pulseops")

# Must stay consistent with detector/config.py defaults for the wait
# budgets below to make sense.
WINDOW_SECONDS = 10
RESOLUTION_GRACE_SECONDS = 40
PIPELINE_LATENCY_BUDGET_S = 10  # span export + collector batch + kafka + processor insert

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}: {name}" + (f" -- {detail}" if detail else ""))


def db_conn() -> psycopg.Connection:
    return psycopg.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME)


def current_max_incident_id() -> int:
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT coalesce(max(id), 0) FROM incidents")
        return cur.fetchone()[0]


def incidents_since(marker_id: int):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, signature, affected_services, severity, status FROM incidents WHERE id > %s ORDER BY id",
            (marker_id,),
        )
        return cur.fetchall()


def wait_until(predicate, timeout_s: float, interval_s: float = 2.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return False


def drive_checkouts(n: int, marker_prefix: str, interval_s: float = 1.0) -> None:
    for i in range(n):
        httpx.post(
            f"{GATEWAY_URL}/checkout",
            json={"item": f"{marker_prefix}-{i}-{uuid.uuid4().hex[:6]}", "amount_cents": 100},
            timeout=15.0,
        )
        time.sleep(interval_s)


def main() -> int:
    httpx.post(f"{PAYMENTS_URL}/admin/faults/reset", timeout=5.0)
    marker = current_max_incident_id()
    print(f"baseline marker: incidents.id > {marker}")

    print("\n=== Phase A: healthy traffic should not open any incident ===")
    drive_checkouts(10, "eval-healthy-a", interval_s=1.2)
    time.sleep(WINDOW_SECONDS + PIPELINE_LATENCY_BUDGET_S)
    healthy_incidents = incidents_since(marker)
    check("no incidents opened during healthy traffic", len(healthy_incidents) == 0, f"found {len(healthy_incidents)}: {healthy_incidents}")

    print("\n=== Phase B: sustained payments error-rate fault should open a correlated incident ===")
    httpx.post(f"{PAYMENTS_URL}/admin/faults", json={"error_probability": 1.0, "error_status_code": 500}, timeout=5.0)
    drive_checkouts(6, "eval-fault-errors", interval_s=1.5)
    httpx.post(f"{PAYMENTS_URL}/admin/faults/reset", timeout=5.0)

    def error_incident_opened():
        rows = incidents_since(marker)
        return any(r[1] == "payments:error_rate" and r[4] == "open" for r in rows)

    opened = wait_until(error_incident_opened, timeout_s=WINDOW_SECONDS + PIPELINE_LATENCY_BUDGET_S + 15)
    check("payments error-rate fault opened an incident within the expected wait", opened)

    error_incident = None
    if opened:
        rows = [r for r in incidents_since(marker) if r[1] == "payments:error_rate"]
        error_incident = rows[-1] if rows else None
        affected = set(error_incident[2]) if error_incident else set()
        check(
            "the incident correlates all three causally-linked services (not 3 separate incidents)",
            affected == {"gateway", "orders", "payments"},
            f"affected_services={affected}",
        )
        check("severity reflects a 100% error rate as critical", error_incident[3] == "critical", f"severity={error_incident[3] if error_incident else None}")

    print("\n=== Phase C: incident resolves once healthy traffic resumes and grace period elapses ===")
    # Keep sending healthy traffic through the wait so the latency baseline
    # stays warm and doesn't itself look anomalous (which could re-open things).
    resolve_deadline = time.monotonic() + RESOLUTION_GRACE_SECONDS + PIPELINE_LATENCY_BUDGET_S + 20
    resolved = False
    while time.monotonic() < resolve_deadline and not resolved:
        drive_checkouts(2, "eval-healthy-c", interval_s=1.0)
        if error_incident:
            with db_conn() as conn, conn.cursor() as cur:
                cur.execute("SELECT status FROM incidents WHERE id = %s", (error_incident[0],))
                row = cur.fetchone()
                resolved = row is not None and row[0] == "resolved"
        time.sleep(3)
    check("error-rate incident transitions to resolved after the grace period", resolved or error_incident is None)

    print("\n=== Phase D: sustained latency fault should produce a distinct 'latency' alert ===")
    marker_d = current_max_incident_id()
    # Re-warm the baseline so there's something meaningful to exceed.
    drive_checkouts(8, "eval-latency-baseline", interval_s=1.2)
    time.sleep(WINDOW_SECONDS)

    httpx.post(f"{PAYMENTS_URL}/admin/faults", json={"latency_ms": 500, "latency_probability": 1.0}, timeout=5.0)
    drive_checkouts(6, "eval-fault-latency", interval_s=1.5)
    httpx.post(f"{PAYMENTS_URL}/admin/faults/reset", timeout=5.0)

    def latency_incident_opened():
        rows = incidents_since(marker_d)
        return any(r[1].endswith(":latency") for r in rows)

    latency_opened = wait_until(latency_incident_opened, timeout_s=WINDOW_SECONDS + PIPELINE_LATENCY_BUDGET_S + 15)
    check("sustained latency fault opened a distinct latency-type incident", latency_opened)

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
