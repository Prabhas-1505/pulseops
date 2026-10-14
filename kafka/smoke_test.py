"""Phase 1 smoke test.

Drives real HTTP traffic through gateway -> orders -> payments, then consumes
the Kafka topic the OTel Collector exports to (otel.raw.v1) and verifies that
spans for all three services, linked by trace_id, actually landed there. Also
exercises one fault-injection scenario (payments outage) end to end.

This is NOT the Phase 2 durable consumer -- it uses its own throwaway
consumer group so it never interferes with real processing consumers.

Usage: python kafka/smoke_test.py
Requires: the docker-compose stack already running (`docker compose up -d`),
gateway reachable at $GATEWAY_URL (default http://localhost:8000), payments
admin reachable at $PAYMENTS_URL (default http://localhost:8002), and Kafka
reachable at $KAFKA_BOOTSTRAP (default localhost:9092, the host listener).

Exit code 0 on success, 1 on failure. Prints a PASS/FAIL line per check.
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid

import httpx
from confluent_kafka import Consumer, KafkaException

from otlp_json import iter_spans

GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:8000")
PAYMENTS_URL = os.environ.get("PAYMENTS_URL", "http://localhost:8002")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_RAW_TELEMETRY_TOPIC", "otel.raw.v1")
CONSUME_TIMEOUT_S = 25

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}: {name}" + (f" -- {detail}" if detail else ""))


def wait_for_health(name: str, url: str, attempts: int = 30) -> bool:
    for _ in range(attempts):
        try:
            r = httpx.get(f"{url}/health", timeout=2.0)
            if r.status_code == 200:
                return True
        except httpx.RequestError:
            pass
        time.sleep(1)
    check(f"{name} became healthy", False, f"{url}/health did not return 200 in {attempts}s")
    return False


def consume_all(timeout_s: int) -> list[dict]:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"pulseops-smoke-test-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    records: list[dict] = []
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            try:
                records.append(json.loads(msg.value()))
            except json.JSONDecodeError:
                continue
    finally:
        consumer.close()
    return records


def main() -> int:
    if not wait_for_health("gateway", GATEWAY_URL):
        return 1
    if not wait_for_health("orders", os.environ.get("ORDERS_URL", "http://localhost:8001")):
        return 1
    if not wait_for_health("payments", PAYMENTS_URL):
        return 1

    # Make sure any previous fault state (from a prior manual run) is cleared.
    httpx.post(f"{PAYMENTS_URL}/admin/faults/reset", timeout=5.0)

    marker_item = f"smoke-item-{uuid.uuid4().hex[:10]}"
    checkout_resp = httpx.post(
        f"{GATEWAY_URL}/checkout",
        json={"item": marker_item, "amount_cents": 1234},
        timeout=15.0,
    )
    check("normal checkout returns 200", checkout_resp.status_code == 200, f"got {checkout_resp.status_code}: {checkout_resp.text}")
    if checkout_resp.status_code != 200:
        return 1
    body = checkout_resp.json()
    order_id = body["order_id"]
    charge_id = body["charge_id"]

    print(f"Consuming '{TOPIC}' for up to {CONSUME_TIMEOUT_S}s to find spans for order_id={order_id} ...")
    records = consume_all(CONSUME_TIMEOUT_S)

    spans = [s for record in records for s in iter_spans(record)]
    check("collector delivered at least one message to Kafka", len(records) > 0, f"{len(records)} kafka messages consumed")

    gateway_span = next((s for s in spans if s.service_name == "gateway" and s.attributes.get("pulseops.item") == marker_item), None)
    orders_span = next((s for s in spans if s.service_name == "orders" and s.attributes.get("pulseops.order_id") == order_id), None)
    payments_span = next((s for s in spans if s.service_name == "payments" and s.attributes.get("pulseops.charge_id") == charge_id), None)

    check("gateway span found with matching item marker", gateway_span is not None)
    check("orders span found with matching order_id", orders_span is not None)
    check("payments span found with matching charge_id", payments_span is not None)

    if gateway_span and orders_span and payments_span:
        same_trace = gateway_span.trace_id == orders_span.trace_id == payments_span.trace_id
        check(
            "all three spans share one trace_id (context propagation works)",
            same_trace,
            f"gateway={gateway_span.trace_id} orders={orders_span.trace_id} payments={payments_span.trace_id}",
        )

    # --- Fault injection scenario: payments outage should bubble up as 502 ---
    httpx.post(f"{PAYMENTS_URL}/admin/faults", json={"outage": True}, timeout=5.0)
    try:
        outage_resp = httpx.post(
            f"{GATEWAY_URL}/checkout",
            json={"item": "outage-check", "amount_cents": 500},
            timeout=15.0,
        )
        check(
            "payments outage fault bubbles up as gateway 502",
            outage_resp.status_code == 502,
            f"got {outage_resp.status_code}: {outage_resp.text}",
        )
    finally:
        httpx.post(f"{PAYMENTS_URL}/admin/faults/reset", timeout=5.0)

    failed = [name for name, ok, _ in results if not ok]
    print("\n--- Summary ---")
    for name, ok, detail in results:
        print(f"{'PASS' if ok else 'FAIL'}: {name}")
    if failed:
        print(f"\n{len(failed)} check(s) failed.")
        return 1
    print(f"\nAll {len(results)} checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
