"""Deterministic unit tests for detector/correlate.py -- pure functions, no stack required.

These are the authoritative tests for correlation correctness. They use
fabricated incidents/trace_ids so the "avoid merging unrelated failures"
property can be checked precisely, which isn't reliably reproducible from
live traffic alone (the real demo services are always causally chained).
"""

from datetime import datetime, timedelta, timezone

from detector.correlate import OpenIncidentView, correlate

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
DEDUP_WINDOW = timedelta(seconds=30)


def test_no_open_incidents_creates_new():
    result = correlate("payments", "error_rate", ["t1", "t2"], NOW, open_incidents=[], dedup_window=DEDUP_WINDOW)
    assert result.incident_id is None
    assert result.affected_services == ["payments"]


def test_same_signature_recent_incident_is_deduped_not_duplicated():
    existing = OpenIncidentView(
        id=42, signature="payments:error_rate", affected_services=["payments"],
        last_seen_at=NOW - timedelta(seconds=10), recent_trace_ids={"t9"},
    )
    result = correlate("payments", "error_rate", ["t1"], NOW, open_incidents=[existing], dedup_window=DEDUP_WINDOW)
    assert result.incident_id == 42
    assert result.affected_services == ["payments"]


def test_same_signature_but_expired_incident_creates_new():
    stale = OpenIncidentView(
        id=42, signature="payments:error_rate", affected_services=["payments"],
        last_seen_at=NOW - timedelta(seconds=60), recent_trace_ids={"t9"},
    )
    result = correlate("payments", "error_rate", ["t1"], NOW, open_incidents=[stale], dedup_window=DEDUP_WINDOW)
    assert result.incident_id is None


def test_different_service_with_shared_trace_id_merges() -> None:
    # Payments is already flagged; orders' window shares a trace_id with
    # payments (gateway -> orders -> payments really did happen together).
    payments_incident = OpenIncidentView(
        id=7, signature="payments:error_rate", affected_services=["payments"],
        last_seen_at=NOW - timedelta(seconds=5), recent_trace_ids={"trace-abc", "trace-def"},
    )
    result = correlate("orders", "latency", ["trace-abc", "trace-zzz"], NOW, open_incidents=[payments_incident], dedup_window=DEDUP_WINDOW)
    assert result.incident_id == 7
    assert result.affected_services == ["orders", "payments"]


def test_unrelated_service_with_no_shared_trace_id_does_not_merge():
    # A completely unrelated alert (no trace overlap with anything open)
    # must become its own incident, not get swept into an existing one.
    payments_incident = OpenIncidentView(
        id=7, signature="payments:error_rate", affected_services=["payments"],
        last_seen_at=NOW - timedelta(seconds=5), recent_trace_ids={"trace-abc", "trace-def"},
    )
    result = correlate("checkout-fraud-detector", "error_rate", ["trace-unrelated-1"], NOW, open_incidents=[payments_incident], dedup_window=DEDUP_WINDOW)
    assert result.incident_id is None
    assert result.affected_services == ["checkout-fraud-detector"]


def test_trace_overlap_outside_dedup_window_does_not_merge():
    stale = OpenIncidentView(
        id=7, signature="payments:error_rate", affected_services=["payments"],
        last_seen_at=NOW - timedelta(seconds=45), recent_trace_ids={"trace-abc"},
    )
    result = correlate("orders", "latency", ["trace-abc"], NOW, open_incidents=[stale], dedup_window=DEDUP_WINDOW)
    assert result.incident_id is None
