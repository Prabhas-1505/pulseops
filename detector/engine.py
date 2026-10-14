"""Wires detector/metrics.py + rules.py + correlate.py to Postgres.

One call to `run_once(conn, now)` is one detection tick: for every service
seen in the events table, compute this window's metrics, check both rules,
update the latency baseline, persist any anomalies as alerts, correlate
each new alert into an incident (new or existing), and resolve incidents
that have gone quiet.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

from sqlalchemy import Connection, text

from detector.config import DEDUP_WINDOW_SECONDS, RESOLUTION_GRACE_SECONDS, WINDOW_SECONDS
from detector.correlate import CorrelationResult, OpenIncidentView, correlate
from detector.metrics import WindowMetrics, compute_window_metrics, fetch_known_services
from detector.rules import Anomaly, check_error_rate, check_latency, update_ewma

_GET_BASELINE = text("SELECT ewma_latency_ms FROM service_baselines WHERE service = :service")
_UPSERT_BASELINE = text(
    """
    INSERT INTO service_baselines (service, ewma_latency_ms, updated_at)
    VALUES (:service, :ewma_latency_ms, :now)
    ON CONFLICT (service) DO UPDATE SET ewma_latency_ms = :ewma_latency_ms, updated_at = :now
    """
)

_OPEN_INCIDENTS = text("SELECT id, signature, affected_services FROM incidents WHERE status = 'open'")
_RECENT_ALERT_TRACE_IDS = text(
    "SELECT trace_ids FROM alerts WHERE incident_id = :incident_id AND created_at >= :since"
)
_INCIDENT_LAST_SEEN = text("SELECT last_seen_at FROM incidents WHERE id = :id")

_CREATE_INCIDENT = text(
    """
    INSERT INTO incidents (signature, affected_services, severity, status, opened_at, last_seen_at)
    VALUES (:signature, CAST(:affected_services AS JSONB), :severity, 'open', :now, :now)
    RETURNING id
    """
)
_UPDATE_INCIDENT = text(
    """
    UPDATE incidents
    SET affected_services = CAST(:affected_services AS JSONB), last_seen_at = :now,
        severity = CASE WHEN :severity = 'critical' THEN 'critical' ELSE severity END
    WHERE id = :id
    """
)
_INSERT_ALERT = text(
    """
    INSERT INTO alerts (incident_id, service, metric_type, window_start, window_end,
                         observed_value, baseline_or_threshold, sample_count, explanation, trace_ids, created_at)
    VALUES (:incident_id, :service, :metric_type, :window_start, :window_end,
            :observed_value, :baseline_or_threshold, :sample_count, :explanation, CAST(:trace_ids AS JSONB), :now)
    RETURNING id
    """
)
_RESOLVE_STALE = text(
    """
    UPDATE incidents SET status = 'resolved', resolved_at = :now
    WHERE status = 'open' AND last_seen_at < :cutoff
    """
)


def severity_for(anomaly: Anomaly) -> str:
    if anomaly.metric_type == "error_rate" and anomaly.observed_value >= 0.5:
        return "critical"
    if anomaly.metric_type == "latency" and anomaly.baseline_or_threshold > 0 and anomaly.observed_value >= anomaly.baseline_or_threshold * 4:
        return "critical"
    return "warning"


def get_baseline(conn: Connection, service: str) -> float | None:
    row = conn.execute(_GET_BASELINE, {"service": service}).fetchone()
    return row[0] if row else None


def upsert_baseline(conn: Connection, service: str, value: float, now: datetime) -> None:
    conn.execute(_UPSERT_BASELINE, {"service": service, "ewma_latency_ms": value, "now": now})


def fetch_open_incidents(conn: Connection, now: datetime, lookback_seconds: int) -> list[OpenIncidentView]:
    since = now - timedelta(seconds=lookback_seconds)
    views = []
    for inc_id, signature, affected_services in conn.execute(_OPEN_INCIDENTS).fetchall():
        last_seen_row = conn.execute(_INCIDENT_LAST_SEEN, {"id": inc_id}).fetchone()
        last_seen_at = last_seen_row[0]
        trace_id_rows = conn.execute(_RECENT_ALERT_TRACE_IDS, {"incident_id": inc_id, "since": since}).fetchall()
        recent_trace_ids: set[str] = set()
        for (trace_ids,) in trace_id_rows:
            recent_trace_ids.update(trace_ids or [])
        views.append(
            OpenIncidentView(
                id=inc_id, signature=signature, affected_services=list(affected_services),
                last_seen_at=last_seen_at, recent_trace_ids=recent_trace_ids,
            )
        )
    return views


def persist_anomaly(conn: Connection, anomaly: Anomaly, metrics: WindowMetrics, now: datetime) -> tuple[int, int]:
    """Correlate, create/update the incident, insert the alert. Returns (alert_id, incident_id)."""
    open_incidents = fetch_open_incidents(conn, now, DEDUP_WINDOW_SECONDS)
    result: CorrelationResult = correlate(
        service=anomaly.service,
        metric_type=anomaly.metric_type,
        trace_ids=metrics.trace_ids,
        now=now,
        open_incidents=open_incidents,
        dedup_window=timedelta(seconds=DEDUP_WINDOW_SECONDS),
    )
    severity = severity_for(anomaly)

    if result.incident_id is None:
        incident_id = conn.execute(
            _CREATE_INCIDENT,
            {
                "signature": f"{anomaly.service}:{anomaly.metric_type}",
                "affected_services": json.dumps(result.affected_services),
                "severity": severity,
                "now": now,
            },
        ).scalar_one()
    else:
        incident_id = result.incident_id
        conn.execute(
            _UPDATE_INCIDENT,
            {"id": incident_id, "affected_services": json.dumps(result.affected_services), "now": now, "severity": severity},
        )

    alert_id = conn.execute(
        _INSERT_ALERT,
        {
            "incident_id": incident_id,
            "service": anomaly.service,
            "metric_type": anomaly.metric_type,
            "window_start": metrics.window_start,
            "window_end": metrics.window_end,
            "observed_value": anomaly.observed_value,
            "baseline_or_threshold": anomaly.baseline_or_threshold,
            "sample_count": metrics.count,
            "explanation": anomaly.explanation,
            "trace_ids": json.dumps(metrics.trace_ids),
            "now": now,
        },
    ).scalar_one()

    return alert_id, incident_id


def resolve_stale_incidents(conn: Connection, now: datetime) -> int:
    cutoff = now - timedelta(seconds=RESOLUTION_GRACE_SECONDS)
    return conn.execute(_RESOLVE_STALE, {"now": now, "cutoff": cutoff}).rowcount


def run_once(conn: Connection, now: datetime) -> list[int]:
    """One detection tick. Returns the list of alert ids created."""
    window_start = now - timedelta(seconds=WINDOW_SECONDS)
    created_alert_ids: list[int] = []

    for service in fetch_known_services(conn):
        metrics = compute_window_metrics(conn, service, window_start, now)

        baseline = get_baseline(conn, service)
        anomalies = [a for a in (check_error_rate(metrics), check_latency(metrics, baseline)) if a is not None]

        if metrics.count > 0 and metrics.median_latency_ms is not None:
            upsert_baseline(conn, service, update_ewma(baseline, metrics.median_latency_ms), now)

        for anomaly in anomalies:
            alert_id, _ = persist_anomaly(conn, anomaly, metrics, now)
            created_alert_ids.append(alert_id)

    resolve_stale_incidents(conn, now)
    return created_alert_ids
