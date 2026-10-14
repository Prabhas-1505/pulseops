from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import Connection, text


@dataclass
class WindowMetrics:
    service: str
    window_start: datetime
    window_end: datetime
    count: int
    error_count: int
    median_latency_ms: float | None
    trace_ids: list[str] = field(default_factory=list)


# A "business request" is the single server span FastAPIInstrumentor
# creates per incoming HTTP request -- identifiable by having an
# `http.route` attribute (verified against real data: the per-request ASGI
# "http send"/"http receive" sub-spans and the outbound httpx client spans
# never carry `http.route`, only the one true server span per request does).
# /admin/* calls are test/control-plane traffic, not business traffic.
_WINDOW_QUERY = text(
    """
    SELECT trace_id, duration_ms, (attributes->>'http.status_code')::int AS status_code
    FROM events
    WHERE service = :service
      AND event_timestamp >= :window_start
      AND event_timestamp < :window_end
      AND attributes ? 'http.route'
      AND attributes->>'http.route' NOT LIKE '/admin%'
    """
)

_DISTINCT_SERVICES_QUERY = text("SELECT DISTINCT service FROM events")


def fetch_known_services(conn: Connection) -> list[str]:
    return [row[0] for row in conn.execute(_DISTINCT_SERVICES_QUERY)]


def compute_window_metrics(conn: Connection, service: str, window_start: datetime, window_end: datetime) -> WindowMetrics:
    rows = conn.execute(_WINDOW_QUERY, {"service": service, "window_start": window_start, "window_end": window_end}).fetchall()

    error_count = sum(1 for _, _, status_code in rows if status_code is not None and status_code >= 500)
    durations = [duration for _, duration, _ in rows if duration is not None]
    trace_ids = sorted({trace_id for trace_id, _, _ in rows})

    return WindowMetrics(
        service=service,
        window_start=window_start,
        window_end=window_end,
        count=len(rows),
        error_count=error_count,
        median_latency_ms=statistics.median(durations) if durations else None,
        trace_ids=trace_ids,
    )
