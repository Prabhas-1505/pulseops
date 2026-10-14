from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from api import schemas
from api.deps import get_current_user, get_db
from api.rate_limit import RateLimiter
from detector.config import WINDOW_SECONDS
from detector.metrics import compute_window_metrics, fetch_known_services

router = APIRouter(
    prefix="/services",
    tags=["services"],
    dependencies=[Depends(get_current_user), Depends(RateLimiter(times=60, seconds=60))],
)

_RECENT_WINDOW_SECONDS = 60  # wider than the detector's own 10s window, for a steadier dashboard reading


@router.get("/health", response_model=list[schemas.ServiceHealth])
def services_health(db: Session = Depends(get_db)) -> list[schemas.ServiceHealth]:
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(seconds=_RECENT_WINDOW_SECONDS)

    results = []
    for service in fetch_known_services(db):
        metrics = compute_window_metrics(db, service, window_start, now)
        baseline_row = db.execute(
            text("SELECT ewma_latency_ms FROM service_baselines WHERE service = :service"), {"service": service}
        ).fetchone()
        open_incidents = db.execute(
            text("SELECT count(*) FROM incidents WHERE status = 'open' AND affected_services ? :service"),
            {"service": service},
        ).scalar_one()

        error_rate = (metrics.error_count / metrics.count) if metrics.count else None
        if metrics.count == 0:
            health_status = "unknown"
        elif open_incidents > 0:
            health_status = "degraded"
        else:
            health_status = "healthy"

        results.append(
            schemas.ServiceHealth(
                service=service,
                status=health_status,
                ewma_latency_ms=baseline_row[0] if baseline_row else None,
                recent_error_rate=error_rate,
                recent_sample_count=metrics.count,
                open_incident_count=open_incidents,
            )
        )
    return results


@router.get("/{service}/latency-history", response_model=list[schemas.LatencyHistoryPoint])
def latency_history(service: str, db: Session = Depends(get_db), windows: int = 20) -> list[schemas.LatencyHistoryPoint]:
    """Last `windows` trailing WINDOW_SECONDS buckets of median latency, oldest first."""
    now = datetime.now(timezone.utc)
    points = []
    for i in range(windows, 0, -1):
        window_end = now - timedelta(seconds=WINDOW_SECONDS * (i - 1))
        window_start = window_end - timedelta(seconds=WINDOW_SECONDS)
        metrics = compute_window_metrics(db, service, window_start, window_end)
        points.append(
            schemas.LatencyHistoryPoint(
                window_end=window_end, median_latency_ms=metrics.median_latency_ms, sample_count=metrics.count
            )
        )
    return points
