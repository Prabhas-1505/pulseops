from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from api import schemas
from api.ai_summary import summarize_incident
from api.deps import get_current_user, get_current_user_sse, get_db, get_engine
from api.models import User
from api.rate_limit import RateLimiter
from detector.models import Alert, Incident

router = APIRouter(
    prefix="/incidents",
    tags=["incidents"],
    dependencies=[Depends(get_current_user), Depends(RateLimiter(times=60, seconds=60))],
)

# Separate router: EventSource can't send an Authorization header, so the
# SSE route authenticates via get_current_user_sse (?token= query param)
# instead of the bearer-only dependency the rest of this file uses.
stream_router = APIRouter(prefix="/incidents", tags=["incidents"])


@router.get("", response_model=schemas.PaginatedIncidents)
def list_incidents(
    db: Session = Depends(get_db),
    status_filter: str | None = Query(default=None, alias="status", pattern="^(open|resolved)$"),
    severity: str | None = Query(default=None, pattern="^(warning|critical)$"),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> schemas.PaginatedIncidents:
    q = db.query(Incident)
    if status_filter:
        q = q.filter(Incident.status == status_filter)
    if severity:
        q = q.filter(Incident.severity == severity)
    total = q.count()
    rows = q.order_by(Incident.opened_at.desc()).offset(offset).limit(limit).all()
    return schemas.PaginatedIncidents(
        items=[schemas.IncidentSummary.model_validate(r) for r in rows], total=total, limit=limit, offset=offset
    )


@stream_router.get("/stream/live", dependencies=[Depends(get_current_user_sse), Depends(RateLimiter(times=10, seconds=60))])
async def stream_incidents() -> StreamingResponse:
    """SSE snapshot stream: every 2s, emit the full set of open incidents plus
    anything resolved in the last 60s. Sending a full snapshot (not a diff)
    means a reconnecting client is correct immediately with no replay state
    to track -- the right tradeoff at this incident volume.

    Opens a short-lived connection per poll (not a request-scoped Session
    held for the stream's whole lifetime) so a long-lived SSE client doesn't
    pin one Postgres connection "idle in transaction" indefinitely.
    """

    async def event_generator():
        engine = get_engine()
        while True:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT id, signature, affected_services, severity, status, opened_at,
                               last_seen_at, resolved_at, acknowledged_at, acknowledged_by
                        FROM incidents
                        WHERE status = 'open' OR resolved_at >= now() - interval '60 seconds'
                        ORDER BY opened_at DESC
                        """
                    )
                ).mappings().all()
            payload = [
                {
                    **dict(r),
                    "opened_at": r["opened_at"].isoformat(),
                    "last_seen_at": r["last_seen_at"].isoformat(),
                    "resolved_at": r["resolved_at"].isoformat() if r["resolved_at"] else None,
                    "acknowledged_at": r["acknowledged_at"].isoformat() if r["acknowledged_at"] else None,
                }
                for r in rows
            ]
            yield f"event: snapshot\ndata: {json.dumps(payload)}\n\n"
            await asyncio.sleep(2)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


def _load_incident_or_404(db: Session, incident_id: int) -> Incident:
    incident = db.get(Incident, incident_id)
    if incident is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="incident not found")
    return incident


def _evidence_for(db: Session, alerts: list[Alert], limit: int = 20) -> list[dict]:
    trace_ids: set[str] = set()
    for a in alerts:
        trace_ids.update(a.trace_ids)
    if not trace_ids:
        return []
    rows = db.execute(
        text(
            """
            SELECT service, event_type, status, duration_ms, trace_id, event_timestamp, attributes
            FROM events
            WHERE trace_id = ANY(:trace_ids) AND attributes ? 'http.route'
            ORDER BY event_timestamp DESC
            LIMIT :limit
            """
        ),
        {"trace_ids": list(trace_ids), "limit": limit},
    ).mappings().all()
    return [dict(r) for r in rows]


@router.get("/{incident_id}", response_model=schemas.IncidentDetail)
def get_incident(incident_id: int, db: Session = Depends(get_db)) -> schemas.IncidentDetail:
    incident = _load_incident_or_404(db, incident_id)
    alerts = db.query(Alert).filter(Alert.incident_id == incident_id).order_by(Alert.created_at).all()
    evidence = _evidence_for(db, alerts)
    return schemas.IncidentDetail(
        **schemas.IncidentSummary.model_validate(incident).model_dump(),
        alerts=[schemas.AlertResponse.model_validate(a) for a in alerts],
        evidence=evidence,
    )


@router.post("/{incident_id}/acknowledge", response_model=schemas.IncidentSummary)
def acknowledge_incident(
    incident_id: int,
    req: schemas.AcknowledgeRequest,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> Incident:
    incident = _load_incident_or_404(db, incident_id)
    incident.acknowledged_at = datetime.now(timezone.utc)
    incident.acknowledged_by = user.username
    db.commit()
    db.refresh(incident)
    return incident


@router.get("/{incident_id}/summary", response_model=schemas.IncidentSummaryAIResponse)
def summarize(incident_id: int, db: Session = Depends(get_db)) -> schemas.IncidentSummaryAIResponse:
    incident = _load_incident_or_404(db, incident_id)
    alerts = db.query(Alert).filter(Alert.incident_id == incident_id).order_by(Alert.created_at).all()
    evidence = _evidence_for(db, alerts)
    evidence_attrs = [e["attributes"] for e in evidence]
    result = summarize_incident(incident, alerts, evidence_attrs)
    return schemas.IncidentSummaryAIResponse(incident_id=incident_id, **result)
