from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, ForeignKey, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from services.common.db import Base


class ServiceBaseline(Base):
    """One row per service: the EWMA latency baseline the detector compares
    each new sliding window against. Persisted so a detector restart doesn't
    reset to a cold-start baseline and spuriously re-alert."""

    __tablename__ = "service_baselines"

    service: Mapped[str] = mapped_column(String, primary_key=True)
    ewma_latency_ms: Mapped[float] = mapped_column(Float, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class Incident(Base):
    """A correlated group of one or more alerts. Lifecycle: open -> resolved.

    `signature` identifies "the same underlying problem" for dedup: alerts
    with a matching signature within the dedup window attach to the existing
    open incident instead of opening a new one. `affected_services` grows
    when a new alert is found to share trace context with this incident's
    existing alerts (cross-service correlation) -- see detector/correlate.py.
    """

    __tablename__ = "incidents"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    signature: Mapped[str] = mapped_column(String, nullable=False, index=True)
    affected_services: Mapped[list] = mapped_column(JSONB, nullable=False)
    severity: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="open", index=True)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    acknowledged_by: Mapped[str | None] = mapped_column(String, nullable=True)


class Alert(Base):
    """One detected anomaly instance for one service, one metric, one window."""

    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    incident_id: Mapped[int | None] = mapped_column(ForeignKey("incidents.id"), nullable=True, index=True)
    service: Mapped[str] = mapped_column(String, nullable=False, index=True)
    metric_type: Mapped[str] = mapped_column(String, nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    observed_value: Mapped[float] = mapped_column(Float, nullable=False)
    baseline_or_threshold: Mapped[float] = mapped_column(Float, nullable=False)
    sample_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    explanation: Mapped[str] = mapped_column(String, nullable=False)
    trace_ids: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
