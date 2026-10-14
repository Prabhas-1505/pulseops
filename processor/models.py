from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, Integer, SmallInteger, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from services.common.db import Base


class Event(Base):
    """Durable, normalized OTel span. One row per (trace_id, span_id).

    `event_id` (= "{trace_id}:{span_id}") is the idempotency key: at-least-once
    delivery from Kafka is made safe via `ON CONFLICT (event_id) DO NOTHING`
    in the consumer, not via any Kafka-level exactly-once guarantee.
    """

    __tablename__ = "events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    service: Mapped[str] = mapped_column(String, nullable=False, index=True)
    event_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    trace_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    span_id: Mapped[str] = mapped_column(String, nullable=False)
    parent_span_id: Mapped[str | None] = mapped_column(String, nullable=True)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    duration_ms: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    severity: Mapped[str] = mapped_column(String, nullable=False, index=True)
    attributes: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    raw_kafka_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    raw_kafka_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
