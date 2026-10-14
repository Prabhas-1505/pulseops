"""Versioned normalized event schema (schema_version=1).

This is the contract between raw OTLP spans and durable storage: every field
required by the Phase 2 spec (event ID, service, timestamp, trace ID, span
ID, type, duration, status, severity, attributes) lives here, explicitly
validated rather than trusted from the wire.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator

from services.common.otlp_json import DecodedSpan

SCHEMA_VERSION = 1

_STATUS_MAP = {
    "STATUS_CODE_OK": "OK",
    "STATUS_CODE_ERROR": "ERROR",
    "STATUS_CODE_UNSET": "UNSET",
}


class NormalizationError(ValueError):
    """Raised when a decoded span cannot be turned into a valid NormalizedEvent."""


class NormalizedEvent(BaseModel):
    schema_version: int = SCHEMA_VERSION
    event_id: str
    service: str
    event_timestamp: datetime
    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    event_type: str
    duration_ms: float = Field(ge=0)
    status: str
    severity: str
    attributes: dict = Field(default_factory=dict)

    @field_validator("trace_id", "span_id", "service", "event_type")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be blank")
        return v


def normalize_span(span: DecodedSpan) -> NormalizedEvent:
    if not span.trace_id or not span.span_id:
        raise NormalizationError("span missing trace_id or span_id")
    if span.end_time_unix_nano < span.start_time_unix_nano:
        raise NormalizationError(
            f"end time {span.end_time_unix_nano} precedes start time {span.start_time_unix_nano}"
        )

    status = _STATUS_MAP.get(span.status_code, "UNSET")
    severity = "error" if status == "ERROR" else "info"
    duration_ms = (span.end_time_unix_nano - span.start_time_unix_nano) / 1_000_000

    try:
        return NormalizedEvent(
            event_id=f"{span.trace_id}:{span.span_id}",
            service=span.service_name,
            event_timestamp=datetime.fromtimestamp(span.start_time_unix_nano / 1_000_000_000, tz=timezone.utc),
            trace_id=span.trace_id,
            span_id=span.span_id,
            parent_span_id=span.parent_span_id or None,
            event_type=span.name,
            duration_ms=duration_ms,
            status=status,
            severity=severity,
            attributes=span.attributes,
        )
    except Exception as exc:  # pydantic ValidationError and friends
        raise NormalizationError(str(exc)) from exc
