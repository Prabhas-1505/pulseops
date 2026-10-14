from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class RegisterRequest(BaseModel):
    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=8, max_length=72)  # bcrypt's own input limit is 72 bytes


class UserResponse(BaseModel):
    id: int
    username: str
    role: str


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class AlertResponse(BaseModel):
    id: int
    service: str
    metric_type: str
    window_start: datetime
    window_end: datetime
    observed_value: float
    baseline_or_threshold: float
    sample_count: int
    explanation: str
    trace_ids: list[str]
    created_at: datetime

    model_config = {"from_attributes": True}


class IncidentSummary(BaseModel):
    id: int
    signature: str
    affected_services: list[str]
    severity: str
    status: str
    opened_at: datetime
    last_seen_at: datetime
    resolved_at: datetime | None
    acknowledged_at: datetime | None
    acknowledged_by: str | None

    model_config = {"from_attributes": True}


class IncidentDetail(IncidentSummary):
    alerts: list[AlertResponse]
    evidence: list[dict]


class PaginatedIncidents(BaseModel):
    items: list[IncidentSummary]
    total: int
    limit: int
    offset: int


class AcknowledgeRequest(BaseModel):
    note: str | None = Field(default=None, max_length=500)


class ServiceHealth(BaseModel):
    service: str
    status: str  # "healthy" | "degraded" | "unknown"
    ewma_latency_ms: float | None
    recent_error_rate: float | None
    recent_sample_count: int
    open_incident_count: int


class LatencyHistoryPoint(BaseModel):
    window_end: datetime
    median_latency_ms: float | None
    sample_count: int


class IncidentSummaryAIResponse(BaseModel):
    incident_id: int
    generated_by: str  # "openai" | "fallback"
    verified_evidence: list[str]
    suspected_cause: str
    summary: str
