"""Minimal decoder for the OTLP-JSON encoding produced by the collector's kafkaexporter.

OTLP JSON follows the protobuf canonical JSON mapping: camelCase field names,
int64 fields (unix-nano timestamps) as decimal strings, oneof typed values as
`{"stringValue": ...}` / `{"intValue": ...}` / etc. This module walks that
structure without depending on the generated OTLP protobuf classes, since the
collector already guarantees the shape as long as `encoding: otlp_json` is set.

Shared by the Phase 1 smoke test and the Phase 2 stream processor -- both
consume the same `otel.raw.v1` topic and need the same decode logic.
"""

from __future__ import annotations

from dataclasses import dataclass, field


def _attr_value(value: dict) -> object:
    if "stringValue" in value:
        return value["stringValue"]
    if "intValue" in value:
        return int(value["intValue"])
    if "doubleValue" in value:
        return value["doubleValue"]
    if "boolValue" in value:
        return value["boolValue"]
    return value


def _attrs_to_dict(attrs: list[dict]) -> dict:
    return {a["key"]: _attr_value(a.get("value", {})) for a in attrs or []}


@dataclass
class DecodedSpan:
    service_name: str
    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    status_code: str
    start_time_unix_nano: int
    end_time_unix_nano: int
    attributes: dict = field(default_factory=dict)


def iter_spans(export_trace_service_request: dict):
    """Yield DecodedSpan for every span in one ExportTraceServiceRequest (JSON) payload."""
    for resource_span in export_trace_service_request.get("resourceSpans", []):
        resource_attrs = _attrs_to_dict(resource_span.get("resource", {}).get("attributes", []))
        service_name = resource_attrs.get("service.name", "unknown")
        for scope_span in resource_span.get("scopeSpans", []):
            for span in scope_span.get("spans", []):
                status = span.get("status", {}) or {}
                yield DecodedSpan(
                    service_name=service_name,
                    trace_id=span.get("traceId", ""),
                    span_id=span.get("spanId", ""),
                    parent_span_id=span.get("parentSpanId"),
                    name=span.get("name", ""),
                    status_code=status.get("code", "STATUS_CODE_UNSET"),
                    start_time_unix_nano=int(span.get("startTimeUnixNano", 0)),
                    end_time_unix_nano=int(span.get("endTimeUnixNano", 0)),
                    attributes=_attrs_to_dict(span.get("attributes", [])),
                )
