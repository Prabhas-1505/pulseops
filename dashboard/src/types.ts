export interface Alert {
  id: number;
  service: string;
  metric_type: string;
  window_start: string;
  window_end: string;
  observed_value: number;
  baseline_or_threshold: number;
  sample_count: number;
  explanation: string;
  trace_ids: string[];
  created_at: string;
}

export interface IncidentSummary {
  id: number;
  signature: string;
  affected_services: string[];
  severity: "warning" | "critical";
  status: "open" | "resolved";
  opened_at: string;
  last_seen_at: string;
  resolved_at: string | null;
  acknowledged_at: string | null;
  acknowledged_by: string | null;
}

export interface EvidenceRow {
  service: string;
  event_type: string;
  status: string;
  duration_ms: number;
  trace_id: string;
  event_timestamp: string;
  attributes: Record<string, unknown>;
}

export interface IncidentDetail extends IncidentSummary {
  alerts: Alert[];
  evidence: EvidenceRow[];
}

export interface PaginatedIncidents {
  items: IncidentSummary[];
  total: number;
  limit: number;
  offset: number;
}

export interface ServiceHealth {
  service: string;
  status: "healthy" | "degraded" | "unknown";
  ewma_latency_ms: number | null;
  recent_error_rate: number | null;
  recent_sample_count: number;
  open_incident_count: number;
}

export interface LatencyHistoryPoint {
  window_end: string;
  median_latency_ms: number | null;
  sample_count: number;
}

export interface IncidentSummaryAIResponse {
  incident_id: number;
  generated_by: "openai" | "fallback";
  verified_evidence: string[];
  suspected_cause: string;
  summary: string;
}
