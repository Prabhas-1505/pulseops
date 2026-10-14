import type { ServiceHealth } from "../types";

function statusColor(status: ServiceHealth["status"]): string {
  if (status === "healthy") return "ok";
  if (status === "degraded") return "critical";
  return "unknown";
}

export function ServiceHealthPanel({
  services,
  selected,
  onSelect,
}: {
  services: ServiceHealth[];
  selected: string | null;
  onSelect: (service: string) => void;
}) {
  return (
    <div className="panel">
      <h2>Service health</h2>
      <div className="health-grid">
        {services.map((s) => (
          <button
            key={s.service}
            className={`health-card ${statusColor(s.status)} ${selected === s.service ? "selected" : ""}`}
            onClick={() => onSelect(s.service)}
          >
            <div className="health-card-title">{s.service}</div>
            <div className={`badge ${statusColor(s.status)}`}>{s.status}</div>
            <div className="health-card-metric">
              latency (ewma): {s.ewma_latency_ms != null ? `${s.ewma_latency_ms.toFixed(1)}ms` : "--"}
            </div>
            <div className="health-card-metric">
              error rate (60s): {s.recent_error_rate != null ? `${(s.recent_error_rate * 100).toFixed(0)}%` : "--"}
              {" "}({s.recent_sample_count} samples)
            </div>
            {s.open_incident_count > 0 && <div className="health-card-metric warn">{s.open_incident_count} open incident(s)</div>}
          </button>
        ))}
        {services.length === 0 && <p className="muted">No service data yet.</p>}
      </div>
    </div>
  );
}
