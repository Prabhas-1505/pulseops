import type { IncidentSummary } from "../types";

export function IncidentList({
  incidents,
  statusFilter,
  severityFilter,
  onStatusFilterChange,
  onSeverityFilterChange,
  selectedId,
  onSelect,
}: {
  incidents: IncidentSummary[];
  statusFilter: string;
  severityFilter: string;
  onStatusFilterChange: (v: string) => void;
  onSeverityFilterChange: (v: string) => void;
  selectedId: number | null;
  onSelect: (id: number) => void;
}) {
  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Incidents</h2>
        <div className="filters">
          <select value={statusFilter} onChange={(e) => onStatusFilterChange(e.target.value)}>
            <option value="">all statuses</option>
            <option value="open">open</option>
            <option value="resolved">resolved</option>
          </select>
          <select value={severityFilter} onChange={(e) => onSeverityFilterChange(e.target.value)}>
            <option value="">all severities</option>
            <option value="critical">critical</option>
            <option value="warning">warning</option>
          </select>
        </div>
      </div>
      <ul className="incident-list">
        {incidents.map((inc) => (
          <li
            key={inc.id}
            className={`incident-row ${inc.severity} ${selectedId === inc.id ? "selected" : ""}`}
            onClick={() => onSelect(inc.id)}
          >
            <span className={`badge ${inc.severity === "critical" ? "critical" : "warn"}`}>{inc.severity}</span>
            <span className="incident-signature">{inc.signature}</span>
            <span className="incident-services">{inc.affected_services.join(", ")}</span>
            <span className={`badge ${inc.status === "open" ? "warn" : "ok"}`}>{inc.status}</span>
            {inc.acknowledged_at && <span className="badge ack">ack'd</span>}
            <time className="muted">{new Date(inc.opened_at).toLocaleString()}</time>
          </li>
        ))}
        {incidents.length === 0 && <p className="muted">No incidents match these filters.</p>}
      </ul>
    </div>
  );
}
