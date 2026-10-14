import { useCallback, useEffect, useState } from "react";
import { clearToken, getServicesHealth, getToken, listIncidents } from "./api";
import { IncidentDetailPanel } from "./components/IncidentDetailPanel";
import { IncidentList } from "./components/IncidentList";
import { LatencyChart } from "./components/LatencyChart";
import { Login } from "./components/Login";
import { ServiceHealthPanel } from "./components/ServiceHealthPanel";
import { useIncidentStream } from "./hooks/useIncidentStream";
import { getLatencyHistory } from "./api";
import type { IncidentSummary, LatencyHistoryPoint, ServiceHealth } from "./types";

function LiveBadge({ status, count }: { status: string; count: number }) {
  const color = status === "open" ? "ok" : status === "closed" ? "unknown" : "warn";
  return (
    <span className={`badge ${color}`} title={`SSE: ${status}`}>
      {status === "open" ? "live" : status} &middot; {count} open now
    </span>
  );
}

export default function App() {
  const [loggedIn, setLoggedIn] = useState(() => !!getToken());
  const [services, setServices] = useState<ServiceHealth[]>([]);
  const [selectedService, setSelectedService] = useState<string | null>(null);
  const [latencyHistory, setLatencyHistory] = useState<LatencyHistoryPoint[]>([]);
  const [incidents, setIncidents] = useState<IncidentSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [statusFilter, setStatusFilter] = useState("");
  const [severityFilter, setSeverityFilter] = useState("");
  const [selectedIncidentId, setSelectedIncidentId] = useState<number | null>(null);

  const { incidents: liveIncidents, status: streamStatus } = useIncidentStream(loggedIn);

  const refreshIncidents = useCallback(async () => {
    const res = await listIncidents({ status: statusFilter || undefined, severity: severityFilter || undefined });
    setIncidents(res.items);
    setTotal(res.total);
  }, [statusFilter, severityFilter]);

  useEffect(() => {
    if (!loggedIn) return;
    refreshIncidents();
    const id = window.setInterval(refreshIncidents, 5000);
    return () => window.clearInterval(id);
  }, [loggedIn, refreshIncidents]);

  useEffect(() => {
    if (!loggedIn) return;
    const fetchHealth = () => getServicesHealth().then(setServices);
    fetchHealth();
    const id = window.setInterval(fetchHealth, 10000);
    return () => window.clearInterval(id);
  }, [loggedIn]);

  useEffect(() => {
    if (!selectedService) return;
    getLatencyHistory(selectedService).then(setLatencyHistory);
    const id = window.setInterval(() => getLatencyHistory(selectedService).then(setLatencyHistory), 10000);
    return () => window.clearInterval(id);
  }, [selectedService]);

  if (!loggedIn) {
    return <Login onLoggedIn={() => setLoggedIn(true)} />;
  }

  return (
    <div className="app">
      <header className="app-header">
        <h1>PulseOps</h1>
        <div className="header-right">
          <LiveBadge status={streamStatus} count={liveIncidents.length} />
          <button
            className="link"
            onClick={() => {
              clearToken();
              setLoggedIn(false);
            }}
          >
            Sign out
          </button>
        </div>
      </header>

      <ServiceHealthPanel services={services} selected={selectedService} onSelect={setSelectedService} />

      {selectedService && (
        <div className="panel">
          <LatencyChart points={latencyHistory} service={selectedService} />
        </div>
      )}

      <div className="split">
        <IncidentList
          incidents={incidents}
          statusFilter={statusFilter}
          severityFilter={severityFilter}
          onStatusFilterChange={setStatusFilter}
          onSeverityFilterChange={setSeverityFilter}
          selectedId={selectedIncidentId}
          onSelect={setSelectedIncidentId}
        />
        {selectedIncidentId != null && (
          <IncidentDetailPanel incidentId={selectedIncidentId} onAcknowledged={refreshIncidents} />
        )}
      </div>
      <p className="muted footer-note">{total} incident(s) match current filters.</p>
    </div>
  );
}
