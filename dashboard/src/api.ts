import type {
  IncidentDetail,
  IncidentSummaryAIResponse,
  LatencyHistoryPoint,
  PaginatedIncidents,
  ServiceHealth,
} from "./types";

export const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8080";

const TOKEN_KEY = "pulseops_token";

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}

export function setToken(token: string): void {
  localStorage.setItem(TOKEN_KEY, token);
}

export function clearToken(): void {
  localStorage.removeItem(TOKEN_KEY);
}

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options.headers as Record<string, string> | undefined),
  };
  if (token) headers["Authorization"] = `Bearer ${token}`;

  const res = await fetch(`${API_BASE}${path}`, { ...options, headers });
  if (res.status === 401) {
    clearToken();
    throw new ApiError(401, "unauthorized");
  }
  if (!res.ok) {
    const body = await res.text();
    throw new ApiError(res.status, body || res.statusText);
  }
  return res.json() as Promise<T>;
}

export async function register(username: string, password: string) {
  return request("/auth/register", { method: "POST", body: JSON.stringify({ username, password }) });
}

export async function login(username: string, password: string): Promise<string> {
  const res = await request<{ access_token: string }>("/auth/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
  setToken(res.access_token);
  return res.access_token;
}

export async function listIncidents(params: {
  status?: string;
  severity?: string;
  limit?: number;
  offset?: number;
}): Promise<PaginatedIncidents> {
  const q = new URLSearchParams();
  if (params.status) q.set("status", params.status);
  if (params.severity) q.set("severity", params.severity);
  q.set("limit", String(params.limit ?? 20));
  q.set("offset", String(params.offset ?? 0));
  return request(`/incidents?${q.toString()}`);
}

export async function getIncident(id: number): Promise<IncidentDetail> {
  return request(`/incidents/${id}`);
}

export async function acknowledgeIncident(id: number, note?: string): Promise<void> {
  await request(`/incidents/${id}/acknowledge`, { method: "POST", body: JSON.stringify({ note: note ?? null }) });
}

export async function summarizeIncident(id: number): Promise<IncidentSummaryAIResponse> {
  return request(`/incidents/${id}/summary`);
}

export async function getStreamTicket(): Promise<string> {
  const res = await request<{ ticket: string }>("/incidents/stream/ticket", { method: "POST" });
  return res.ticket;
}

export async function getServicesHealth(): Promise<ServiceHealth[]> {
  return request("/services/health");
}

export async function getLatencyHistory(service: string): Promise<LatencyHistoryPoint[]> {
  return request(`/services/${service}/latency-history`);
}
