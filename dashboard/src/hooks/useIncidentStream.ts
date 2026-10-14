import { useEffect, useRef, useState } from "react";
import { API_BASE, getToken } from "../api";
import type { IncidentSummary } from "../types";

export type StreamStatus = "connecting" | "open" | "reconnecting" | "closed";

/**
 * SSE client for /incidents/stream/live with exponential-backoff reconnect
 * (capped at 15s). The backend sends a full snapshot every 2s rather than a
 * diff, so a fresh connection after any drop is immediately correct with no
 * client-side replay/resync logic needed.
 */
export function useIncidentStream(enabled: boolean) {
  const [incidents, setIncidents] = useState<IncidentSummary[]>([]);
  const [status, setStatus] = useState<StreamStatus>("connecting");
  const retryRef = useRef(0);

  useEffect(() => {
    if (!enabled) {
      setStatus("closed");
      return;
    }

    let cancelled = false;
    let retryTimer: number | undefined;
    let es: EventSource | undefined;

    function connect() {
      const token = getToken();
      if (!token) {
        setStatus("closed");
        return;
      }
      setStatus(retryRef.current === 0 ? "connecting" : "reconnecting");
      es = new EventSource(`${API_BASE}/incidents/stream/live?token=${encodeURIComponent(token)}`);

      es.addEventListener("snapshot", (evt) => {
        if (cancelled) return;
        retryRef.current = 0;
        setStatus("open");
        try {
          const data = JSON.parse((evt as MessageEvent).data) as IncidentSummary[];
          setIncidents(data);
        } catch {
          // malformed frame -- the next 2s tick will self-correct, nothing to do here
        }
      });

      es.onerror = () => {
        es?.close();
        if (cancelled) return;
        setStatus("reconnecting");
        const delay = Math.min(1000 * 2 ** retryRef.current, 15000);
        retryRef.current += 1;
        retryTimer = window.setTimeout(connect, delay);
      };
    }

    connect();
    return () => {
      cancelled = true;
      es?.close();
      if (retryTimer) window.clearTimeout(retryTimer);
    };
  }, [enabled]);

  return { incidents, status };
}
