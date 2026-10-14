"""Pure incident correlation: no DB, no I/O, fully deterministic given inputs.

Two rules, applied in order:

1. Dedup: a new alert with the same (service, metric_type) signature as an
   already-open incident, seen recently, attaches to that incident instead
   of opening a new one every detection tick for the same ongoing problem.
2. Cross-service correlation: a new alert whose window shares at least one
   trace_id with an open incident's recent alerts gets merged into that
   incident (and that service is added to its affected_services) -- this is
   how a real payments outage becomes ONE incident spanning
   payments+orders+gateway (they share trace_ids because gateway really did
   call orders really did call payments), not three unrelated-looking
   incidents.

If neither rule matches, a new incident is opened. Alerts whose trace_ids
don't overlap with anything open are deliberately NOT merged -- that is the
"avoid merging unrelated failures" requirement, and it falls out naturally
from rule 2 requiring an actual trace_id intersection rather than just
temporal proximity.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class OpenIncidentView:
    id: int
    signature: str
    affected_services: list[str]
    last_seen_at: datetime
    recent_trace_ids: set[str]


@dataclass
class CorrelationResult:
    incident_id: int | None  # None means: caller should create a new incident
    affected_services: list[str]


def correlate(
    service: str,
    metric_type: str,
    trace_ids: list[str],
    now: datetime,
    open_incidents: list[OpenIncidentView],
    dedup_window: timedelta,
) -> CorrelationResult:
    signature = f"{service}:{metric_type}"
    trace_id_set = set(trace_ids)

    for inc in open_incidents:
        if inc.signature == signature and (now - inc.last_seen_at) <= dedup_window:
            return CorrelationResult(incident_id=inc.id, affected_services=inc.affected_services)

    for inc in open_incidents:
        if (now - inc.last_seen_at) <= dedup_window and trace_id_set & inc.recent_trace_ids:
            merged_services = sorted(set(inc.affected_services) | {service})
            return CorrelationResult(incident_id=inc.id, affected_services=merged_services)

    return CorrelationResult(incident_id=None, affected_services=[service])
