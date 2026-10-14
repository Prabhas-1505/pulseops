# PulseOps

**Real-Time Distributed Observability & AI Incident Intelligence Platform.**

A portfolio project demonstrating backend/distributed-systems engineering:
Kafka event streaming, OpenTelemetry instrumentation, fault-tolerant
ingestion, and (in later phases) anomaly detection, incident correlation,
and a management API + dashboard. AI is a secondary, optional enrichment
feature (Phase 4), not the core architecture.

**Status: Phase 3 of 5 complete and verified.** See [Project phases](#project-phases)
for what exists today vs. what's planned.

## Architecture (current: Phase 3)

```mermaid
flowchart LR
    subgraph Demo["Demo services (FastAPI, OTel-instrumented)"]
        GW["gateway\nPOST /checkout"] -->|httpx, trace-propagated| ORD["orders\nPOST /orders"]
        ORD -->|httpx, trace-propagated| PAY["payments\nPOST /charge"]
    end

    GW -. OTLP/gRPC :4317 .-> OTEL["OTel Collector\n(otlp receiver -> batch -> kafkaexporter)"]
    ORD -. OTLP/gRPC :4317 .-> OTEL
    PAY -. OTLP/gRPC :4317 .-> OTEL

    OTEL -->|otlp_json, keyed by trace_id| RAW["Kafka topic\notel.raw.v1\n(3 partitions)"]

    RAW --> PROC["processor\n(pulseops-event-normalizer consumer group)\ndecode -> validate -> idempotent upsert"]
    PROC -->|malformed messages| DLQ["Kafka topic\notel.dlq.v1"]
    PROC -->|ON CONFLICT DO NOTHING on event_id| PG[("PostgreSQL\nevents, service_baselines,\nalerts, incidents")]

    PG --> DET["detector\n(10s sliding window, every 10s)\nerror-rate threshold + EWMA latency baseline"]
    DET -->|anomaly| ALERT["alerts table"]
    ALERT -->|correlate via trace_id overlap + dedup| INC["incidents table\n(open -> resolved)"]

    RAW -.->|Phase 1 smoke test only| SMOKE["kafka/smoke_test.py\n(throwaway consumer group)"]

    REDIS[("Redis\n(provisioned, unused until Phase 4)")]
```

Every demo-service request is a real HTTP call across three independent
FastAPI processes, trace-propagated end to end (W3C traceparent via
instrumented httpx), exported over OTLP/gRPC to an OpenTelemetry Collector,
which batches and exports spans as OTLP-JSON Kafka messages. A durable
consumer (`processor/`) then normalizes those spans into a versioned schema
and persists them to Postgres idempotently. A periodic `detector/` service
then watches Postgres for sliding-window anomalies and correlates them into
incidents.

## What's actually implemented

### Phase 1 -- infra, demo services, OTel -> Kafka ingestion

- `docker-compose.yml`: single-node KRaft Kafka (`apache/kafka:4.2.2`),
  Postgres 16, Redis 7, `otel/opentelemetry-collector-contrib:0.162.0`, and
  the three demo services, with health checks and named volumes.
- Three FastAPI services (`services/gateway`, `services/orders`,
  `services/payments`) forming a real call chain
  (`gateway -> orders -> payments`), each instrumented with the OpenTelemetry
  SDK (`FastAPIInstrumentor` + `HTTPXClientInstrumentor`), exporting traces
  over OTLP/gRPC. `/health` is excluded from tracing (see `docs/DECISIONS.md`
  -- Docker's healthchecks alone generated thousands of noise spans).
- Reproducible fault injection per service via `POST /admin/faults`
  (in-memory, per-process, unauthenticated): `latency_ms` /
  `latency_probability`, `error_probability` / `error_status_code`, and
  `outage` (immediate 503). All four traffic patterns required by the spec
  were manually verified against the running stack: normal traffic, a
  payments 5xx spike, injected latency, and a full payments outage that
  bubbles up through orders to the gateway as a 502.
- OTel Collector config (`infra/otel-collector/otel-collector-config.yaml`):
  `otlp` receiver (gRPC+HTTP) -> `memory_limiter`+`batch` processors ->
  `kafkaexporter` (encoding `otlp_json`, keyed by `trace_id`,
  `retry_on_failure` enabled) into `otel.raw.v1`.
- A real, executable smoke test (`kafka/smoke_test.py`, wrapped for pytest in
  `tests/phase1/test_smoke.py`) that drives an actual `/checkout` request
  through all three services, consumes `otel.raw.v1` with a throwaway
  consumer group, decodes OTLP-JSON, and asserts that gateway/orders/payments
  spans for that request exist, carry matching business-id attributes, and
  share one `trace_id` -- then separately verifies the payments-outage fault
  produces a 502 at the gateway.

### Phase 2 -- normalized event schema, durable persistence, idempotent consumer

- `processor/schema.py`: a versioned (`schema_version=1`) `NormalizedEvent`
  pydantic model (event ID, service, timestamp, trace ID, span ID, type,
  duration, status, severity, attributes), with real validation (blank IDs,
  end-before-start timestamps, etc. are rejected, not just typed-and-trusted).
- `processor/models.py` + Alembic migration (`processor/migrations/`):
  Postgres `events` table. `event_id` (`"{trace_id}:{span_id}"`) is a unique
  column and the idempotency key.
- `processor/consumer.py`: a real Kafka consumer group
  (`pulseops-event-normalizer`) reading `otel.raw.v1` with manual offset
  commits. Persistence is `INSERT ... ON CONFLICT (event_id) DO NOTHING`
  (idempotent upsert), wrapped in `tenacity` retry with exponential backoff
  for transient Postgres errors. Offsets are only committed after a
  message's events are durably persisted (or dead-lettered), so an
  at-least-once redelivery can never silently lose an event.
- Malformed messages (bad JSON, invalid span data) are produced to
  `otel.dlq.v1` with the original bytes and a reason header, and their
  offset is still committed -- a poison pill cannot block the partition.
- Three recovery scenarios actually exercised against the live stack
  (`tests/phase2/recovery_test.py`, 13/13 checks passing, also runs under
  pytest): **(1)** resetting the consumer group to earliest and replaying
  the entire topic reprocesses every message (proven via the processor's own
  logs) but inserts zero new rows; **(2)** a hand-crafted garbage Kafka
  message lands on the DLQ topic and the consumer group's lag still drains
  to zero afterward; **(3)** stopping Postgres, sending a real request, and
  confirming the processor retries and then crash-restarts (Docker's
  `restart: unless-stopped`) rather than dropping the event -- once Postgres
  comes back, the event is persisted exactly once with no duplicates.

### Phase 3 -- anomaly detection, incident correlation, fault-injection evaluation

- `detector/metrics.py`: sliding-window metrics per service, computed
  directly from Postgres. A "business request" is identified by the
  presence of an `http.route` attribute -- verified against real data that
  this cleanly isolates exactly one row per incoming HTTP request (not the
  ASGI send/receive sub-spans or outbound httpx client spans FastAPI's
  auto-instrumentation also emits).
- `detector/rules.py` (pure, unit-tested): two **explainable** anomaly
  rules, each skipped below `MIN_SAMPLES=3` to avoid single-request noise.
  **Error rate**: fixed threshold, flagged if more than 20% of a window's
  requests returned a 5xx. **Latency**: an EWMA baseline per service
  (`alpha=0.3`, persisted in `service_baselines`, survives detector
  restarts), flagged if the window's median latency exceeds
  `max(baseline * 2, 20ms floor)` -- the floor stops a near-zero cold-start
  baseline from flagging ordinary jitter as a 2x spike.
- `detector/correlate.py` (pure, unit-tested): alerts become incidents via
  two rules applied in order -- **(1) dedup**: a new alert with the same
  `(service, metric_type)` signature as an already-open incident, seen
  recently, attaches to it instead of opening a duplicate; **(2) cross-service
  correlation**: a new alert whose window shares a `trace_id` with an open
  incident's recent alerts gets merged into it. Alerts with no shared
  `trace_id` to anything open become their own incident -- this is what
  stops unrelated failures from being merged. Incidents auto-resolve after
  `RESOLUTION_GRACE_SECONDS=40s` with no new alerts.
- `detector/engine.py` + `detector/main.py`: a long-running service, one
  detection tick every 10 seconds, wired into `docker-compose.yml`.
- 15 deterministic unit tests (`tests/phase3/test_rules.py`,
  `test_correlate.py`) covering both rule modules with fabricated inputs,
  including the "avoid merging unrelated failures" property specifically
  (live traffic can't produce a genuinely unrelated failure, since the
  three demo services are always causally chained).
- A real fault-injection evaluation harness
  (`tests/phase3/evaluation.py`, 6/6 checks passing, also runs under
  pytest) driving labeled healthy/faulty windows through the **actual
  running detector service**: healthy traffic opens zero incidents; a
  sustained payments error-rate fault opens one `critical` incident
  correlated across all three services within the expected wait; that
  incident resolves once healthy traffic resumes and the grace period
  elapses; a separate sustained latency fault produces a distinct
  `latency`-type incident.

### Not yet implemented

Everything from Phase 4 onward: the authenticated management API, the React
dashboard, and AI incident summaries. Redis is provisioned in
`docker-compose.yml` but nothing reads or writes it yet (that's Phase 4 --
rate limiting). No load/benchmark numbers exist yet -- see
`docs/BENCHMARK_RESULTS.md`, which is intentionally a stub until Phase 5.

## Running it

```bash
cp .env.example .env
docker compose up -d --build
```

This starts Kafka (KRaft, single node), Postgres, and Redis; runs
`kafka-init` (creates the two topics) and `migrate` (`alembic upgrade head`)
as one-shot jobs; starts the OTel Collector; then builds and starts
`payments`, `orders`, `gateway`, `processor`, and `detector` in dependency
order. Service ports (host side, overridable via `.env`): gateway `:8000`,
orders `:8001`, payments `:8002`.

**Postgres host port is `55432`, not `5432`** -- if your machine already
runs a native Postgres on `5432` (common on macOS), it silently intercepts
connections meant for the container. In-network traffic (the processor
talking to `postgres:5432`) is unaffected either way; this only matters for
host-side tooling (`psql`, `alembic`) -- see `docs/DECISIONS.md`.

Check everything is healthy:

```bash
docker compose ps
```

### Tests (Phases 1-3)

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r kafka/requirements.txt -r processor/requirements.txt pytest

python kafka/smoke_test.py               # Phase 1, standalone, prints PASS/FAIL per check
pytest tests/phase1/test_smoke.py -v     # same, via pytest

python tests/phase2/recovery_test.py     # Phase 2, standalone
pytest tests/phase2/test_recovery.py -v  # same, via pytest

pytest tests/phase3/test_rules.py tests/phase3/test_correlate.py -v  # Phase 3, deterministic unit tests (fast, no stack needed)
python tests/phase3/evaluation.py        # Phase 3, live fault-injection harness (~2 min)
pytest tests/phase3/test_evaluation.py -v
```

The Phase 2 recovery tests actively stop/start the `postgres` and
`processor` containers as part of the scenarios; the Phase 3 evaluation
harness actively toggles `/admin/faults` on the live `payments` service --
don't run either against a stack holding data you care about. All of the
above were actually run against the live stack described here; see the
commands and output transcribed in `docs/ARCHITECTURE.md`.

### Database migrations

```bash
export POSTGRES_HOST=localhost POSTGRES_PORT=55432 \
       POSTGRES_USER=pulseops POSTGRES_PASSWORD=change-me-in-local-env POSTGRES_DB=pulseops
alembic upgrade head                              # apply
alembic revision --autogenerate -m "description"  # create a new migration
```

### Manually driving traffic / fault injection

```bash
curl -X POST localhost:8000/checkout -H 'Content-Type: application/json' \
  -d '{"item":"demo","amount_cents":500}'

# 100% error rate on payments:
curl -X POST localhost:8002/admin/faults -H 'Content-Type: application/json' \
  -d '{"error_probability": 1.0, "error_status_code": 500}'

# reset:
curl -X POST localhost:8002/admin/faults/reset
```

### Inspecting incidents

```bash
PGPASSWORD=change-me-in-local-env psql -h localhost -p 55432 -U pulseops -d pulseops \
  -c "SELECT id, signature, affected_services, severity, status, opened_at FROM incidents ORDER BY id DESC LIMIT 10;"
```

## Project phases

1. **Infra, demo services, OTel -> Kafka ingestion, smoke test.** Done.
2. **Normalized event schema, Postgres persistence, idempotent consumer, DLQ wiring, recovery tests.** Done.
3. **Anomaly detection, incident correlation, fault-injection evaluation harness.** Done (this README).
4. Authenticated management API (JWT, rate limiting), React dashboard (SSE), optional AI incident summaries.
5. Full test/benchmark suite, CI, security checks, final docs.

See `docs/ARCHITECTURE.md` for component detail and verified run transcripts,
`docs/DECISIONS.md` for why specific versions/approaches were chosen, and
`docs/BENCHMARK_RESULTS.md` for the (currently empty) benchmark methodology
and results template.
