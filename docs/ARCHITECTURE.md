# Architecture — Phases 1, 2, 3 & 4

## Components

| Component | What it is | Image/version |
|---|---|---|
| `kafka` | Single-node Kafka in KRaft combined mode (broker+controller) | `apache/kafka:4.2.2` |
| `kafka-init` | One-shot container that creates `otel.raw.v1` (3 partitions) and `otel.dlq.v1` (1 partition), then exits | `apache/kafka:4.2.2` |
| `postgres` | Relational store. Host port `55432` (see `docs/DECISIONS.md`) | `postgres:16-alpine` |
| `redis` | Rate-limit counter store for `api` | `redis:7-alpine` |
| `otel-collector` | OTLP receiver -> batch -> Kafka exporter | `otel/opentelemetry-collector-contrib:0.162.0` |
| `payments` | Innermost demo service; applies fault injection; "charges" an order | FastAPI, this repo |
| `orders` | Calls `payments`; applies fault injection; creates an order | FastAPI, this repo |
| `gateway` | Public entrypoint; calls `orders`; applies fault injection | FastAPI, this repo |
| `migrate` | One-shot `alembic upgrade head` against Postgres, then exits | this repo (`processor/Dockerfile`) |
| `processor` | Durable Kafka consumer group (`pulseops-event-normalizer`): decode -> validate -> idempotent upsert into Postgres, DLQ on malformed input | this repo (`processor/`) |
| `detector` | Periodic (10s) sliding-window anomaly detector + incident correlator against Postgres | this repo (`detector/`) |
| `api` | Authenticated management API: JWT auth, Redis rate limiting, incidents/services endpoints, SSE, AI summary | this repo (`api/`) |
| `dashboard` | React SPA, built at image-build time and served statically | this repo (`dashboard/`), `nginx:1.30-alpine` |

## Phase 2: normalized event schema and persistence

`processor/schema.py` defines `NormalizedEvent` (schema_version=1): a
pydantic model with `event_id`, `service`, `event_timestamp`, `trace_id`,
`span_id`, `parent_span_id`, `event_type`, `duration_ms`, `status`,
`severity`, `attributes`. `normalize_span()` converts a `DecodedSpan` (the
same OTLP-JSON decoder from Phase 1, now shared at
`services/common/otlp_json.py`) into this schema, computing `duration_ms`
from the span's start/end unix-nano timestamps, mapping OTel's
`STATUS_CODE_*` to a plain `status` string, and deriving a simple
`severity` (`"error"` if status is `ERROR`, else `"info"` — this is a label,
not the anomaly detection itself, which is Phase 3's job). Blank
trace/span IDs or an end-before-start timestamp raise `NormalizationError`
rather than being silently coerced.

`processor/models.py` + the Alembic migration in
`processor/migrations/versions/` create the Postgres `events` table.
`event_id = "{trace_id}:{span_id}"` is a unique, deterministic column — the
idempotency key described below.

## Phase 2: idempotent consumer, retry, and dead-lettering

`processor/consumer.py` runs consumer group `pulseops-event-normalizer`
against `otel.raw.v1` with `enable.auto.commit: False`. Per message:

1. Decode the OTLP-JSON payload and normalize every span in it. On
   `json.JSONDecodeError` / `NormalizationError` / similar: produce the
   **original raw bytes** plus a reason header to `otel.dlq.v1`, commit the
   offset, and move on — a malformed message cannot block the partition.
2. Otherwise, bulk upsert via
   `INSERT ... ON CONFLICT (event_id) DO NOTHING RETURNING id`, wrapped in a
   `tenacity` retry (5 attempts, exponential backoff capped at 10s) that
   only triggers on `sqlalchemy.exc.OperationalError` (transient DB
   connectivity issues). The insert count is derived by counting
   `RETURNING` rows, not `cursor.rowcount` (see `docs/DECISIONS.md` for why).
3. Only commit the Kafka offset after step 2 succeeds. If retries are
   exhausted, the exception propagates and the process exits — the
   container's `restart: unless-stopped` policy brings it back, and it
   resumes from the last *committed* offset, so a DB outage delays an event
   rather than losing it.

This gives at-least-once delivery from Kafka's side, made safe by
application-level idempotency (the `event_id` unique constraint), not by
any Kafka exactly-once transaction machinery.

## Request path

`POST /checkout` on gateway -> `POST /orders` on orders -> `POST /charge` on
payments. Each hop is a real network call (httpx, inside the Docker bridge
network `pulseops`), and each service's `HTTPXClientInstrumentor` propagates
the W3C `traceparent` header, so the three spans share one `trace_id`.

Each service also sets business-identifying span attributes
(`pulseops.item`, `pulseops.order_id`, `pulseops.charge_id`, ...) — this is
what lets the Phase 1 smoke test (and later, incident evidence inspection in
Phase 4) correlate a specific span back to a specific business event rather
than just "some request to this service."

## Telemetry path

Each service's `TracerProvider` (`services/common/telemetry.py`) exports via
`OTLPSpanExporter` (gRPC) to `otel-collector:4317`. The collector
(`infra/otel-collector/otel-collector-config.yaml`):

1. Receives via the `otlp` receiver (gRPC `:4317`, HTTP `:4318`).
2. Runs `memory_limiter` (400 MiB limit, 100 MiB spike allowance) then
   `batch` (up to 1024 spans or 2s, whichever first).
3. Exports via `kafkaexporter` to `otel.raw.v1`, encoding `otlp_json`
   (human-inspectable; the Phase 2 consumer and the Phase 1 smoke test both
   decode this with a small hand-written walker in
   `services/common/otlp_json.py` rather than depending on generated OTLP
   protobuf classes).
4. `retry_on_failure` is enabled on the Kafka exporter (1s initial backoff,
   15s max, 60s max elapsed) to survive a momentarily-unavailable broker —
   this is delivery-retry, not payload dead-lettering.

As of Phase 2, `otel.dlq.v1` is actively written to by `processor/` (not the
collector) whenever a raw-telemetry message fails to decode or validate.

## Fault injection design

`services/common/faults.py` holds a single `FaultConfig` per process
(`latency_ms`, `latency_probability`, `error_probability`,
`error_status_code`, `outage`), guarded by an `asyncio.Lock`, mutated via
`GET/POST /admin/faults` and `POST /admin/faults/reset`. It is intentionally
not shared across replicas or authenticated in Phase 1 — see
`docs/DECISIONS.md`.

`apply_fault_injection()` is called at the top of each business endpoint
(not `/health` or `/admin/*`), so a demo service under fault injection still
reports itself healthy — the fault is a property of the business logic path,
not the process.

## Phase 3: anomaly detection

`detector/metrics.py` computes, per service and per 10-second sliding
window, directly from Postgres: request count, error count (`http.status_code
>= 500`), median latency, and the distinct `trace_id`s present. The unit of
counting is "one row with an `http.route` attribute, excluding `/admin/*`"
— verified against real data to be exactly the one true FastAPI server
span per incoming request, not the several ASGI/httpx sub-spans each
request also produces (see `docs/DECISIONS.md`).

Two rules (`detector/rules.py`, pure functions, no I/O):

- **Error rate**: fires if more than 20% of a window's requests returned a
  5xx, and the window has at least 3 samples.
- **Latency**: fires if the window's median latency exceeds
  `max(ewma_baseline * 2, 20ms)`. The EWMA baseline (`alpha=0.3`) is
  persisted per service in `service_baselines` and updated every tick
  regardless of whether that tick was anomalous, so a sustained new normal
  (not just a single spike) eventually becomes the new baseline.

Both rules require their own metric (`status`'s span field is unusable —
see `docs/DECISIONS.md`) and a cold-start baseline (`None`) always suppresses
the latency rule rather than firing on absent data.

## Phase 3: incident correlation

`detector/correlate.py` (pure, no I/O) decides, for each new alert, whether
to attach it to an existing open incident or start a new one:

1. **Dedup**: same `(service, metric_type)` signature as an open incident,
   seen within `DEDUP_WINDOW_SECONDS` (30s) — attach, don't duplicate.
2. **Cross-service correlation**: shares at least one `trace_id` with an
   open incident's alerts from the last `DEDUP_WINDOW_SECONDS` — attach,
   and add this alert's service to the incident's `affected_services`.
3. Otherwise — open a new incident.

`detector/engine.py` wires this to Postgres (`persist_anomaly`), and
`resolve_stale_incidents` marks any open incident `resolved` once
`RESOLUTION_GRACE_SECONDS` (40s) has passed with no new alerts attached.
`detector/main.py` runs one tick every 10 seconds as a long-running Docker
service (`restart: unless-stopped`).

## Phase 4: management API

`api/` is a separate FastAPI app from the three demo services, talking to
the same Postgres (read-mostly: `incidents`, `alerts`, plus its own
`users` table) and to Redis (rate-limit counters only).

- **Auth** (`api/auth.py`, `api/models.py`): `users.hashed_password` via
  `bcrypt`; `POST /auth/register` creates a user, `POST /auth/login`
  verifies and returns a JWT (`PyJWT`, HS256, `JWT_SECRET_KEY`,
  30-minute expiry by default). `api/deps.py`'s `get_current_user` is a
  dependency on every other route (`HTTPBearer` — `Authorization: Bearer
  <token>`), except the SSE stream (see below).
- **Rate limiting** (`api/rate_limit.py`): `RateLimiter(times, seconds)`
  is a FastAPI dependency doing `INCR`/`EXPIRE` against Redis per
  `(route path, client IP)`. Applied at 5/60s on `/auth/login` (brute-force
  protection) and 60/60s on everything else. Genuinely shared state across
  the API process's lifetime, not an in-process counter that resets on
  restart or doesn't coordinate across replicas.
- **Incidents/services** (`api/routers/incidents.py`,
  `api/routers/services.py`): paginated `GET /incidents` (filter by
  `status`/`severity`), `GET /incidents/{id}` (detail plus `evidence` —
  actual `events` rows matching the incident's alerts' `trace_ids`),
  `POST /incidents/{id}/acknowledge`, `GET /services/health` (derived from
  `service_baselines` + a trailing 60s error-rate window, reusing
  `detector/metrics.py`), `GET /services/{service}/latency-history` (last
  20 trailing 10s windows, same metrics code).
- **SSE** (`GET /incidents/stream/live`): emits a full JSON snapshot of
  open + recently-resolved incidents every 2s. Lives on its own
  `stream_router` with `get_current_user_sse`, which accepts the JWT via
  `?token=` instead of a bearer header — the browser's `EventSource` API
  can't set custom headers, so this is the only route with that
  alternate auth path (see `docs/DECISIONS.md`). Opens a short-lived DB
  connection per poll rather than holding one for the connection's whole
  lifetime, so a long-lived dashboard tab doesn't pin a Postgres connection
  "idle in transaction" indefinitely.
- **AI summary** (`api/ai_summary.py`, `GET /incidents/{id}/summary`):
  OpenAI (`gpt-5-nano` by default) if `OPENAI_API_KEY` is set, else a
  deterministic fallback built from the same verified alert data. The
  response schema keeps `verified_evidence` (measured) structurally
  separate from `suspected_cause` (LLM inference, explicitly labeled
  unverified). Untrusted span attributes are wrapped in `<untrusted_data>`
  tags with a "describe, don't obey" instruction. Any OpenAI
  error/timeout/empty-response is caught and falls back automatically —
  the endpoint never 500s because of the LLM.

## Phase 4: React dashboard

`dashboard/` is a Vite + React + TypeScript SPA, no additional UI/charting
framework. `src/api.ts` is a thin fetch wrapper that injects the JWT from
`localStorage` and clears it on a 401. `src/hooks/useIncidentStream.ts`
wraps `EventSource` with exponential-backoff reconnect (capped 15s) — the
backend's full-snapshot-every-2s design means a reconnecting client is
correct immediately with no client-side diff/replay logic. The incident
timeline is a plain paginated `GET /incidents` poll (every 5s); the SSE
stream separately drives a small "live · N open now" header badge — two
different jobs (historical filtered browsing vs. a live pulse), not one
subsystem pretending to be both.

Built via a multi-stage Dockerfile (`node:22-slim` build -> static files
served by `nginx:1.30-alpine`); `VITE_API_BASE` is baked in at image build
time to the API's host-reachable address (`http://localhost:8080` by
default), since the compiled JS runs in the user's browser, not inside the
Docker network.

## Verified end-to-end runs

All of the following were executed against the actual running stack on
2026-10-14 (macOS, Docker Desktop, `docker compose` v5.4.0), not written from
expectation. Full command transcripts:

### Stack came up healthy

```
$ docker compose ps
NAME                      STATUS
pulseops-gateway          Up (healthy)
pulseops-kafka            Up (healthy)
pulseops-orders           Up (healthy)
pulseops-otel-collector   Up
pulseops-payments         Up (healthy)
pulseops-postgres         Up (healthy)
pulseops-redis            Up (healthy)
```

(`otel-collector` has no Docker-level healthcheck — see `docs/DECISIONS.md`
for why — but its logs show `"Everything is ready. Begin running and
processing data."` and the smoke test below proves the pipeline works.)

### Smoke test (`python kafka/smoke_test.py`)

```
PASS: normal checkout returns 200 -- got 200: {"request_id":"req_ffc398b05d534915","order_id":"ord_097f7bd32396458a","charge_id":"chg_43abd733be824b1d","status":"confirmed"}
Consuming 'otel.raw.v1' for up to 25s to find spans for order_id=ord_097f7bd32396458a ...
PASS: collector delivered at least one message to Kafka -- 11 kafka messages consumed
PASS: gateway span found with matching item marker
PASS: orders span found with matching order_id
PASS: payments span found with matching charge_id
PASS: all three spans share one trace_id (context propagation works) -- gateway=3822e178bc94091f5c16a7fddcfdabcf orders=3822e178bc94091f5c16a7fddcfdabcf payments=3822e178bc94091f5c16a7fddcfdabcf
PASS: payments outage fault bubbles up as gateway 502 -- got 502: {"detail":"orders rejected request: 502 {\"detail\":\"payments rejected charge: 503 {\\\"detail\\\":\\\"service unavailable (fault injection: outage)\\\"}\"}"}

All 7 checks passed.
```

Also run via `pytest tests/phase1/test_smoke.py -v`: `1 passed in 25.70s`.

### Phase 2: a real request lands in Postgres, correctly normalized

```
$ curl -X POST localhost:8000/checkout -d '{"item":"phase2-verify","amount_cents":777}'
{"request_id":"req_d55514d3c1ab40b5","order_id":"ord_da09aff3d674414f","charge_id":"chg_5d8b3c14d48445bf","status":"confirmed"}

$ psql ... -c "SELECT service, event_type, status, severity, duration_ms, ... FROM events WHERE ..."
 service  |         event_type          | status | severity | duration_ms
----------+-----------------------------+--------+----------+-------------
 gateway  | POST /checkout              | UNSET  | info     |       60.53
 gateway  | POST /checkout http receive | UNSET  | info     |        0.26
 gateway  | POST                        | UNSET  | info     |       40.32
 orders   | POST /orders                | UNSET  | info     |       28.20
 payments | POST /charge                | UNSET  | info     |        3.66
 ... (14 rows total per request -- FastAPIInstrumentor/HTTPXClientInstrumentor
      emit several ASGI/client sub-spans per business call, not just one)
```

### Phase 2 recovery tests (`python tests/phase2/recovery_test.py`, 13/13 passing)

```
=== Scenario 1: consumer replay does not duplicate events ===
rows before replay: 108
PASS: consumer group offsets reset to earliest
PASS: processor drained the replayed backlog
PASS: replayed backlog was actually reprocessed and deduped (not just skipped) -- processor logged 108 duplicates since replay restart, expected 108
PASS: row count unchanged after full topic replay -- before=108 after=108

=== Scenario 2: malformed message is dead-lettered, not dropped or blocking ===
PASS: processor settled (caught up) before injecting the poison pill
PASS: malformed message landed on DLQ topic with original bytes intact
PASS: consumer group lag still drains after the poison pill (partition not blocked)

=== Scenario 3: Postgres outage is retried, not silently dropped ===
PASS: checkout still succeeds at the HTTP layer while DB is down -- got 200
PASS: processor restarted (crash-and-retry) rather than hanging forever -- RestartCount=2
PASS: event eventually persisted after Postgres recovers (no data loss)
PASS: every span for this request has a unique event_id (no duplicate rows at this point) -- total=14 distinct=14
PASS: span count for this trace is stable after settling (no late duplicate inserts from the crash-retry cycle)
PASS: overall row count only grew by this request's own spans, nothing duplicated -- before=108 after=122 this_trace_spans=14

All 13 checks passed.
```

Also run via `pytest tests/phase2/test_recovery.py -v`: `1 passed in 61.30s`.

Scenario 1's "replayed and deduped" check is itself a story about a bug
caught mid-build: a first version compared `cursor.rowcount` and log output
captured via `subprocess`'s `.stdout` only, both of which looked like
failures (`rowcount` reported `-1`; `.stdout` was empty because Python's
`logging` module writes to stderr by default and `docker logs` keeps
stdout/stderr as separate streams). Neither was a real correctness bug —
`SELECT count(*), count(DISTINCT event_id)` against Postgres confirmed no
duplicates existed the whole time — but both were real bugs in how this
test *observed* correctness, fixed before trusting the result. See
`docs/DECISIONS.md` for both.

### Fault injection scenarios verified manually (all four required patterns)

```
$ curl -X POST localhost:8002/admin/faults -d '{"error_probability": 1.0, "error_status_code": 500}'
$ time curl -X POST localhost:8000/checkout -d '{"item":"spike-test","amount_cents":100}'
{"detail":"orders rejected request: 502 {\"detail\":\"payments rejected charge: 500 {...injected failure (fault injection: error_probability=1.0)...}\"}"}
# -> 0.075s total, HTTP 502 (5xx spike, bubbled through orders->gateway)

$ curl -X POST localhost:8002/admin/faults -d '{"latency_ms": 1500, "latency_probability": 1.0}'
$ time curl -X POST localhost:8000/checkout -d '{"item":"latency-test","amount_cents":100}'
{"request_id":"...","order_id":"...","charge_id":"...","status":"confirmed"}
# -> 1.545s total, HTTP 200 (latency injected and observable end-to-end)
```

Combined with the smoke test's outage check and ordinary `/checkout` calls,
this covers all four traffic patterns required by Phase 1: normal, 5xx
spike, increased latency, and downstream outage.

### Phase 3 unit tests (`pytest tests/phase3/test_rules.py tests/phase3/test_correlate.py -v`)

```
15 passed in 0.15s
```

### Phase 3 live fault-injection evaluation (`python tests/phase3/evaluation.py`)

```
=== Phase A: healthy traffic should not open any incident ===
PASS: no incidents opened during healthy traffic -- found 0: []

=== Phase B: sustained payments error-rate fault should open a correlated incident ===
PASS: payments error-rate fault opened an incident within the expected wait
PASS: the incident correlates all three causally-linked services (not 3 separate incidents) -- affected_services={'gateway', 'payments', 'orders'}
PASS: severity reflects a 100% error rate as critical -- severity=critical

=== Phase C: incident resolves once healthy traffic resumes and grace period elapses ===
PASS: error-rate incident transitions to resolved after the grace period

=== Phase D: sustained latency fault should produce a distinct 'latency' alert ===
PASS: sustained latency fault opened a distinct latency-type incident

All 6 checks passed.
```

Also run via `pytest tests/phase3/ -v` (unit tests + this harness together):
`16 passed in 123.35s`.

A direct query during the Phase B run shows exactly what the detector
produced -- one `critical` incident spanning all three services from three
separate per-service alerts, correlated via shared `trace_id`s:

```
 id |      signature      |         affected_services          | severity | status
----+----------------------+------------------------------------+----------+--------
  2 | payments:error_rate  | ["gateway", "orders", "payments"]  | critical | open

 id | incident_id | service  | metric_type | observed |                        explanation
----+-------------+----------+-------------+----------+-------------------------------------------------------------
  4 |           2 | payments | error_rate  |    1.000 | 6/6 requests (100%) returned a 5xx status ... exceeding 20%
  5 |           2 | gateway  | error_rate  |    1.000 | 6/6 requests (100%) returned a 5xx status ... exceeding 20%
  6 |           2 | orders   | error_rate  |    1.000 | 6/6 requests (100%) returned a 5xx status ... exceeding 20%
```

### Phase 4: management API, driven live end-to-end

```
$ curl -X POST localhost:8080/auth/register -d '{"username":"demo_admin","password":"supersecret123"}'
{"id":1,"username":"demo_admin","role":"viewer"}

$ TOKEN=$(curl -s -X POST localhost:8080/auth/login -d '{...}' | jq -r .access_token)

$ curl localhost:8080/incidents -H "Authorization: Bearer $TOKEN"     # 200, real incident list
$ curl localhost:8080/incidents                                       # no token -> 401
$ curl localhost:8080/services/health -H "Authorization: Bearer $TOKEN"
[{"service":"payments","status":"unknown","ewma_latency_ms":129.6,...}, ...]

# Rate limiting: 7 rapid /auth/login attempts against a 5-per-60s limit
attempt 1-4: HTTP 401 (wrong password, correctly rejected)
attempt 5-7: HTTP 429 (limit exceeded -- some quota was already consumed
             by earlier manual testing within the same Redis window,
             which is itself proof the limiter's state is real and shared,
             not reset per-request or per-process)
```

Incident detail, acknowledge, and the live AI summary (real OpenAI call,
`generated_by: "openai"`):

```json
{
  "incident_id": 6,
  "generated_by": "openai",
  "verified_evidence": [
    "Incident #6: signature=payments:latency, severity=critical, ...",
    "Alert #19 [payments/latency]: observed=504.446 vs baseline_or_threshold=9.398 ..."
  ],
  "suspected_cause": "Unverified inference: The latency spike appears systemic—median latencies around 504–561 ms across payments, orders, and gateway, well above baselines (as low as ~9 ms). This pattern points to a shared upstream bottleneck or dependency contention...",
  "summary": "CRITICAL: widespread latency spike across gateway, orders, and payments; median ~505 ms (vs baselines ~9–208 ms); incident resolved; suspected upstream bottleneck."
}
```

### Phase 4: dashboard, verified in an actual headless browser

Driven with Playwright against headless Chromium (driver script in a
scratch directory, not part of the repo) through the real golden path:
login as `demo_admin` -> dashboard renders -> service health cards show
real `ewma_latency_ms`/error-rate data -> incident list shows 6 real rows
-> clicking one shows the full detail panel (alerts, evidence table,
"ACK'D BY DEMO_ADMIN" badge) -> header badge reads "LIVE · 0 OPEN NOW" via
the SSE connection. Run twice: once against the Vite dev server, once
against the production `nginx`-served Docker build (`docker compose up -d
--build dashboard`) — identical result both times.

```
--- console messages ---
(none)

--- network errors (4xx/5xx) ---
(none)
```

Screenshots were visually inspected (not just text-scraped) at each step;
the login page and incident-detail views rendered correctly with no
layout breakage.

## Known limitations (stated up front, not discovered later)

- `/admin/faults` has no auth. It's an internal dev/test control surface;
  Phase 4's management API is where real authenticated control belongs.
- No container-level healthcheck on `otel-collector` (distroless image, no
  shell/wget — see `docs/DECISIONS.md`).
- Single Kafka broker, single partition replication factor 1: fine for local
  dev, explicitly not a multi-broker durability story.
- The processor is a single instance (one consumer group member). It never
  needed to prove multi-consumer partition rebalancing, only restart/replay
  recovery. Scaling to multiple processor replicas is unexercised.
- `events.severity` (Phase 2) is a simple per-span status-code-derived
  label, distinct from `alerts`/`incidents.severity` (Phase 3), which is
  derived from the detector's own rules (error rate >= 50%, or latency >=
  4x baseline, maps to `"critical"`; otherwise `"warning"`). Don't confuse
  the two columns.
- The DB-outage recovery test (`tests/phase2/recovery_test.py`) exercises
  one processor instance crash-restarting against one stopped-then-restarted
  Postgres. It does not exercise Postgres data corruption, split-brain, or
  multi-replica coordination during the outage.
- The detector is a single instance polling Postgres every 10s. It was not
  built or tested for multiple concurrent detector replicas (which would
  need to coordinate to avoid double-counting alerts); one instance is
  sufficient for this system's scale.
- Detection granularity is per-service only, not per-endpoint. A real
  anomaly in one endpoint of a multi-endpoint service could be diluted by
  that service's other healthy traffic in the same window. Acceptable here
  since each demo service only exposes one meaningful business endpoint.
- The "avoid merging unrelated failures" property is verified with
  fabricated data (`tests/phase3/test_correlate.py`), not live traffic —
  the three real demo services are always causally chained, so there is no
  way to produce a genuinely unrelated failure between them live. See
  `docs/DECISIONS.md`.
- `api/` has no role-based authorization beyond "authenticated or not" --
  every registered user can acknowledge any incident. The `users.role`
  column exists for future use but nothing currently branches on it.
- CORS on the management API is permissive (`allow_origins=["*"]`),
  appropriate for a local/demo system with no production deployment
  target, not for a real one. See `docs/DECISIONS.md`.
- The dashboard has no automated test suite (no Vitest/React Testing
  Library) -- correctness here was established via a real headless-browser
  golden-path run (see above), not via component unit tests. Phase 5 is
  the place for that if it's judged worth adding.
- Rate limiting is per-route-path + client IP, fixed-window (not sliding),
  and the "client IP" as seen by the API container is whatever Docker's
  network presents it as -- behind a real reverse proxy/load balancer this
  would need `X-Forwarded-For` handling, which doesn't exist here.
- The AI summary's "suspected cause" quality depends entirely on the
  configured model; it was tested with exactly one model (`gpt-5-nano`)
  against exactly the incidents this project's own fault injection
  produced. No evaluation of summary quality/accuracy across a broader
  incident corpus was performed or is claimed.
