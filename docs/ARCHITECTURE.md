# Architecture — Phase 1

## Components

| Component | What it is | Image/version |
|---|---|---|
| `kafka` | Single-node Kafka in KRaft combined mode (broker+controller) | `apache/kafka:4.2.2` |
| `kafka-init` | One-shot container that creates `otel.raw.v1` (3 partitions) and `otel.dlq.v1` (1 partition), then exits | `apache/kafka:4.2.2` |
| `postgres` | Relational store, provisioned but unused until Phase 2 | `postgres:16-alpine` |
| `redis` | Cache/rate-limit store, provisioned but unused until Phase 4 | `redis:7-alpine` |
| `otel-collector` | OTLP receiver -> batch -> Kafka exporter | `otel/opentelemetry-collector-contrib:0.162.0` |
| `payments` | Innermost demo service; applies fault injection; "charges" an order | FastAPI, this repo |
| `orders` | Calls `payments`; applies fault injection; creates an order | FastAPI, this repo |
| `gateway` | Public entrypoint; calls `orders`; applies fault injection | FastAPI, this repo |

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
   decode this with a small hand-written walker in `kafka/otlp_json.py`
   rather than depending on generated OTLP protobuf classes).
4. `retry_on_failure` is enabled on the Kafka exporter (1s initial backoff,
   15s max, 60s max elapsed) to survive a momentarily-unavailable broker —
   this is delivery-retry, not payload dead-lettering.

`otel.dlq.v1` exists in Kafka but nothing writes to it yet; it's reserved for
the Phase 2 stream processor to use when it encounters a malformed/invalid
raw-telemetry message.

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

## Known Phase 1 limitations (stated up front, not discovered later)

- `/admin/faults` has no auth. It's an internal dev/test control surface;
  Phase 4's management API is where real authenticated control belongs.
- No container-level healthcheck on `otel-collector` (distroless image, no
  shell/wget — see `docs/DECISIONS.md`).
- The smoke test's Kafka consumer group is throwaway and single-run; it does
  not exercise consumer-group rebalancing, offset commits, or replay — that
  is Phase 2 scope (durable processor) and will get its own tests.
- Single Kafka broker, single partition replication factor 1: fine for local
  dev, explicitly not a multi-broker durability story.
