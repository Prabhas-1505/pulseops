# Decisions log

Format: decision, date, why, alternatives considered. Append, don't rewrite
history — if a decision changes later, add a new entry rather than editing
this one, so the log stays honest.

## 2026-10-14 — Kafka: `apache/kafka:4.2.2`, KRaft combined mode, single node

The spec asked for "single-node KRaft for local development." Kafka 4.x
removed ZooKeeper support entirely and made KRaft the only mode, which
simplifies this: no separate ZK service, no migration config. Used the
official Apache-published image (`docker.io/apache/kafka`, KIP-1028) rather
than Confluent's or Bitnami's community images, since it's the canonical
upstream image and its env-var contract for KRaft (`KAFKA_PROCESS_ROLES`,
`KAFKA_CONTROLLER_QUORUM_VOTERS`, `CLUSTER_ID`, etc.) is documented in the
Kafka repo itself (`docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml`),
which this project's `docker-compose.yml` adapts directly (three listeners:
`PLAINTEXT_HOST` for host access on `:9092`, `PLAINTEXT` for in-network
access on `:19092`, `CONTROLLER` for KRaft quorum on `:29093`).

`CLUSTER_ID` must be a base64url-encoded 16-byte value (22 chars, no
padding); generated with `python -c "import uuid,base64; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='))"`.

Replication factor is 1 everywhere (single broker) — explicitly a dev
convenience, not a durability guarantee. Documented as a limitation, not
glossed over.

## 2026-10-14 — OTel Collector: `otel/opentelemetry-collector-contrib:0.162.0`

Needed the `kafkaexporter`, which only ships in the `-contrib` distribution,
not the core `otelcol` image. Pinned to the latest tag found at
implementation time (checked against the GitHub releases page) rather than
`:latest`, so the pipeline doesn't silently change behavior on a `docker
compose pull`.

**Consequence discovered during Phase 1, not anticipated going in:** this
image is distroless-ish — no shell, no `wget`/`curl` inside it
(`docker exec ... sh` and `docker exec ... wget` both fail with "executable
file not found in $PATH"). That means a Docker `healthcheck:` using
`CMD`/`CMD-SHELL` cannot work against this image at all, regardless of what
command you ask it to run. Removed the healthcheck entirely rather than
fighting the image; downstream services depend on `service_started` instead
of `service_healthy` for `otel-collector`, and correctness is instead proven
by the smoke test actually finding spans in Kafka (if the collector were
broken, that check would fail, which is a more meaningful signal than a
process-level health probe anyway).

## 2026-10-14 — Kafka encoding: `otlp_json` over `otlp_proto`

Chose JSON over protobuf for the raw-telemetry topic. Protobuf would be more
compact and is what most production OTel pipelines use, but JSON means the
Phase 1/2 consumers can decode messages with a ~50-line hand-written walker
(`kafka/otlp_json.py`) instead of depending on generated
`opentelemetry-proto` Python classes, and messages are directly inspectable
with `kafka-console-consumer.sh` or any JSON tool during development. This
is a deliberate simplicity-over-throughput tradeoff appropriate for a
portfolio/dev-scale system; if this were a real production pipeline at
volume, `otlp_proto` + schema registry would be the better call, and that
tradeoff is exactly what `docs/BENCHMARK_RESULTS.md` should eventually
quantify (message size, serialize/deserialize cost) once Phase 5 benchmarks
exist.

## 2026-10-14 — Python dependency versions: `pip freeze` snapshot, not hand-picked

Rather than guessing at compatible version numbers, created a throwaway
venv (`python3.11 -m venv .venv`), installed the floor set of packages
needed (fastapi, uvicorn, httpx, pydantic, opentelemetry-sdk +
instrumentation-fastapi + instrumentation-httpx + exporter-otlp-proto-grpc,
confluent-kafka), and committed the actual resolved versions from `pip
freeze` into `services/requirements.txt` / `kafka/requirements.txt`. This
guarantees every pinned version is one that genuinely installs and
resolves together, rather than a plausible-looking number that might not
exist or might conflict.

## 2026-10-14 — Fault injection: in-memory per-process, unauthenticated

`/admin/faults` has no auth in Phase 1. This is a conscious scope
boundary, not an oversight: Phase 1's job is to prove the telemetry and
ingestion pipeline works under controllable failure conditions; real
authenticated control of production-like surfaces is explicitly Phase 4's
job (JWT-protected management API). Documented in both `README.md` and
`docs/ARCHITECTURE.md` so it isn't mistaken for a security decision made by
accident.

## 2026-10-14 — Exclude `/health` from tracing

**Discovered, not anticipated:** Docker's container healthcheck hits
`/health` on each of the three services every 10s. `FastAPIInstrumentor`
instruments *every* incoming request by default, so after several hours of
local dev the `otel.raw.v1` topic had accumulated 3601 pure-noise messages
from health probes alone — zero business content, would have flooded
Postgres with meaningless rows once Phase 2's processor started writing.
Fixed by passing `excluded_urls="/health"` to
`FastAPIInstrumentor.instrument_app` in `services/common/telemetry.py`.
Verified by deleting and recreating `otel.raw.v1` and confirming 15s of idle
container uptime produced zero new messages.

**Related gotcha:** deleting and recreating a Kafka topic while the OTel
Collector's Kafka client (franz-go) has already cached that topic's
metadata causes a stuck `UNKNOWN_TOPIC_ID` retry loop — the client doesn't
notice the topic was swapped out for a new one with a new internal ID. Fix
is just `docker compose restart otel-collector` to force fresh metadata.
Not a bug in this project's config; a property of topic deletion generally,
worth knowing before doing it against a long-running producer in Phase 2+.

## 2026-10-14 — Idempotency key: `event_id = "{trace_id}:{span_id}"`

Rather than a synthetic UUID generated at ingest time, the idempotency key
is deterministic: the same span decoded twice (e.g. after a consumer
restart or a full topic replay) always produces the same `event_id`, so
`INSERT ... ON CONFLICT (event_id) DO NOTHING` is sufficient for
exactly-once-effect persistence over an at-least-once Kafka delivery
guarantee. No external dedup store needed.

## 2026-10-14 — Postgres host port 5432 collides with a pre-existing native install

**Discovered, not anticipated:** this machine already runs a native
Postgres on `127.0.0.1:5432` and `[::1]:5432` (`lsof -nP -iTCP:5432` showed
PID 1063). Docker's published port for the `postgres` service also bound
`:5432` without erroring, but host-side connections (`alembic`, `psql`)
silently went to the WRONG Postgres (the native one, which has no
`pulseops` role), producing a confusing "role does not exist" error rather
than a connection-refused. Fixed by moving the host-side mapping to
`${POSTGRES_HOST_PORT:-55432}:5432` in `docker-compose.yml`. In-network
traffic (`processor` -> `postgres:5432`) was never affected, since
container-to-container DNS resolution doesn't go through the host network
stack at all.

## 2026-10-14 — `RETURNING` instead of `cursor.rowcount` for insert counts

The processor logs how many of a batch's events were newly inserted vs.
already-seen duplicates, for observability and for the Phase 2 recovery
tests to verify replay behavior. The first implementation used
`conn.execute(stmt).rowcount` on a multi-row `INSERT ... ON CONFLICT DO
NOTHING` -- this returned `-1` with psycopg3/SQLAlchemy 2.1 in practice
(rowcount is documented as unreliable for statements where the driver can't
cheaply determine affected-row count, and multi-row inserts via
`insertmanyvalues` batching are exactly such a case). Switched to
`.returning(Event.id)` and counting the returned rows instead: rows skipped
by the conflict simply don't appear in `RETURNING`, so the count is exact
regardless of driver-level rowcount quirks. Verified this was purely a
logging bug, not a correctness bug -- `SELECT count(*), count(DISTINCT
event_id)` against the real table showed no duplicates even while rowcount
was misreporting `-1`.

## 2026-10-14 — Consumer `session.timeout.ms` tuned down to 10s

librdkafka's default consumer group session timeout is 45s. A stopped
Python process doesn't send Kafka's `LeaveGroup` request (no SIGTERM
handler), so Kafka only notices the member is gone via a missed-heartbeat
timeout -- at the default, tooling like
`kafka-consumer-groups.sh --reset-offsets` has to wait out the full 45s
before the group leaves "Stable" state. Set
`session.timeout.ms: 10000` / `heartbeat.interval.ms: 3000` in
`processor/consumer.py`. This is a reasonable default for this system
regardless of testing convenience -- a demo/dev system doesn't need 45s of
tolerance for a missing consumer before rebalancing.

## 2026-10-14 — Crash-and-restart (not infinite in-process retry) on exhausted DB retries

`persist_events` retries transient `OperationalError`s with `tenacity`
(5 attempts, exponential backoff capped at 10s). If still failing after
that, the exception is allowed to propagate and kill the process, rather
than looping forever in-process. Reasoning: a crashed container restarts
via `restart: unless-stopped` and resumes from the last *committed* Kafka
offset (the failing message's offset was never committed), so no event is
lost -- but the failure becomes visible (container restart count, exit
logs) instead of silently spinning forever inside a process that looks
"up" in `docker compose ps`. Verified in
`tests/phase2/recovery_test.py::scenario_db_outage_retried_not_dropped`:
stopping Postgres mid-flight produces `RestartCount >= 1`, and the event
is persisted exactly once once Postgres returns.

## 2026-10-14 — Alembic migration generated via `--autogenerate`, not hand-written

`processor/migrations/versions/..._create_events_table.py` was produced by
running `alembic revision --autogenerate` against a real running Postgres
with the SQLAlchemy models already defined, then inspected (not blindly
trusted) before applying. This guarantees the migration's DDL actually
matches what SQLAlchemy would generate for the ORM models, rather than a
hand-written migration silently drifting from `processor/models.py` over
time.

## 2026-10-14 — Project repo separate from home-directory git repo

The user's home directory (`~`) is itself a pre-existing, unrelated git
repo containing hundreds of personal/unrelated files. `~/pulseops` was
initialized as its own fresh git repository rather than committing into the
home-directory repo, to keep this project's history clean and independently
shareable (e.g. as its own GitHub repo later).
