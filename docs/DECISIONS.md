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

## 2026-10-14 — Project repo separate from home-directory git repo

The user's home directory (`~`) is itself a pre-existing, unrelated git
repo containing hundreds of personal/unrelated files. `~/pulseops` was
initialized as its own fresh git repository rather than committing into the
home-directory repo, to keep this project's history clean and independently
shareable (e.g. as its own GitHub repo later).
