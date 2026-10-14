# Benchmark results

**Status: not yet run.** This file is a methodology template, populated with
real numbers in Phase 5 (load testing, consumer lag, resource usage). No
numbers in this document are invented placeholders — until Phase 5 runs,
there are none.

## What will be measured

- **Throughput**: requests/sec the `gateway -> orders -> payments` chain can
  sustain before p99 latency or error rate degrades, under a Locust/k6 load
  profile defined in `scripts/` (not yet written).
- **Latency**: p50/p95/p99 for `/checkout`, measured at the load generator,
  under normal conditions and under each fault-injection scenario.
- **Kafka consumer lag**: lag (messages and estimated time) for the Phase 2
  durable consumer group under sustained load and during a simulated
  consumer restart/replay.
- **Resource usage**: CPU and memory for each container (`docker stats` or
  equivalent) during the load test window.
- **Failure behavior**: what actually happens (not what's expected to
  happen) when Kafka, the collector, or a downstream service is killed
  mid-load — duplicate counts after consumer restart, data loss (if any),
  recovery time.

## Methodology (to be filled in when run)

For every run, this document will record:

- **Hardware**: CPU model/core count, RAM, whether Docker Desktop resource
  limits were in effect.
- **Duration**: wall-clock length of the load test.
- **Data volume**: total requests sent, total Kafka messages produced,
  total DB rows written.
- **Load tool + config**: Locust or k6, concurrency/arrival-rate settings,
  exact command used.
- **Raw output**: the actual tool output (or a link to a saved log file in
  this repo), not a paraphrase.

## Why this is empty right now

Phase 1 only proves the pipeline works for individual, hand-driven requests
(see `docs/ARCHITECTURE.md` for the verified smoke test and fault-injection
transcripts). It does not yet have a durable consumer, a database to write
load into, or a load-generation script — benchmarking before those exist
would produce numbers that don't reflect the system described in later
phases. Claiming throughput/latency numbers now would violate this
project's own engineering rules (no invented results, no claims beyond what
was measured).
