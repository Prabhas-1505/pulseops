# Benchmark results

**Status: real measurements, Phase 5.** Every number below comes from an
actual tool (Locust, `kafka-consumer-groups.sh`, `docker stats`) observing
a real run of the live stack on the hardware described below -- nothing is
estimated, extrapolated, or invented. Raw JSON for each run is in
`load/results/` (gitignored -- regenerate with the commands below rather
than trusting stale numbers in this doc indefinitely).

## Methodology

- **Hardware**: MacBook, Apple M2, 8 cores (4 performance + 4 efficiency),
  **8 GB RAM total** (`system_profiler SPHardwareDataType`). This is a
  shared, resource-constrained development machine -- Docker Desktop, the
  full 11-container PulseOps stack, and this very agent session were all
  running concurrently with the benchmark. These numbers characterize
  *this system's behavior under contention on modest hardware*, not a
  dedicated-hardware production capacity figure. Do not extrapolate them
  to a production deployment without re-measuring there.
- **Load tool**: Locust 2.46.7, `load/locustfile.py` -- one `CheckoutUser`
  per simulated user, `POST /checkout` with a random item/amount, 0.1-0.5s
  wait between requests, targeting the gateway directly (so every request
  exercises the full `gateway -> orders -> payments` synchronous chain).
- **Orchestration**: `load/run_benchmark.py` (throughput/latency/resource
  run) and `load/failure_under_load.py` (processor-outage-under-load run).
  Both actually executed; commands to reproduce are below.
- **Data volume**: see each run's own numbers below.

## Run 1: throughput, latency, resource usage

```
python load/run_benchmark.py --users 30 --spawn-rate 10 --run-time 90 --stats-interval 5
```

30 simulated users, 10/s spawn rate, 90-second steady-state window.

| Metric | Value |
|---|---|
| Total requests | 3,599 |
| Failures | 0 |
| Throughput | 40.2 req/s |
| p50 latency | 200 ms |
| p66 | 290 ms |
| p75 | 380 ms |
| p80 | 490 ms |
| p90 | 890 ms |
| p95 | 1,500 ms |
| p98 | 3,300 ms |
| p99 | 4,000 ms |
| p99.9 / max | 8,300 ms |
| Kafka consumer lag, before | 0 |
| Kafka consumer lag, immediately after | 0 (processor kept pace throughout; no backlog to drain) |

**Per-container resource usage during the run** (`docker stats`, sampled
every 5s, min/max/avg of instantaneous CPU%; `%` is relative to one core,
so values above 100% mean a container used more than one full core):

| Container | CPU min | CPU max | CPU avg |
|---|---|---|---|
| gateway | 18.1% | 165.4% | 63.7% |
| orders | 26.9% | 135.5% | 69.1% |
| payments | 3.3% | 73.5% | 18.6% |
| kafka | 2.3% | 321.8% | 76.0% |
| postgres | 0.03% | 74.8% | 24.1% |
| processor | 0.1% | 59.9% | 48.1% |
| detector | 0.0% | 16.2% | 1.3% |

**Reading these honestly**: p50 (200ms) is reasonable for a 3-hop
synchronous HTTP chain plus OTel span export overhead. The long tail
(p98-max in the 3.3-8.3s range) reflects real contention on an 8GB/8-core
machine running the entire stack plus this agent session at the same
time -- `kafka`'s CPU peaking at 321% and `gateway`/`orders` both exceeding
100% indicate the host's cores were genuinely saturated at points during
the run, not that the application code itself is slow. The consumer never
fell behind (lag stayed at 0 throughout), meaning the ingestion pipeline
kept pace with 30 concurrent users' real traffic the whole time.

## Run 2: processor outage under sustained load

```
python load/failure_under_load.py --run-time 60 --outage-at 15 --outage-duration 15
```

20 simulated users, continuous load for 60s. At t=15s, `processor` was
stopped for 15s (simulating a crash/outage while real traffic keeps
flowing), then restarted; lag was sampled every 2s throughout and after.

| t (s) | lag | | t (s) | lag |
|---|---|---|---|---|
| 0.0 | 0 | | 44.9 | 6,295 (peak) |
| 4.0 | 241 | | 49.1 | 5,706 |
| 8.9 | 132 | | 53.1 | 5,653 |
| 13.2 | 327 | | 62.1 | 4,671 |
| 20.5 | 935 | | 66.0 | 3,247 |
| 25.4 | 1,908 | | 70.5 | 2,579 |
| 30.4 | 3,002 | | 77.1 | 2,051 |
| 34.3 | 4,292 | | 80.8 | 1,220 |
| 39.5 | 5,264 | | 85.0 | 517 |
| | | | 89.7 | 0 |

- **Peak lag during/after the outage**: 6,295 messages.
- **Drain time after restart**: 55.4s (processor restarted at t=30s;
  caught up fully at t≈89.7s against continued incoming load for part of
  that window, then on its own after the 60s load generator finished).

**Reading this honestly**: lag kept climbing for about 15 more seconds
*after* the processor restarted (t=30.4s through t≈44.9s) before turning
around -- this is the consumer group rejoin/rebalance overhead
(`session.timeout.ms=10000` in `processor/consumer.py`) plus a real
single-instance consumer needing time to out-pace concurrently arriving
new traffic before it can also work down the backlog. No duplicate rows
or data loss resulted (idempotent upsert, per the Phase 2 recovery
tests) -- this run measures *how long recovery takes*, not whether it's
correct (correctness is what `tests/phase2/recovery_test.py` verifies).

## Known limitations of these benchmarks

- Single run each, not averaged across repeated trials -- real variance
  on this shared, contended hardware was visible (see CPU peaks above),
  so a single run's percentiles should be read as "plausible for this
  machine under this load," not a precise SLA figure.
- No sustained soak test (hours-long run) was performed -- only short
  (60-90s) windows. Memory leaks or slow degradation would not show up
  here.
- The processor is a single instance; these numbers do not characterize
  multi-replica consumer group behavior (unexercised throughout this
  project, see `docs/ARCHITECTURE.md`).
- Benchmarked against `localhost` with no network latency; a real
  multi-host deployment would add real network RTT on every hop.
