#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> docker compose up -d (build if needed)"
# No separate "wait for kafka-init" step needed: otel-collector depends on
# kafka-init via `condition: service_completed_successfully`, so `up`
# itself already blocks until kafka-init has finished before returning.
# (`docker compose wait kafka-init` here would actually fail -- it only
# works on a still-running container, and by this point kafka-init has
# already exited; verified by hitting this exact failure in CI.)
docker compose up -d --build

echo "==> running smoke test"
source .venv/bin/activate 2>/dev/null || true
python kafka/smoke_test.py
