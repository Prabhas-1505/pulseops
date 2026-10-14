#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> docker compose up -d (build if needed)"
docker compose up -d --build

echo "==> waiting for kafka-init to finish creating topics"
docker compose wait kafka-init

echo "==> running smoke test"
source .venv/bin/activate 2>/dev/null || true
python kafka/smoke_test.py
