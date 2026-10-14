"""Pytest wrapper around kafka/smoke_test.py.

This is an integration test, not a unit test: it requires the full
docker-compose stack to already be running (`docker compose up -d`). It is
skipped automatically if the gateway isn't reachable, so `pytest` run without
the stack up doesn't report a false failure.
"""

import os
import sys
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "kafka"))

GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:8000")


def _stack_is_up() -> bool:
    try:
        return httpx.get(f"{GATEWAY_URL}/health", timeout=2.0).status_code == 200
    except httpx.RequestError:
        return False


@pytest.mark.skipif(not _stack_is_up(), reason="docker-compose stack is not running (docker compose up -d)")
def test_phase1_end_to_end_smoke():
    import smoke_test

    assert smoke_test.main() == 0
