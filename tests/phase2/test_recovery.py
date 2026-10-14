"""Pytest wrapper around tests/phase2/recovery_test.py.

Integration test, not a unit test: requires the full docker-compose stack
(including `processor` and `postgres`) to already be running, and it
actively stops/starts the `postgres` and `processor` containers as part of
the scenarios. Don't run this against a stack you care about keeping
untouched. Skipped automatically if the gateway isn't reachable.
"""

import os
import sys
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "tests" / "phase2"))

GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:8000")


def _stack_is_up() -> bool:
    try:
        return httpx.get(f"{GATEWAY_URL}/health", timeout=2.0).status_code == 200
    except httpx.RequestError:
        return False


@pytest.mark.skipif(not _stack_is_up(), reason="docker-compose stack is not running (docker compose up -d)")
def test_phase2_recovery_scenarios():
    import recovery_test

    assert recovery_test.main() == 0
