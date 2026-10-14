"""Pytest wrapper around tests/phase3/evaluation.py.

Integration test, not a unit test: requires the full docker-compose stack
(including `detector`) to already be running, and actively drives fault
traffic through it. Takes a few minutes (it waits out real detection
windows and the resolution grace period). Skipped automatically if the
gateway isn't reachable.
"""

import os
import sys
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "tests" / "phase3"))

GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:8000")


def _stack_is_up() -> bool:
    try:
        return httpx.get(f"{GATEWAY_URL}/health", timeout=2.0).status_code == 200
    except httpx.RequestError:
        return False


@pytest.mark.skipif(not _stack_is_up(), reason="docker-compose stack is not running (docker compose up -d)")
def test_phase3_fault_injection_evaluation():
    import evaluation

    assert evaluation.main() == 0
