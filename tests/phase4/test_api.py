"""Pytest wrapper around tests/phase4/api_test.py.

Integration test, not a unit test: requires the full docker-compose stack
(including `api`) to already be running. Skipped automatically if the API
isn't reachable.
"""

import sys
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "tests" / "phase4"))

API_BASE = "http://localhost:8080"


def _api_is_up() -> bool:
    try:
        return httpx.get(f"{API_BASE}/health", timeout=2.0).status_code == 200
    except httpx.RequestError:
        return False


@pytest.mark.skipif(not _api_is_up(), reason="management API is not running (docker compose up -d)")
def test_phase4_management_api():
    import api_test

    assert api_test.main() == 0
