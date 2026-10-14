"""Phase 4 management API integration tests, run against the live stack.

Exercises real HTTP requests against the running `api` service: auth
(register/login/401 enforcement), pagination and filtering on /incidents,
incident detail shape, acknowledge, rate limiting, the SSE stream, and that
OpenAPI docs are actually served. Does not mock anything -- this is the
same API a real client would call.
"""

from __future__ import annotations

import sys
import time
import uuid

import httpx
import redis

API_BASE = "http://localhost:8080"


def _clear_login_rate_limit() -> None:
    """Flush this test's own rate-limit keys so back-to-back runs within the
    60s window don't see a 429 from the PREVIOUS run's deliberate brute-force
    check -- that's a test-isolation artifact, not a product bug."""
    try:
        r = redis.Redis(host="localhost", port=63790, decode_responses=True)
        for key in r.scan_iter("ratelimit:/auth/login:*"):
            r.delete(key)
    except redis.RedisError:
        pass

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}: {name}" + (f" -- {detail}" if detail else ""))


def main() -> int:
    _clear_login_rate_limit()
    username = f"apitest_{uuid.uuid4().hex[:10]}"
    password = "supersecret123"

    # --- health & docs ---
    health = httpx.get(f"{API_BASE}/health", timeout=5.0)
    check("GET /health returns 200", health.status_code == 200, str(health.json()))

    openapi = httpx.get(f"{API_BASE}/openapi.json", timeout=5.0)
    check("GET /openapi.json returns a valid schema with expected paths", "/incidents" in openapi.json().get("paths", {}))

    docs = httpx.get(f"{API_BASE}/docs", timeout=5.0)
    check("GET /docs (Swagger UI) returns 200", docs.status_code == 200)

    # --- auth: register + login ---
    reg = httpx.post(f"{API_BASE}/auth/register", json={"username": username, "password": password}, timeout=5.0)
    check("register succeeds for a fresh username", reg.status_code == 201, f"got {reg.status_code}: {reg.text}")

    dup = httpx.post(f"{API_BASE}/auth/register", json={"username": username, "password": password}, timeout=5.0)
    check("registering the same username again returns 409", dup.status_code == 409, f"got {dup.status_code}")

    bad_login = httpx.post(f"{API_BASE}/auth/login", json={"username": username, "password": "wrong"}, timeout=5.0)
    check("login with wrong password returns 401", bad_login.status_code == 401)

    login = httpx.post(f"{API_BASE}/auth/login", json={"username": username, "password": password}, timeout=5.0)
    check("login with correct password returns a token", login.status_code == 200 and "access_token" in login.json())
    token = login.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # --- authorization enforcement ---
    no_auth = httpx.get(f"{API_BASE}/incidents", timeout=5.0)
    check("GET /incidents with no token returns 401", no_auth.status_code == 401)

    bad_token = httpx.get(f"{API_BASE}/incidents", headers={"Authorization": "Bearer garbage.not.a.jwt"}, timeout=5.0)
    check("GET /incidents with a malformed token returns 401", bad_token.status_code == 401)

    # --- pagination & filtering ---
    page1 = httpx.get(f"{API_BASE}/incidents", headers=headers, params={"limit": 2, "offset": 0}, timeout=5.0)
    check("GET /incidents respects limit=2", page1.status_code == 200 and len(page1.json()["items"]) <= 2, str(page1.json()))

    all_incidents = httpx.get(f"{API_BASE}/incidents", headers=headers, params={"limit": 100}, timeout=5.0).json()
    resolved_only = httpx.get(f"{API_BASE}/incidents", headers=headers, params={"status": "resolved", "limit": 100}, timeout=5.0).json()
    check(
        "status=resolved filter only returns resolved incidents",
        all(item["status"] == "resolved" for item in resolved_only["items"]),
        f"{len(resolved_only['items'])} items checked",
    )

    bad_filter = httpx.get(f"{API_BASE}/incidents", headers=headers, params={"status": "bogus"}, timeout=5.0)
    check("invalid status filter value is rejected with 422", bad_filter.status_code == 422)

    # --- incident detail + acknowledge ---
    if all_incidents["items"]:
        incident_id = all_incidents["items"][0]["id"]
        detail = httpx.get(f"{API_BASE}/incidents/{incident_id}", headers=headers, timeout=5.0)
        body = detail.json()
        check(
            "incident detail includes alerts and evidence",
            detail.status_code == 200 and "alerts" in body and "evidence" in body,
            f"keys={list(body.keys())}",
        )

        ack = httpx.post(f"{API_BASE}/incidents/{incident_id}/acknowledge", headers=headers, json={"note": "test"}, timeout=5.0)
        check(
            "acknowledge sets acknowledged_by to the calling user",
            ack.status_code == 200 and ack.json()["acknowledged_by"] == username,
            str(ack.json()),
        )
    else:
        check("incident detail includes alerts and evidence", False, "no incidents exist to test against")
        check("acknowledge sets acknowledged_by to the calling user", False, "no incidents exist to test against")

    missing = httpx.get(f"{API_BASE}/incidents/99999999", headers=headers, timeout=5.0)
    check("nonexistent incident id returns 404", missing.status_code == 404)

    # --- services ---
    svc_health = httpx.get(f"{API_BASE}/services/health", headers=headers, timeout=5.0)
    check(
        "GET /services/health returns known services",
        svc_health.status_code == 200 and {s["service"] for s in svc_health.json()} >= {"gateway", "orders", "payments"},
        str(svc_health.json()),
    )

    # --- rate limiting (bounded: don't assume exact attempt count, prior
    # test runs in the same Redis window may have already consumed quota) ---
    saw_429 = False
    for _ in range(10):
        resp = httpx.post(f"{API_BASE}/auth/login", json={"username": "nonexistent", "password": "x"}, timeout=5.0)
        if resp.status_code == 429:
            saw_429 = True
            break
    check("repeated /auth/login attempts eventually trigger 429", saw_429)
    _clear_login_rate_limit()  # don't leak this into the next test run

    # --- SSE smoke check: short-lived ticket, not the long-lived bearer token ---
    ticket_resp = httpx.post(f"{API_BASE}/incidents/stream/ticket", headers=headers, timeout=5.0)
    check("POST /incidents/stream/ticket issues a ticket", ticket_resp.status_code == 200 and "ticket" in ticket_resp.json())
    ticket = ticket_resp.json().get("ticket", "")

    sse_ok = False
    try:
        with httpx.stream("GET", f"{API_BASE}/incidents/stream/live", params={"ticket": ticket}, timeout=10.0) as resp:
            if resp.status_code == 200:
                for line in resp.iter_lines():
                    if line.startswith("event: snapshot"):
                        sse_ok = True
                        break
    except httpx.ReadTimeout:
        pass
    check("SSE stream delivers at least one snapshot event", sse_ok)

    sse_long_lived_rejected = httpx.get(f"{API_BASE}/incidents/stream/live", params={"ticket": token}, timeout=5.0)
    check(
        "SSE stream rejects a normal long-lived bearer token used as a ticket",
        sse_long_lived_rejected.status_code == 401,
    )

    sse_unauth = httpx.get(f"{API_BASE}/incidents/stream/live", timeout=5.0)
    check("SSE stream without a ticket returns 401", sse_unauth.status_code == 401)

    print("\n--- Summary ---")
    for name, ok, _ in results:
        print(f"{'PASS' if ok else 'FAIL'}: {name}")
    failed = [n for n, ok, _ in results if not ok]
    if failed:
        print(f"\n{len(failed)} check(s) failed.")
        return 1
    print(f"\nAll {len(results)} checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
