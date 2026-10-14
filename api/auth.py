from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt

_PLACEHOLDER_SECRET = "change-me-in-local-env"
JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY")
if not JWT_SECRET_KEY or JWT_SECRET_KEY == _PLACEHOLDER_SECRET:
    raise RuntimeError(
        "JWT_SECRET_KEY is unset or still the placeholder value from .env.example. "
        "Every JWT this API would ever issue or accept is signed with this key -- leaving "
        "it as the publicly-committed default means anyone who has read this repo can forge "
        "a valid token for any user. Set a real secret before starting the API, e.g.: "
        "openssl rand -hex 32"
    )

JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = int(os.environ.get("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "30"))

# Scope claim distinguishes a normal API bearer token from the short-lived,
# single-purpose SSE ticket (see create_sse_ticket) -- so a ticket leaked via
# the SSE URL (query string, subject to log/history/referrer exposure) can't
# be replayed against any other endpoint, and a normal bearer token can't be
# used to open the SSE stream even if someone tries.
SCOPE_API = "api"
SCOPE_SSE = "sse"


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(password.encode("utf-8"), hashed_password.encode("utf-8"))


def _create_token(username: str, scope: str, expires_in_minutes: float) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=expires_in_minutes)
    payload = {"sub": username, "exp": expire, "scope": scope}
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def create_access_token(username: str) -> str:
    return _create_token(username, SCOPE_API, JWT_ACCESS_TOKEN_EXPIRE_MINUTES)


def create_sse_ticket(username: str) -> str:
    """A short-lived (60s), single-purpose token for opening the SSE stream.

    Only valid at connection time (the FastAPI dependency checks it once,
    when the request arrives; the stream itself is then trusted for its
    whole lifetime, same as any other long-lived connection's handshake
    auth) -- 60s is ample for "fetch a ticket, immediately open the
    EventSource," while keeping the window an intercepted query-string
    token would be useful for small.
    """
    return _create_token(username, SCOPE_SSE, expires_in_minutes=1)


def _decode(token: str, expected_scope: str) -> str | None:
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
    except jwt.PyJWTError:
        return None
    if payload.get("scope") != expected_scope:
        return None
    return payload.get("sub")


def decode_access_token(token: str) -> str | None:
    """Returns the username (sub claim) for a normal API-scoped token, or None."""
    return _decode(token, SCOPE_API)


def decode_sse_ticket(token: str) -> str | None:
    """Returns the username (sub claim) for an SSE-scoped ticket, or None."""
    return _decode(token, SCOPE_SSE)
