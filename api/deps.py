from __future__ import annotations

import os
from collections.abc import Generator

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from api.auth import decode_access_token, decode_sse_ticket
from api.models import User
from services.common.db import build_engine

_engine: Engine | None = None
_bearer_scheme = HTTPBearer(auto_error=False)


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = build_engine()
    return _engine


def get_db() -> Generator[Session, None, None]:
    with Session(get_engine()) as session:
        yield session


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
    db: Session = Depends(get_db),
) -> User:
    if credentials is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing bearer token")
    username = decode_access_token(credentials.credentials)
    if username is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid or expired token")
    user = db.query(User).filter(User.username == username).one_or_none()
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="user no longer exists")
    return user


def client_identity(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def get_current_user_sse(
    ticket: str | None = None,
    db: Session = Depends(get_db),
) -> User:
    """Auth for the SSE stream via a short-lived, single-purpose `?ticket=`
    (see `POST /incidents/stream/ticket`), not the general-purpose bearer
    JWT. The browser's native EventSource API cannot set custom headers, so
    putting *something* in the URL is unavoidable for SSE -- but a normal
    30-minute bearer token sitting in a query string is a known exposure
    (proxy/server access logs, browser history, Referer headers). A 60s
    ticket, scoped to this endpoint only and useless after it's been used
    to open the connection, bounds that exposure to nearly nothing instead
    of a third of an hour of full API access.
    """
    if ticket is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing ?ticket=")
    username = decode_sse_ticket(ticket)
    if username is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid or expired ticket")
    user = db.query(User).filter(User.username == username).one_or_none()
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="user no longer exists")
    return user
