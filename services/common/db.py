from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Shared declarative base so Alembic's autogenerate sees every table
    (processor/models.py, detector/models.py, ...) against one metadata
    object, regardless of which backend component owns which table."""


def database_url() -> str:
    user = os.environ.get("POSTGRES_USER", "pulseops")
    password = os.environ.get("POSTGRES_PASSWORD", "change-me-in-local-env")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "pulseops")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"


def build_engine() -> Engine:
    return create_engine(database_url(), pool_pre_ping=True, future=True)
