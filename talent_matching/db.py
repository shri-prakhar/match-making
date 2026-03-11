"""Centralized PostgreSQL engine factory.

All IO managers and resources share a single SQLAlchemy engine per process.
Since Dagster's DefaultRunLauncher spawns one subprocess per run, this means
each run gets exactly one engine instead of 4+ independent engines.

Uses NullPool (same as dagster_postgres internals): connections are opened on
demand and returned to the OS immediately after use. This means zero idle
connections per run — only connections actively executing a query count against
PostgreSQL's max_connections. Since each run processes assets sequentially, at
most 1 app connection is open at a time per run.

Connection budget (max_connections=200):
  25 runs × 1 active app conn = 25
  Dagster transient (NullPool): ~50 peak
  Daemon + webserver: ~10
  Total peak: ~85, well within 200
"""

import os
import threading

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

_lock = threading.Lock()
_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def _build_url() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "talent")
    password = os.getenv("POSTGRES_PASSWORD", "talent_dev")
    database = os.getenv("POSTGRES_DB", "talent_matching")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def get_engine() -> Engine:
    """Return the process-wide SQLAlchemy engine, creating it on first call."""
    global _engine, _session_factory
    if _engine is None:
        with _lock:
            if _engine is None:
                _engine = create_engine(_build_url(), poolclass=NullPool)
                _session_factory = sessionmaker(bind=_engine)
    return _engine


def get_session() -> Session:
    """Create a new session from the shared engine."""
    get_engine()
    assert _session_factory is not None
    return _session_factory()
