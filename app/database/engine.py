from __future__ import annotations

from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from app.config import settings

_engine: Optional[Engine] = None

# MUST be callable at import time
SessionLocal = sessionmaker(autocommit=False, autoflush=False, future=True)


def init_engine() -> None:
    """Initialize global engine and bind SessionLocal (safe to call multiple times)."""
    global _engine
    if _engine is not None:
        return

    url = str(settings.DATABASE_URL).strip()
    connect_args = {}

    if url.startswith("sqlite:"):
        connect_args["check_same_thread"] = False

    _engine = create_engine(
        url,
        future=True,
        pool_pre_ping=True,
        connect_args=connect_args,
    )

    SessionLocal.configure(bind=_engine)


def get_engine() -> Engine:
    if _engine is None:
        init_engine()
    assert _engine is not None
    return _engine


def init_schema_check() -> None:
    """Connectivity check + create tables if missing."""
    eng = get_engine()
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))
        conn.commit()

    from app.database.models import Base

    Base.metadata.create_all(bind=eng)
