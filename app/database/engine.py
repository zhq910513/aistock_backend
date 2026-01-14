from __future__ import annotations

from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import sessionmaker

from app.config import settings

_engine: Optional[Engine] = None

# IMPORTANT:
# - SessionLocal must be callable at import time.
# - We configure its bind lazily in init_engine().
SessionLocal = sessionmaker(autocommit=False, autoflush=False, future=True)


def _sanitize_database_url(url_str: str) -> str:
    """
    SQLite DB-API (sqlite3.connect) does NOT accept 'options' kwarg.
    Sometimes 'options' sneaks in via URL query or shared Postgres tuning logic.
    We hard-strip it for sqlite to guarantee startup.
    """
    u = make_url(url_str)
    if u.get_backend_name() == "sqlite":
        q = dict(u.query or {})
        q.pop("options", None)
        u = u.set(query=q)
    return str(u)


def init_engine() -> None:
    """Initialize the global SQLAlchemy Engine + bind SessionLocal.

    Safe to call multiple times.
    """
    global _engine
    if _engine is not None:
        return

    raw_url = str(settings.DATABASE_URL).strip()
    url = _sanitize_database_url(raw_url)

    connect_args: dict = {}

    # SQLite needs special handling for threads.
    if url.startswith("sqlite:"):
        connect_args["check_same_thread"] = False

    # HARD GUARD: sqlite cannot accept 'options' kwarg.
    connect_args.pop("options", None)

    _engine = create_engine(
        url,
        future=True,
        pool_pre_ping=True,
        connect_args=connect_args,
    )

    # Bind the already-imported SessionLocal factory (fixes 'NoneType is not callable').
    SessionLocal.configure(bind=_engine)


def get_engine() -> Engine:
    if _engine is None:
        init_engine()
    assert _engine is not None
    return _engine


def init_schema_check() -> None:
    """Connectivity + schema bootstrap.

    We treat schema as fresh:
    - verify connectivity
    - create tables if missing (create_all is safe on empty DB)
    """
    eng = get_engine()
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))
        conn.commit()

    from app.database.models import Base

    Base.metadata.create_all(bind=eng)
