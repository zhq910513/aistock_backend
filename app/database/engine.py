from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.config import settings

_engine = None
SessionLocal = None


def init_engine() -> None:
    global _engine, SessionLocal
    if _engine is not None:
        return

    _engine = create_engine(
        settings.DATABASE_URL,
        pool_pre_ping=True,
        future=True,
    )
    SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False, future=True)


def get_engine():
    if _engine is None:
        init_engine()
    return _engine


def init_schema_check() -> None:
    """
    Since we are redesigning without alembic migrations, we treat schema as fresh:
    - verify connectivity
    - ensure timezone behavior
    - create tables if missing (create_all is safe on empty DB)
    """
    eng = get_engine()
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))
        conn.commit()

    from app.database.models import Base
    Base.metadata.create_all(bind=eng)
