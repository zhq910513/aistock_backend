from __future__ import annotations

from typing import Optional

from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from app.config import settings

_engine: Optional[Engine] = None

# IMPORTANT:
# - SessionLocal must be callable at import time.
# - We configure its bind lazily in init_engine().
SessionLocal = sessionmaker(autocommit=False, autoflush=False, future=True)


def _apply_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
    # NOTE: sqlite3.Connection interface
    try:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.execute("PRAGMA busy_timeout=5000;")
        cursor.close()
    except Exception:
        # best-effort; do not block engine creation
        pass


def init_engine() -> None:
    """Initialize the global SQLAlchemy Engine + bind SessionLocal.

    Safe to call multiple times.
    """
    global _engine

    if _engine is not None:
        return

    url = str(settings.DATABASE_URL).strip()

    connect_args: dict = {}

    # SQLite needs special handling for threads.
    is_sqlite = url.startswith("sqlite:")
    if is_sqlite:
        connect_args["check_same_thread"] = False
    else:
        # For Postgres (and many DBs supporting this option), enforce session timezone.
        # This reduces offset-naive/offset-aware surprises when DB server timezone differs.
        connect_args["options"] = "-c timezone=Asia/Shanghai"

    _engine = create_engine(
        url,
        future=True,
        pool_pre_ping=True,
        connect_args=connect_args,
    )

    if is_sqlite:
        event.listen(_engine, "connect", _apply_sqlite_pragmas)

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
