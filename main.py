import asyncio
from fastapi import FastAPI

from app.api.router import router
from app.core.orchestrator import Orchestrator
from app.database.engine import init_engine, init_schema_check
from app.utils.time import now_shanghai_str


app = FastAPI(title="AIStock_backend (QEE-S³/S³.1)", version="2.0.0")
app.include_router(router)

_orchestrator: Orchestrator | None = None
_orchestrator_task: asyncio.Task | None = None


@app.on_event("startup")
async def _startup() -> None:
    init_engine()
    init_schema_check()

    global _orchestrator, _orchestrator_task
    _orchestrator = Orchestrator()
    _orchestrator_task = asyncio.create_task(_orchestrator.run(), name="orchestrator.run")

    # Startup audit
    from app.database.repo import Repo
    from app.database.engine import SessionLocal

    with SessionLocal() as s:
        Repo(s).system_events.write_event(
            event_type="SYSTEM_STARTUP",
            correlation_id=None,
            severity="INFO",
            payload={"time": now_shanghai_str(), "message": "Backend started"},
        )
        s.commit()


@app.on_event("shutdown")
async def _shutdown() -> None:
    global _orchestrator, _orchestrator_task

    if _orchestrator:
        _orchestrator.stop()

    if _orchestrator_task:
        _orchestrator_task.cancel()
        try:
            await _orchestrator_task
        except Exception:
            # ignore cancel/teardown errors
            pass
