import asyncio

from fastapi import FastAPI

from app.api.router import router
from app.config import settings
from app.core.orchestrator import Orchestrator
from app.database.engine import init_engine, init_schema_check, SessionLocal
from app.database.repo import Repo


app = FastAPI(
    title="AIStock_backend (QEE-S³/S³.1)",
    version="2.0.0",
    root_path=settings.API_ROOT_PATH,
    docs_url="/docs",
    redoc_url=None,
    openapi_url="/openapi.json",
    swagger_ui_oauth2_redirect_url="/docs/oauth2-redirect",
)

app.include_router(router)

_orchestrator: Orchestrator | None = None
_orchestrator_task: asyncio.Task | None = None


@app.on_event("startup")
async def _startup() -> None:
    global _orchestrator, _orchestrator_task

    init_engine()
    init_schema_check()

    # Ensure SystemStatus row exists to make /status deterministic
    with SessionLocal() as s:
        Repo(s).system_status.get_for_update()
        s.commit()

    if settings.START_ORCHESTRATOR:
        try:
            _orchestrator = Orchestrator()
            _orchestrator_task = asyncio.create_task(_orchestrator.run())
        except Exception:
            _orchestrator = None
            _orchestrator_task = None


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
            pass
