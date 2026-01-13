import asyncio

from fastapi import FastAPI

from app.api.router import router
from app.config import settings
from app.core.orchestrator import Orchestrator
from app.core.labeling_pipeline import LabelingPipeline
from app.database.engine import init_engine, init_schema_check, SessionLocal
from app.database.repo import Repo


app = FastAPI(
    title="AIStock_backend (QEE-S³/S³.1)",
    version="2.0.0",
    # Reverse-proxy aware Swagger/OpenAPI paths:
    root_path=settings.API_ROOT_PATH,
    docs_url="/docs",
    redoc_url=None,
    openapi_url="/openapi.json",
    swagger_ui_oauth2_redirect_url="/docs/oauth2-redirect",
)

app.include_router(router)

_orchestrator: Orchestrator | None = None
_orchestrator_task: asyncio.Task | None = None
_labeling_pipeline: LabelingPipeline | None = None


@app.on_event("startup")
async def _startup() -> None:
    global _orchestrator, _orchestrator_task, _labeling_pipeline

    init_engine()
    init_schema_check()

    # Ensure SystemStatus row exists (keeps /status deterministic and avoids first-hit races).
    with SessionLocal() as s:
        Repo(s).system_status.get_for_update()
        s.commit()

    # Optional: start the continuous labeling research factory.
    if settings.LABELING_AUTO_FETCH_ENABLED:
        try:
            _labeling_pipeline = LabelingPipeline()
            _labeling_pipeline.start()
        except Exception:
            _labeling_pipeline = None

    # Orchestrator is optional; default off for API-only deployments/tests.
    if settings.START_ORCHESTRATOR:
        try:
            _orchestrator = Orchestrator()
            _orchestrator_task = asyncio.create_task(_orchestrator.run())
        except Exception:
            # Keep API up even if orchestrator init fails.
            _orchestrator = None
            _orchestrator_task = None


@app.on_event("shutdown")
async def _shutdown() -> None:
    global _orchestrator, _orchestrator_task, _labeling_pipeline

    if _labeling_pipeline:
        _labeling_pipeline.stop()

    if _orchestrator:
        _orchestrator.stop()

    if _orchestrator_task:
        _orchestrator_task.cancel()
        try:
            await _orchestrator_task
        except Exception:
            # ignore cancel/teardown errors
            pass
