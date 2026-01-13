from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ENV: str = "prod"
    TZ: str = "Asia/Shanghai"

    # Reverse proxy mount prefix
    API_ROOT_PATH: str = "/aistock/api"

    # Orchestrator optional
    START_ORCHESTRATOR: bool = False

    # Database
    DATABASE_URL: str = "sqlite:////app/db/aistock.sqlite3"

    # Data sources
    DATA_PROVIDER: str = "IFIND_HTTP"
    THS_MODE: str = "MOCK"

    IFIND_HTTP_BASE_URL: str = "https://quantapi.51ifind.com"
    IFIND_HTTP_REFRESH_TOKEN: str = ""
    IFIND_HTTP_TOKEN: str = ""

    # Strategy / objectives
    HOLD_DAYS_MIN: int = 1
    HOLD_DAYS_MAX: int = 3
    TARGET_RETURN_MIN: float = 0.05
    TARGET_RETURN_MAX: float = 0.08

    # Orchestrator loop
    ORCH_SYMBOLS: str = "000001.SZ,600000.SH"
    ORCH_LOOP_INTERVAL_MS: int = 1000

    # Governance / gates
    REQUIRE_SELF_CHECK_FOR_TRADING: bool = False
    SELF_CHECK_MAX_AGE_SEC: int = 3600
    SCHED_DRIFT_THRESHOLD_MS: int = 2000
    EPSILON_MIN_MS: int = 200

    # Outbox
    OUTBOX_MAX_ATTEMPTS: int = 8
    OUTBOX_BACKOFF_BASE_MS: int = 200
    OUTBOX_BACKOFF_MAX_MS: int = 5000

    # Audit / shadow
    SHADOW_LIVE_ENABLED: bool = False
    POST_MARKET_RECONCILE_ENABLED: bool = False

    # Versions
    API_SCHEMA_VERSION: str = "dev"
    RULESET_VERSION_HASH: str = "dev"
    STRATEGY_CONTRACT_HASH: str = "dev"
    MODEL_SNAPSHOT_UUID: str = "dev"
    COST_MODEL_VERSION: str = "dev"
    CANONICALIZATION_VERSION: str = "dev"
    FEATURE_EXTRACTOR_VERSION: str = "dev"

    @field_validator("API_ROOT_PATH", mode="before")
    @classmethod
    def _root_path_strip(cls, v: object) -> str:
        s = "" if v is None else str(v).strip()
        if not s:
            return ""
        if not s.startswith("/"):
            s = "/" + s
        return s.rstrip("/")


settings = Settings()
