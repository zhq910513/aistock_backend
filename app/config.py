from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Runtime / env ---
    ENV: str = "prod"
    TZ: str = "Asia/Shanghai"

    # Reverse proxy prefix (Nginx mounts the API at /aistock/api/*)
    API_ROOT_PATH: str = "/aistock/api"

    # Orchestrator background loop is optional for API-only deployments/testing.
    START_ORCHESTRATOR: bool = False

    # --- Database ---
    DATABASE_URL: str = "sqlite:////app/db/aistock.sqlite3"

    # --- Data sources ---
    DATA_PROVIDER: str = "IFIND_HTTP"
    THS_MODE: str = "MOCK"

    IFIND_HTTP_BASE_URL: str = "https://quantapi.51ifind.com"
    IFIND_HTTP_REFRESH_TOKEN: str = ""
    IFIND_HTTP_TOKEN: str = ""

    # --- Strategy / objectives ---
    HOLD_DAYS_MIN: int = 1
    HOLD_DAYS_MAX: int = 3
    TARGET_RETURN_MIN: float = 0.05
    TARGET_RETURN_MAX: float = 0.08

    # --- Orchestrator loop ---
    ORCH_SYMBOLS: str = "000001.SZ,600000.SH"
    ORCH_LOOP_INTERVAL_MS: int = 1000

    # --- Governance / gates ---
    REQUIRE_SELF_CHECK_FOR_TRADING: bool = False
    SELF_CHECK_MAX_AGE_SEC: int = 3600
    SCHED_DRIFT_THRESHOLD_MS: int = 2000
    EPSILON_MIN_MS: int = 200

    # --- Outbox ---
    OUTBOX_MAX_ATTEMPTS: int = 8
    OUTBOX_BACKOFF_BASE_MS: int = 200
    OUTBOX_BACKOFF_MAX_MS: int = 5000

    # --- Audit / shadow ---
    SHADOW_LIVE_ENABLED: bool = False
    POST_MARKET_RECONCILE_ENABLED: bool = False

    # --- Versions (S³ invariants) ---
    API_SCHEMA_VERSION: str = "dev"
    RULESET_VERSION_HASH: str = "dev"
    STRATEGY_CONTRACT_HASH: str = "dev"
    MODEL_SNAPSHOT_UUID: str = "dev"
    COST_MODEL_VERSION: str = "dev"
    CANONICALIZATION_VERSION: str = "dev"
    FEATURE_EXTRACTOR_VERSION: str = "dev"

    # --- Accounts ---
    DEFAULT_ACCOUNT_ID: str = "SIM-PRIMARY"
    ACCOUNT_IDS: str = ""

    # --- Agent limits ---
    AGENT_MAX_REQUESTS_PER_SYMBOL: int = 4
    AGENT_VERIFY_MIN_CONFIDENCE: float = 0.55

    @field_validator("API_ROOT_PATH", mode="before")
    @classmethod
    def _root_path_strip(cls, v: object) -> str:
        s = "" if v is None else str(v).strip()
        if not s:
            return ""
        # normalize: ensure leading slash, no trailing slash
        if not s.startswith("/"):
            s = "/" + s
        return s.rstrip("/")

    @field_validator("IFIND_HTTP_BASE_URL", mode="before")
    @classmethod
    def _coerce_base_url(cls, v: object) -> str:
        if v is None:
            return "https://quantapi.51ifind.com"
        s = str(v).strip()
        return s or "https://quantapi.51ifind.com"

    @field_validator("IFIND_HTTP_TOKEN", "IFIND_HTTP_REFRESH_TOKEN", mode="before")
    @classmethod
    def _coerce_token(cls, v: object) -> str:
        if v is None:
            return ""
        return str(v).strip()


settings = Settings()
