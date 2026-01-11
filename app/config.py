from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ENV: str = "prod"
    TZ: str = "Asia/Shanghai"
    DATABASE_URL: str = "sqlite:////app/db/aistock.sqlite3"
    # --- Profit objective / horizon (1-3 days, 5-8%) ---
    HOLD_DAYS_MIN: int = 1
    HOLD_DAYS_MAX: int = 3
    TARGET_RETURN_MIN: float = 0.05
    TARGET_RETURN_MAX: float = 0.08

    # --- Frozen daily versions ---
    CANONICALIZATION_VERSION: str = "canon_v1"
    RULESET_VERSION_HASH: str = "ruleset_dev_hash_001"
    STRATEGY_CONTRACT_HASH: str = "contract_dev_hash_001"
    MODEL_SNAPSHOT_UUID: str = "stable_model_001"
    COST_MODEL_VERSION: str = "cost_v1"
    FEATURE_EXTRACTOR_VERSION: str = "fx_v1"

    # --- Anti-leakage ---
    EPSILON_MIN_MS: int = 200

    # --- Orchestrator ---
    ORCH_SYMBOLS: str = "000001.SZ,600000.SH"
    ORCH_LOOP_INTERVAL_MS: int = 750
    SCHED_DRIFT_THRESHOLD_MS: int = 250  # drift event threshold

    # --- THS adapter (market events source) ---
    # MOCK / IFIND_HTTP
    THS_MODE: str = "MOCK"

    # --- Data providers (agent loop evidence collection) ---
    DATA_PROVIDER: str = "IFIND_HTTP"  # MOCK / IFIND_HTTP

    # --- iFinD QuantAPI (HTTP) ---
    IFIND_HTTP_BASE_URL: str = "https://quantapi.51ifind.com"
    # Short-lived token (expires ~7 days). Prefer using refresh token.
    IFIND_HTTP_TOKEN: str = ""
    # Long-lived refresh token (aligned with account expiry). Used to obtain/refresh access_token.
    IFIND_HTTP_REFRESH_TOKEN: str = ""
    # If we don't know the token expiry precisely, we can still proactively refresh on a cadence.
    IFIND_HTTP_TOKEN_MAX_AGE_SEC: int = 6 * 24 * 3600  # 6 days (safety buffer vs 7 days)
    IFIND_HTTP_TOKEN_EARLY_REFRESH_SEC: int = 15 * 60  # refresh early when nearing max-age

    # --- Multi-account (execution plane) ---
    DEFAULT_ACCOUNT_ID: str = "ACC_PRIMARY"
    ACCOUNT_IDS: str = "ACC_PRIMARY"  # comma-separated known accounts
    MAX_CONCURRENT_POSITIONS_PER_ACCOUNT: int = 10

    # --- Governance: trade gate ---
    REQUIRE_SELF_CHECK_FOR_TRADING: bool = True
    SELF_CHECK_MAX_AGE_SEC: int = 3600  # 1h

    # --- Outbox retry policy (deterministic) ---
    OUTBOX_MAX_ATTEMPTS: int = 12
    OUTBOX_BACKOFF_BASE_MS: int = 250
    OUTBOX_BACKOFF_MAX_MS: int = 30_000

    # --- Agentic loop ---
    AGENT_MAX_REQUESTS_PER_SYMBOL: int = 6
    AGENT_VERIFY_MIN_CONFIDENCE: float = 0.60

    # --- API schema versioning ---
    API_SCHEMA_VERSION: str = "1"

    @field_validator("IFIND_HTTP_BASE_URL", mode="before")
    @classmethod
    def _coerce_base_url(cls, v: object) -> str:
        # Important: env can override defaults with an empty string; treat it as "unset".
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
