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

    # HARD BOUNDARY (v1整改要求):
    # 本轮必须禁用任何自动下单/交易执行链路。即便代码残留，也不得被运行路径触达。
    EXECUTION_DISABLED: bool = True

    # --- Database ---
    DATABASE_URL: str = "sqlite:////app/db/aistock.sqlite3"

    # --- Data sources ---
    DATA_PROVIDER: str = "IFIND_HTTP"
    THS_MODE: str = "MOCK"

    # --- Limit-up candidate pool (T+0 16:00) ---
    # 说明：候选池接口由外部提供；本服务仅负责定时拉取、过滤、落库、供UI编辑。
    POOL_FETCH_ENABLED: bool = True
    POOL_FETCH_AT_HHMM: str = "16:00"  # Asia/Shanghai

    # Candidate pool endpoint
    POOL_FETCH_URL: str = ""
    # Optional HTTP method: GET/POST
    POOL_FETCH_METHOD: str = "GET"
    # Optional headers/body as JSON strings (kept as str to avoid env parsing surprises)
    POOL_FETCH_HEADERS_JSON: str = "{}"
    POOL_FETCH_BODY_JSON: str = "{}"

    # Filter rules (configurable)
    POOL_ALLOWED_PREFIXES: str = "0"          # e.g. "0" or "0,6"
    POOL_ALLOWED_EXCHANGES: str = "SZ"        # e.g. "SZ" or "SZ,SH"

    # Pool filter rules source
    # - ENV: use POOL_ALLOWED_PREFIXES / POOL_ALLOWED_EXCHANGES
    # - DB: read from system_settings (pool.allowed_prefixes / pool.allowed_exchanges)
    # - VERSIONED: read from pool_filter_rule_sets (effective_ts based), fallback to DB/ENV
    POOL_RULES_SOURCE: str = "ENV"

    # --- Recommendation output ---
    RECOMMEND_TOPN: int = 10
    RECOMMEND_DEADLINE_HHMM: str = "08:30"    # T+1 deadline

    IFIND_HTTP_BASE_URL: str = "https://quantapi.51ifind.com"
    IFIND_HTTP_REFRESH_TOKEN: str = ""
    IFIND_HTTP_TOKEN: str = ""

    # Token manager tuning (used when IFIND_HTTP_REFRESH_TOKEN is provided)
    # access_token is said to be ~7 days; we refresh a bit early.
    IFIND_HTTP_TOKEN_MAX_AGE_SEC: int = 6 * 24 * 3600
    IFIND_HTTP_TOKEN_EARLY_REFRESH_SEC: int = 15 * 60

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

    # --- Labeling research factory (待打标数据工厂) ---
    # Enable automatic iFinD fetching + continuous feature snapshots driven by uploaded candidates.
    LABELING_AUTO_FETCH_ENABLED: bool = False

    # Background pipeline loop (ms)
    LABELING_PIPELINE_POLL_MS: int = 1000

    # Refresh cadence (seconds). A symbol with higher hit_count is refreshed more frequently.
    LABELING_REFRESH_BASE_SEC: int = 6 * 3600
    LABELING_REFRESH_ACTIVE_SEC: int = 30 * 60

    # Planner knobs
    LABELING_HISTORY_DAYS_BASE: int = 60
    LABELING_HISTORY_DAYS_EXPAND: int = 180
    LABELING_HISTORY_DAYS_MAX: int = 360
    LABELING_HF_LIMIT_BASE: int = 240

    # Safety: cap how many symbols we plan/enqueue per pipeline cycle
    LABELING_MAX_SYMBOLS_PER_CYCLE: int = 50

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
