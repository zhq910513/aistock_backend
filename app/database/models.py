from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    BigInteger,
    String,
    UniqueConstraint,
    PrimaryKeyConstraint,
    Text,
)
from sqlalchemy import JSON
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

# IMPORTANT (SQLite autoincrement):
# SQLite only auto-increments when the PRIMARY KEY column is exactly "INTEGER PRIMARY KEY".
# Using BIGINT for an autoincrement PK will NOT bind to rowid and will fail inserts (id stays NULL).
AUTO_PK = Integer().with_variant(BigInteger, "postgresql")


# ---------------------------
# Accounts
# ---------------------------

class Account(Base):
    __tablename__ = "accounts"

    account_id = Column(String(32), primary_key=True)
    broker_type = Column(String(32), nullable=False, default="MOCK")  # MOCK / BROKER_X
    config = Column(JSON, nullable=False, default=dict)  # tokens, routing metadata (non-secret)
    created_at = Column(DateTime(timezone=True), nullable=False)


# ---------------------------
# Data plane: raw events + requests/responses
# ---------------------------

class RawMarketEvent(Base):
    __tablename__ = "raw_market_events"

    id = Column(AUTO_PK, primary_key=True, autoincrement=True)
    api_schema_version = Column(String(32), nullable=False)

    source = Column(String(32), nullable=False)
    ths_product = Column(String(32), nullable=False)
    ths_function = Column(String(128), nullable=False)
    ths_indicator_set = Column(String(512), nullable=False)
    ths_params_canonical = Column(String(2048), nullable=False)
    ths_errorcode = Column(String(64), nullable=False, default="0")
    ths_quota_context = Column(String(512), nullable=False, default="")

    source_clock_quality = Column(String(32), nullable=False)
    channel_id = Column(String(64), nullable=False)
    channel_seq = Column(BigInteger, nullable=False)
    symbol = Column(String(32), nullable=False)

    data_ts = Column(DateTime(timezone=True), nullable=False, index=True)
    ingest_ts = Column(DateTime(timezone=True), nullable=False, index=True)

    payload = Column(JSON, nullable=False)
    payload_sha256 = Column(String(64), nullable=False)

    data_status = Column(String(16), nullable=False, default="VALID")
    latency_ms = Column(Integer, nullable=False, default=0)
    completion_rate = Column(Float, nullable=False, default=1.0)

    realtime_flag = Column(Boolean, nullable=False, default=True)
    audit_flag = Column(Boolean, nullable=False, default=True)
    research_only = Column(Boolean, nullable=False, default=False)

    request_id = Column(String(64), nullable=False)
    producer_instance = Column(String(64), nullable=False)

    __table_args__ = (
        UniqueConstraint("channel_id", "channel_seq", name="uq_event_channel_seq"),
        Index("ix_event_symbol_data_ts", "symbol", "data_ts"),
    )


class DataRequest(Base):
    __tablename__ = "data_requests"

    request_id = Column(String(32), primary_key=True)
    dedupe_key = Column(String(128), nullable=False, unique=True)

    correlation_id = Column(String(64), nullable=True, index=True)
    account_id = Column(String(32), nullable=True, index=True)

    # IMPORTANT: allow NULL, but never allow empty string.
    symbol = Column(String(32), nullable=True, index=True)
    purpose = Column(String(32), nullable=False)  # PLAN/VERIFY/RESEARCH/INGEST
    provider = Column(String(32), nullable=False)  # IFIND_HTTP/MOCK/...
    endpoint = Column(String(64), nullable=False)  # real_time_quotation / cmd_history_quotation / ...

    params_canonical = Column(String(2048), nullable=False)
    request_payload = Column(JSON, nullable=False, default=dict)

    status = Column(String(16), nullable=False, default="PENDING")  # PENDING/SENT/RECEIVED/FAILED
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(String(512), nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False)
    sent_at = Column(DateTime(timezone=True), nullable=True)
    deadline_at = Column(DateTime(timezone=True), nullable=True)

    response_id = Column(String(32), nullable=True, index=True)

    __table_args__ = (
        # No empty symbol; NULL is permitted for non-symbol requests.
        CheckConstraint("(symbol IS NULL) OR (length(symbol) > 0)", name="ck_data_requests_symbol_not_empty"),
        Index("ix_data_requests_status_created", "status", "created_at"),
        Index("ix_data_requests_symbol_created", "symbol", "created_at"),
    )


class DataResponse(Base):
    __tablename__ = "data_responses"

    response_id = Column(String(32), primary_key=True)

    request_id = Column(String(32), ForeignKey("data_requests.request_id", ondelete="CASCADE"), nullable=False, index=True)
    provider = Column(String(32), nullable=False)
    endpoint = Column(String(64), nullable=False)

    http_status = Column(Integer, nullable=True)
    errorcode = Column(String(64), nullable=False, default="0")
    errmsg = Column(String(512), nullable=False, default="")

    quota_context = Column(String(512), nullable=False, default="")
    raw = Column(JSON, nullable=False, default=dict)
    payload_sha256 = Column(String(64), nullable=False)

    received_at = Column(DateTime(timezone=True), nullable=False)
    data_ts = Column(DateTime(timezone=True), nullable=True)

    request = relationship("DataRequest")


class ValidationRecord(Base):
    __tablename__ = "validations"

    validation_id = Column(String(64), primary_key=True)
    decision_id = Column(String(64), nullable=False, index=True)
    symbol = Column(String(32), nullable=False, index=True)

    hypothesis = Column(String(512), nullable=False)
    request_ids = Column(JSON, nullable=False, default=list)  # list[str]
    evidence = Column(JSON, nullable=False, default=dict)
    conclusion = Column(String(32), nullable=False)  # PASS/FAIL/INCONCLUSIVE

    score = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime(timezone=True), nullable=False)


class ChannelCursor(Base):
    __tablename__ = "channel_cursor"

    channel_id = Column(String(64), primary_key=True)
    last_seq = Column(BigInteger, nullable=False, default=0)
    last_ingest_ts = Column(DateTime(timezone=True), nullable=False)
    quality_score = Column(Float, nullable=False, default=1.0)

    p99_latency_ms = Column(Integer, nullable=False, default=200)
    p99_state = Column(JSON, nullable=False, default=dict)

    fidelity_score = Column(Float, nullable=False, default=1.0)
    fidelity_low_streak = Column(Integer, nullable=False, default=0)

    updated_at = Column(DateTime(timezone=True), nullable=False)


class InstrumentRuleCache(Base):
    __tablename__ = "instrument_rule_cache"

    symbol = Column(String(32), primary_key=True)
    tick_rule_version = Column(String(64), nullable=False)
    lot_rule_version = Column(String(64), nullable=False)
    tick_size = Column(Float, nullable=False)
    lot_size = Column(Integer, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class NonceCursor(Base):
    __tablename__ = "nonce_cursor"

    trading_day = Column(String(8), primary_key=True)
    symbol = Column(String(32), primary_key=True)
    strategy_id = Column(String(64), primary_key=True)
    account_id = Column(String(32), primary_key=True)

    last_nonce = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class SymbolLock(Base):
    __tablename__ = "symbol_locks"

    account_id = Column(String(32), primary_key=True)
    symbol = Column(String(32), primary_key=True)

    locked = Column(Boolean, nullable=False, default=False)
    lock_reason = Column(String(64), nullable=False, default="")
    lock_ref = Column(String(128), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


# ---------------------------
# Decision plane
# ---------------------------

class Signal(Base):
    __tablename__ = "signals"

    cid = Column(String(64), primary_key=True)
    account_id = Column(String(32), nullable=False, index=True)

    trading_day = Column(String(8), nullable=False)
    symbol = Column(String(32), nullable=False)
    strategy_id = Column(String(64), nullable=False)
    signal_ts = Column(DateTime(timezone=True), nullable=False)

    nonce = Column(Integer, nullable=False)
    side = Column(String(8), nullable=False)
    intended_qty_or_notional = Column(BigInteger, nullable=False)

    confidence = Column(Float, nullable=False, default=0.0)

    rule_set_version_hash = Column(String(64), nullable=False)
    strategy_contract_hash = Column(String(64), nullable=False)
    model_snapshot_uuid = Column(String(64), nullable=False)
    cost_model_version = Column(String(64), nullable=False)
    feature_extractor_version = Column(String(64), nullable=False)

    lineage_ref = Column(String(64), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_signals_symbol_day", "symbol", "trading_day"),)


class LabelingCandidate(Base):
    """Daily operator/analyst supplied candidates for next-day limit-up labeling (待打标)."""

    __tablename__ = "labeling_candidates"

    candidate_id = Column(String(64), primary_key=True)

    trading_day = Column(String(8), nullable=False, index=True)  # YYYYMMDD (input day, Beijing)
    target_day = Column(String(8), nullable=False, index=True)   # YYYYMMDD (usually next day)
    symbol = Column(String(32), nullable=False, index=True)

    p_limit_up = Column(Float, nullable=False)  # 0..1
    name = Column(String(128), nullable=False, default="")
    source = Column(String(32), nullable=False, default="UI")  # UI/IMPORT/...

    extra = Column(JSON, nullable=False, default=dict)

    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("trading_day", "symbol", name="uq_labeling_candidates_day_symbol"),
        Index("ix_labeling_candidates_day_plimit", "trading_day", "p_limit_up"),
    )


# ---------------------------
# Canonical Schema v1: limit-up candidate pool (input layer)
# ---------------------------

class LimitupPoolBatch(Base):
    __tablename__ = "limitup_pool_batches"

    batch_id = Column(String(64), primary_key=True)
    trading_day = Column(String(8), nullable=False, index=True)  # YYYYMMDD (Asia/Shanghai)
    fetch_ts = Column(DateTime(timezone=True), nullable=False, index=True)
    source = Column(String(64), nullable=False, default="EXTERNAL")

    status = Column(String(16), nullable=False, default="FETCHED", index=True)  # FETCHED/EDITING/COMMITTED/CANCELLED
    filter_rules = Column(JSON, nullable=False, default=dict)
    raw_hash = Column(String(64), nullable=False)

    __table_args__ = (Index("ix_limitup_pool_batches_day_status", "trading_day", "status"),)


class LimitupCandidate(Base):
    __tablename__ = "limitup_candidates"

    # MUST be INTEGER PRIMARY KEY in SQLite to autoincrement
    id = Column(AUTO_PK, primary_key=True, autoincrement=True)

    batch_id = Column(String(64), ForeignKey("limitup_pool_batches.batch_id", ondelete="CASCADE"), nullable=False, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    name = Column(String(128), nullable=False, default="")

    p_limit_up = Column(Float, nullable=True)
    p_source = Column(String(32), nullable=False, default="UI")
    edited_ts = Column(DateTime(timezone=True), nullable=True)

    candidate_status = Column(String(16), nullable=False, default="PENDING_EDIT", index=True)  # PENDING_EDIT/READY/DROPPED
    raw_json = Column(JSON, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint("batch_id", "symbol", name="uq_limitup_candidates_batch_symbol"),
        Index("ix_limitup_candidates_batch_plimit", "batch_id", "p_limit_up"),
    )


# ---------------------------
# Runtime settings / versioned pool filter rules
# ---------------------------

class SystemSetting(Base):
    __tablename__ = "system_settings"

    key = Column(String(128), primary_key=True)
    value = Column(JSON, nullable=False, default=dict)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class PoolFilterRuleSet(Base):
    __tablename__ = "pool_filter_rule_sets"

    id = Column(AUTO_PK, primary_key=True, autoincrement=True)
    rule_set_id = Column(String(64), nullable=False, unique=True, index=True)

    allowed_prefixes = Column(JSON, nullable=False, default=list)
    allowed_exchanges = Column(JSON, nullable=False, default=list)

    effective_ts = Column(DateTime(timezone=True), nullable=False, index=True)
    note = Column(String(256), nullable=False, default="")

    created_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_pool_filter_rules_effective", "effective_ts"),)


class SymbolWatchlist(Base):
    __tablename__ = "symbol_watchlist"

    symbol = Column(String(32), primary_key=True)

    first_seen_day = Column(String(8), nullable=False, index=True)
    last_seen_day = Column(String(8), nullable=False, index=True)

    hit_count = Column(Integer, nullable=False, default=0)
    active = Column(Boolean, nullable=False, default=True)

    planner_state = Column(JSON, nullable=False, default=dict)
    next_refresh_at = Column(DateTime(timezone=True), nullable=True, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_watchlist_active_refresh", "active", "next_refresh_at"),)


class SymbolFeatureSnapshot(Base):
    __tablename__ = "symbol_feature_snapshots"

    snapshot_id = Column(String(64), primary_key=True)

    symbol = Column(String(32), nullable=False, index=True)
    feature_set = Column(String(64), nullable=False, default="AUTO")
    asof_ts = Column(DateTime(timezone=True), nullable=False, index=True)

    request_ids = Column(JSON, nullable=False, default=list)
    planner_version = Column(String(32), nullable=False, default="planner_v1")

    features = Column(JSON, nullable=False, default=dict)

    created_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_feature_snapshots_symbol_asof", "symbol", "asof_ts"),)


# ---------------------------
# Collector outputs
# ---------------------------

class EquityEODSnapshot(Base):
    __tablename__ = "equity_eod_snapshot"

    trading_day = Column(String(8), nullable=False)
    symbol = Column(String(32), nullable=False)

    prev_close = Column(Float, nullable=True)
    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=True)

    volume = Column(Float, nullable=True)
    amount = Column(Float, nullable=True)
    turnover_rate = Column(Float, nullable=True)
    amplitude = Column(Float, nullable=True)
    float_market_cap = Column(Float, nullable=True)

    is_limit_up_close = Column(Boolean, nullable=True)

    source = Column(String(32), nullable=False, default="COLLECTOR")
    raw_ref = Column(String(128), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("trading_day", "symbol", name="pk_equity_eod_snapshot"),
        Index("ix_eod_symbol_day", "symbol", "trading_day"),
    )


class EquityThemeMap(Base):
    __tablename__ = "equity_theme_map"

    trading_day = Column(String(8), nullable=False)
    symbol = Column(String(32), nullable=False)
    theme_id = Column(String(64), nullable=False)
    theme_name = Column(String(128), nullable=False, default="")
    theme_rank = Column(Integer, nullable=True)

    source = Column(String(32), nullable=False, default="COLLECTOR")
    raw_ref = Column(String(128), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("trading_day", "symbol", "theme_id", name="pk_equity_theme_map"),
        Index("ix_theme_map_symbol_day", "symbol", "trading_day"),
        Index("ix_theme_map_theme_day", "theme_id", "trading_day"),
    )


class ThemeDailyStats(Base):
    __tablename__ = "theme_daily_stats"

    trading_day = Column(String(8), nullable=False)
    theme_id = Column(String(64), nullable=False)

    theme_name = Column(String(128), nullable=False, default="")
    theme_strength_score = Column(Float, nullable=True)
    limitup_count_in_theme = Column(Integer, nullable=True)

    source = Column(String(32), nullable=False, default="COLLECTOR")
    raw_ref = Column(String(128), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("trading_day", "theme_id", name="pk_theme_daily_stats"),
        Index("ix_theme_daily_stats_day", "trading_day"),
    )


class PipelineStep(Base):
    __tablename__ = "pipeline_steps"

    id = Column(AUTO_PK, primary_key=True, autoincrement=True)
    batch_id = Column(String(64), nullable=False, index=True)
    step_name = Column(String(64), nullable=False, index=True)
    status = Column(String(16), nullable=False, default="PENDING")
    detail = Column(JSON, nullable=False, default=dict)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("batch_id", "step_name", name="uq_pipeline_step_batch_name"),
        Index("ix_pipeline_steps_batch_status", "batch_id", "status"),
    )


class DecisionBundle(Base):
    __tablename__ = "decision_bundles"

    decision_id = Column(String(64), primary_key=True)
    cid = Column(String(64), nullable=True, index=True)
    account_id = Column(String(32), nullable=True, index=True)
    symbol = Column(String(32), nullable=False, index=True)

    decision = Column(String(16), nullable=False)
    reason_code = Column(String(64), nullable=False)
    params = Column(JSON, nullable=False, default=dict)

    request_ids = Column(JSON, nullable=False, default=list)
    model_hash = Column(String(64), nullable=False, default="")
    feature_hash = Column(String(64), nullable=False, default="")
    seed_set_hash = Column(String(64), nullable=False, default="")
    rng_seed_hash = Column(String(64), nullable=False, default="")

    guard_status = Column(JSON, nullable=False, default=dict)
    data_quality = Column(JSON, nullable=False, default=dict)

    rule_set_version_hash = Column(String(64), nullable=False)
    model_snapshot_uuid = Column(String(64), nullable=False)
    strategy_contract_hash = Column(String(64), nullable=False)
    feature_extractor_version = Column(String(64), nullable=False)
    cost_model_version = Column(String(64), nullable=False)

    lineage_ref = Column(String(64), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_decision_symbol_time", "symbol", "created_at"),)


class ModelDecision(Base):
    __tablename__ = "model_decisions"

    decision_id = Column(String(64), primary_key=True)

    trading_day = Column(String(8), nullable=False, index=True)
    decision_day = Column(String(8), nullable=False, index=True)

    symbol = Column(String(32), nullable=False, index=True)
    action = Column(String(8), nullable=False)
    score = Column(Float, nullable=False, default=0.0)
    confidence = Column(Float, nullable=False, default=0.0)

    created_ts = Column(DateTime(timezone=True), nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint("decision_day", "symbol", name="uq_model_decisions_day_symbol"),
        Index("ix_model_decisions_day_score", "decision_day", "score"),
    )


class DecisionEvidence(Base):
    __tablename__ = "decision_evidence"

    id = Column(AUTO_PK, primary_key=True, autoincrement=True)
    decision_id = Column(String(64), ForeignKey("model_decisions.decision_id", ondelete="CASCADE"), nullable=False, index=True)

    reason_code = Column(String(64), nullable=False)
    reason_text = Column(String(512), nullable=False)

    evidence_fields = Column(JSON, nullable=False, default=dict)
    evidence_refs = Column(JSON, nullable=False, default=dict)

    __table_args__ = (Index("ix_decision_evidence_decision", "decision_id"),)


class DecisionLabel(Base):
    __tablename__ = "decision_labels"

    id = Column(AUTO_PK, primary_key=True, autoincrement=True)
    decision_id = Column(String(64), ForeignKey("model_decisions.decision_id", ondelete="CASCADE"), nullable=False, index=True)
    label_day = Column(String(8), nullable=False, index=True)

    hit_limitup = Column(Boolean, nullable=False, default=False)
    close_return = Column(Float, nullable=False, default=0.0)
    max_return = Column(Float, nullable=False, default=0.0)
    drawdown = Column(Float, nullable=False, default=0.0)
    error_tags = Column(JSON, nullable=False, default=list)

    __table_args__ = (UniqueConstraint("decision_id", "label_day", name="uq_decision_labels_decision_label_day"),)


class ModelMetricsDaily(Base):
    __tablename__ = "model_metrics_daily"

    trading_day = Column(String(8), primary_key=True)

    hit_rate_at_k = Column(Float, nullable=True)
    avg_return_at_k = Column(Float, nullable=True)
    drawdown_at_k = Column(Float, nullable=True)
    coverage = Column(Float, nullable=True)
    brier_score = Column(Float, nullable=True)

    extra = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False)


class RuleSet(Base):
    __tablename__ = "rule_sets"
    rule_set_version_hash = Column(String(64), primary_key=True)
    definition = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)


class StrategyContract(Base):
    __tablename__ = "strategy_contracts"
    strategy_contract_hash = Column(String(64), primary_key=True)
    definition = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)


class DailyFrozenVersions(Base):
    __tablename__ = "daily_frozen_versions"

    trading_day = Column(String(8), primary_key=True)
    rule_set_version_hash = Column(String(64), nullable=False)
    strategy_contract_hash = Column(String(64), nullable=False)
    model_snapshot_uuid = Column(String(64), nullable=False)
    cost_model_version = Column(String(64), nullable=False)
    canonicalization_version = Column(String(32), nullable=False)
    feature_extractor_version = Column(String(64), nullable=False)

    report_hash = Column(String(64), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)


# ---------------------------
# Execution plane
# ---------------------------

class Order(Base):
    __tablename__ = "orders"

    cid = Column(String(64), primary_key=True)
    account_id = Column(String(32), nullable=False, index=True)

    client_order_id = Column(String(96), nullable=False, unique=True)
    broker_order_id = Column(String(96), nullable=True, index=True)

    symbol = Column(String(32), nullable=False, index=True)
    side = Column(String(8), nullable=False)
    order_type = Column(String(16), nullable=False)

    limit_price_int64 = Column(BigInteger, nullable=False, default=0)
    qty_int = Column(BigInteger, nullable=False)

    tick_rule_version = Column(String(64), nullable=False)
    lot_rule_version = Column(String(64), nullable=False)
    canonicalization_version = Column(String(32), nullable=False)

    metadata_hash = Column(String(64), nullable=False, index=True)

    state = Column(String(20), nullable=False, default="CREATED")
    version_id = Column(Integer, nullable=False, default=1)

    last_transition_id = Column(String(64), nullable=True)

    strategy_contract_hash = Column(String(64), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint("qty_int >= 0", name="ck_order_qty_nonneg"),
        Index("ix_orders_state", "state"),
        Index("ix_orders_symbol_account", "symbol", "account_id"),
    )


class OrderTransition(Base):
    __tablename__ = "order_transitions"
    id = Column(AUTO_PK, primary_key=True, autoincrement=True)
    cid = Column(String(64), ForeignKey("orders.cid", ondelete="CASCADE"), nullable=False, index=True)
    transition_id = Column(String(64), nullable=False)
    from_state = Column(String(20), nullable=False)
    to_state = Column(String(20), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (UniqueConstraint("cid", "transition_id", name="uq_order_transition_cid_tid"),)
    order = relationship("Order")


class OrderAnchor(Base):
    __tablename__ = "order_anchors"

    cid = Column(String(64), primary_key=True)
    account_id = Column(String(32), nullable=False, index=True)

    client_order_id = Column(String(96), nullable=False, index=True)
    broker_order_id = Column(String(96), nullable=True, index=True)

    request_uuid = Column(String(64), nullable=False, unique=True)
    ack_hash = Column(String(64), nullable=False)
    raw_request_hash = Column(String(64), nullable=False)
    raw_response_hash = Column(String(64), nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_order_anchor_broker", "broker_order_id"),)


class TradeFill(Base):
    __tablename__ = "trade_fills"

    id = Column(AUTO_PK, primary_key=True, autoincrement=True)
    broker_fill_id = Column(String(128), nullable=False, unique=True)

    cid = Column(String(64), nullable=True, index=True)
    broker_order_id = Column(String(96), nullable=True, index=True)
    account_id = Column(String(32), nullable=True, index=True)

    symbol = Column(String(32), nullable=False, index=True)
    side = Column(String(8), nullable=False)

    fill_price_int64 = Column(BigInteger, nullable=False)
    fill_qty_int = Column(BigInteger, nullable=False)
    fill_ts = Column(DateTime(timezone=True), nullable=False, index=True)

    fill_fingerprint = Column(String(64), nullable=False, unique=True)
    created_at = Column(DateTime(timezone=True), nullable=False)


class TradeFillLink(Base):
    __tablename__ = "trade_fill_links"

    fill_fingerprint = Column(String(64), primary_key=True)
    cid = Column(String(64), nullable=False, index=True)
    broker_order_id = Column(String(96), nullable=True)
    account_id = Column(String(32), nullable=True, index=True)

    snapshot_id = Column(String(64), nullable=True, index=True)
    decision_id = Column(String(64), nullable=True, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False)


class ReconcileSnapshot(Base):
    __tablename__ = "reconcile_snapshots"
    snapshot_id = Column(String(64), primary_key=True)

    symbol = Column(String(32), nullable=False, index=True)
    account_id = Column(String(32), nullable=True, index=True)

    anchor_type = Column(String(32), nullable=False)
    anchor_fingerprint = Column(String(64), nullable=False, index=True)

    candidates = Column(JSON, nullable=False)
    report_hash = Column(String(64), nullable=False)

    status = Column(String(32), nullable=False, default="OPEN")
    created_at = Column(DateTime(timezone=True), nullable=False)


class ReconcileDecision(Base):
    __tablename__ = "reconcile_decisions"
    decision_id = Column(String(64), primary_key=True)
    snapshot_id = Column(String(64), ForeignKey("reconcile_snapshots.snapshot_id"), nullable=False, index=True)

    decided_cid = Column(String(64), nullable=False)
    decided_broker_order_id = Column(String(96), nullable=True)

    signer_key_id = Column(String(64), nullable=False)
    signature = Column(String(512), nullable=False)

    prev_decision_id = Column(String(64), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)

    snapshot = relationship("ReconcileSnapshot")


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id = Column(AUTO_PK, primary_key=True, autoincrement=True)
    event_type = Column(String(64), nullable=False, index=True)
    dedupe_key = Column(String(128), nullable=False, unique=True)

    status = Column(String(16), nullable=False, default="PENDING")
    attempts = Column(Integer, nullable=False, default=0)

    available_at = Column(DateTime(timezone=True), nullable=False, index=True)

    payload = Column(JSON, nullable=False, default=dict)
    last_error = Column(String(512), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, index=True)
    sent_at = Column(DateTime(timezone=True), nullable=True)


# ---------------------------
# Portfolio / research (minimal placeholders)
# ---------------------------

class PortfolioPosition(Base):
    __tablename__ = "portfolio_positions"
    account_id = Column(String(32), primary_key=True)
    symbol = Column(String(32), primary_key=True)

    current_qty = Column(BigInteger, nullable=False, default=0)
    frozen_qty = Column(BigInteger, nullable=False, default=0)
    avg_price_int64 = Column(BigInteger, nullable=False, default=0)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class TradeLog(Base):
    __tablename__ = "trade_log"
    id = Column(AUTO_PK, primary_key=True, autoincrement=True)
    correlation_id = Column(String(64), nullable=False, index=True)
    cid = Column(String(64), nullable=True, index=True)
    account_id = Column(String(32), nullable=True, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    execution_state = Column(String(32), nullable=False)
    feature_snapshot = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False)


class T1Constraint(Base):
    __tablename__ = "t1_constraints"
    account_id = Column(String(32), primary_key=True)
    symbol = Column(String(32), primary_key=True)

    locked_qty = Column(BigInteger, nullable=False, default=0)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class TrainingFeatureRow(Base):
    __tablename__ = "training_feature_store"

    id = Column(AUTO_PK, primary_key=True, autoincrement=True)
    symbol = Column(String(32), nullable=False, index=True)

    data_ts = Column(DateTime(timezone=True), nullable=False)
    ingest_ts = Column(DateTime(timezone=True), nullable=False)

    audit_flag = Column(Boolean, nullable=False)
    realtime_equivalent = Column(Boolean, nullable=False)

    payload_sha256 = Column(String(64), nullable=False)
    channel_id = Column(String(64), nullable=False)
    channel_seq = Column(BigInteger, nullable=False)
    source_clock_quality = Column(String(32), nullable=False)

    feature_extractor_version = Column(String(64), nullable=False)
    rule_set_version_hash = Column(String(64), nullable=False)
    strategy_contract_hash = Column(String(64), nullable=False)

    features = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint("audit_flag = true AND realtime_equivalent = true", name="ck_training_only_realtime_equiv"),
        Index("ix_training_symbol_ts", "symbol", "data_ts"),
    )


class ModelSnapshot(Base):
    __tablename__ = "model_snapshots"
    model_snapshot_uuid = Column(String(64), primary_key=True)
    weights = Column(JSON, nullable=False, default=dict)
    eval_report = Column(JSON, nullable=False, default=dict)
    cost_model_version = Column(String(64), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)


class GuardianKey(Base):
    __tablename__ = "guardian_keys"
    key_id = Column(String(64), primary_key=True)
    role = Column(String(32), nullable=False)
    public_key_b64 = Column(String(512), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    revoked = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), nullable=False)


class SystemStatus(Base):
    __tablename__ = "system_status"
    id = Column(Integer, primary_key=True, default=1)

    guard_level = Column(Integer, nullable=False, default=0)
    veto = Column(Boolean, nullable=False, default=False)
    veto_code = Column(String(64), nullable=False, default="")
    panic_halt = Column(Boolean, nullable=False, default=False)
    challenge_code = Column(String(128), nullable=True)

    last_self_check_report_hash = Column(String(64), nullable=True)
    last_self_check_time = Column(DateTime(timezone=True), nullable=True)

    updated_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (CheckConstraint("id = 1", name="ck_system_status_singleton"),)


class RuntimeControls(Base):
    __tablename__ = "runtime_controls"
    id = Column(Integer, primary_key=True, default=1)

    auto_trading_enabled = Column(Boolean, nullable=False, default=False)
    dry_run = Column(Boolean, nullable=False, default=True)
    only_when_data_ok = Column(Boolean, nullable=False, default=True)

    max_orders_per_day = Column(Integer, nullable=False, default=10)
    max_notional_per_order = Column(BigInteger, nullable=False, default=0)

    allowed_symbols = Column(JSON, nullable=False, default=list)
    blocked_symbols = Column(JSON, nullable=False, default=list)

    updated_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (CheckConstraint("id = 1", name="ck_runtime_controls_singleton"),)


class SystemEvent(Base):
    __tablename__ = "system_events"

    id = Column(AUTO_PK, primary_key=True, autoincrement=True)
    event_type = Column(String(64), nullable=False, index=True)
    severity = Column(String(16), nullable=False, default="INFO")

    correlation_id = Column(String(64), nullable=True, index=True)
    symbol = Column(String(32), nullable=True, index=True)

    payload = Column(JSON, nullable=False, default=dict)
    time = Column(DateTime(timezone=True), nullable=False, index=True)

    __table_args__ = (Index("ix_system_events_type_time", "event_type", "time"),)

# ==============================
# Model V2 (Target-return within holding window)
# ==============================

class RawMarketPayload(Base):
    """Raw market payloads from any upstream platform.

    This table is the canonical lineage anchor for high-frequency and slow-changing facts.
    All normalized fact rows should reference `raw_hash` where possible.

    Notes:
    - `raw_hash` is a deterministic hash of (provider, endpoint, symbol, data_ts, payload_sha256)
      and is used as a stable reference in the UI for traceability.
    - `schema_name` and `schema_version` describe the payload format to help future decoding.
    """

    __tablename__ = "raw_market_payload"

    id = Column(Integer, primary_key=True)
    provider = Column(String(64), nullable=False, index=True)  # e.g. baidu/ifind/eastmoney
    endpoint = Column(String(128), nullable=False)  # e.g. quotation_minute_ab
    symbol = Column(String(16), nullable=False, index=True)
    trading_day = Column(String(10), nullable=True, index=True)  # YYYY-MM-DD (Asia/Shanghai)

    data_ts = Column(DateTime, nullable=True, index=True)  # upstream data timestamp
    ingest_ts = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    schema_name = Column(String(64), nullable=True)
    schema_version = Column(String(32), nullable=True)

    payload_sha256 = Column(String(64), nullable=False)
    raw_hash = Column(String(64), nullable=False, unique=True, index=True)
    payload = Column(JSON, nullable=False)


class FactIntradayBar1m(Base):
    """1-minute bar fact table (high-frequency)."""

    __tablename__ = "fact_intraday_bar_1m"

    id = Column(Integer, primary_key=True)
    symbol = Column(String(16), nullable=False, index=True)
    trading_day = Column(String(10), nullable=False, index=True)
    bar_ts = Column(DateTime, nullable=False, index=True)  # minute timestamp

    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=True)

    volume = Column(BigInteger, nullable=True)
    amount = Column(Float, nullable=True)

    vwap = Column(Float, nullable=True)

    raw_hash = Column(String(64), nullable=True, index=True)  # link to RawMarketPayload.raw_hash

    __table_args__ = (
        UniqueConstraint("symbol", "bar_ts", name="uq_fact_intraday_bar_1m_symbol_ts"),
        Index("ix_fact_intraday_bar_1m_symbol_day_ts", "symbol", "trading_day", "bar_ts"),
    )


class FactTradeTick(Base):
    """Tick-by-tick trades fact table (high-frequency)."""

    __tablename__ = "fact_trade_tick"

    id = Column(Integer, primary_key=True)
    symbol = Column(String(16), nullable=False, index=True)
    trading_day = Column(String(10), nullable=False, index=True)
    trade_ts = Column(DateTime, nullable=False, index=True)

    price = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False)

    # bs_flag: 'B' buy, 'S' sell, 'N' unknown
    bs_flag = Column(String(1), nullable=True)

    # If upstream provides sequence/trade_id, store it for stable dedupe.
    seq = Column(Integer, nullable=True)

    raw_hash = Column(String(64), nullable=True, index=True)

    __table_args__ = (
        UniqueConstraint("symbol", "trade_ts", "seq", name="uq_fact_trade_tick_symbol_ts_seq"),
        Index("ix_fact_trade_tick_symbol_day_ts", "symbol", "trading_day", "trade_ts"),
    )


class FactOrderBook5(Base):
    """5-level order book snapshot fact table (high-frequency).

    Each row represents a snapshot at `snapshot_ts`.
    """

    __tablename__ = "fact_orderbook_5"

    id = Column(Integer, primary_key=True)
    symbol = Column(String(16), nullable=False, index=True)
    trading_day = Column(String(10), nullable=False, index=True)
    snapshot_ts = Column(DateTime, nullable=False, index=True)

    bid1_price = Column(Float, nullable=True)
    bid1_vol = Column(BigInteger, nullable=True)
    bid2_price = Column(Float, nullable=True)
    bid2_vol = Column(BigInteger, nullable=True)
    bid3_price = Column(Float, nullable=True)
    bid3_vol = Column(BigInteger, nullable=True)
    bid4_price = Column(Float, nullable=True)
    bid4_vol = Column(BigInteger, nullable=True)
    bid5_price = Column(Float, nullable=True)
    bid5_vol = Column(BigInteger, nullable=True)

    ask1_price = Column(Float, nullable=True)
    ask1_vol = Column(BigInteger, nullable=True)
    ask2_price = Column(Float, nullable=True)
    ask2_vol = Column(BigInteger, nullable=True)
    ask3_price = Column(Float, nullable=True)
    ask3_vol = Column(BigInteger, nullable=True)
    ask4_price = Column(Float, nullable=True)
    ask4_vol = Column(BigInteger, nullable=True)
    ask5_price = Column(Float, nullable=True)
    ask5_vol = Column(BigInteger, nullable=True)

    raw_hash = Column(String(64), nullable=True, index=True)

    __table_args__ = (
        UniqueConstraint("symbol", "snapshot_ts", name="uq_fact_orderbook_5_symbol_ts"),
        Index("ix_fact_orderbook_5_symbol_day_ts", "symbol", "trading_day", "snapshot_ts"),
    )


class FeatIntradayCutoff(Base):
    """Derived intraday features at a specific cutoff timestamp (e.g. 15:30).

    This is the feature row used by the model for that day.
    """

    __tablename__ = "feat_intraday_cutoff"

    id = Column(Integer, primary_key=True)
    symbol = Column(String(16), nullable=False, index=True)
    trading_day = Column(String(10), nullable=False, index=True)
    cutoff_ts = Column(DateTime, nullable=False, index=True)

    feature_hash = Column(String(64), nullable=False, index=True)
    features = Column(JSON, nullable=False, default=dict)

    # lineage summary
    raw_hashes = Column(JSON, nullable=False, default=list)  # list[str]

    created_ts = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    __table_args__ = (
        UniqueConstraint("symbol", "trading_day", "cutoff_ts", name="uq_feat_intraday_cutoff_symbol_day_cutoff"),
    )


class FeatDaily(Base):
    """Slow-changing / daily features."""

    __tablename__ = "feat_daily"

    id = Column(Integer, primary_key=True)
    symbol = Column(String(16), nullable=False, index=True)
    trading_day = Column(String(10), nullable=False, index=True)

    feature_hash = Column(String(64), nullable=False, index=True)
    features = Column(JSON, nullable=False, default=dict)

    raw_hashes = Column(JSON, nullable=False, default=list)

    created_ts = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    __table_args__ = (
        UniqueConstraint("symbol", "trading_day", name="uq_feat_daily_symbol_day"),
    )


class ModelRunV2(Base):
    """A model run produces a batch of recommendations for a decision day.

    The model objective is probability of hitting target return within holding window.
    """

    __tablename__ = "model_run_v2"

    run_id = Column(String(64), primary_key=True)
    model_name = Column(String(64), nullable=False, index=True)
    model_version = Column(String(32), nullable=False)

    decision_day = Column(String(10), nullable=False, index=True)  # the day user will buy (T+1)
    asof_ts = Column(DateTime, nullable=False, index=True)  # data cutoff (e.g. 15:30 of T)

    target_return_low = Column(Float, nullable=False, default=0.05)
    target_return_high = Column(Float, nullable=False, default=0.08)
    holding_days = Column(Integer, nullable=False, default=3)

    params = Column(JSON, nullable=False, default=dict)
    label_version = Column(String(64), nullable=True, index=True)

    created_ts = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class ModelRecoV2(Base):
    """Per-symbol recommendation within a model run."""

    __tablename__ = "model_reco_v2"

    reco_id = Column(Integer, primary_key=True)
    run_id = Column(String(64), ForeignKey("model_run_v2.run_id"), nullable=False, index=True)

    symbol = Column(String(16), nullable=False, index=True)

    action = Column(String(16), nullable=False)  # BUY/WATCH/IGNORE
    score = Column(Float, nullable=False)
    confidence = Column(Float, nullable=False)

    # primary objective outputs
    p_hit_target = Column(Float, nullable=False)
    expected_max_return = Column(Float, nullable=True)

    # optional auxiliary predictions
    p_limit_up_next_day = Column(Float, nullable=True)

    # snapshot for UI
    signals = Column(JSON, nullable=False, default=dict)

    created_ts = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    __table_args__ = (
        UniqueConstraint("run_id", "symbol", name="uq_model_reco_v2_run_symbol"),
    )


class ModelRecoEvidenceV2(Base):
    __tablename__ = "model_reco_evidence_v2"

    id = Column(Integer, primary_key=True)
    reco_id = Column(Integer, ForeignKey("model_reco_v2.reco_id"), nullable=False, index=True)

    reason_code = Column(String(64), nullable=False, index=True)
    reason_text = Column(Text, nullable=False)

    evidence_fields = Column(JSON, nullable=False, default=dict)
    evidence_refs = Column(JSON, nullable=False, default=dict)

    created_ts = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    __table_args__ = (
        Index("ix_model_reco_evidence_v2_reco", "reco_id"),
    )


class OnlineFeedbackEventV2(Base):
    """Online feedback / labels generated after market close.

    This table supports continuous learning and backtest.
    """

    __tablename__ = "online_feedback_event_v2"

    id = Column(Integer, primary_key=True)

    symbol = Column(String(16), nullable=False, index=True)
    decision_day = Column(String(10), nullable=False, index=True)  # day the position would be opened

    holding_days = Column(Integer, nullable=False, default=3)
    target_return_low = Column(Float, nullable=False, default=0.05)
    target_return_high = Column(Float, nullable=False, default=0.08)

    # realized outcomes
    entry_price_ref = Column(Float, nullable=True)  # e.g. next-day open
    max_return = Column(Float, nullable=True)
    hit_target = Column(Boolean, nullable=True)
    hit_day_offset = Column(Integer, nullable=True)  # 1..holding_days

    label_version = Column(String(64), nullable=False, index=True)

    # lineage
    raw_refs = Column(JSON, nullable=False, default=dict)

    created_ts = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    __table_args__ = (
        UniqueConstraint("symbol", "decision_day", "label_version", name="uq_online_feedback_event_v2_symbol_day_version"),
    )
