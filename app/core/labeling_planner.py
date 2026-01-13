from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any
import hashlib

from app.config import settings
from app.utils.time import now_shanghai


@dataclass
class PlannedRequest:
    symbol: str
    endpoint: str
    params: dict
    purpose: str
    priority: int
    dedupe_key: str
    correlation_id: str


@dataclass
class Plan:
    symbol: str
    trading_day: str
    stage: int
    requests: list[PlannedRequest]
    planner_state: dict[str, Any]


def _stage_from_hits(hit_count: int) -> int:
    hc = int(hit_count or 0)
    if hc >= 5:
        return 2
    if hc >= 2:
        return 1
    return 0


def _dedupe_key(symbol: str, endpoint: str, params: dict) -> str:
    items = sorted((k, str(v)) for k, v in (params or {}).items())
    joined = "&".join([f"{k}={v}" for k, v in items])
    return f"{symbol}|{endpoint}|{joined}"


def _mk_correlation_id(trading_day: str, stage: int, dedupe_key: str) -> str:
    h = hashlib.sha256(dedupe_key.encode("utf-8")).hexdigest()[:10]
    return f"LP:{trading_day}:{stage}:{h}"


def build_plan(
    symbol: str,
    trading_day: str | None = None,
    hit_count: int = 0,
    planner_state: dict[str, Any] | None = None,
    **_: Any,
) -> Plan:
    """
    Rule-based planner (v1),兼容 router/pipeline 两种调用方式：
      - router:   build_plan(symbol=..., hit_count=..., planner_state={...})
      - pipeline: build_plan(symbol=..., trading_day=..., hit_count=...)

    返回 Plan 时一定带 planner_state；PlannedRequest 带 correlation_id，
    与 router 的 enqueue 入参对齐，避免 500。
    """
    symbol = (symbol or "").strip()
    now = now_shanghai()

    td = (trading_day or "").replace("-", "").strip()
    if len(td) != 8:
        td = now.strftime("%Y%m%d")

    stage = _stage_from_hits(hit_count)

    # history length grows with stage
    base_days = int(getattr(settings, "LABELING_HISTORY_DAYS_BASE", 60))
    expand_days = int(getattr(settings, "LABELING_HISTORY_DAYS_EXPAND", 180))
    max_days = int(getattr(settings, "LABELING_HISTORY_DAYS_MAX", 360))

    history_days = base_days if stage == 0 else (expand_days if stage == 1 else max_days)
    history_days = max(10, min(max_days, history_days))

    # high-frequency sample length
    hf_limit = int(getattr(settings, "LABELING_HF_LIMIT_BASE", 240))
    hf_limit = max(60, min(2000, hf_limit + stage * 120))

    # iFinD HTTP: params里既放 ths_code，也放 symbol
    rt_payload = {"symbol": symbol, "ths_code": symbol}

    end_dt = now
    start_dt = now - timedelta(days=history_days)

    hist_payload = {
        "symbol": symbol,
        "ths_code": symbol,
        "start": start_dt.strftime("%Y-%m-%d"),
        "end": end_dt.strftime("%Y-%m-%d"),
        "period": "D",
    }

    hf_payload = {
        "symbol": symbol,
        "ths_code": symbol,
        "day": td,
        "limit": hf_limit,
    }

    reqs: list[PlannedRequest] = []

    # 1) real-time quote
    dk1 = _dedupe_key(symbol, "IFIND_HTTP.real_time_quotation", rt_payload)
    reqs.append(
        PlannedRequest(
            symbol=symbol,
            endpoint="IFIND_HTTP.real_time_quotation",
            params=rt_payload,
            purpose="LABELING_BASE",
            priority=100,
            dedupe_key=dk1,
            correlation_id=_mk_correlation_id(td, stage, dk1),
        )
    )

    # 2) daily history
    dk2 = _dedupe_key(symbol, "IFIND_HTTP.cmd_history_quotation", hist_payload)
    reqs.append(
        PlannedRequest(
            symbol=symbol,
            endpoint="IFIND_HTTP.cmd_history_quotation",
            params=hist_payload,
            purpose="LABELING_BASE" if stage == 0 else "LABELING_EXPAND",
            priority=80,
            dedupe_key=dk2,
            correlation_id=_mk_correlation_id(td, stage, dk2),
        )
    )

    # 3) high frequency
    dk3 = _dedupe_key(symbol, "IFIND_HTTP.high_frequency", hf_payload)
    reqs.append(
        PlannedRequest(
            symbol=symbol,
            endpoint="IFIND_HTTP.high_frequency",
            params=hf_payload,
            purpose="LABELING_BASE" if stage == 0 else "LABELING_REFRESH",
            priority=60,
            dedupe_key=dk3,
            correlation_id=_mk_correlation_id(td, stage, dk3),
        )
    )

    # ---- planner_state：持久化到 watchlist ----
    st: dict[str, Any] = dict(planner_state or {})
    st["stage"] = stage
    st["last_planned_day"] = td
    st["history_days"] = history_days
    st["hf_limit"] = hf_limit

    return Plan(symbol=symbol, trading_day=td, stage=stage, requests=reqs, planner_state=st)


def calc_refresh_seconds(hit_count: int) -> int:
    """hit_count 越高，刷新越频繁。"""
    hc = int(hit_count or 0)
    base = int(getattr(settings, "LABELING_REFRESH_BASE_SEC", 21600))     # 6h
    active = int(getattr(settings, "LABELING_REFRESH_ACTIVE_SEC", 1800))  # 30min

    if hc >= 5:
        return max(60, min(base, active // 2))
    if hc >= 2:
        return max(60, min(base, active))
    return max(60, base)


def calc_refresh_sec(hit_count: int) -> int:
    return calc_refresh_seconds(hit_count)
