from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

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


@dataclass
class Plan:
    symbol: str
    trading_day: str
    stage: int
    requests: list[PlannedRequest]


def _stage_from_hits(hit_count: int) -> int:
    hc = int(hit_count or 0)
    if hc >= 5:
        return 2
    if hc >= 2:
        return 1
    return 0


def _dedupe_key(symbol: str, endpoint: str, params: dict) -> str:
    # stable key for avoiding duplicate requests
    # intentionally simple: endpoint + sorted params
    items = sorted((k, str(v)) for k, v in (params or {}).items())
    joined = "&".join([f"{k}={v}" for k, v in items])
    return f"{symbol}|{endpoint}|{joined}"


def build_plan(symbol: str, trading_day: str, hit_count: int) -> Plan:
    """
    Rule-based planner (v1):
      - stage 0: base package
      - stage 1: longer history + more HF
      - stage 2: max history + more HF
    """
    stage = _stage_from_hits(hit_count)

    now = now_shanghai()
    td = (trading_day or "").replace("-", "").strip()
    if len(td) != 8:
        td = now.strftime("%Y%m%d")

    # history length grows with stage
    base_days = int(settings.LABELING_HISTORY_DAYS_BASE)
    expand_days = int(settings.LABELING_HISTORY_DAYS_EXPAND)
    max_days = int(settings.LABELING_HISTORY_DAYS_MAX)

    history_days = base_days if stage == 0 else (expand_days if stage == 1 else max_days)
    history_days = max(10, min(max_days, history_days))

    # high-frequency sample length
    hf_limit = int(settings.LABELING_HF_LIMIT_BASE)
    hf_limit = max(60, min(2000, hf_limit + stage * 120))

    # iFinD HTTP examples typically accept ths_code; we also pass symbol for our mock.
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
    reqs.append(
        PlannedRequest(
            symbol=symbol,
            endpoint="IFIND_HTTP.real_time_quotation",
            params=rt_payload,
            purpose="LABELING_BASE",
            priority=100,
            dedupe_key=_dedupe_key(symbol, "IFIND_HTTP.real_time_quotation", rt_payload),
        )
    )

    # 2) daily history
    reqs.append(
        PlannedRequest(
            symbol=symbol,
            endpoint="IFIND_HTTP.cmd_history_quotation",
            params=hist_payload,
            purpose="LABELING_BASE" if stage == 0 else "LABELING_EXPAND",
            priority=80,
            dedupe_key=_dedupe_key(symbol, "IFIND_HTTP.cmd_history_quotation", hist_payload),
        )
    )

    # 3) high frequency
    reqs.append(
        PlannedRequest(
            symbol=symbol,
            endpoint="IFIND_HTTP.high_frequency",
            params=hf_payload,
            purpose="LABELING_BASE" if stage == 0 else "LABELING_REFRESH",
            priority=60,
            dedupe_key=_dedupe_key(symbol, "IFIND_HTTP.high_frequency", hf_payload),
        )
    )

    return Plan(symbol=symbol, trading_day=td, stage=stage, requests=reqs)


def calc_refresh_seconds(hit_count: int) -> int:
    """Higher hit_count -> more frequent refresh."""
    hc = int(hit_count or 0)
    base = int(settings.LABELING_REFRESH_BASE_SEC)
    active = int(settings.LABELING_REFRESH_ACTIVE_SEC)

    # stage-based cadence
    if hc >= 5:
        return max(60, min(base, active // 2))
    if hc >= 2:
        return max(60, min(base, active))
    return max(60, base)


# Backward-compatible alias
def calc_refresh_sec(hit_count: int) -> int:
    return calc_refresh_seconds(hit_count)
