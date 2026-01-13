from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta

from app.config import settings
from app.utils.crypto import sha256_hex
from app.utils.time import now_shanghai, trading_day_str


@dataclass
class PlannedRequest:
    endpoint: str
    purpose: str
    params_canonical: str
    payload: dict
    dedupe_key: str
    deadline_sec: int | None = None


def _canon(obj: dict) -> str:
    try:
        return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return json.dumps({"_unserializable": True, "repr": repr(obj)}, sort_keys=True, ensure_ascii=False)


def _bucket_minute(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M")


def _bucket_day(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _stage_from_hits(hit_count: int) -> int:
    # v1: simple, stable staging.
    # 0: first time -> BASE
    # 1: >=2 hits  -> EXPAND
    # 2: >=5 hits  -> DEEP
    if hit_count >= 5:
        return 2
    if hit_count >= 2:
        return 1
    return 0


def build_plan(symbol: str, hit_count: int, planner_state: dict | None = None) -> list[PlannedRequest]:
    """A conservative, explainable planner ("模型") that decides what to fetch.

    It starts with a base package, then gradually expands dimensions as the same symbol
    repeatedly hits the candidate criteria (hit_count increases).

    NOTE: This is intentionally deterministic + auditable for production safety.
    """
    symbol = (symbol or "").strip()
    if not symbol:
        return []

    st = dict(planner_state or {})
    stage = _stage_from_hits(int(hit_count or 0))

    now = now_shanghai()
    td = trading_day_str(now)

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
    hist_payload = {"symbol": symbol, "ths_code": symbol, "limit": int(history_days)}
    hf_payload = {"symbol": symbol, "ths_code": symbol, "limit": int(hf_limit)}

    # Dedupe buckets:
    # - realtime/hf: per-minute
    # - history: per-day
    b_min = _bucket_minute(now)
    b_day = _bucket_day(now)

    reqs: list[PlannedRequest] = []

    # BASE
    reqs.append(
        PlannedRequest(
            endpoint="real_time_quotation",
            purpose="LABELING_BASE",
            params_canonical=_canon(rt_payload),
            payload=rt_payload,
            dedupe_key=f"LBL|{symbol}|rt|{b_min}",
            deadline_sec=10,
        )
    )
    reqs.append(
        PlannedRequest(
            endpoint="cmd_history_quotation",
            purpose="LABELING_BASE",
            params_canonical=_canon(hist_payload),
            payload=hist_payload,
            dedupe_key=f"LBL|{symbol}|hist|{b_day}|{history_days}",
            deadline_sec=30,
        )
    )

    # EXPAND/DEEP: add high frequency sampling
    if stage >= 1:
        reqs.append(
            PlannedRequest(
                endpoint="high_frequency",
                purpose="LABELING_EXPAND" if stage == 1 else "LABELING_DEEP",
                params_canonical=_canon(hf_payload),
                payload=hf_payload,
                dedupe_key=f"LBL|{symbol}|hf|{b_min}|{hf_limit}",
                deadline_sec=15,
            )
        )

    # Record stage in state (returned to caller to persist)
    st["planner_version"] = "planner_v1"
    st["stage"] = stage
    st["last_plan_at"] = now.isoformat()
    st["history_days"] = history_days
    st["hf_limit"] = hf_limit

    return reqs


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
