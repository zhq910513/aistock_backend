from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta

from app.config import settings
from app.utils.symbols import normalize_symbol
from app.utils.time import now_shanghai, now_shanghai_str, trading_day_str


@dataclass(frozen=True)
class PlannedRequest:
    dedupe_key: str
    correlation_id: str
    purpose: str
    endpoint: str
    params_canonical: str
    payload: dict
    deadline_sec: int
    # IMPORTANT: symbol must be carried end-to-end so DataRequest.symbol is never empty.
    symbol: str


@dataclass(frozen=True)
class Plan:
    planner_version: str
    planner_state: dict
    requests: list[PlannedRequest]


def _json_canonical(obj: dict) -> str:
    # stable canonical form for hashing / dedupe
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _params_canonical(payload: dict) -> str:
    return hashlib.sha256(_json_canonical(payload).encode("utf-8")).hexdigest()


def _mk_dedupe_key(trading_day: str, endpoint: str, params_canonical: str) -> str:
    h = hashlib.sha256(f"{endpoint}|{params_canonical}".encode("utf-8")).hexdigest()[:16]
    return f"LP:{trading_day}:{endpoint}:{h}"


def _mk_correlation_id(trading_day: str, stage: int, dedupe_key: str) -> str:
    # correlation id length is small; keep it readable but unique-ish
    h = hashlib.sha256(dedupe_key.encode("utf-8")).hexdigest()[:10]
    return f"LP:{trading_day}:{stage}:{h}"


def _infer_td(trading_day: str | None) -> str:
    """Return trading_day as YYYYMMDD."""
    if trading_day:
        s = trading_day.strip()
        if len(s) == 8 and s.isdigit():
            return s
        # allow YYYY-MM-DD
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y%m%d")
    return trading_day_str(now_shanghai())


def _hf_time_window(td: str) -> tuple[str, str]:
    """Return (starttime, endtime) in Beijing time string for iFinD quantapi."""
    day = datetime.strptime(td, "%Y%m%d").strftime("%Y-%m-%d")
    # quantapi examples use 09:15 -> 15:15; keep it conservative
    return f"{day} 09:15:00", f"{day} 15:15:00"


def _hist_date_window(td: str, days: int) -> tuple[str, str]:
    end = datetime.strptime(td, "%Y%m%d")
    start = end - timedelta(days=max(1, int(days)))
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def build_plan(
    symbol: str,
    hit_count: int,
    planner_state: dict | None = None,
    trading_day: str | None = None,
) -> Plan:
    """Build a data-fetch plan for labeling pipeline.

    IMPORTANT: QuantAPI (iFinD) expects payload keys like:
      - real_time_quotation: {codes, indicators}
      - cmd_history_quotation: {codes, indicators, startdate, enddate, functionpara}
      - high_frequency: {codes, indicators, starttime, endtime}
    See official examples in iFinD QuantAPI docs.
    """
    td = _infer_td(trading_day)
    sym = normalize_symbol(symbol)

    st = dict(planner_state or {})
    stage = int(st.get("stage", 0))

    # Expand history / high-frequency depth with hit_count, but cap it.
    hist_days = min(
        int(settings.LABELING_HISTORY_DAYS_MAX),
        int(settings.LABELING_HISTORY_DAYS_BASE)
        + max(0, int(hit_count) - 1) * int(settings.LABELING_HISTORY_DAYS_EXPAND),
    )
    hf_limit = max(1, int(settings.LABELING_HF_LIMIT_BASE))

    # NOTE: current dispatcher ignores hf_limit; we keep it for future use.
    st.update(
        {
            "stage": stage,
            "planner_version": "LPv1",
            "td": td,
            "symbol": sym,
            "hit_count": int(hit_count),
            "hist_days": int(hist_days),
            "hf_limit": int(hf_limit),
            # project convention: Asia/Shanghai with milliseconds
            "built_at": now_shanghai_str(),
        }
    )

    # --- 1) real-time quotation ---
    rt_payload = {
        "codes": sym,
        "indicators": "latest,open,high,low,preClose,amt,vol,changeRatio",
        # compatibility: many downstreams treat thscode/symbol as canonical symbol key
        "thscode": sym,
        "symbol": sym,
    }
    pc_rt = _params_canonical(rt_payload)
    dk_rt = _mk_dedupe_key(td, "real_time_quotation", pc_rt)

    # --- 2) historical daily quotation ---
    startdate, enddate = _hist_date_window(td, hist_days)
    hist_payload = {
        "codes": sym,
        "indicators": "open,high,low,close,volume,amount,changeRatio",
        "startdate": startdate,
        "enddate": enddate,
        "functionpara": {"Fill": "Blank"},
        "thscode": sym,
        "symbol": sym,
    }
    pc_hist = _params_canonical(hist_payload)
    dk_hist = _mk_dedupe_key(td, "cmd_history_quotation", pc_hist)

    # --- 3) intraday high frequency ---
    starttime, endtime = _hf_time_window(td)
    hf_payload = {
        "codes": sym,
        "indicators": "open,high,low,close,volume,amount,changeRatio",
        "starttime": starttime,
        "endtime": endtime,
        "thscode": sym,
        "symbol": sym,
    }
    pc_hf = _params_canonical(hf_payload)
    dk_hf = _mk_dedupe_key(td, "high_frequency", pc_hf)

    # deadlines (seconds) – keep short; dispatcher should retry if needed
    reqs = [
        PlannedRequest(
            dedupe_key=dk_rt,
            correlation_id=_mk_correlation_id(td, stage, dk_rt),
            purpose="LABELING_BASE",
            endpoint="real_time_quotation",
            params_canonical=pc_rt,
            payload=rt_payload,
            deadline_sec=5,
            symbol=sym,
        ),
        PlannedRequest(
            dedupe_key=dk_hist,
            correlation_id=_mk_correlation_id(td, stage, dk_hist),
            purpose="LABELING_BASE",
            endpoint="cmd_history_quotation",
            params_canonical=pc_hist,
            payload=hist_payload,
            deadline_sec=8,
            symbol=sym,
        ),
        PlannedRequest(
            dedupe_key=dk_hf,
            correlation_id=_mk_correlation_id(td, stage, dk_hf),
            purpose="LABELING_BASE",
            endpoint="high_frequency",
            params_canonical=pc_hf,
            payload=hf_payload,
            deadline_sec=10,
            symbol=sym,
        ),
    ]

    return Plan(planner_version="LPv1", planner_state=st, requests=reqs)
