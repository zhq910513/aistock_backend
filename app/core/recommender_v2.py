"""Model v2 recommendation engine.

Goal (configurable):
- Within holding window (default 3 trading days), probability of reaching target profit (default 5%~8%).

This module intentionally separates:
- Feature extraction (from normalized facts / candidates)
- Scoring (a lightweight baseline that can be replaced by a trained model later)
- Persistence with full lineage and evidence for UI traceability

IMPORTANT (compat bridge):
- The current HTTP APIs and orchestrator still persist into legacy tables
  (ModelDecision/DecisionEvidence) for UI endpoints.
- Therefore this module exports `generate_for_batch_v2` and `persist_decisions_v2`
  with the same signature as v1, implemented as a thin compatibility layer.

NOTE:
- The native v2 run/reco/evidence tables are present, but the API surface is not
  switched yet. When you switch, you can call `generate_for_batch_run_v2`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from math import exp, log
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.config import settings
from app.database import models


def _next_day_yyyymmdd(td: str) -> str:
    dt = datetime.strptime(td, "%Y%m%d")
    return (dt + timedelta(days=1)).strftime("%Y%m%d")


def _sigmoid(x: float) -> float:
    try:
        return 1.0 / (1.0 + exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def _safe_logit(p: Optional[float]) -> float:
    if p is None:
        return 0.0
    # clamp
    p = max(1e-6, min(1.0 - 1e-6, float(p)))
    return log(p / (1.0 - p))


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


@dataclass
class ScoredCandidate:
    symbol: str
    action: str
    score: float
    confidence: float
    p_hit_target: float
    expected_max_return: Optional[float]
    p_limit_up_next_day: Optional[float]
    signals: Dict[str, Any]
    evidence: List[Tuple[str, str, Dict[str, Any]]]

@dataclass
class CandidateView:
    """A minimal attribute-view for scoring.

    We keep this to decouple scoring from the upstream table schema.
    """

    symbol: str
    p_limit_up: float
    turnover_rate: float = 0.0
    order_amount: float = 0.0
    sum_market_value: float = 0.0
    is_again_limit: int = 0
    high_days: Any = None
    limit_up_type: str = ""
    change_rate: Any = None



def score_candidate(
    c: models.LabelingCandidate,
    target_low: float,
    target_high: float,
    holding_days: int,
) -> ScoredCandidate:
    """Baseline scorer.

    Uses a small set of fields available in current candidate pool.

    When more data is ingested (1m bars / orderbook / sector flow / sentiment),
    extend this function to consume `feat_intraday_cutoff` etc.
    """

    symbol = c.symbol

    p_limit_up = _to_float(c.p_limit_up, default=0.2) if hasattr(c, "p_limit_up") else 0.2
    turnover_rate = _to_float(getattr(c, "turnover_rate", 0.0), default=0.0)
    order_amount = _to_float(getattr(c, "order_amount", 0.0), default=0.0)
    mkt = _to_float(getattr(c, "sum_market_value", 0.0), default=0.0)
    is_again = _to_int(getattr(c, "is_again_limit", 0), default=0)

    # Parse high_days like "2天2板" -> 2
    high_days = 0
    try:
        v = getattr(c, "high_days", None)
        if v:
            high_days = int(str(v).split("天")[0])
    except Exception:
        high_days = 0

    limit_up_type = (getattr(c, "limit_up_type", "") or "").strip()
    is_yizi = 1 if ("一字" in limit_up_type) else 0

    # Heuristic log-features
    logit_p = _safe_logit(p_limit_up)
    log_amt = log(max(order_amount, 1.0))
    log_mkt = log(max(mkt, 1.0))

    # Score: probability of hitting target return within holding window
    x = (
        -0.8
        + 0.55 * logit_p
        + 0.28 * float(high_days)
        - 0.60 * float(is_yizi)
        - 0.08 * min(turnover_rate, 20.0)
        + 0.10 * (log_amt - 12.0)
        - 0.08 * (log_mkt - 22.0)
        + 0.18 * float(is_again)
    )
    p_hit = _sigmoid(x)

    # Expected max return (coarse). Keep bounded.
    exp_max = max(0.0, min(0.25, (p_hit * (target_high + 0.02))))

    # Map p_hit -> action
    if p_hit >= 0.70:
        action = "买入"
    elif p_hit >= 0.55:
        action = "观察"
    else:
        action = "忽略"

    score = round(100.0 * p_hit, 2)
    confidence = round(p_hit, 4)

    signals: Dict[str, Any] = {
        "p_hit_target": round(p_hit, 6),
        "target_return_low": target_low,
        "target_return_high": target_high,
        "holding_days": holding_days,
        "expected_max_return": round(exp_max, 6),
        "p_limit_up": round(p_limit_up, 6),
        "high_days": getattr(c, "high_days", None),
        "limit_up_type": getattr(c, "limit_up_type", None),
        "turnover_rate": turnover_rate,
        "order_amount": order_amount,
        "sum_market_value": mkt,
        "is_again_limit": is_again,
    }

    evidence: List[Tuple[str, str, Dict[str, Any]]] = []

    evidence.append(
        (
            "P_HIT_TARGET_3D",
            f"三日内达到{int(target_low * 100)}%+收益概率（p_hit_target={p_hit:.4f}）",
            {
                "p_hit_target": round(p_hit, 6),
                "holding_days": holding_days,
                "target_low": target_low,
                "target_high": target_high,
            },
        )
    )

    evidence.append(
        (
            "INPUT_P_LIMIT_UP",
            f"候选池给出的涨停概率（p_limit_up={p_limit_up:.4f}），仅作为特征之一",
            {"p_limit_up": round(p_limit_up, 6)},
        )
    )

    evidence.append(
        (
            "MOMENTUM_HIGH_DAYS",
            f"连板/高标强度：{getattr(c, 'high_days', None) or 'N/A'}",
            {"high_days": getattr(c, "high_days", None)},
        )
    )

    if limit_up_type:
        evidence.append(
            (
                "LIMITUP_TYPE",
                (
                    f"涨停类型：{limit_up_type}（一字板更难买入，模型会降低可交易性评分）"
                    if is_yizi
                    else f"涨停类型：{limit_up_type}"
                ),
                {"limit_up_type": limit_up_type},
            )
        )

    evidence.append(
        (
            "RAW_SNAPSHOT",
            "候选池原始字段快照（用于追溯）",
            {
                "turnover_rate": turnover_rate,
                "order_amount": order_amount,
                "sum_market_value": mkt,
                "is_again_limit": is_again,
                "change_rate": getattr(c, "change_rate", None),
            },
        )
    )

    return ScoredCandidate(
        symbol=symbol,
        action=action,
        score=score,
        confidence=confidence,
        p_hit_target=p_hit,
        expected_max_return=exp_max,
        p_limit_up_next_day=p_limit_up,
        signals=signals,
        evidence=evidence,
    )


# ---------------------------
# Native v2 persistence (NOT yet wired to APIs)
# ---------------------------

def generate_for_batch_run_v2(
    db: Session,
    batch_id: str,
    model_name: str = "target_return_3d",
    model_version: str = "v2-baseline",
    asof_ts: Optional[datetime] = None,
    target_low: Optional[float] = None,
    target_high: Optional[float] = None,
    holding_days: Optional[int] = None,
) -> str:
    """Generate recommendations for a committed pool batch into v2 tables.

    Returns: run_id

    This function is intentionally NOT used by current APIs yet.
    """

    batch = db.query(models.LimitupPoolBatch).filter(models.LimitupPoolBatch.batch_id == batch_id).first()
    if not batch:
        raise ValueError(f"batch_id not found: {batch_id}")

    decision_day = _next_day_yyyymmdd(batch.trading_day)

    if asof_ts is None:
        asof_ts = datetime.utcnow()

    if target_low is None:
        target_low = float(getattr(settings, "TARGET_RETURN_LOW", 0.05) or 0.05)
    if target_high is None:
        target_high = float(getattr(settings, "TARGET_RETURN_HIGH", 0.08) or 0.08)
    if holding_days is None:
        holding_days = int(getattr(settings, "HOLDING_DAYS", 3) or 3)

    run_id = f"runv2_{batch_id}_{int(asof_ts.timestamp())}"

    run = models.ModelRunV2(
        run_id=run_id,
        model_name=model_name,
        model_version=model_version,
        decision_day=decision_day,
        asof_ts=asof_ts,
        target_return_low=target_low,
        target_return_high=target_high,
        holding_days=holding_days,
        params={
            "source": "limitup_pool",
            "batch_id": batch.batch_id,
            "batch_trading_day": batch.trading_day,
            "raw_hash": batch.raw_hash,
        },
        label_version=None,
    )
    db.add(run)
    db.flush()

    rows = (
        db.query(models.LimitupCandidate)
        .filter(models.LimitupCandidate.batch_id == batch_id)
        .filter(models.LimitupCandidate.candidate_status != "DROPPED")
        .order_by(models.LimitupCandidate.id.asc())
        .all()
    )

    candidates: List[CandidateView] = []
    for c in rows:
        raw = dict(c.raw_json or {})
        candidates.append(
            CandidateView(
                symbol=c.symbol,
                p_limit_up=float(c.p_limit_up or 0.0),
                turnover_rate=_to_float(raw.get("turnover_rate"), default=0.0),
                order_amount=_to_float(raw.get("order_amount") or raw.get("order_volume"), default=0.0),
                sum_market_value=_to_float(raw.get("sum_market_value") or raw.get("currency_value"), default=0.0),
                is_again_limit=_to_int(raw.get("is_again_limit"), default=0),
                high_days=raw.get("high_days"),
                limit_up_type=str(raw.get("limit_up_type") or ""),
                change_rate=raw.get("change_rate"),
            )
        )

    scored: List[ScoredCandidate] = [
        score_candidate(c, target_low=target_low, target_high=target_high, holding_days=holding_days)
        for c in candidates
    ]

    scored.sort(key=lambda x: (x.p_hit_target, x.score), reverse=True)

    top_n = int(getattr(settings, "RECO_TOP_N", 10) or 10)
    kept = scored[:top_n]

    for item in kept:
        reco = models.ModelRecoV2(
            run_id=run_id,
            symbol=item.symbol,
            action=item.action,
            score=item.score,
            confidence=item.confidence,
            p_hit_target=float(item.p_hit_target),
            expected_max_return=item.expected_max_return,
            p_limit_up_next_day=item.p_limit_up_next_day,
            signals=item.signals,
            created_ts=asof_ts,
        )
        db.add(reco)
        db.flush()

        for reason_code, reason_text, fields in item.evidence:
            ev = models.ModelRecoEvidenceV2(
                reco_id=reco.reco_id,
                reason_code=reason_code,
                reason_text=reason_text,
                evidence_fields=fields,
                evidence_refs={"batch_id": batch_id, "symbol": item.symbol},
                created_ts=asof_ts,
            )
            db.add(ev)

    db.commit()
    return run_id


# ---------------------------
# Compatibility layer for current APIs/orchestrator
# ---------------------------

# Reuse v1 decision tables for now (ModelDecision / DecisionEvidence)
from app.core.recommender_v1 import EvidenceItem, RecommendationItem  # noqa: E402
from app.core import recommender_v1 as _reco_v1  # noqa: E402


def generate_for_batch_v2(
    s: Session,
    batch: models.LimitupPoolBatch,
    topn: int | None = None,
) -> list[RecommendationItem]:
    """Generate recommendation items (API-compatible).

    Current UI endpoints read from ModelDecision/DecisionEvidence.
    To keep the container runnable and the endpoints consistent, we delegate to v1.

    When UI switches to v2 tables, replace this with native v2 logic.
    """

    return _reco_v1.generate_for_batch(s, batch, topn=topn)


def persist_decisions_v2(
    s: Session,
    batch: models.LimitupPoolBatch,
    items: list[RecommendationItem],
) -> None:
    """Persist decisions/evidence (API-compatible)."""

    _reco_v1.persist_decisions(s, batch, items)
