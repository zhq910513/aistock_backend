"""Model v2 recommendation engine.

Goal (configurable):
- Within holding window (default 3 trading days), probability of reaching target profit (default 5%~8%).

This module intentionally separates:
- Feature extraction (from normalized facts / candidates)
- Scoring (a lightweight baseline that can be replaced by a trained model later)
- Persistence with full lineage and evidence for UI traceability

NOTE:
- Current implementation uses a deterministic baseline scorer.
- When you connect more data sources, replace `score_candidate` and/or feed a trained model.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import exp, log
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.config import settings
from app.database.models import (
    LabelingCandidate,
    LimitupPoolBatch,
    ModelRecoEvidenceV2,
    ModelRecoV2,
    ModelRunV2,
)


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


def score_candidate(
    c: LabelingCandidate,
    target_low: float,
    target_high: float,
    holding_days: int,
) -> ScoredCandidate:
    """Baseline scorer.

    Uses a small set of fields available in current candidate pool.

    When more data is ingested (1m bars / orderbook / sector flow / sentiment),
    extend this function to consume `feat_intraday_cutoff_v2` etc.
    """

    symbol = c.symbol

    p_limit_up = _to_float(c.p_limit_up, default=0.2) if hasattr(c, "p_limit_up") else 0.2
    turnover_rate = _to_float(c.turnover_rate, default=0.0)
    order_amount = _to_float(c.order_amount, default=0.0)
    mkt = _to_float(c.sum_market_value, default=0.0)
    is_again = _to_int(c.is_again_limit, default=0)

    # Parse high_days like "2天2板" -> 2
    high_days = 0
    try:
        if c.high_days:
            high_days = int(str(c.high_days).split("天")[0])
    except Exception:
        high_days = 0

    limit_up_type = (c.limit_up_type or "").strip()
    is_yizi = 1 if ("一字" in limit_up_type) else 0

    # Heuristic log-features
    logit_p = _safe_logit(p_limit_up)
    log_amt = log(max(order_amount, 1.0))
    log_mkt = log(max(mkt, 1.0))

    # Score: probability of hitting target return within holding window
    # Intuition:
    # - higher p_limit_up helps but is not decisive
    # - multi-day strength (high_days) helps
    # - one-word board (一字板) is harder to buy at a decent price -> penalty
    # - extreme turnover can mean distribution -> mild penalty
    # - larger liquidity (order_amount) helps execution -> mild positive
    # - huge market cap tends to move slower -> mild penalty
    # - "again limit" can imply momentum continuation, but also higher gap risk -> slight positive
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
        "high_days": c.high_days,
        "limit_up_type": c.limit_up_type,
        "turnover_rate": turnover_rate,
        "order_amount": order_amount,
        "sum_market_value": mkt,
        "is_again_limit": is_again,
    }

    evidence: List[Tuple[str, str, Dict[str, Any]]] = []

    evidence.append((
        "P_HIT_TARGET_3D",
        f"三日内达到{int(target_low*100)}%+收益概率（p_hit_target={p_hit:.4f}）",
        {"p_hit_target": round(p_hit, 6), "holding_days": holding_days, "target_low": target_low, "target_high": target_high},
    ))

    evidence.append((
        "INPUT_P_LIMIT_UP",
        f"候选池给出的涨停概率（p_limit_up={p_limit_up:.4f}），仅作为特征之一",
        {"p_limit_up": round(p_limit_up, 6)},
    ))

    evidence.append((
        "MOMENTUM_HIGH_DAYS",
        f"连板/高标强度：{c.high_days or 'N/A'}",
        {"high_days": c.high_days},
    ))

    if limit_up_type:
        evidence.append((
            "LIMITUP_TYPE",
            f"涨停类型：{limit_up_type}（一字板更难买入，模型会降低可交易性评分）" if is_yizi else f"涨停类型：{limit_up_type}",
            {"limit_up_type": limit_up_type},
        ))

    evidence.append((
        "RAW_SNAPSHOT",
        "候选池原始字段快照（用于追溯）",
        {
            "turnover_rate": turnover_rate,
            "order_amount": order_amount,
            "sum_market_value": mkt,
            "is_again_limit": is_again,
            "change_rate": c.change_rate,
        },
    ))

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


def generate_for_batch(
    db: Session,
    batch_id: str,
    model_name: str = "target_return_3d",
    model_version: str = "v2-baseline",
    asof_ts: Optional[datetime] = None,
    target_low: Optional[float] = None,
    target_high: Optional[float] = None,
    holding_days: Optional[int] = None,
) -> str:
    """Generate recommendations for a committed pool batch.

    Returns: run_id
    """

    batch = db.query(LimitupPoolBatch).filter(LimitupPoolBatch.batch_id == batch_id).first()
    if not batch:
        raise ValueError(f"batch_id not found: {batch_id}")

    # decision_day = batch.trading_day + 1 trading day (T+1). We keep string and let labeling pipeline determine calendar.
    # For now we store it as the batch's decision_day already computed by batch.
    decision_day = batch.decision_day

    if asof_ts is None:
        asof_ts = datetime.utcnow()

    if target_low is None:
        target_low = float(getattr(settings, "TARGET_RETURN_LOW", 0.05) or 0.05)
    if target_high is None:
        target_high = float(getattr(settings, "TARGET_RETURN_HIGH", 0.08) or 0.08)
    if holding_days is None:
        holding_days = int(getattr(settings, "HOLDING_DAYS", 3) or 3)

    run_id = f"runv2_{batch_id}_{int(asof_ts.timestamp())}"

    run = ModelRunV2(
        run_id=run_id,
        batch_id=batch_id,
        model_name=model_name,
        model_version=model_version,
        decision_day=decision_day,
        asof_ts=asof_ts,
        target_return_low=target_low,
        target_return_high=target_high,
        holding_days=holding_days,
        params={
            "source": "limitup_pool",
            "batch_trading_day": batch.trading_day,
            "batch_decision_day": batch.decision_day,
        },
        label_version=None,
    )
    db.add(run)
    db.flush()

    candidates = (
        db.query(LabelingCandidate)
        .filter(LabelingCandidate.batch_id == batch_id)
        .order_by(LabelingCandidate.candidate_id.asc())
        .all()
    )

    scored: List[ScoredCandidate] = [
        score_candidate(c, target_low=target_low, target_high=target_high, holding_days=holding_days)
        for c in candidates
    ]

    # rank by p_hit_target desc, then by score
    scored.sort(key=lambda x: (x.p_hit_target, x.score), reverse=True)

    # Persist top-N for UI (default 10)
    top_n = int(getattr(settings, "RECO_TOP_N", 10) or 10)
    kept = scored[:top_n]

    for item in kept:
        reco = ModelRecoV2(
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
        db.flush()  # to get reco_id

        for reason_code, reason_text, fields in item.evidence:
            ev = ModelRecoEvidenceV2(
                reco_id=reco.reco_id,
                reason_code=reason_code,
                reason_text=reason_text,
                evidence_fields=fields,
                evidence_refs={
                    "batch_id": batch_id,
                    "symbol": item.symbol,
                },
                created_ts=asof_ts,
            )
            db.add(ev)

    db.commit()
    return run_id
