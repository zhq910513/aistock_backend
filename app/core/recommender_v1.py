from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.database import models
from app.utils.crypto import sha256_hex
from app.utils.symbols import normalize_symbol
from app.utils.time import now_shanghai


@dataclass(frozen=True)
class EvidenceItem:
    reason_code: str
    reason_text: str
    evidence_fields: dict[str, Any]
    evidence_refs: dict[str, Any]


@dataclass(frozen=True)
class RecommendationItem:
    decision_id: str
    symbol: str
    name: str
    action: str
    score: float
    confidence: float
    evidence: list[EvidenceItem]


def _next_day_yyyymmdd(td: str) -> str:
    dt = datetime.strptime(td, "%Y%m%d")
    return (dt + timedelta(days=1)).strftime("%Y%m%d")


def _pick_action(p: float) -> tuple[str, str, str]:
    """Map probability to (action, reason_code, reason_text)."""
    if p >= 0.7:
        return "BUY", "P_LIMITUP_HIGH", "涨停概率较高"
    if p >= 0.45:
        return "WATCH", "P_LIMITUP_MED", "涨停概率中等，建议观察"
    return "AVOID", "P_LIMITUP_LOW", "涨停概率偏低，建议规避"


def _to_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return None
        return float(x)
    except Exception:
        return None


def _parse_high_days(v: Any) -> int | None:
    """Parse high_days from common representations like 3 or '3天3板'."""
    if v is None:
        return None
    if isinstance(v, int):
        return v
    s = str(v)
    # pick first integer in the string
    import re

    m = re.search(r"(\d+)", s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def generate_for_batch(s: Session, batch: models.LimitupPoolBatch, topn: int | None = None) -> list[RecommendationItem]:
    """Generate recommendations for a COMMITTED batch (P0 v1).

    P0 原则：先闭环，再补齐采集/特征。
    - score: p_limit_up * 100 (0~100)
    - confidence: clamp(p_limit_up, 0~1)
    - evidence: 每票至少 3 条可追溯理由（reason_code/reason_text + evidence_fields/refs）
    """
    n = int(topn or settings.RECOMMEND_TOPN or 10)
    cands = (
        s.execute(
            select(models.LimitupCandidate)
            .where(models.LimitupCandidate.batch_id == batch.batch_id)
            .where(models.LimitupCandidate.candidate_status != "DROPPED")
        )
        .scalars()
        .all()
    )

    items: list[RecommendationItem] = []
    decision_day = _next_day_yyyymmdd(batch.trading_day)

    for c in cands:
        p = float(c.p_limit_up) if c.p_limit_up is not None else 0.0
        p = min(max(p, 0.0), 1.0)
        action, p_reason_code, p_reason_text = _pick_action(p)

        raw = dict(c.raw_json or {})

        # Common raw fields (best-effort)
        high_days = _parse_high_days(raw.get("high_days"))
        limit_up_type = raw.get("limit_up_type")
        turnover_rate = _to_float(raw.get("turnover_rate"))
        order_amount = _to_float(raw.get("order_amount") or raw.get("order_volume"))
        sum_market_value = _to_float(raw.get("sum_market_value") or raw.get("currency_value"))
        is_again_limit = raw.get("is_again_limit")
        change_rate = _to_float(raw.get("change_rate"))

        # Refs: keep it dead simple in P0 but traceable
        refs_base = {
            "batch_id": batch.batch_id,
            "candidate_id": c.id,
            "candidate_symbol": c.symbol,
            "raw_hash": batch.raw_hash,
        }

        evidence: list[EvidenceItem] = []

        # 1) Probability is always the primary reason
        evidence.append(
            EvidenceItem(
                reason_code=p_reason_code,
                reason_text=f"{p_reason_text}（p_limit_up={p:.4f}）",
                evidence_fields={"p_limit_up": p},
                evidence_refs=refs_base,
            )
        )

        # 2) 连板/高标特征（如果缺失也给出说明，避免无声黑箱）
        if high_days is not None:
            if high_days >= 2:
                txt = f"连板/高标强度：high_days={high_days}"
                code = "HIGH_DAYS_STRONG"
            else:
                txt = f"连板信息：high_days={high_days}"
                code = "HIGH_DAYS_INFO"
            evidence.append(
                EvidenceItem(
                    reason_code=code,
                    reason_text=txt,
                    evidence_fields={"high_days": raw.get("high_days")},
                    evidence_refs=refs_base,
                )
            )
        else:
            evidence.append(
                EvidenceItem(
                    reason_code="HIGH_DAYS_MISSING",
                    reason_text="缺少 high_days（连板/高标强度）字段，已降低置信度参考",
                    evidence_fields={"high_days": None},
                    evidence_refs=refs_base,
                )
            )

        # 3) 交易热度/封板类型（尽量从 limit_up_type / turnover_rate / order_amount 取一条）
        if limit_up_type:
            evidence.append(
                EvidenceItem(
                    reason_code="LIMITUP_TYPE",
                    reason_text=f"涨停类型：{limit_up_type}",
                    evidence_fields={"limit_up_type": limit_up_type},
                    evidence_refs=refs_base,
                )
            )
        elif turnover_rate is not None:
            hint = "换手率偏高" if turnover_rate >= 10 else "换手率适中/偏低"
            evidence.append(
                EvidenceItem(
                    reason_code="TURNOVER_RATE",
                    reason_text=f"{hint}（turnover_rate={turnover_rate:.2f}）",
                    evidence_fields={"turnover_rate": raw.get("turnover_rate")},
                    evidence_refs=refs_base,
                )
            )
        elif order_amount is not None:
            evidence.append(
                EvidenceItem(
                    reason_code="ORDER_AMOUNT",
                    reason_text=f"成交/封单量级：{order_amount}",
                    evidence_fields={"order_amount": raw.get("order_amount") or raw.get("order_volume")},
                    evidence_refs=refs_base,
                )
            )
        else:
            evidence.append(
                EvidenceItem(
                    reason_code="HEAT_MISSING",
                    reason_text="缺少 limit_up_type/turnover_rate/order_amount 关键热度字段，已降低置信度参考",
                    evidence_fields={
                        "limit_up_type": None,
                        "turnover_rate": None,
                        "order_amount": None,
                    },
                    evidence_refs=refs_base,
                )
            )

        # Extra: attach a compact evidence_fields snapshot for UI convenience (not a separate reason)
        snapshot_fields = {
            "p_limit_up": p,
            "high_days": raw.get("high_days"),
            "limit_up_type": limit_up_type,
            "turnover_rate": raw.get("turnover_rate"),
            "order_amount": raw.get("order_amount") or raw.get("order_volume"),
            "sum_market_value": raw.get("sum_market_value") or raw.get("currency_value"),
            "is_again_limit": is_again_limit,
            "change_rate": raw.get("change_rate"),
        }

        # score: 0~100 (per product doc)
        score = round(p * 100.0, 4)
        confidence = p

        # Deterministic decision_id for idempotency
        decision_id = sha256_hex(
            f"{batch.trading_day}|{decision_day}|{c.symbol}|{p:.6f}|{p_reason_code}".encode("utf-8")
        )[:64]

        # Make sure at least 3 evidence entries exist
        if len(evidence) < 3:
            evidence.append(
                EvidenceItem(
                    reason_code="EVIDENCE_PADDING",
                    reason_text="证据不足（P0 版），已降低置信度参考",
                    evidence_fields=snapshot_fields,
                    evidence_refs=refs_base,
                )
            )

        # Add snapshot as the last evidence_refs if you want UI to show raw fields quickly
        evidence.append(
            EvidenceItem(
                reason_code="RAW_SNAPSHOT",
                reason_text="候选池原始字段快照（用于追溯）",
                evidence_fields=snapshot_fields,
                evidence_refs=refs_base,
            )
        )

        items.append(
            RecommendationItem(
                decision_id=decision_id,
                symbol=normalize_symbol(c.symbol),
                name=str(c.name or ""),
                action=action,
                score=score,
                confidence=confidence,
                evidence=evidence,
            )
        )

    # rank by score desc
    items.sort(key=lambda x: (x.score, x.confidence), reverse=True)
    return items[:n]


def persist_decisions(s: Session, batch: models.LimitupPoolBatch, items: list[RecommendationItem]) -> None:
    """Upsert decisions and evidence (idempotent for same decision_id)."""
    now = now_shanghai()
    decision_day = _next_day_yyyymmdd(batch.trading_day)

    for it in items:
        existing = s.get(models.ModelDecision, it.decision_id)
        if existing is None:
            s.add(
                models.ModelDecision(
                    decision_id=it.decision_id,
                    trading_day=batch.trading_day,
                    decision_day=decision_day,
                    symbol=it.symbol,
                    action=it.action,
                    score=float(it.score),
                    confidence=float(it.confidence),
                    created_ts=now,
                )
            )
            s.flush()

        # Insert evidence rows (idempotent-ish: delete old then insert is simplest, but keep minimal write)
        # To avoid duplicates, we delete existing evidence for this decision_id and reinsert.
        s.query(models.DecisionEvidence).filter(models.DecisionEvidence.decision_id == it.decision_id).delete()

        for ev in it.evidence:
            s.add(
                models.DecisionEvidence(
                    decision_id=it.decision_id,
                    reason_code=ev.reason_code,
                    reason_text=ev.reason_text,
                    evidence_fields=ev.evidence_fields,
                    evidence_refs=ev.evidence_refs,
                )
            )
