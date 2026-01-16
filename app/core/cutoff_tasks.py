"""End-of-day cutoff tasks (15:30 Asia/Shanghai).

This module is called by the scheduler endpoint `/tasks/eod_1530`.

Design goals (P0):
- Make the "15:30 cutoff" runnable and idempotent.
- Materialize a per-symbol feature snapshot row for the day so later
  model runs (or UI trace) have a stable input reference.

Important notes:
- We do NOT fetch external market data here. Collectors/ingestors should have
  already ingested raw payloads before this task runs.
- Current schema uses `FeatIntradayCutoff` as the materialized snapshot table.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.database.engine import SessionLocal
from app.database import models
from app.utils.crypto import sha256_hex
from app.utils.time import now_shanghai, to_shanghai, trading_day_str


def _normalize_trading_day(day: str) -> str:
    """Accept YYYYMMDD or YYYY-MM-DD; return YYYYMMDD."""
    d = (day or "").strip()
    if not d:
        raise ValueError("trading_day is required")
    if len(d) == 8 and d.isdigit():
        return d
    # allow YYYY-MM-DD
    dt = datetime.strptime(d, "%Y-%m-%d")
    return dt.strftime("%Y%m%d")


def _cutoff_dt_1530(td_yyyymmdd: str) -> datetime:
    """Return the 15:30 cutoff datetime in Asia/Shanghai."""
    base = datetime.strptime(td_yyyymmdd, "%Y%m%d")
    base = to_shanghai(base)
    return base.replace(hour=15, minute=30, second=0, microsecond=0)


def materialize_feat_intraday_cutoff(
    trading_day: str,
    cutoff_ts: Optional[datetime] = None,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    """Compute and upsert `FeatIntradayCutoff` rows.

    Parameters:
    - trading_day: 'YYYYMMDD' or 'YYYY-MM-DD' (treated as Asia/Shanghai calendar)
    - cutoff_ts: datetime; defaults to trading_day 15:30 (Asia/Shanghai)
    - symbol: optional single symbol filter

    Returns summary counters.
    """

    td = _normalize_trading_day(trading_day)
    cutoff_dt = to_shanghai(cutoff_ts) if cutoff_ts else _cutoff_dt_1530(td)

    with SessionLocal() as s:
        q = s.query(models.LabelingCandidate).filter(models.LabelingCandidate.trading_day == td)
        if symbol:
            q = q.filter(models.LabelingCandidate.symbol == symbol)
        candidates: List[models.LabelingCandidate] = q.all()

        upserted = 0

        for c in candidates:
            # P0: store what we have. Later, merge real intraday features here.
            feats: Dict[str, Any] = {
                "p_limit_up": float(c.p_limit_up),
                "name": c.name,
                "source": c.source,
                "extra": dict(c.extra or {}),
                "materialized_at": datetime.utcnow().isoformat(),
            }

            feature_hash = sha256_hex(json.dumps(feats, ensure_ascii=False, sort_keys=True).encode("utf-8"))

            raw_hashes: list[str] = []
            extra = dict(c.extra or {})
            if isinstance(extra.get("raw_hashes"), list):
                raw_hashes = [str(x) for x in extra.get("raw_hashes") if str(x)]
            elif extra.get("raw_hash"):
                raw_hashes = [str(extra.get("raw_hash"))]

            existing = (
                s.query(models.FeatIntradayCutoff)
                .filter(
                    models.FeatIntradayCutoff.symbol == c.symbol,
                    models.FeatIntradayCutoff.trading_day == td,
                    models.FeatIntradayCutoff.cutoff_ts == cutoff_dt,
                )
                .one_or_none()
            )

            if existing:
                existing.feature_hash = feature_hash
                existing.features = feats
                existing.raw_hashes = raw_hashes
            else:
                s.add(
                    models.FeatIntradayCutoff(
                        symbol=c.symbol,
                        trading_day=td,
                        cutoff_ts=cutoff_dt,
                        feature_hash=feature_hash,
                        features=feats,
                        raw_hashes=raw_hashes,
                        created_ts=datetime.utcnow(),
                    )
                )

            upserted += 1

        s.commit()

    return {
        "trading_day": td,
        "cutoff_ts": cutoff_dt.isoformat(),
        "symbol": symbol,
        "candidates": len(candidates),
        "upserted": upserted,
    }


def run_eod_cutoff_1530(trading_day: str | None = None) -> Dict[str, Any]:
    """Entry point for the /tasks/eod_1530 endpoint."""

    td = _normalize_trading_day(trading_day or trading_day_str(now_shanghai()))
    res = materialize_feat_intraday_cutoff(trading_day=td)
    return {"task": "eod_1530", **res}
