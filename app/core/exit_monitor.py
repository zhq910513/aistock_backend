from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.database import models
from app.database.repo import Repo
from app.utils.crypto import sha256_hex
from app.utils.time import now_shanghai, to_shanghai


@dataclass
class ExitDecision:
    decision: str  # SELL / HOLD
    confidence: float
    reason_code: str
    params: dict[str, Any]


class ExitMonitor:
    """
    退出监控（可审计/可配置的最小闭环版本）：

    数据来源：
    - 最新行情：raw_market_events（并把 request_id 串进 request_ids）
    - 成交回放：trade_fills（反推 entry_ts / hold_days）
    - 策略参数：strategy_contracts.definition.exit_policy（若缺失回退到 settings）

    目标（默认）：
    - 持有 1-3 天
    - 收益 5%-8%
    """

    def evaluate(self, s: Session, *, account_id: str, symbol: str) -> ExitDecision:
        repo = Repo(s)

        # --------
        # position
        # --------
        pos = s.get(models.PortfolioPosition, {"account_id": account_id, "symbol": symbol})
        if pos is None or int(pos.current_qty) <= 0:
            return ExitDecision(
                decision="HOLD",
                confidence=0.0,
                reason_code="RC_EXIT_NO_POSITION",
                params={"account_id": account_id, "symbol": symbol},
            )

        avg_int64 = int(pos.avg_price_int64)
        if avg_int64 <= 0:
            return ExitDecision(
                decision="HOLD",
                confidence=0.2,
                reason_code="RC_EXIT_NO_COST_BASIS",
                params={"account_id": account_id, "symbol": symbol, "avg_price_int64": avg_int64},
            )

        # --------
        # policy (contract-driven)
        # --------
        policy = self._load_exit_policy(repo)
        hold_days_min = int(policy.get("hold_days_min", settings.HOLD_DAYS_MIN))
        hold_days_max = int(policy.get("hold_days_max", settings.HOLD_DAYS_MAX))
        tp_min = float(policy.get("tp_min", settings.TARGET_RETURN_MIN))
        tp_max = float(policy.get("tp_max", settings.TARGET_RETURN_MAX))
        sl_pct = float(policy.get("sl_pct", 0.03))

        policy_hash = sha256_hex(self._stable_json(policy).encode("utf-8"))

        # --------
        # latest price + request lineage
        # --------
        last_price, last_req_id, last_payload_sha = self._latest_price_and_lineage(s, symbol)
        request_ids: list[str] = [x for x in [last_req_id] if x]

        if last_price <= 0:
            params = {
                "account_id": account_id,
                "symbol": symbol,
                "request_ids": request_ids,
                "policy": {"policy": policy, "policy_hash": policy_hash},
                "reason": "missing_last_price",
            }
            params["feature_hash"] = sha256_hex(self._stable_json({"features": params}).encode("utf-8"))
            return ExitDecision("HOLD", 0.2, "RC_EXIT_NO_LAST_PRICE", params)

        # --------
        # hold_days inferred from fills (no extra requests)
        # --------
        entry_ts = self._infer_entry_ts_from_fills(s, account_id=account_id, symbol=symbol, open_qty=int(pos.current_qty))
        hold_days = self._days_held(entry_ts)

        entry_price = float(avg_int64) / 10000.0
        pnl_pct = (float(last_price) / entry_price) - 1.0 if entry_price > 0 else 0.0

        features = {
            "account_id": account_id,
            "symbol": symbol,
            "open_qty": int(pos.current_qty),
            "avg_price_int64": avg_int64,
            "entry_price": entry_price,
            "last_price": float(last_price),
            "pnl_pct": float(pnl_pct),
            "entry_ts": entry_ts.isoformat() if entry_ts else None,
            "hold_days": int(hold_days),
            "last_market_payload_sha256": last_payload_sha,
        }
        feature_hash = sha256_hex(self._stable_json(features).encode("utf-8"))

        params = {
            "features": features,
            "feature_hash": feature_hash,
            "request_ids": request_ids,
            "policy": {
                "hold_days_min": hold_days_min,
                "hold_days_max": hold_days_max,
                "tp_min": tp_min,
                "tp_max": tp_max,
                "sl_pct": sl_pct,
                "policy_hash": policy_hash,
            },
        }

        # -----------------
        # exit criteria
        # -----------------
        # stop loss first
        if pnl_pct <= -sl_pct:
            return ExitDecision("SELL", 0.95, "RC_EXIT_STOP_LOSS", params)

        # hard take profit
        if pnl_pct >= tp_max:
            return ExitDecision("SELL", 0.98, "RC_EXIT_TAKE_PROFIT_MAX", params)

        # soft take profit (requires min hold)
        if pnl_pct >= tp_min and hold_days >= hold_days_min:
            return ExitDecision("SELL", 0.90, "RC_EXIT_TAKE_PROFIT_MIN", params)

        # time-based exit
        if hold_days >= hold_days_max:
            return ExitDecision("SELL", 0.85, "RC_EXIT_TIME_MAX_HOLD", params)

        return ExitDecision("HOLD", 0.50, "RC_EXIT_HOLD", params)

    # --------------------------
    # Policy loader
    # --------------------------
    def _load_exit_policy(self, repo: Repo) -> dict[str, Any]:
        # Prefer StrategyContract (DB) to avoid code changes; fallback to settings
        try:
            repo.strategy_contracts.ensure_seeded(settings.STRATEGY_CONTRACT_HASH)
            definition = repo.strategy_contracts.get_definition(settings.STRATEGY_CONTRACT_HASH)
            exit_policy = dict((definition or {}).get("exit_policy") or {})
            return exit_policy
        except Exception:
            # ultra conservative fallback
            return {
                "hold_days_min": int(settings.HOLD_DAYS_MIN),
                "hold_days_max": int(settings.HOLD_DAYS_MAX),
                "tp_min": float(settings.TARGET_RETURN_MIN),
                "tp_max": float(settings.TARGET_RETURN_MAX),
                "sl_pct": 0.03,
            }

    # --------------------------
    # Latest market lineage
    # --------------------------
    def _latest_price_and_lineage(self, s: Session, symbol: str) -> tuple[float, str, str]:
        row = (
            s.execute(
                select(models.RawMarketEvent)
                .where(models.RawMarketEvent.symbol == symbol)
                .order_by(models.RawMarketEvent.data_ts.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        if row is None:
            return 0.0, "", ""

        payload = row.payload or {}
        try:
            price = float(payload.get("price", 0.0))
        except Exception:
            price = 0.0

        return price, str(row.request_id or ""), str(row.payload_sha256 or "")

    # --------------------------
    # Holding time inference
    # --------------------------
    def _days_held(self, entry_ts: Optional[datetime]) -> int:
        if entry_ts is None:
            return 0
        now = now_shanghai()
        dt = to_shanghai(entry_ts)
        delta = now - dt
        return max(0, int(delta.total_seconds() // 86400))

    def _infer_entry_ts_from_fills(self, s: Session, *, account_id: str, symbol: str, open_qty: int) -> Optional[datetime]:
        """
        从 fills 反推“本次持仓打开”的近似起点：
        - 从最新成交开始向过去回溯，累计 BUY - SELL，直到覆盖 open_qty
        - 返回达到覆盖时对应的 fill_ts
        """
        if open_qty <= 0:
            return None

        fills = (
            s.execute(
                select(models.TradeFill)
                .where(models.TradeFill.account_id == account_id, models.TradeFill.symbol == symbol)
                .order_by(models.TradeFill.fill_ts.desc())
                .limit(2000)
            )
            .scalars()
            .all()
        )
        if not fills:
            return None

        needed = int(open_qty)
        entry_ts: Optional[datetime] = None

        for f in fills:
            side = str(f.side).upper()
            q = int(f.fill_qty_int)
            if side == "BUY":
                needed -= q
                entry_ts = f.fill_ts
                if needed <= 0:
                    break
            elif side == "SELL":
                needed += q

        return entry_ts

    def _stable_json(self, obj: Any) -> str:
        import json
        return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
