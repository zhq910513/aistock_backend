from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database import models
from app.database.repo import Repo
from app.core.guard import Guard
from app.core.order_manager import OrderManager
from app.utils.crypto import sha256_hex, verify_ed25519_signature
from app.utils.time import now_shanghai


@dataclass
class ReconcileResult:
    fills_upserted: int
    orphan_fills: int
    ambiguous_snapshots: int


class Reconciler:
    """
    对账引擎：Fills-first + AMBIGUOUS 冲突挂起 + ORPHAN_FILL_ALARM
    """
    def __init__(self) -> None:
        self.guard = Guard()
        self.om = OrderManager()

    def _fill_fingerprint(self, fill: dict[str, Any]) -> str:
        parts = (
            f"{fill.get('broker_fill_id','')}|{fill.get('symbol','')}|{fill.get('side','')}|"
            f"{fill.get('price_int64','')}|{fill.get('qty_int','')}|{fill.get('fill_ts','')}"
        )
        return sha256_hex(parts.encode("utf-8"))

    def upsert_fills_first(self, s: Session, fills: list[dict[str, Any]]) -> ReconcileResult:
        repo = Repo(s)
        upserted = 0
        orphan = 0
        ambiguous = 0

        for f in fills:
            broker_fill_id = f["broker_fill_id"]
            symbol = f["symbol"]
            side = f["side"]
            price_int64 = int(f["fill_price_int64"])
            qty_int = int(f["fill_qty_int"])
            fill_ts = f["fill_ts"]

            fp = self._fill_fingerprint(
                {
                    "broker_fill_id": broker_fill_id,
                    "symbol": symbol,
                    "side": side,
                    "price_int64": price_int64,
                    "qty_int": qty_int,
                    "fill_ts": fill_ts.isoformat(),
                }
            )

            exists = (
                s.execute(select(models.TradeFill).where(models.TradeFill.broker_fill_id == broker_fill_id))
                .scalar_one_or_none()
            )
            if exists is not None:
                continue

            cid = f.get("cid")
            broker_order_id = f.get("broker_order_id")
            account_id = f.get("account_id")

            # 1) direct mapping by broker_order_id
            if cid is None and broker_order_id:
                candidates = (
                    s.execute(select(models.Order).where(models.Order.broker_order_id == broker_order_id))
                    .scalars()
                    .all()
                )
                if len(candidates) == 1:
                    cid = candidates[0].cid
                    account_id = candidates[0].account_id
                elif len(candidates) > 1:
                    snapshot_id = self._create_ambiguous_snapshot(
                        s=s,
                        symbol=symbol,
                        account_id=None,
                        anchor_type="AMBIGUOUS_BROKER_ORDER_ID",
                        anchor_fingerprint=fp,
                        candidates=[{"cid": o.cid, "broker_order_id": o.broker_order_id, "account_id": o.account_id, "score": 1.0, "reason": "same_broker_order_id"} for o in candidates],
                    )
                    repo.symbol_lock.lock(symbol, "AMBIGUOUS", snapshot_id, account_id="GLOBAL")
                    ambiguous += 1

            # 2) fallback: symbol+side candidate (heuristic)
            if cid is None and broker_order_id is None:
                cand = (
                    s.execute(
                        select(models.Order).where(
                            models.Order.symbol == symbol,
                            models.Order.side == side,
                            models.Order.state.in_(["CREATED", "SUBMITTED", "PENDING", "PARTIALLY_FILLED"]),
                        )
                    )
                    .scalars()
                    .all()
                )
                if len(cand) == 1:
                    cid = cand[0].cid
                    account_id = cand[0].account_id
                elif len(cand) > 1:
                    snapshot_id = self._create_ambiguous_snapshot(
                        s=s,
                        symbol=symbol,
                        account_id=None,
                        anchor_type="AMBIGUOUS_MATCH",
                        anchor_fingerprint=fp,
                        candidates=[{"cid": o.cid, "broker_order_id": o.broker_order_id, "account_id": o.account_id, "score": 0.5, "reason": "symbol_side_window"} for o in cand],
                    )
                    repo.symbol_lock.lock(symbol, "AMBIGUOUS", snapshot_id, account_id="GLOBAL")
                    ambiguous += 1

                    for o in cand:
                        if o.state != "AMBIGUOUS":
                            try:
                                self.om.transition(
                                    s,
                                    o.cid,
                                    o.state,
                                    "AMBIGUOUS",
                                    transition_id=sha256_hex(f"AMBIGUOUS|{snapshot_id}|{o.cid}".encode("utf-8"))[:32],
                                )
                            except Exception:
                                pass

                    repo.system_events.write_event(
                        event_type="AMBIGUOUS_ENTERED",
                        correlation_id=None,
                        severity="ERROR",
                        symbol=symbol,
                        payload={"snapshot_id": snapshot_id, "candidate_cids": [o.cid for o in cand]},
                    )

            tf = models.TradeFill(
                broker_fill_id=broker_fill_id,
                cid=cid,
                broker_order_id=broker_order_id,
                account_id=account_id,
                symbol=symbol,
                side=side,
                fill_price_int64=price_int64,
                fill_qty_int=qty_int,
                fill_ts=fill_ts,
                fill_fingerprint=fp,
                created_at=now_shanghai(),
            )
            s.add(tf)
            upserted += 1

            # Maintain mutable portfolio projection (append-only inputs remain intact)
            self._apply_fill_to_position(
                s,
                account_id=str(f.get("account_id") or ""),
                symbol=symbol,
                side=side,
                price_int64=price_int64,
                qty_int=qty_int,
            )

            if cid is None and broker_order_id is None:
                orphan += 1
                repo.system_events.write_event(
                    event_type="ORPHAN_FILL_ALARM",
                    correlation_id=None,
                    severity="CRITICAL",
                    symbol=symbol,
                    payload={
                        "account_id": account_id or "unknown",
                        "fill_fingerprint": fp,
                        "fill_detail": {"symbol": symbol, "side": side, "price": price_int64, "qty": qty_int, "fill_ts": fill_ts.isoformat()},
                        "missing_links": {
                            "client_order_id_found": False,
                            "broker_order_id_found": False,
                            "cid_found": False,
                            "signal_found": False,
                        },
                        "guard_transition": {"from_level": None, "to_level": 2, "veto_code": "ORPHAN_FILL"},
                        "time": now_shanghai().isoformat(),
                    },
                )
                self.guard.enter_orange_on_orphan_fill(s, veto_code="ORPHAN_FILL")

        self._derive_order_states_from_fills(s)
        return ReconcileResult(fills_upserted=upserted, orphan_fills=orphan, ambiguous_snapshots=ambiguous)

    def _apply_fill_to_position(
        self,
        s: Session,
        *,
        account_id: str,
        symbol: str,
        side: str,
        price_int64: int,
        qty_int: int,
    ) -> None:
        """Update portfolio_positions as a derived view (mutable).

        - BUY: increases qty, updates weighted-average cost
        - SELL: decreases qty, keeps avg cost; if position closed => avg cost reset
        """
        if not account_id:
            # still record fill, but skip position update if account_id unknown
            return
        if qty_int <= 0:
            return

        repo = Repo(s)
        key = {"account_id": account_id, "symbol": symbol}
        pos = s.get(models.PortfolioPosition, key)
        if pos is None:
            pos = models.PortfolioPosition(
                account_id=account_id,
                symbol=symbol,
                current_qty=0,
                frozen_qty=0,
                avg_price_int64=0,
                updated_at=now_shanghai(),
            )
            s.add(pos)
            s.flush()

        side_u = str(side).upper()
        cur_qty = int(pos.current_qty)
        cur_avg = int(pos.avg_price_int64)

        if side_u == "BUY":
            new_qty = cur_qty + int(qty_int)
            if new_qty <= 0:
                pos.current_qty = 0
                pos.avg_price_int64 = 0
            else:
                # weighted average cost (int64)
                total_cost = cur_avg * cur_qty + int(price_int64) * int(qty_int)
                pos.current_qty = new_qty
                pos.avg_price_int64 = int(round(total_cost / new_qty))
            pos.updated_at = now_shanghai()
            return

        if side_u == "SELL":
            new_qty = cur_qty - int(qty_int)
            if new_qty < 0:
                repo.system_events.write_event(
                    event_type="POSITION_NEGATIVE_CLAMP",
                    correlation_id=None,
                    severity="WARN",
                    symbol=symbol,
                    payload={
                        "account_id": account_id,
                        "cur_qty": cur_qty,
                        "sell_qty": int(qty_int),
                        "clamped_to": 0,
                    },
                )
                new_qty = 0

            pos.current_qty = int(new_qty)
            if int(pos.current_qty) == 0:
                pos.avg_price_int64 = 0
            pos.updated_at = now_shanghai()
            return

        # unknown side
        repo.system_events.write_event(
            event_type="POSITION_SIDE_UNKNOWN",
            correlation_id=None,
            severity="WARN",
            symbol=symbol,
            payload={"account_id": account_id, "side": side_u, "qty_int": int(qty_int)},
        )

    def _create_ambiguous_snapshot(
        self,
        s: Session,
        symbol: str,
        account_id: str | None,
        anchor_type: str,
        anchor_fingerprint: str,
        candidates: list[dict[str, Any]],
    ) -> str:
        repo = Repo(s)
        snap_material = f"{anchor_type}|{anchor_fingerprint}|{symbol}|{account_id or ''}|{candidates}"
        snapshot_id = sha256_hex(snap_material.encode("utf-8"))
        report_hash = sha256_hex(snap_material.encode("utf-8"))

        exists = s.get(models.ReconcileSnapshot, snapshot_id)
        if exists is None:
            s.add(
                models.ReconcileSnapshot(
                    snapshot_id=snapshot_id,
                    symbol=symbol,
                    account_id=account_id,
                    anchor_type=anchor_type,
                    anchor_fingerprint=anchor_fingerprint,
                    candidates=candidates,
                    report_hash=report_hash,
                    status="OPEN",
                    created_at=now_shanghai(),
                )
            )
            repo.system_events.write_event(
                event_type="RECONCILE_SNAPSHOT_CREATED",
                correlation_id=None,
                severity="ERROR",
                symbol=symbol,
                payload={"snapshot_id": snapshot_id, "anchor_type": anchor_type, "anchor_fingerprint": anchor_fingerprint, "candidates": candidates},
            )

        return snapshot_id

    def _derive_order_states_from_fills(self, s: Session) -> None:
        repo = Repo(s)
        orders = s.execute(select(models.Order)).scalars().all()

        links = s.execute(select(models.TradeFillLink)).scalars().all()
        fp_to_cid = {x.fill_fingerprint: x.cid for x in links}

        for o in orders:
            if o.state == "AMBIGUOUS":
                continue

            fills = s.execute(select(models.TradeFill).where(models.TradeFill.symbol == o.symbol)).scalars().all()
            if not fills:
                continue

            filled_qty = 0
            for x in fills:
                eff_cid = x.cid or fp_to_cid.get(x.fill_fingerprint)
                if eff_cid == o.cid:
                    filled_qty += int(x.fill_qty_int)

            if filled_qty >= int(o.qty_int):
                if o.state != "FILLED":
                    try:
                        self.om.transition(
                            s,
                            o.cid,
                            o.state,
                            "FILLED",
                            transition_id=sha256_hex(f"DERIVE_FILLED|{o.cid}|{filled_qty}".encode("utf-8"))[:32],
                        )
                    except Exception:
                        pass
                    repo.system_events.write_event(
                        event_type="ORDER_DERIVED_FILLED",
                        correlation_id=o.cid,
                        severity="INFO",
                        symbol=o.symbol,
                        payload={"cid": o.cid, "filled_qty": filled_qty, "qty_int": int(o.qty_int)},
                    )
            elif filled_qty > 0:
                if o.state not in {"PARTIALLY_FILLED", "FILLED"}:
                    try:
                        self.om.transition(
                            s,
                            o.cid,
                            o.state,
                            "PARTIALLY_FILLED",
                            transition_id=sha256_hex(f"DERIVE_PARTIAL|{o.cid}|{filled_qty}".encode("utf-8"))[:32],
                        )
                    except Exception:
                        pass
                    repo.system_events.write_event(
                        event_type="ORDER_DERIVED_PARTIAL",
                        correlation_id=o.cid,
                        severity="INFO",
                        symbol=o.symbol,
                        payload={"cid": o.cid, "filled_qty": filled_qty, "qty_int": int(o.qty_int)},
                    )

    def apply_reconcile_decision_signed(
        self,
        s: Session,
        snapshot_id: str,
        decided_cid: str,
        decided_broker_order_id: str | None,
        signer_key_id: str,
        signature_b64: str,
    ) -> str:
        repo = Repo(s)
        snap = s.get(models.ReconcileSnapshot, snapshot_id)
        if snap is None or snap.status != "OPEN":
            raise ValueError("snapshot_not_open")

        key = s.get(models.GuardianKey, signer_key_id)
        if key is None or key.revoked or key.role not in {"RISK_OFFICER", "COMPLIANCE"}:
            raise ValueError("invalid_signer_key")

        msg = f"RECONCILE_DECISION|{snapshot_id}|{decided_cid}|{decided_broker_order_id or ''}"
        if not verify_ed25519_signature(key.public_key_b64, msg.encode("utf-8"), signature_b64):
            raise ValueError("signature_invalid")

        prev = (
            s.execute(
                select(models.ReconcileDecision)
                .where(models.ReconcileDecision.snapshot_id == snapshot_id)
                .order_by(models.ReconcileDecision.created_at.desc())
                .limit(1)
            )
            .scalar_one_or_none()
        )
        prev_decision_id = prev.decision_id if prev else None

        decision_material = sha256_hex(f"{msg}|{signer_key_id}|{signature_b64}|{prev_decision_id or ''}".encode("utf-8"))
        decision_id = decision_material[:64]

        s.add(
            models.ReconcileDecision(
                decision_id=decision_id,
                snapshot_id=snapshot_id,
                decided_cid=decided_cid,
                decided_broker_order_id=decided_broker_order_id,
                signer_key_id=signer_key_id,
                signature=signature_b64,
                prev_decision_id=prev_decision_id,
                created_at=now_shanghai(),
            )
        )

        snap.status = "DECIDED"

        repo.symbol_lock.unlock(snap.symbol, account_id="GLOBAL")

        order = s.get(models.Order, decided_cid)
        if order is None:
            raise ValueError("order_not_found")

        if decided_broker_order_id:
            order.broker_order_id = decided_broker_order_id
        order.updated_at = now_shanghai()

        if order.state in {"AMBIGUOUS", "SUBMITTED", "CREATED"}:
            try:
                self.om.transition(
                    s,
                    decided_cid,
                    order.state,
                    "PENDING",
                    transition_id=sha256_hex(f"RECONCILE_DECISION_APPLY|{decision_id}".encode("utf-8"))[:32],
                )
            except Exception:
                pass

        tf = s.execute(select(models.TradeFill).where(models.TradeFill.fill_fingerprint == snap.anchor_fingerprint)).scalar_one_or_none()
        if tf is not None:
            exists_link = s.get(models.TradeFillLink, tf.fill_fingerprint)
            if exists_link is None:
                s.add(
                    models.TradeFillLink(
                        fill_fingerprint=tf.fill_fingerprint,
                        cid=decided_cid,
                        broker_order_id=decided_broker_order_id,
                        account_id=order.account_id,
                        snapshot_id=snapshot_id,
                        decision_id=decision_id,
                        created_at=now_shanghai(),
                    )
                )

        repo.system_events.write_event(
            event_type="RECONCILE_DECISION_APPLIED",
            correlation_id=decided_cid,
            severity="WARN",
            symbol=snap.symbol,
            payload={"snapshot_id": snapshot_id, "decision_id": decision_id, "decided_cid": decided_cid, "decided_broker_order_id": decided_broker_order_id},
        )

        return decision_id
