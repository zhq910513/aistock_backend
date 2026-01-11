from __future__ import annotations

from app.utils.crypto import sha256_hex


def deterministic_id(prefix: str, material: str) -> str:
    return sha256_hex(f"{prefix}|{material}".encode("utf-8"))


def new_request_id(material: str) -> str:
    # Deterministic request id for audit/replay
    return deterministic_id("REQ", material)[:32]


def new_transition_id(cid: str, from_state: str, to_state: str, version_id: int) -> str:
    # Deterministic transition id (idempotency token) if caller doesn't provide one
    material = f"{cid}|{from_state}|{to_state}|v{int(version_id)}"
    return deterministic_id("TR", material)[:32]


def make_cid(
    trading_day: str,
    symbol: str,
    strategy_id: str,
    signal_ts_millis: str,
    nonce: int,
    side: str,
    intended_qty_or_notional: int,
    account_id: str,
) -> str:
    """
    QEE-S³ 4.2 (extended for multi-account)
    CID = Hash(TradingDay, Symbol, StrategyID, SignalTS, Nonce, Side, IntendedQtyOrNotional, AccountId)
    - signal_ts_millis must be provided from decision time (not generated inside).
    """
    canonical = (
        f"{trading_day}|{symbol}|{strategy_id}|{signal_ts_millis}|{int(nonce)}|"
        f"{side.upper()}|{int(intended_qty_or_notional)}|{account_id}"
    )
    return sha256_hex(canonical.encode("utf-8"))
