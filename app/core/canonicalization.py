from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from sqlalchemy.orm import Session

from app.config import settings
from app.database import models
from app.utils.crypto import sha256_hex


@dataclass
class CanonicalizedOrder:
    canonicalization_version: str
    tick_rule_version: str
    lot_rule_version: str
    tick_size: float
    lot_size: int

    limit_price_int64: int  # round(price * 10000) after tick discretization
    qty_int: int            # lot discretization (shares)
    metadata_hash: str      # SHA256(Serialize(...))


def _int64_be(x: int) -> bytes:
    return int(x).to_bytes(8, byteorder="big", signed=True)


def _u32_be(x: int) -> bytes:
    return int(x).to_bytes(4, byteorder="big", signed=False)


def _bstr(s: str) -> bytes:
    b = s.encode("utf-8")
    return _u32_be(len(b)) + b


def _enum_side(side: str) -> int:
    s = side.upper()
    if s == "BUY":
        return 1
    if s == "SELL":
        return 2
    raise ValueError("side must be BUY/SELL")


def _enum_order_type(order_type: str) -> int:
    t = order_type.upper()
    if t == "LIMIT":
        return 1
    if t == "MARKET":
        return 2
    return 99


def _tick_discretize(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return price
    n = round(price / tick_size)
    return n * tick_size


def _lot_discretize(qty: int, lot_size: int) -> int:
    if lot_size <= 0:
        return qty
    if qty < 0:
        raise ValueError("qty must be non-negative")
    return (qty // lot_size) * lot_size


def _ensure_instrument_rules(s: Session, symbol: str) -> models.InstrumentRuleCache:
    rule = s.get(models.InstrumentRuleCache, symbol)
    if rule is None:
        from app.utils.time import now_shanghai
        rule = models.InstrumentRuleCache(
            symbol=symbol,
            tick_rule_version="tick_default",
            lot_rule_version="lot_default",
            tick_size=0.01,
            lot_size=1,
            updated_at=now_shanghai(),
        )
        s.add(rule)
        s.flush()
    return rule


def canonicalize_order(
    s: Session,
    cid: str,
    order_type: str,
    symbol: str,
    side: str,
    limit_price: Optional[float],
    qty: int,
) -> CanonicalizedOrder:
    rule = _ensure_instrument_rules(s, symbol)

    tick_size = float(rule.tick_size)
    lot_size = int(rule.lot_size)

    if order_type.upper() == "MARKET":
        limit_price_int64 = 0
    else:
        if limit_price is None:
            raise ValueError("LIMIT order requires limit_price")
        p = _tick_discretize(float(limit_price), tick_size)
        limit_price_int64 = int(round(p * 10000))

    qty_int = int(_lot_discretize(int(qty), lot_size))

    b = b"".join(
        [
            _bstr(settings.CANONICALIZATION_VERSION),
            _bstr(cid),
            _u32_be(_enum_order_type(order_type)),
            _int64_be(limit_price_int64),
            _int64_be(qty_int),
            _u32_be(_enum_side(side)),
            _bstr(rule.tick_rule_version),
            _bstr(rule.lot_rule_version),
        ]
    )
    metadata_hash = sha256_hex(b)

    return CanonicalizedOrder(
        canonicalization_version=settings.CANONICALIZATION_VERSION,
        tick_rule_version=rule.tick_rule_version,
        lot_rule_version=rule.lot_rule_version,
        tick_size=tick_size,
        lot_size=lot_size,
        limit_price_int64=limit_price_int64,
        qty_int=qty_int,
        metadata_hash=metadata_hash,
    )
