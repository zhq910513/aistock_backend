from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, Any

from app.utils.crypto import sha256_hex
from app.utils.time import now_shanghai


@dataclass
class BrokerSendResult:
    broker_order_id: str
    raw_response: dict[str, Any]


class BrokerAdapter(Protocol):
    def send_order(self, raw_req: dict[str, Any]) -> BrokerSendResult:
        raise NotImplementedError

    def query_orders(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def query_fills(self) -> list[dict[str, Any]]:
        raise NotImplementedError


@dataclass
class MockBrokerAdapter:
    """In-memory broker for local runs.

    Behavior:
    - send_order() records an order and immediately produces a synthetic fill
      (fills-first pipeline relies on fills to advance state).
    - query_fills() is drain-on-read for idempotency.
    """

    account_id: str = "ACC_PRIMARY"
    producer_instance: str = "mock-broker-1"
    _orders: dict[str, dict[str, Any]] = field(default_factory=dict)
    _fills: list[dict[str, Any]] = field(default_factory=list)

    def send_order(self, raw_req: dict[str, Any]) -> BrokerSendResult:
        req_json = str(raw_req)
        broker_order_id = sha256_hex(f"{self.account_id}|{req_json}|{now_shanghai().isoformat()}".encode("utf-8"))[:24]

        order = {
            "broker_order_id": broker_order_id,
            "account_id": self.account_id,
            "status": "ACK",
            "raw_request": raw_req,
            "ack_ts": now_shanghai(),
        }
        self._orders[broker_order_id] = order

        # Create a synthetic fill immediately.
        fill_ts = now_shanghai()
        broker_fill_id = sha256_hex(f"{broker_order_id}|FILL|{fill_ts.isoformat()}".encode("utf-8"))[:24]
        fill = {
            "broker_fill_id": broker_fill_id,
            "broker_order_id": broker_order_id,
            "cid": str(raw_req.get("cid") or ""),
            "account_id": self.account_id,
            "symbol": str(raw_req.get("symbol", "")),
            "side": str(raw_req.get("side", "")),
            "fill_price_int64": int(raw_req.get("limit_price_int64", 0) or 0),
            "fill_qty_int": int(raw_req.get("qty_int", 0)),
            "fill_ts": fill_ts,
        }
        self._fills.append(fill)

        raw_response = {
            "errorcode": "0",
            "errmsg": "",
            "broker_order_id": broker_order_id,
            "producer": self.producer_instance,
            "ack_ts": fill_ts.isoformat(),
        }
        return BrokerSendResult(broker_order_id=broker_order_id, raw_response=raw_response)

    def query_orders(self) -> list[dict[str, Any]]:
        return list(self._orders.values())

    def query_fills(self) -> list[dict[str, Any]]:
        out = list(self._fills)
        self._fills.clear()
        return out
