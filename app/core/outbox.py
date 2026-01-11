from __future__ import annotations

from dataclasses import dataclass
import json

from sqlalchemy.orm import Session

from app.database.repo import Repo
from app.database import models
from app.utils.time import now_shanghai
from app.utils.crypto import sha256_hex
from app.core.order_manager import OrderManager
from app.core.execution_router import ExecutionRouter
from app.core.broker_adapter import BrokerSendResult


class OutboxDispatcher:
    """
    Outbox dispatcher:
    - SEND_ORDER: send to broker (per account), then write Orders.broker_order_id and OrderAnchor.
    """
    def __init__(self, router: ExecutionRouter) -> None:
        self._router = router
        self._om = OrderManager()

    def pump_once(self, s: Session, limit: int = 50) -> int:
        repo = Repo(s)
        pending = repo.outbox.fetch_pending(limit=limit)
        sent = 0

        for ev in pending:
            try:
                if ev.event_type == "SEND_ORDER":
                    payload = dict(ev.payload or {})
                    cid = str(payload["cid"])
                    order = s.get(models.Order, cid)
                    if order is None:
                        repo.outbox.mark_failed(ev, "order_not_found", write_op=True)
                        continue

                    route = self._router.route(s, order.symbol)
                    # sanity: must match order.account_id
                    if route.account_id != order.account_id:
                        repo.outbox.mark_failed(ev, "account_route_mismatch", write_op=True)
                        continue

                    # deterministic request_uuid for audit (stable within this outbox row+attempt)
                    request_uuid = sha256_hex(f"SEND_ORDER|{cid}|{int(ev.id)}|{int(ev.attempts)}".encode("utf-8"))[:32]

                    raw_req = {
                        "request_uuid": request_uuid,
                        "account_id": order.account_id,
                        "client_order_id": order.client_order_id,
                        "cid": cid,
                        "symbol": order.symbol,
                        "side": order.side,
                        "order_type": order.order_type,
                        "limit_price_int64": int(order.limit_price_int64),
                        "qty_int": int(order.qty_int),
                        "metadata_hash": order.metadata_hash,
                    }
                    raw_request_hash = sha256_hex(json.dumps(raw_req, sort_keys=True).encode("utf-8"))

                    res: BrokerSendResult = route.broker.send_order(raw_req)
                    broker_order_id = res.broker_order_id
                    raw_resp = res.raw_response

                    raw_response_hash = sha256_hex(json.dumps(raw_resp, sort_keys=True).encode("utf-8"))
                    ack_hash = sha256_hex(f"{request_uuid}|{broker_order_id}|{raw_response_hash}".encode("utf-8"))

                    # apply broker_order_id (mutable, OK)
                    order.broker_order_id = broker_order_id
                    order.updated_at = now_shanghai()

                    # state advance must go through state machine (idempotent)
                    if order.state == "CREATED":
                        self._om.transition(
                            s,
                            cid,
                            "CREATED",
                            "SUBMITTED",
                            transition_id=sha256_hex(f"AUTO_SUBMIT|{cid}".encode("utf-8"))[:32],
                        )
                        self._om.transition(
                            s,
                            cid,
                            "SUBMITTED",
                            "PENDING",
                            transition_id=sha256_hex(f"BROKER_ACK|{cid}|{ack_hash}".encode("utf-8"))[:32],
                        )
                    elif order.state == "SUBMITTED":
                        self._om.transition(
                            s,
                            cid,
                            "SUBMITTED",
                            "PENDING",
                            transition_id=sha256_hex(f"BROKER_ACK|{cid}|{ack_hash}".encode("utf-8"))[:32],
                        )

                    repo.anchors.upsert_anchor(
                        cid=cid,
                        account_id=order.account_id,
                        client_order_id=order.client_order_id,
                        broker_order_id=broker_order_id,
                        request_uuid=request_uuid,
                        ack_hash=ack_hash,
                        raw_request_hash=raw_request_hash,
                        raw_response_hash=raw_response_hash,
                    )

                    repo.system_events.write_event(
                        event_type="BROKER_ACK",
                        correlation_id=cid,
                        severity="INFO",
                        symbol=order.symbol,
                        payload={
                            "account_id": order.account_id,
                            "request_uuid": request_uuid,
                            "broker_order_id": broker_order_id,
                            "ack_hash": ack_hash,
                            "raw_request_hash": raw_request_hash,
                            "raw_response_hash": raw_response_hash,
                        },
                    )

                repo.outbox.mark_sent(ev)
                sent += 1
            except Exception as e:
                # SEND_ORDER is write op: do NOT retry (avoid duplicates)
                repo.outbox.mark_failed(ev, str(e), write_op=(ev.event_type == "SEND_ORDER"))

        return sent
