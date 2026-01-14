from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from datetime import timedelta
import json

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.database import models
from app.utils.time import now_shanghai, trading_day_str, to_shanghai
from app.utils.crypto import sha256_hex
from app.utils.symbols import normalize_symbol


@dataclass
class SystemEventsRepo:
    s: Session

    def write_event(
        self,
        event_type: str,
        correlation_id: str | None,
        severity: str,
        payload: dict,
        symbol: str | None = None,
    ) -> None:
        self.s.add(
            models.SystemEvent(
                event_type=event_type,
                severity=severity,
                correlation_id=correlation_id,
                symbol=symbol,
                payload=payload,
                time=now_shanghai(),
            )
        )


@dataclass
class SystemStatusRepo:
    s: Session

    def get_for_update(self) -> models.SystemStatus:
        row = self.s.execute(select(models.SystemStatus).where(models.SystemStatus.id == 1)).scalar_one_or_none()
        if row is None:
            row = models.SystemStatus(id=1, updated_at=now_shanghai())
            self.s.add(row)
            self.s.flush()
        return row

    def set_panic_halt(self, veto_code: str) -> None:
        st = self.get_for_update()
        st.panic_halt = True
        st.guard_level = 3
        st.veto = True
        st.veto_code = veto_code
        st.updated_at = now_shanghai()

    def set_guard_level(self, level: int, veto: bool, veto_code: str) -> None:
        st = self.get_for_update()
        st.guard_level = int(level)
        st.veto = bool(veto)
        st.veto_code = str(veto_code or "")
        st.updated_at = now_shanghai()

    def set_challenge(self, code: str | None) -> None:
        st = self.get_for_update()
        st.challenge_code = code
        st.updated_at = now_shanghai()

    def set_self_check(self, report_hash: str) -> None:
        st = self.get_for_update()
        st.last_self_check_report_hash = report_hash
        st.last_self_check_time = now_shanghai()
        st.updated_at = now_shanghai()

    def reset_from_panic(self) -> None:
        st = self.get_for_update()
        st.panic_halt = False
        st.veto = False
        st.veto_code = ""
        st.guard_level = 0
        st.challenge_code = None
        st.updated_at = now_shanghai()


@dataclass
class ControlsRepo:
    s: Session

    def get_for_update(self) -> models.RuntimeControls:
        row = self.s.execute(select(models.RuntimeControls).where(models.RuntimeControls.id == 1)).scalar_one_or_none()
        if row is None:
            row = models.RuntimeControls(id=1, updated_at=now_shanghai())
            self.s.add(row)
            self.s.flush()
        return row

    def as_dict(self) -> dict:
        c = self.get_for_update()
        return {
            "auto_trading_enabled": bool(c.auto_trading_enabled),
            "dry_run": bool(c.dry_run),
            "only_when_data_ok": bool(c.only_when_data_ok),
            "max_orders_per_day": int(c.max_orders_per_day),
            "max_notional_per_order": int(c.max_notional_per_order),
            "allowed_symbols": list(c.allowed_symbols or []),
            "blocked_symbols": list(c.blocked_symbols or []),
            "updated_at": c.updated_at.isoformat() if c.updated_at else None,
        }

    def patch(self, payload: dict) -> dict:
        c = self.get_for_update()

        def _to_list(v) -> list[str]:
            if v is None:
                return []
            if isinstance(v, str):
                return [x.strip() for x in v.split(",") if x.strip()]
            if isinstance(v, list):
                return [str(x).strip() for x in v if str(x).strip()]
            return []

        if "auto_trading_enabled" in payload:
            c.auto_trading_enabled = bool(payload["auto_trading_enabled"])
        if "dry_run" in payload:
            c.dry_run = bool(payload["dry_run"])
        if "only_when_data_ok" in payload:
            c.only_when_data_ok = bool(payload["only_when_data_ok"])

        if "max_orders_per_day" in payload and payload["max_orders_per_day"] is not None:
            c.max_orders_per_day = int(payload["max_orders_per_day"])
        if "max_notional_per_order" in payload and payload["max_notional_per_order"] is not None:
            c.max_notional_per_order = int(payload["max_notional_per_order"])

        if "allowed_symbols" in payload:
            c.allowed_symbols = _to_list(payload["allowed_symbols"])
        if "blocked_symbols" in payload:
            c.blocked_symbols = _to_list(payload["blocked_symbols"])

        c.updated_at = now_shanghai()
        return self.as_dict()


@dataclass
class AccountsRepo:
    s: Session

    def ensure_accounts_seeded(self) -> None:
        now = now_shanghai()
        ids = [x.strip() for x in (settings.ACCOUNT_IDS or "").split(",") if x.strip()]
        if not ids:
            ids = [settings.DEFAULT_ACCOUNT_ID]

        for aid in ids:
            row = self.s.get(models.Account, aid)
            if row is None:
                self.s.add(models.Account(account_id=aid, broker_type="MOCK", config={}, created_at=now))

    def list_accounts(self) -> list[models.Account]:
        return self.s.execute(select(models.Account).order_by(models.Account.account_id.asc())).scalars().all()


@dataclass
class NonceRepo:
    s: Session

    def next_nonce(self, symbol: str, strategy_id: str, account_id: str) -> int:
        td = trading_day_str(now_shanghai())
        row = (
            self.s.execute(
                select(models.NonceCursor)
                .where(
                    models.NonceCursor.trading_day == td,
                    models.NonceCursor.symbol == symbol,
                    models.NonceCursor.strategy_id == strategy_id,
                    models.NonceCursor.account_id == account_id,
                )
                .with_for_update()
            )
            .scalar_one_or_none()
        )
        if row is None:
            row = models.NonceCursor(
                trading_day=td,
                symbol=symbol,
                strategy_id=strategy_id,
                account_id=account_id,
                last_nonce=0,
                updated_at=now_shanghai(),
            )
            self.s.add(row)
            self.s.flush()
        row.last_nonce = int(row.last_nonce) + 1
        row.updated_at = now_shanghai()
        return int(row.last_nonce)


@dataclass
class SymbolLockRepo:
    s: Session

    def is_locked(self, symbol: str, account_id: str = "GLOBAL") -> bool:
        row = self.s.get(models.SymbolLock, {"account_id": account_id, "symbol": symbol})
        return bool(row.locked) if row else False

    def lock(self, symbol: str, reason: str, ref: str | None, account_id: str = "GLOBAL") -> None:
        now = now_shanghai()
        row = self.s.get(models.SymbolLock, {"account_id": account_id, "symbol": symbol})
        if row is None:
            row = models.SymbolLock(
                account_id=account_id,
                symbol=symbol,
                locked=True,
                lock_reason=reason,
                lock_ref=ref,
                created_at=now,
                updated_at=now,
            )
            self.s.add(row)
        else:
            row.locked = True
            row.lock_reason = reason
            row.lock_ref = ref
            row.updated_at = now

    def unlock(self, symbol: str, account_id: str = "GLOBAL") -> None:
        row = self.s.get(models.SymbolLock, {"account_id": account_id, "symbol": symbol})
        if row:
            row.locked = False
            row.lock_reason = ""
            row.lock_ref = None
            row.updated_at = now_shanghai()


@dataclass
class OutboxRepo:
    s: Session

    def enqueue(self, event_type: str, dedupe_key: str, payload: dict) -> None:
        exists = self.s.execute(select(models.OutboxEvent).where(models.OutboxEvent.dedupe_key == dedupe_key)).scalar_one_or_none()
        if exists is not None:
            return
        now = now_shanghai()
        self.s.add(
            models.OutboxEvent(
                event_type=event_type,
                dedupe_key=dedupe_key,
                status="PENDING",
                attempts=0,
                available_at=now,
                payload=payload,
                created_at=now,
                sent_at=None,
            )
        )

    def fetch_pending(self, limit: int = 50) -> list[models.OutboxEvent]:
        now = now_shanghai()
        return (
            self.s.execute(
                select(models.OutboxEvent)
                .where(
                    models.OutboxEvent.status == "PENDING",
                    models.OutboxEvent.available_at <= now,
                )
                .order_by(models.OutboxEvent.id.asc())
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            .scalars()
            .all()
        )

    def mark_sent(self, ev: models.OutboxEvent) -> None:
        ev.status = "SENT"
        ev.sent_at = now_shanghai()
        ev.last_error = None

    def _backoff_ms(self, attempts: int) -> int:
        base = int(settings.OUTBOX_BACKOFF_BASE_MS)
        cap = int(settings.OUTBOX_BACKOFF_MAX_MS)
        ms = base * (2 ** max(0, attempts - 1))
        return int(min(cap, ms))

    def mark_failed(self, ev: models.OutboxEvent, err: str, write_op: bool = False) -> None:
        ev.attempts = int(ev.attempts) + 1
        ev.last_error = (err or "")[:500]

        if write_op:
            ev.status = "DEAD"
            ev.available_at = now_shanghai()
            return

        if int(ev.attempts) >= int(settings.OUTBOX_MAX_ATTEMPTS):
            ev.status = "DEAD"
            ev.available_at = now_shanghai()
            return

        wait_ms = self._backoff_ms(int(ev.attempts))
        ev.status = "PENDING"
        ev.available_at = now_shanghai() + timedelta(milliseconds=wait_ms)


@dataclass
class OrderAnchorRepo:
    s: Session

    def upsert_anchor(
        self,
        cid: str,
        account_id: str,
        client_order_id: str,
        broker_order_id: str | None,
        request_uuid: str,
        ack_hash: str,
        raw_request_hash: str,
        raw_response_hash: str,
    ) -> None:
        row = self.s.get(models.OrderAnchor, cid)
        if row is None:
            row = models.OrderAnchor(
                cid=cid,
                account_id=account_id,
                client_order_id=client_order_id,
                broker_order_id=broker_order_id,
                request_uuid=request_uuid,
                ack_hash=ack_hash,
                raw_request_hash=raw_request_hash,
                raw_response_hash=raw_response_hash,
                created_at=now_shanghai(),
            )
            self.s.add(row)
        else:
            row.broker_order_id = broker_order_id
            row.ack_hash = ack_hash
            row.raw_request_hash = raw_request_hash
            row.raw_response_hash = raw_response_hash


@dataclass
class FrozenVersionsRepo:
    s: Session

    def ensure_today_frozen(
        self,
        rule_set_version_hash: str,
        strategy_contract_hash: str,
        model_snapshot_uuid: str,
        cost_model_version: str,
        canonicalization_version: str,
        feature_extractor_version: str,
    ) -> None:
        td = trading_day_str(now_shanghai())
        row = self.s.get(models.DailyFrozenVersions, td)
        report = {
            "trading_day": td,
            "rule_set_version_hash": rule_set_version_hash,
            "strategy_contract_hash": strategy_contract_hash,
            "model_snapshot_uuid": model_snapshot_uuid,
            "cost_model_version": cost_model_version,
            "canonicalization_version": canonicalization_version,
            "feature_extractor_version": feature_extractor_version,
        }
        report_hash = sha256_hex(str(report).encode("utf-8"))

        if row is None:
            self.s.add(
                models.DailyFrozenVersions(
                    trading_day=td,
                    rule_set_version_hash=rule_set_version_hash,
                    strategy_contract_hash=strategy_contract_hash,
                    model_snapshot_uuid=model_snapshot_uuid,
                    cost_model_version=cost_model_version,
                    canonicalization_version=canonicalization_version,
                    feature_extractor_version=feature_extractor_version,
                    report_hash=report_hash,
                    created_at=now_shanghai(),
                )
            )
        else:
            if row.report_hash != report_hash:
                raise ValueError("daily_frozen_versions_mismatch")


@dataclass
class StrategyContractsRepo:
    s: Session

    def ensure_seeded(self, strategy_contract_hash: str) -> None:
        row = self.s.get(models.StrategyContract, strategy_contract_hash)
        if row is not None:
            return

        definition = {
            "version": 1,
            "objective": {
                "hold_days_min": int(settings.HOLD_DAYS_MIN),
                "hold_days_max": int(settings.HOLD_DAYS_MAX),
                "target_return_min": float(settings.TARGET_RETURN_MIN),
                "target_return_max": float(settings.TARGET_RETURN_MAX),
            },
            "exit_policy": {
                "hold_days_min": int(settings.HOLD_DAYS_MIN),
                "hold_days_max": int(settings.HOLD_DAYS_MAX),
                "tp_min": float(settings.TARGET_RETURN_MIN),
                "tp_max": float(settings.TARGET_RETURN_MAX),
                "sl_pct": 0.03,
            },
        }
        self.s.add(
            models.StrategyContract(
                strategy_contract_hash=strategy_contract_hash,
                definition=definition,
                created_at=now_shanghai(),
            )
        )
        self.s.flush()

    def get_definition(self, strategy_contract_hash: str) -> dict:
        row = self.s.get(models.StrategyContract, strategy_contract_hash)
        if row is None:
            self.ensure_seeded(strategy_contract_hash)
            row = self.s.get(models.StrategyContract, strategy_contract_hash)
        return dict(row.definition or {}) if row is not None else {}


@dataclass
class DataRequestsRepo:
    s: Session

    def _norm_symbol(self, symbol: str | None) -> str | None:
        # Canonicalize and ensure non-empty.
        sym = normalize_symbol(symbol)
        return sym if sym else None

    def _try_parse_json_obj(self, v: Any) -> Any:
        if isinstance(v, (dict, list)):
            return v
        if not isinstance(v, str):
            return None
        s = v.strip()
        if not s:
            return None
        if not (s.startswith("{") or s.startswith("[")):
            return None
        try:
            return json.loads(s)
        except Exception:
            return None

    def _extract_symbol_from_obj(self, obj: Any) -> str | None:
        if not isinstance(obj, dict):
            return None

        # direct keys (provider/raw conventions)
        for k in ("symbol", "thscode", "ths_code", "ts_code", "code", "ticker"):
            if k in obj:
                v = self._norm_symbol(str(obj.get(k) or ""))
                if v:
                    return v

        # planner v1: payload uses "codes": "<sym>"
        if "codes" in obj:
            v = self._norm_symbol(str(obj.get("codes") or ""))
            if v:
                return v

        # common nested patterns
        for nest_key in ("params", "data", "request", "payload", "result"):
            nest = obj.get(nest_key)
            v = self._extract_symbol_from_obj(nest)
            if v:
                return v

        return None

    def _extract_symbol_from_planned(self, pr: Any) -> str | None:
        # 1) direct field
        v = self._norm_symbol(getattr(pr, "symbol", None))
        if v:
            return v

        # 2) payload dict (most reliable for provider calls)
        payload = getattr(pr, "payload", None)
        v = self._extract_symbol_from_obj(payload)
        if v:
            return v

        # 3) params_canonical may be dict/json string (some planners embed json)
        pc = getattr(pr, "params_canonical", None)
        pc_obj = self._try_parse_json_obj(pc)
        v = self._extract_symbol_from_obj(pc_obj)
        if v:
            return v

        return None

    def enqueue(
        self,
        *,
        dedupe_key: str,
        correlation_id: str | None,
        account_id: str | None,
        symbol: str | None,
        purpose: str,
        provider: str,
        endpoint: str,
        params_canonical: str,
        request_payload: dict,
        deadline_sec: int | None = None,
    ) -> str:
        exists = self.s.execute(select(models.DataRequest).where(models.DataRequest.dedupe_key == dedupe_key)).scalar_one_or_none()
        if exists is not None:
            return str(exists.request_id)

        now = now_shanghai()
        rid = sha256_hex(f"{dedupe_key}|{now.isoformat()}".encode("utf-8"))[:32]

        self.s.add(
            models.DataRequest(
                request_id=rid,
                dedupe_key=dedupe_key,
                correlation_id=correlation_id,
                account_id=account_id,
                symbol=self._norm_symbol(symbol),
                purpose=purpose,
                provider=provider,
                endpoint=endpoint,
                params_canonical=params_canonical,
                request_payload=request_payload,
                status="PENDING",
                attempts=0,
                last_error=None,
                created_at=now,
                sent_at=None,
                deadline_at=(now + timedelta(seconds=int(deadline_sec))) if deadline_sec else None,
                response_id=None,
            )
        )
        return rid

    def enqueue_planned(self, pr: Any, provider: str) -> tuple[str, bool]:
        """
        Compatibility helper: accept PlannedRequest (labeling_planner.PlannedRequest) and enqueue once.
        Returns (request_id, created_bool).

        IMPORTANT:
        - Do NOT allow symbol="" to silently enter DB.
        - Try extracting symbol from multiple planned-request fields.
        - If still missing, write a FAILED DataRequest (non-PENDING) to avoid polluting pipeline.
        """
        dedupe_key = str(getattr(pr, "dedupe_key"))
        existing = self.s.execute(select(models.DataRequest).where(models.DataRequest.dedupe_key == dedupe_key)).scalar_one_or_none()
        if existing is not None:
            return str(existing.request_id), False

        symbol = self._extract_symbol_from_planned(pr)

        # If still missing, record a FAILED request (observable), but do not enqueue into PENDING pipeline.
        if not symbol:
            now = now_shanghai()
            rid = sha256_hex(f"{dedupe_key}|{now.isoformat()}".encode("utf-8"))[:32]
            self.s.add(
                models.DataRequest(
                    request_id=rid,
                    dedupe_key=dedupe_key,
                    correlation_id=getattr(pr, "correlation_id", None),
                    account_id=None,
                    symbol=None,
                    purpose=str(getattr(pr, "purpose", "")),
                    provider=provider,
                    endpoint=str(getattr(pr, "endpoint")),
                    params_canonical=str(getattr(pr, "params_canonical", "")),
                    request_payload=getattr(pr, "payload", {}) or {},
                    status="FAILED",
                    attempts=0,
                    last_error="symbol_missing_in_planned_request",
                    created_at=now,
                    sent_at=None,
                    deadline_at=(now + timedelta(seconds=int(getattr(pr, "deadline_sec", 0) or 0)))
                    if int(getattr(pr, "deadline_sec", 0) or 0)
                    else None,
                    response_id=None,
                )
            )
            return rid, True

        rid = self.enqueue(
            dedupe_key=dedupe_key,
            correlation_id=getattr(pr, "correlation_id", None),
            account_id=None,
            symbol=symbol,
            purpose=str(getattr(pr, "purpose", "")),
            provider=provider,
            endpoint=str(getattr(pr, "endpoint")),
            params_canonical=str(getattr(pr, "params_canonical", "")),
            request_payload=getattr(pr, "payload", {}) or {},
            deadline_sec=int(getattr(pr, "deadline_sec", 0) or 0) or None,
        )
        return rid, True

    def fetch_pending(self, limit: int = 50) -> list[models.DataRequest]:
        return (
            self.s.execute(
                select(models.DataRequest)
                .where(models.DataRequest.status == "PENDING")
                .order_by(models.DataRequest.created_at.asc())
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            .scalars()
            .all()
        )

    def mark_sent(self, req: models.DataRequest) -> None:
        """
        IMPORTANT FIX:
        deadline_at was previously computed at enqueue-time (created_at + deadline_sec).
        If the queue/poll delays dispatch, deadline_at can end up < sent_at, which is nonsense.
        We preserve the intended TTL (deadline_at - created_at) and rebase it at sent_at when needed.
        """
        now = now_shanghai()
        req.status = "SENT"
        req.sent_at = now
        req.attempts = int(req.attempts) + 1

        if req.deadline_at is None:
            return

        try:
            dl = to_shanghai(req.deadline_at)
        except Exception:
            dl = None

        if dl is None:
            return

        if dl <= now:
            # Rebase TTL (best-effort). If created_at missing/bad, fallback to 1 second.
            ttl_sec = 1
            try:
                ca = to_shanghai(req.created_at) if req.created_at is not None else None
                if ca is not None:
                    ttl = (dl - ca).total_seconds()
                    ttl_sec = max(1, int(round(ttl)))
            except Exception:
                ttl_sec = 1

            req.deadline_at = now + timedelta(seconds=ttl_sec)

    def mark_failed(self, req: models.DataRequest, err: str) -> None:
        req.status = "FAILED"
        req.last_error = (err or "")[:500]

    def attach_response(self, req: models.DataRequest, resp: models.DataResponse) -> None:
        req.status = "RECEIVED"
        req.response_id = resp.response_id


@dataclass
class DataResponsesRepo:
    s: Session

    def write(
        self,
        *,
        request_id: str,
        provider: str,
        endpoint: str,
        http_status: int | None,
        errorcode: str,
        errmsg: str,
        quota_context: str,
        raw: dict,
        payload_sha256: str,
        data_ts=None,
    ) -> str:
        resp_id = sha256_hex(f"RESP|{request_id}|{payload_sha256}".encode("utf-8"))[:32]
        row = self.s.get(models.DataResponse, resp_id)
        if row is not None:
            return resp_id

        self.s.add(
            models.DataResponse(
                response_id=resp_id,
                request_id=request_id,
                provider=provider,
                endpoint=endpoint,
                http_status=http_status,
                errorcode=str(errorcode or "0"),
                errmsg=str(errmsg or ""),
                quota_context=str(quota_context or ""),
                raw=raw,
                payload_sha256=payload_sha256,
                received_at=now_shanghai(),
                data_ts=data_ts,
            )
        )
        return resp_id


@dataclass
class ValidationsRepo:
    s: Session

    def write(
        self,
        *,
        decision_id: str,
        symbol: str,
        hypothesis: str,
        request_ids: list[str],
        evidence: dict,
        conclusion: str,
        score: float,
    ) -> str:
        material = f"{decision_id}|{symbol}|{hypothesis}|{request_ids}|{conclusion}|{score}"
        vid = sha256_hex(material.encode("utf-8"))[:64]
        row = self.s.get(models.ValidationRecord, vid)
        if row is not None:
            return vid

        self.s.add(
            models.ValidationRecord(
                validation_id=vid,
                decision_id=decision_id,
                symbol=symbol,
                hypothesis=hypothesis,
                request_ids=request_ids,
                evidence=evidence,
                conclusion=conclusion,
                score=float(score),
                created_at=now_shanghai(),
            )
        )
        return vid


@dataclass
class LabelingCandidatesRepo:
    s: Session

    def upsert_batch(self, trading_day: str, target_day: str, items: list[dict[str, Any]], source: str = "UI") -> dict[str, int]:
        created = 0
        updated = 0

        for it in items:
            symbol = normalize_symbol(str(it.get("symbol") or "").strip())
            if not symbol:
                continue

            p = it.get("p_limit_up")
            try:
                p = float(p)
            except Exception:
                continue

            if p > 1.0 and p <= 100.0:
                p = p / 100.0
            p = max(0.0, min(1.0, p))

            name = str(it.get("name") or "").strip()
            extra = dict(it.get("extra") or {})
            for k, v in it.items():
                if k in {"symbol", "p_limit_up", "name", "extra"}:
                    continue
                extra[k] = v

            candidate_id = sha256_hex(f"{trading_day}|{symbol}|{source}".encode("utf-8"))[:64]
            row = self.s.get(models.LabelingCandidate, candidate_id)
            if row is None:
                created += 1
                self.s.add(
                    models.LabelingCandidate(
                        candidate_id=candidate_id,
                        trading_day=trading_day,
                        target_day=target_day,
                        symbol=symbol,
                        p_limit_up=p,
                        name=name,
                        source=source,
                        extra=extra,
                        created_at=now_shanghai(),
                        updated_at=now_shanghai(),
                    )
                )
            else:
                updated += 1
                row.trading_day = trading_day
                row.target_day = target_day
                row.symbol = symbol
                row.p_limit_up = p
                row.name = name
                row.source = source
                row.extra = extra
                row.updated_at = now_shanghai()

        return {"created": created, "updated": updated, "total": created + updated}

    def list_by_day(self, trading_day: str) -> list[models.LabelingCandidate]:
        q = (
            select(models.LabelingCandidate)
            .where(models.LabelingCandidate.trading_day == trading_day)
            .order_by(models.LabelingCandidate.p_limit_up.desc(), models.LabelingCandidate.symbol.asc())
        )
        return list(self.s.execute(q).scalars().all())


@dataclass
class WatchlistRepo:
    s: Session

    def upsert_hit(self, *, symbol: str, trading_day: str) -> models.SymbolWatchlist:
        now = now_shanghai()
        symbol = normalize_symbol(symbol)

        row = self.s.get(models.SymbolWatchlist, symbol)
        if row is None:
            row = models.SymbolWatchlist(
                symbol=symbol,
                first_seen_day=trading_day,
                last_seen_day=trading_day,
                hit_count=1,
                active=True,
                planner_state={},
                next_refresh_at=now,
                created_at=now,
                updated_at=now,
            )
            self.s.add(row)
            self.s.flush()
            return row

        row.last_seen_day = trading_day
        row.hit_count = int(row.hit_count or 0) + 1
        row.updated_at = now
        if row.active:
            cur = row.next_refresh_at
            if cur is not None:
                # IMPORTANT: unify tz before comparing (DB may store naive datetime)
                try:
                    cur = to_shanghai(cur)
                except Exception:
                    cur = now
            row.next_refresh_at = min(cur or now, now)
        return row

    # ---- compatibility aliases (older code may call these) ----
    def upsert_seen(self, symbol: str, trading_day: str) -> models.SymbolWatchlist:
        return self.upsert_hit(symbol=symbol, trading_day=trading_day)

    def list_due(self, max_symbols: int = 50) -> list[models.SymbolWatchlist]:
        return self.due_for_refresh(max_symbols=max_symbols)
    # ---------------------------------------------------------

    def set_active(self, symbol: str, active: bool) -> models.SymbolWatchlist:
        now = now_shanghai()
        symbol = normalize_symbol(symbol)
        if not symbol:
            raise ValueError("invalid_symbol")

        row = self.s.get(models.SymbolWatchlist, symbol)
        if row is None:
            td = trading_day_str(now)
            row = models.SymbolWatchlist(
                symbol=symbol,
                first_seen_day=td,
                last_seen_day=td,
                hit_count=0,
                active=bool(active),
                planner_state={},
                next_refresh_at=(now if active else None),
                created_at=now,
                updated_at=now,
            )
            self.s.add(row)
            self.s.flush()
            return row

        row.active = bool(active)
        row.updated_at = now
        row.next_refresh_at = (now if row.active else None)
        return row

    def list(self, limit: int = 200) -> list[models.SymbolWatchlist]:
        return (
            self.s.execute(
                select(models.SymbolWatchlist).order_by(models.SymbolWatchlist.updated_at.desc()).limit(limit)
            )
            .scalars()
            .all()
        )

    def due_for_refresh(self, limit: int = 50, max_symbols: int | None = None) -> list[models.SymbolWatchlist]:
        """
        Return active watchlist rows due for refresh.

        Compatibility: labeling_pipeline may call due_for_refresh(max_symbols=...).
        """
        if max_symbols is not None:
            limit = int(max_symbols)

        now = now_shanghai()
        return (
            self.s.execute(
                select(models.SymbolWatchlist)
                .where(models.SymbolWatchlist.active == True)  # noqa: E712
                .where(models.SymbolWatchlist.next_refresh_at.is_not(None))
                .where(models.SymbolWatchlist.next_refresh_at <= now)
                .order_by(models.SymbolWatchlist.next_refresh_at.asc())
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            .scalars()
            .all()
        )

    def set_next_refresh_in(self, symbol: str, seconds: int) -> None:
        now = now_shanghai()
        symbol = normalize_symbol(symbol)
        row = self.s.get(models.SymbolWatchlist, symbol)
        if row is None:
            return
        if not row.active:
            row.next_refresh_at = None
        else:
            row.next_refresh_at = now + timedelta(seconds=int(seconds))
        row.updated_at = now


@dataclass
class FeatureSnapshotsRepo:
    s: Session

    def write(
        self,
        *,
        symbol: str,
        feature_set: str,
        asof_ts,
        features: dict,
        request_ids: list[str],
        planner_version: str = "planner_v1",
    ) -> str:
        material = f"{symbol}|{feature_set}|{asof_ts.isoformat()}|{sha256_hex(str(features).encode('utf-8'))}"
        sid = sha256_hex(material.encode("utf-8"))[:64]
        row = self.s.get(models.SymbolFeatureSnapshot, sid)
        if row is not None:
            return sid

        self.s.add(
            models.SymbolFeatureSnapshot(
                snapshot_id=sid,
                symbol=symbol,
                feature_set=str(feature_set or "AUTO"),
                asof_ts=asof_ts,
                request_ids=list(request_ids or []),
                planner_version=str(planner_version or "planner_v1"),
                features=features or {},
                created_at=now_shanghai(),
            )
        )
        return sid

    def list_by_symbol(self, symbol: str, limit: int = 50) -> list[models.SymbolFeatureSnapshot]:
        return (
            self.s.execute(
                select(models.SymbolFeatureSnapshot)
                .where(models.SymbolFeatureSnapshot.symbol == symbol)
                .order_by(models.SymbolFeatureSnapshot.asof_ts.desc())
                .limit(limit)
            )
            .scalars()
            .all()
        )

    def latest_by_symbol(self, symbol: str) -> models.SymbolFeatureSnapshot | None:
        return (
            self.s.execute(
                select(models.SymbolFeatureSnapshot)
                .where(models.SymbolFeatureSnapshot.symbol == symbol)
                .order_by(models.SymbolFeatureSnapshot.asof_ts.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )


@dataclass
class Repo:
    s: Session

    @property
    def system_events(self) -> SystemEventsRepo:
        return SystemEventsRepo(self.s)

    @property
    def system_status(self) -> SystemStatusRepo:
        return SystemStatusRepo(self.s)

    @property
    def controls(self) -> ControlsRepo:
        return ControlsRepo(self.s)

    @property
    def accounts(self) -> AccountsRepo:
        return AccountsRepo(self.s)

    @property
    def labeling_candidates(self) -> LabelingCandidatesRepo:
        return LabelingCandidatesRepo(self.s)

    @property
    def watchlist(self) -> WatchlistRepo:
        return WatchlistRepo(self.s)

    @property
    def feature_snapshots(self) -> FeatureSnapshotsRepo:
        return FeatureSnapshotsRepo(self.s)

    @property
    def nonce(self) -> NonceRepo:
        return NonceRepo(self.s)

    @property
    def symbol_lock(self) -> SymbolLockRepo:
        return SymbolLockRepo(self.s)

    @property
    def outbox(self) -> OutboxRepo:
        return OutboxRepo(self.s)

    @property
    def anchors(self) -> OrderAnchorRepo:
        return OrderAnchorRepo(self.s)

    @property
    def strategy_contracts(self) -> StrategyContractsRepo:
        return StrategyContractsRepo(self.s)

    @property
    def frozen_versions(self) -> FrozenVersionsRepo:
        return FrozenVersionsRepo(self.s)

    @property
    def data_requests(self) -> DataRequestsRepo:
        return DataRequestsRepo(self.s)

    @property
    def data_responses(self) -> DataResponsesRepo:
        return DataResponsesRepo(self.s)

    @property
    def validations(self) -> ValidationsRepo:
        return ValidationsRepo(self.s)
