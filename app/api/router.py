from __future__ import annotations

from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Body
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database.engine import SessionLocal
from app.database.repo import Repo
from app.database import models
from app.utils.time import now_shanghai_str, now_shanghai, to_shanghai, trading_day_str
from app.utils.crypto import sha256_hex
from app.config import settings
from app.core.labeling_planner import build_plan


router = APIRouter()


def _parse_ui_day(day: str | None) -> tuple[str | None, datetime | None, datetime | None]:
    """Accepts YYYY-MM-DD or YYYYMMDD. Returns (trading_day_YYYYMMDD, start_dt, end_dt) in Asia/Shanghai."""
    if not day:
        return None, None, None
    d = day.strip()
    if not d:
        return None, None, None
    if len(d) == 8 and d.isdigit():
        td = d
        dt = datetime.strptime(td, "%Y%m%d")
    else:
        # allow YYYY-MM-DD
        dt = datetime.strptime(d, "%Y-%m-%d")
        td = dt.strftime("%Y%m%d")
    start = to_shanghai(dt).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return td, start, end




@router.get("/health")
def health() -> dict:
    return {"ok": True, "time": now_shanghai_str()}


@router.get("/status")
def status() -> dict:
    with SessionLocal() as s:
        repo = Repo(s)
        st = repo.system_status.get_for_update()
        s.commit()
        return {
            "time": now_shanghai_str(),
            "panic_halt": bool(st.panic_halt),
            "guard_level": int(st.guard_level),
            "veto": bool(st.veto),
            "veto_code": st.veto_code,
            "last_self_check_report_hash": st.last_self_check_report_hash,
            "last_self_check_time": st.last_self_check_time.isoformat() if st.last_self_check_time else None,
        }


@router.post("/admin/self_check")
def run_self_check() -> dict:
    """
    Minimal self-check report, required by trade gate in this skeleton.
    """
    with SessionLocal() as s:
        repo = Repo(s)
        st = repo.system_status.get_for_update()

        report = {
            "time": now_shanghai_str(),
            "versions": {
                "RuleSetVersionHash": settings.RULESET_VERSION_HASH,
                "StrategyContractHash": settings.STRATEGY_CONTRACT_HASH,
                "ModelSnapshotUUID": settings.MODEL_SNAPSHOT_UUID,
                "CostModelVersion": settings.COST_MODEL_VERSION,
                "CanonicalizationVersion": settings.CANONICALIZATION_VERSION,
                "FeatureExtractorVersion": settings.FEATURE_EXTRACTOR_VERSION,
            },
            "note": "minimal self-check: schema+versions+connectivity assumed OK",
        }
        report_hash = sha256_hex(str(report).encode("utf-8"))
        repo.system_status.set_self_check(report_hash)
        repo.system_events.write_event(
            event_type="SELF_CHECK_REPORT",
            correlation_id=None,
            severity="INFO",
            payload={"report": report, "report_hash": report_hash},
        )
        s.commit()
        return {"ok": True, "report_hash": report_hash, "report": report}


@router.get("/decisions")
def list_decisions(limit: int = 50, day: str | None = None) -> list[dict]:
    td, start, end = _parse_ui_day(day)
    with SessionLocal() as s:
        q = select(models.DecisionBundle).order_by(models.DecisionBundle.created_at.desc())
        if start and end:
            q = q.where(models.DecisionBundle.created_at >= start, models.DecisionBundle.created_at < end)
        rows = s.execute(q.limit(limit)).scalars().all()
        return [
            {
                "decision_id": r.decision_id,
                "cid": r.cid,
                "account_id": r.account_id,
                "symbol": r.symbol,
                "decision": r.decision,
                "confidence": float((r.params or {}).get("confidence", 0.0)) if isinstance(r.params, dict) else None,
                "reason_code": r.reason_code,
                "params": r.params,
                "request_ids": r.request_ids,
                "model_hash": r.model_hash,
                "feature_hash": r.feature_hash,
                "guard_status": r.guard_status,
                "data_quality": r.data_quality,
                "created_at": r.created_at.isoformat(),
            }
            for r in rows
        ]


@router.get("/data_requests")
def list_data_requests(status: str | None = None, limit: int = 50) -> list[dict]:
    with SessionLocal() as s:
        q = select(models.DataRequest).order_by(models.DataRequest.created_at.desc())
        if status:
            q = q.where(models.DataRequest.status == status)
        rows = s.execute(q.limit(limit)).scalars().all()
        return [
            {
                "request_id": r.request_id,
                "dedupe_key": r.dedupe_key,
                "correlation_id": r.correlation_id,
                "account_id": r.account_id,
                "symbol": r.symbol,
                "purpose": r.purpose,
                "provider": r.provider,
                "endpoint": r.endpoint,
                "status": r.status,
                "attempts": r.attempts,
                "created_at": r.created_at.isoformat(),
                "sent_at": r.sent_at.isoformat() if r.sent_at else None,
                "deadline_at": r.deadline_at.isoformat() if r.deadline_at else None,
                "response_id": r.response_id,
                "last_error": r.last_error,
            }
            for r in rows
        ]


@router.get("/data_responses/{response_id}")
def get_data_response(response_id: str) -> dict:
    with SessionLocal() as s:
        r = s.get(models.DataResponse, response_id)
        if r is None:
            raise HTTPException(status_code=404, detail="not_found")
        return {
            "response_id": r.response_id,
            "request_id": r.request_id,
            "provider": r.provider,
            "endpoint": r.endpoint,
            "http_status": r.http_status,
            "errorcode": r.errorcode,
            "errmsg": r.errmsg,
            "quota_context": r.quota_context,
            "payload_sha256": r.payload_sha256,
            "received_at": r.received_at.isoformat(),
            "raw": r.raw,
        }



@router.get("/ui/signal_inputs")
def ui_signal_inputs(day: str) -> list[dict]:
    """List daily '待打标' candidates (preferred), fallback to internal Signals if none exist."""
    td, start, end = _parse_ui_day(day)
    if not td:
        raise HTTPException(status_code=400, detail="day is required (YYYY-MM-DD or YYYYMMDD)")

    with SessionLocal() as s:
        repo = Repo(s)

        # Preferred path: operator-supplied candidates (front-end填报/上传)
        cands = repo.labeling_candidates.list_by_day(td)
        if cands:
            trading_day_fmt = datetime.strptime(td, "%Y%m%d").strftime("%Y-%m-%d")
            target_td = cands[0].target_day or (datetime.strptime(td, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
            target_day_fmt = datetime.strptime(target_td, "%Y%m%d").strftime("%Y-%m-%d")

            out: list[dict] = []
            for i, r in enumerate(cands, start=1):
                out.append(
                    {
                        "id": r.candidate_id,
                        "trading_day": trading_day_fmt,
                        "target_day": target_day_fmt,
                        "symbol": r.symbol,
                        "name": r.name,
                        "input_ts": r.updated_at.isoformat(),
                        "p_limit_up": float(r.p_limit_up),
                        "rank": i,
                        "source": r.source,
                        "extra": r.extra,
                    }
                )
            return out

        # Fallback: internal Signals pool (legacy behavior / dev mode)
        sigs = (
            s.execute(
                select(models.Signal)
                .where(models.Signal.trading_day == td)
                .order_by(models.Signal.confidence.desc())
            )
            .scalars()
            .all()
        )

        symbols = [x.symbol for x in sigs]
        latest_dec_by_sym: dict[str, models.DecisionBundle] = {}
        if symbols and start and end:
            decs = (
                s.execute(
                    select(models.DecisionBundle)
                    .where(models.DecisionBundle.created_at >= start, models.DecisionBundle.created_at < end)
                    .where(models.DecisionBundle.symbol.in_(symbols))
                    .order_by(models.DecisionBundle.created_at.desc())
                )
                .scalars()
                .all()
            )
            for drow in decs:
                if drow.symbol not in latest_dec_by_sym:
                    latest_dec_by_sym[drow.symbol] = drow

        target_day = (datetime.strptime(td, "%Y%m%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        trading_day_fmt = datetime.strptime(td, "%Y%m%d").strftime("%Y-%m-%d")

        out: list[dict] = []
        for i, r in enumerate(sigs, start=1):
            dec = latest_dec_by_sym.get(r.symbol)
            params = dec.params if (dec and isinstance(dec.params, dict)) else {}
            out.append(
                {
                    "id": r.cid,
                    "trading_day": trading_day_fmt,
                    "target_day": target_day,
                    "symbol": r.symbol,
                    "strategy_id": r.strategy_id,
                    "input_ts": r.signal_ts.isoformat(),
                    # Proxy: p_limit_up currently equals model confidence (0..1)
                    "p_limit_up": float(r.confidence),
                    "rank": i,
                    "reason_code": (dec.reason_code if dec else ""),
                    "top_features": params.get("top_features"),
                    "features_snapshot": params.get("features_snapshot") or params.get("evidence") or params,
                }
            )
        return out


@router.post("/ui/signal_inputs")
def upsert_ui_signal_inputs(payload: dict = Body(...)) -> dict:
    """Upsert daily candidates for labeling (待打标).

    Expected payload:
    {
      "day": "YYYY-MM-DD" | "YYYYMMDD",
      "items": [{"symbol": "...", "p_limit_up": 0.23, "name": "...", ...}, ...]
    }
    """
    day = str(payload.get("day") or payload.get("trading_day") or "").strip()
    if not day:
        day = now_shanghai().strftime("%Y-%m-%d")

    td, _, _ = _parse_ui_day(day)
    if not td:
        raise HTTPException(status_code=400, detail="Invalid day. Use YYYY-MM-DD or YYYYMMDD.")

    items = payload.get("items")
    if not isinstance(items, list):
        raise HTTPException(status_code=400, detail="items must be a list")

    target_td = (datetime.strptime(td, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")

    with SessionLocal() as s:
        repo = Repo(s)
        res = repo.labeling_candidates.upsert_batch(trading_day=td, target_day=target_td, items=items, source="UI")

        # Update watchlist (symbols may appear repeatedly across days).
        # When LABELING_AUTO_FETCH_ENABLED is on, enqueue a model-driven (planner) data fetch plan.
        planned = 0
        for it in items:
            symbol = str((it or {}).get("symbol") or "").strip()
            if not symbol:
                continue
            wl = repo.watchlist.upsert_hit(symbol=symbol, trading_day=td)
            if settings.LABELING_AUTO_FETCH_ENABLED and wl.active:
                plan = build_plan(symbol=symbol, hit_count=int(wl.hit_count), planner_state=dict(wl.planner_state or {}))
                # Persist planner state back
                wl.planner_state = plan.planner_state
                for pr in plan.requests:
                    repo.data_requests.enqueue(
                        dedupe_key=pr.dedupe_key,
                        correlation_id=pr.correlation_id,
                        account_id=None,
                        symbol=symbol,
                        purpose=pr.purpose,
                        provider=settings.DATA_PROVIDER,
                        endpoint=pr.endpoint,
                        params_canonical=pr.params_canonical,
                        request_payload=pr.payload,
                        deadline_sec=pr.deadline_sec,
                    )
                    planned += 1

        s.commit()
        res["planned_requests"] = planned

    return {
        "trading_day": datetime.strptime(td, "%Y%m%d").strftime("%Y-%m-%d"),
        "target_day": datetime.strptime(target_td, "%Y%m%d").strftime("%Y-%m-%d"),
        **res,
    }

@router.get("/ui/controls")
def ui_controls() -> dict:
    with SessionLocal() as s:
        repo = Repo(s)
        st = repo.system_status.get_for_update()
        c = repo.controls.get_for_update()

        self_check_valid_until = None
        if st.last_self_check_time:
            self_check_valid_until = (st.last_self_check_time + timedelta(seconds=int(settings.SELF_CHECK_MAX_AGE_SEC))).isoformat()

        # best-effort: "data_degraded" not fully modeled yet (defaults to False)
        resp = {
            # writable
            "auto_trading_enabled": bool(c.auto_trading_enabled),
            "dry_run": bool(c.dry_run),
            "only_when_data_ok": bool(c.only_when_data_ok),
            "max_orders_per_day": int(c.max_orders_per_day),
            "max_notional_per_order": int(c.max_notional_per_order),
            "allowed_symbols": list(c.allowed_symbols or []),
            "blocked_symbols": list(c.blocked_symbols or []),

            # readonly status / governance
            "panic_halt": bool(st.panic_halt),
            "guard_level": int(st.guard_level),
            "veto": bool(st.veto),
            "veto_code": st.veto_code,
            "data_degraded": False,
            "self_check_valid_until": self_check_valid_until,
            "ths_mode": settings.THS_MODE,
            "time": now_shanghai_str(),
        }
        s.commit()
        return resp


@router.patch("/ui/controls")
def ui_controls_patch(payload: dict) -> dict:
    with SessionLocal() as s:
        repo = Repo(s)
        updated = repo.controls.patch(payload or {})
        s.commit()
        return updated



@router.get("/ui/watchlist")
def ui_watchlist(limit: int = 200) -> list[dict]:
    with SessionLocal() as s:
        repo = Repo(s)
        rows = repo.watchlist.list(limit=limit)
        s.commit()
        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "symbol": r.symbol,
                    "first_seen_day": r.first_seen_day,
                    "last_seen_day": r.last_seen_day,
                    "hit_count": int(r.hit_count or 0),
                    "active": bool(r.active),
                    "next_refresh_at": r.next_refresh_at.isoformat() if r.next_refresh_at else None,
                    "planner_state": r.planner_state,
                    "updated_at": r.updated_at.isoformat() if r.updated_at else None,
                }
            )
        return out


@router.patch("/ui/watchlist/{symbol}")
def ui_watchlist_patch(symbol: str, payload: dict = Body(...)) -> dict:
    active = payload.get("active")
    if active is None:
        raise HTTPException(status_code=400, detail="active is required")
    with SessionLocal() as s:
        repo = Repo(s)
        row = repo.watchlist.set_active(symbol, bool(active))
        s.commit()
        return {
            "symbol": row.symbol,
            "active": bool(row.active),
            "hit_count": int(row.hit_count or 0),
            "next_refresh_at": row.next_refresh_at.isoformat() if row.next_refresh_at else None,
        }


@router.get("/ui/symbol/{symbol}/snapshots")
def ui_symbol_snapshots(symbol: str, limit: int = 50) -> list[dict]:
    symbol = (symbol or "").strip()
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol required")
    with SessionLocal() as s:
        repo = Repo(s)
        rows = repo.feature_snapshots.list_by_symbol(symbol, limit=limit)
        s.commit()
        return [
            {
                "snapshot_id": r.snapshot_id,
                "symbol": r.symbol,
                "feature_set": r.feature_set,
                "asof_ts": r.asof_ts.isoformat(),
                "planner_version": r.planner_version,
                "request_ids": r.request_ids,
                "features": r.features,
                "created_at": r.created_at.isoformat(),
            }
            for r in rows
        ]


@router.get("/ui/labeling_settings")
def ui_labeling_settings() -> dict:
    # read-only view of pipeline knobs (controlled via env)
    return {
        "auto_fetch_enabled": bool(settings.LABELING_AUTO_FETCH_ENABLED),
        "poll_ms": int(settings.LABELING_PIPELINE_POLL_MS),
        "refresh_base_sec": int(settings.LABELING_REFRESH_BASE_SEC),
        "refresh_active_sec": int(settings.LABELING_REFRESH_ACTIVE_SEC),
        "history_days_base": int(settings.LABELING_HISTORY_DAYS_BASE),
        "history_days_expand": int(settings.LABELING_HISTORY_DAYS_EXPAND),
        "history_days_max": int(settings.LABELING_HISTORY_DAYS_MAX),
        "hf_limit_base": int(settings.LABELING_HF_LIMIT_BASE),
        "max_symbols_per_cycle": int(settings.LABELING_MAX_SYMBOLS_PER_CYCLE),
    }


@router.get("/accounts")
def list_accounts() -> list[dict]:
    with SessionLocal() as s:
        repo = Repo(s)
        repo.accounts.ensure_accounts_seeded()
        rows = repo.accounts.list_accounts()
        s.commit()
        return [{"account_id": r.account_id, "broker_type": r.broker_type, "created_at": r.created_at.isoformat()} for r in rows]
