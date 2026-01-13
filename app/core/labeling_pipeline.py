from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import select

from app.config import settings
from app.core.data_dispatcher import DataRequestDispatcher
from app.core.labeling_planner import build_plan, calc_refresh_sec
from app.database.engine import SessionLocal
from app.database.repo import Repo
from app.database import models
from app.utils.time import now_shanghai, now_shanghai_str


def _extract_ifind_table(raw: dict) -> dict:
    """Best-effort extract of iFinD 'table' payload.

    Our MockDataProvider returns:
      {"tables": [{"table": {...}}]}

    Real iFinD responses may differ; we keep this defensive.
    """
    if not isinstance(raw, dict):
        return {}
    tables = raw.get("tables")
    if isinstance(tables, list) and tables:
        t0 = tables[0]
        if isinstance(t0, dict):
            table = t0.get("table")
            if isinstance(table, dict):
                return table
    data = raw.get("data")
    if isinstance(data, dict):
        return data
    result = raw.get("result")
    if isinstance(result, dict):
        return result
    return {}


def _safe_first(v, default=None):
    if isinstance(v, list) and v:
        return v[0]
    return v if v is not None else default


def _compute_features_from_responses(symbol: str, rows: list[tuple[models.DataRequest, models.DataResponse]], hit_count: int) -> dict:
    """Build a compact, stable feature dict from the latest responses.

    This is intentionally conservative: it stores derived numeric summaries + basic metadata.
    As you iterate, this can be upgraded into a richer 'model-driven' planner/extractor.
    """
    feat: dict = {
        "symbol": symbol,
        "time": now_shanghai_str(),
        "hit_count": int(hit_count),
        "sources": {},
        "rt": {},
        "hist": {},
        "hf": {},
    }

    for req, resp in rows:
        ep = str(req.endpoint or "")
        raw = resp.raw or {}
        tbl = _extract_ifind_table(raw)
        feat["sources"][ep] = {
            "request_id": req.request_id,
            "response_id": resp.response_id,
            "http_status": resp.http_status,
            "errorcode": resp.errorcode,
            "errmsg": resp.errmsg,
            "quota_context": resp.quota_context,
            "received_at": resp.received_at.isoformat() if resp.received_at else None,
            "payload_sha256": resp.payload_sha256,
        }

        if ep in {"real_time_quotation", "realtime_quotation", "rtq"}:
            feat["rt"] = {
                "latest": _safe_first(tbl.get("latest")),
                "open": _safe_first(tbl.get("open")),
                "high": _safe_first(tbl.get("high")),
                "low": _safe_first(tbl.get("low")),
                "close": _safe_first(tbl.get("close")),
                "volume": _safe_first(tbl.get("volume")),
                "amount": _safe_first(tbl.get("amount")),
                "time": _safe_first(tbl.get("time")),
            }

        elif ep in {"cmd_history_quotation", "history_quotation", "hq"}:
            closes = tbl.get("close") if isinstance(tbl.get("close"), list) else []
            vols = tbl.get("volume") if isinstance(tbl.get("volume"), list) else []
            last_close = closes[-1] if closes else None
            ret_5d = None
            if len(closes) >= 6 and closes[-6] not in (0, None):
                try:
                    ret_5d = float(closes[-1]) / float(closes[-6]) - 1.0
                except Exception:
                    ret_5d = None
            vol_mean = None
            if vols:
                try:
                    vol_mean = sum(float(x) for x in vols if x is not None) / max(1, len([x for x in vols if x is not None]))
                except Exception:
                    vol_mean = None
            feat["hist"] = {
                "n": len(closes),
                "last_close": last_close,
                "ret_5d": ret_5d,
                "vol_mean": vol_mean,
            }

        elif ep in {"high_frequency", "hf"}:
            feat["hf"] = {
                "latest": _safe_first(tbl.get("latest")),
                "close": _safe_first(tbl.get("close")),
            }

    return feat


@dataclass
class PipelineStats:
    loop_count: int = 0
    last_loop_at: str | None = None
    last_dispatch_sent: int = 0
    last_dispatch_received: int = 0
    last_dispatch_failed: int = 0


class LabelingPipeline:
    """Background pipeline for auto-fetch + continuous feature snapshots.

    - reads watchlist due symbols
    - plans/enqueues DataRequests
    - pumps pending requests through DataRequestDispatcher
    - writes SymbolFeatureSnapshot when new data arrives

    Designed to be safe under retries/deduplication.
    """

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._t: threading.Thread | None = None
        self._dispatcher = DataRequestDispatcher()
        self.stats = PipelineStats()

    def start(self) -> None:
        if self._t and self._t.is_alive():
            return
        self._stop.clear()
        self._t = threading.Thread(target=self._run, name="labeling-pipeline", daemon=True)
        self._t.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        poll_s = max(0.2, float(settings.LABELING_PIPELINE_POLL_MS) / 1000.0)
        while not self._stop.is_set():
            t0 = time.time()
            try:
                self._tick_once()
            except Exception:
                # Keep the API alive even if pipeline errors.
                pass
            dt = time.time() - t0
            sleep_for = max(0.0, poll_s - dt)
            if sleep_for:
                time.sleep(sleep_for)

    def _tick_once(self) -> None:
        if not settings.LABELING_AUTO_FETCH_ENABLED:
            return

        self.stats.loop_count += 1
        self.stats.last_loop_at = now_shanghai_str()

        with SessionLocal() as s:
            repo = Repo(s)

            # 1) Plan & enqueue for due symbols
            due = repo.watchlist.due_for_refresh(limit=int(settings.LABELING_MAX_SYMBOLS_PER_CYCLE))
            planned_symbols: set[str] = set()

            for row in due:
                symbol = str(row.symbol)
                planned_symbols.add(symbol)
                plan = build_plan(symbol=symbol, hit_count=int(row.hit_count or 0), planner_state=dict(row.planner_state or {}))

                for pr in plan.requests:
                    repo.data_requests.enqueue(
                        dedupe_key=pr.dedupe_key,
                        correlation_id=None,
                        account_id=None,
                        symbol=symbol,
                        purpose=pr.purpose,
                        provider=settings.DATA_PROVIDER,
                        endpoint=pr.endpoint,
                        params_canonical=pr.params_canonical,
                        request_payload=pr.payload,
                        deadline_sec=pr.deadline_sec,
                    )

                # schedule next refresh
                repo.watchlist.set_next_refresh_in(symbol, calc_refresh_sec(int(row.hit_count or 0)))

                # persist planner state marker
                st = dict(row.planner_state or {})
                st["planner_version"] = plan.planner_version
                st["stage"] = plan.stage
                st["last_plan_at"] = now_shanghai().isoformat()
                row.planner_state = st

            # 2) Pump pending requests
            dr = self._dispatcher.pump_once(s, limit=50)
            self.stats.last_dispatch_sent = int(dr.sent)
            self.stats.last_dispatch_received = int(dr.received)
            self.stats.last_dispatch_failed = int(dr.failed)

            # 3) Write feature snapshots for symbols with fresh received data
            # Find received labeling requests in the recent window.
            now = now_shanghai()
            window_start = now - timedelta(seconds=10)
            q = (
                select(models.DataRequest, models.DataResponse)
                .join(models.DataResponse, models.DataResponse.request_id == models.DataRequest.request_id)
                .where(models.DataRequest.status == "RECEIVED")
                .where(models.DataRequest.purpose.like("LABELING%"))
                .where(models.DataResponse.received_at >= window_start)
                .order_by(models.DataResponse.received_at.desc())
                .limit(500)
            )
            pairs = list(s.execute(q).all())

            by_symbol: dict[str, list[tuple[models.DataRequest, models.DataResponse]]] = {}
            for req, resp in pairs:
                sym = str(req.symbol or "")
                if not sym:
                    continue
                by_symbol.setdefault(sym, []).append((req, resp))

            for sym, rows in by_symbol.items():
                w = s.get(models.SymbolWatchlist, sym)
                hit_count = int(w.hit_count or 0) if w else 0

                # Keep only the latest response per endpoint to stabilize features.
                latest_by_ep: dict[str, tuple[models.DataRequest, models.DataResponse]] = {}
                for req, resp in rows:
                    ep = str(req.endpoint or "")
                    if ep not in latest_by_ep:
                        latest_by_ep[ep] = (req, resp)

                rows2 = list(latest_by_ep.values())
                features = _compute_features_from_responses(sym, rows2, hit_count)

                # Snapshot asof_ts is the newest response time
                asof_ts = max((resp.received_at for _req, resp in rows2 if resp.received_at), default=now)

                sid = repo.feature_snapshots.write(
                    symbol=sym,
                    feature_set="AUTO",
                    asof_ts=asof_ts,
                    features=features,
                    request_ids=[req.request_id for req, _resp in rows2],
                    planner_version=(w.planner_state or {}).get("planner_version", "planner_v1") if w else "planner_v1",
                )

                if w is not None:
                    st = dict(w.planner_state or {})
                    st["last_snapshot_id"] = sid
                    st["last_snapshot_at"] = asof_ts.isoformat()
                    w.planner_state = st
                    w.updated_at = now

            s.commit()


_pipeline_singleton: LabelingPipeline | None = None


def get_labeling_pipeline() -> LabelingPipeline:
    global _pipeline_singleton
    if _pipeline_singleton is None:
        _pipeline_singleton = LabelingPipeline()
    return _pipeline_singleton
