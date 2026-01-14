from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.adapters.data_provider import get_data_provider
from app.core.feature_extractor import FeatureExtractor
from app.database import models
from app.database.repo import Repo
from app.utils.symbols import normalize_symbol
from app.utils.time import now_shanghai, to_shanghai


@dataclass
class DispatchResult:
    sent: int
    received: int
    failed: int


class DataRequestDispatcher:
    """
    Execute pending DataRequests via configured provider and persist DataResponses.

    Additionally (labeling path):
    - After receiving responses, aggregate by (correlation_id, symbol) and write feature snapshots.
    """

    def __init__(self) -> None:
        self._provider = get_data_provider()
        self._extractor = FeatureExtractor()

    def _norm_symbol(self, v: Any) -> str | None:
        s = normalize_symbol(str(v or "").strip())
        return s if s else None

    def _is_success_resp(self, resp_row: models.DataResponse | None) -> bool:
        if resp_row is None:
            return False
        # http_status may be None for some providers; treat None as "unknown" (still allow if errorcode==0)
        ok_http = True
        if resp_row.http_status is not None:
            ok_http = 200 <= int(resp_row.http_status) < 300
        return ok_http and str(resp_row.errorcode or "") == "0"

    def _try_extract_symbol_from_raw(self, raw: Any) -> str | None:
        """
        Best-effort extraction from provider raw payload.
        Common shapes:
          - {"tables":[{"thscode":"000785.SZ", ...}], ...}
          - {"data":{"thscode":...}}
        """
        if not isinstance(raw, dict):
            return None

        # direct keys
        for k in ("symbol", "thscode", "ths_code", "ts_code"):
            if k in raw:
                v = self._norm_symbol(raw.get(k))
                if v:
                    return v

        # tables[0].thscode
        tables = raw.get("tables")
        if isinstance(tables, list) and tables:
            t0 = tables[0]
            if isinstance(t0, dict):
                for k in ("thscode", "symbol", "ths_code", "ts_code"):
                    if k in t0:
                        v = self._norm_symbol(t0.get(k))
                        if v:
                            return v

        # nested common keys
        for nest_key in ("data", "payload", "result"):
            nest = raw.get(nest_key)
            v = self._try_extract_symbol_from_raw(nest)
            if v:
                return v

        return None

    def _snapshot_exists(self, repo: Repo, *, symbol: str, request_ids: list[str]) -> bool:
        """
        Best-effort idempotency:
        If we already wrote a snapshot for this symbol with the same request_ids set,
        skip writing again to avoid duplicate spam.
        """
        want = set(request_ids)
        if not want:
            return True

        # Only check a small recent tail (fast enough; this is labeling path only)
        recent = repo.feature_snapshots.list_by_symbol(symbol, limit=20)
        for r in recent:
            if str(r.feature_set or "") != "LABELING_BASE":
                continue
            if str(r.planner_version or "") != "LPv1":
                continue
            have = set(r.request_ids or [])
            if have == want:
                return True
        return False

    def _maybe_write_snapshot(self, repo: Repo, req: models.DataRequest) -> None:
        """
        Aggregate RECEIVED requests for the same (correlation_id, symbol) and write a snapshot.

        Requirements:
          - symbol present
          - correlation_id present
          - at least realtime + history raw exists (success responses only)
        """
        sym = self._norm_symbol(getattr(req, "symbol", None))
        corr = str(getattr(req, "correlation_id", "") or "").strip()
        if not sym or not corr:
            return

        # Join DataRequest + DataResponse by request_id (DataResponse.request_id)
        q = (
            select(models.DataRequest, models.DataResponse)
            .join(models.DataResponse, models.DataResponse.request_id == models.DataRequest.request_id, isouter=True)
            .where(models.DataRequest.correlation_id == corr)
            .where(models.DataRequest.symbol == sym)
            .where(models.DataRequest.status == "RECEIVED")
        )

        rows = list(repo.s.execute(q).all())
        if not rows:
            return

        endpoint_to_raw: dict[str, dict] = {}
        request_ids: list[str] = []
        max_received_at = None

        for dr, resp in rows:
            request_ids.append(str(dr.request_id))

            ep = str(dr.endpoint or "").strip()
            if not ep:
                continue
            if not self._is_success_resp(resp):
                continue
            if resp is None or not isinstance(resp.raw, dict):
                continue

            endpoint_to_raw[ep] = resp.raw

            if resp.received_at is not None:
                ra = to_shanghai(resp.received_at)
                if max_received_at is None or ra > max_received_at:
                    max_received_at = ra

        # Minimal bundle requirement for a meaningful snapshot
        if "real_time_quotation" not in endpoint_to_raw:
            return
        if "cmd_history_quotation" not in endpoint_to_raw:
            return

        # Idempotency: skip if already wrote the same request_ids snapshot
        req_ids_sorted = sorted(set(request_ids))
        if self._snapshot_exists(repo, symbol=sym, request_ids=req_ids_sorted):
            return

        asof_ts = max_received_at or now_shanghai()

        fx = self._extractor.extract_from_bundle(
            symbol=sym,
            endpoint_to_raw=endpoint_to_raw,
            asof_ts=asof_ts,
        )

        repo.feature_snapshots.write(
            symbol=sym,
            feature_set="LABELING_BASE",
            asof_ts=fx.asof_ts or asof_ts,
            features=fx.features,
            request_ids=req_ids_sorted,
            planner_version="LPv1",
        )

    def pump_once(self, s: Session, limit: int = 50) -> DispatchResult:
        repo = Repo(s)
        pending = repo.data_requests.fetch_pending(limit=limit)
        sent = 0
        received = 0
        failed = 0

        for req in pending:
            try:
                repo.data_requests.mark_sent(req)
                sent += 1

                # Build provider payload
                payload = dict(req.request_payload or {})
                payload["_params_canonical"] = req.params_canonical

                # HARD GUARANTEE: provider call payload must include symbol/thscode when we have it.
                sym = self._norm_symbol(getattr(req, "symbol", None))
                if sym:
                    payload.setdefault("symbol", sym)
                    payload.setdefault("thscode", sym)
                    # planner payload uses "codes" for iFinD quantapi; keep it consistent if missing
                    if not payload.get("codes"):
                        payload["codes"] = sym

                resp = self._provider.call(req.endpoint, payload)

                resp_id = repo.data_responses.write(
                    request_id=req.request_id,
                    provider=req.provider,
                    endpoint=req.endpoint,
                    http_status=resp.http_status,
                    errorcode=resp.errorcode,
                    errmsg=resp.errmsg,
                    quota_context=resp.quota_context,
                    raw=resp.raw,
                    payload_sha256=resp.payload_sha256,
                    data_ts=None,
                )

                # If request somehow has no symbol, best-effort backfill from raw to keep downstream aggregation sane.
                if not sym:
                    inferred = self._try_extract_symbol_from_raw(getattr(resp, "raw", None))
                    if inferred:
                        req.symbol = inferred
                        sym = inferred

                req.status = "RECEIVED"
                req.response_id = resp_id
                req.last_error = None
                received += 1

                # Try writing snapshot after each received response (deduped by request_ids set)
                self._maybe_write_snapshot(repo, req)

            except Exception as e:
                repo.data_requests.mark_failed(req, str(e))
                failed += 1

        return DispatchResult(sent=sent, received=received, failed=failed)
