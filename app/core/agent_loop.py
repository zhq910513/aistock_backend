from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from datetime import timedelta

from sqlalchemy.orm import Session

from app.config import settings
from app.database import models
from app.database.repo import Repo
from app.core.data_dispatcher import DataRequestDispatcher
from app.core.feature_extractor import FeatureExtractor
from app.core.reasoning import ReasoningEngine
from app.utils.time import now_shanghai


@dataclass
class AgentDecision:
    decision: str  # BUY/SELL/HOLD
    confidence: float
    reason_code: str
    params: dict[str, Any]
    request_ids: list[str]
    feature_hash: str
    model_hash: str
    validation_id: str | None


class AgentLoop:
    """
    PLAN -> ACT (enqueue requests) -> OBSERVE (dispatch and read) -> VERIFY -> DECIDE.

    Round-3 upgrade:
    - Structured parsing for iFind responses via FeatureExtractor
    - On BUY-candidate, proactively request intraday high_frequency as verification evidence
      (so decisions are not made on sparse/brittle signals).
    """

    def __init__(self) -> None:
        self._dispatcher = DataRequestDispatcher()
        self._fx = FeatureExtractor()
        self._re = ReasoningEngine()

    def run_for_symbol(self, s: Session, symbol: str, account_id: str | None, correlation_id: str | None) -> AgentDecision:
        repo = Repo(s)

        # --- PLAN (base evidence) ---
        req_ids: list[str] = []

        rt_payload = {
            "codes": symbol,
            "indicators": "open,high,low,latest,close,volume,amount",
        }

        today = now_shanghai().date()
        startdate = (today - timedelta(days=45)).strftime("%Y-%m-%d")
        enddate = today.strftime("%Y-%m-%d")
        hist_payload = {
            "codes": symbol,
            "indicators": "open,high,low,close,volume,amount",
            "startdate": startdate,
            "enddate": enddate,
            "functionpara": {"Fill": "Blank"},
        }

        minute_key = now_shanghai().strftime("%Y%m%d%H%M")

        rid_rt = repo.data_requests.enqueue(
            dedupe_key=f"PLAN:RT:{symbol}:{minute_key}",
            correlation_id=correlation_id,
            account_id=account_id,
            symbol=symbol,
            purpose="PLAN",
            provider=settings.DATA_PROVIDER,
            endpoint="real_time_quotation",
            params_canonical=f"codes={symbol}&indicators=open,high,low,latest,close,volume,amount",
            request_payload=rt_payload,
            deadline_sec=5,
        )
        req_ids.append(rid_rt)

        rid_hist = repo.data_requests.enqueue(
            dedupe_key=f"PLAN:HIST:{symbol}:{minute_key}",
            correlation_id=correlation_id,
            account_id=account_id,
            symbol=symbol,
            purpose="PLAN",
            provider=settings.DATA_PROVIDER,
            endpoint="cmd_history_quotation",
            params_canonical=f"codes={symbol}&indicators=open,high,low,close,volume,amount&startdate={startdate}&enddate={enddate}",
            request_payload=hist_payload,
            deadline_sec=8,
        )
        req_ids.append(rid_hist)

        s.flush()

        # --- ACT/OBSERVE (base requests) ---
        self._dispatcher.pump_once(s, limit=settings.AGENT_MAX_REQUESTS_PER_SYMBOL)

        rt_resp = self._load_response_raw(s, rid_rt)
        hist_resp = self._load_response_raw(s, rid_hist)

        # --- FEATURE EXTRACTION (base) ---
        fxr = self._fx.extract(symbol=symbol, realtime_raw=rt_resp, history_raw=hist_resp)

        # --- INFER ---
        ro = self._re.infer(fxr.features)

        # --- VERIFY (base) ---
        hypothesis = (
            f"Hold {settings.HOLD_DAYS_MIN}-{settings.HOLD_DAYS_MAX} days, "
            f"target return {settings.TARGET_RETURN_MIN:.2%}-{settings.TARGET_RETURN_MAX:.2%}"
        )

        def _has_provider_error(raw: dict[str, Any] | None) -> bool:
            if not raw:
                return True
            ec = str(raw.get("errorcode", "0"))
            # iFind convention: '0' means OK
            return ec not in {"0", ""}

        # Minimal sanity checks: avoid "BUY" if data is obviously broken
        data_ok = (
            (not _has_provider_error(rt_resp))
            and (not _has_provider_error(hist_resp))
            and float(fxr.features.get("price_now", 0.0)) > 0.0
            and int(fxr.features.get("n_hist_points", 0)) >= 10
        )

        buy_candidate = data_ok and (ro.score > 0.01) and (float(fxr.features.get("momentum_3d", 0.0)) >= settings.TARGET_RETURN_MIN / 3.0)

        # On BUY-candidate, proactively collect intraday evidence for verification.
        intraday_req_id: str | None = None
        intraday_resp: dict[str, Any] | None = None
        fxr_final = fxr

        if buy_candidate:
            # iFind example endpoint: /api/v1/high_frequency
            starttime = now_shanghai().replace(hour=9, minute=15, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
            endtime = now_shanghai().strftime("%Y-%m-%d %H:%M:%S")

            intraday_payload = {
                "codes": symbol,
                "indicators": "open,high,low,close,volume,amount",
                "starttime": starttime,
                "endtime": endtime,
            }

            intraday_req_id = repo.data_requests.enqueue(
                dedupe_key=f"VERIFY:INTRADAY:{symbol}:{minute_key}",
                correlation_id=correlation_id,
                account_id=account_id,
                symbol=symbol,
                purpose="VERIFY",
                provider=settings.DATA_PROVIDER,
                endpoint="high_frequency",
                params_canonical=f"codes={symbol}&indicators=open,high,low,close,volume,amount&starttime={starttime}&endtime={endtime}",
                request_payload=intraday_payload,
                deadline_sec=10,
            )
            req_ids.append(intraday_req_id)
            s.flush()

            # dispatch the verification request
            self._dispatcher.pump_once(s, limit=settings.AGENT_MAX_REQUESTS_PER_SYMBOL)
            intraday_resp = self._load_response_raw(s, intraday_req_id)

            # recompute features with intraday evidence attached
            fxr_final = self._fx.extract(symbol=symbol, realtime_raw=rt_resp, history_raw=hist_resp, intraday_raw=intraday_resp)

        # verification rules (coarse but deterministic):
        # - bullish momentum in last 3 days
        # - not too volatile (vol proxy)
        # - if intraday evidence exists: avoid buying into an intraday dump
        base_passes = (
            (float(fxr_final.features.get("momentum_3d", 0.0)) >= settings.TARGET_RETURN_MIN / 3.0)
            and (float(fxr_final.features.get("vol_proxy", 0.0)) <= 0.15)
        )

        intraday_points = int(fxr_final.features.get("intraday_points", 0))
        intraday_ret = float(fxr_final.features.get("intraday_ret", 0.0))

        intraday_passes = True
        if buy_candidate:
            # if we couldn't collect intraday evidence (no points), be conservative
            if intraday_points <= 0 or _has_provider_error(intraday_resp):
                intraday_passes = False
            else:
                # allow small red, block big intraday drawdowns
                intraday_passes = intraday_ret >= -0.01

        passes = data_ok and base_passes and intraday_passes
        concl = "PASS" if (passes and ro.confidence >= settings.AGENT_VERIFY_MIN_CONFIDENCE) else "INCONCLUSIVE"

        evidence = {
            "features": fxr_final.features,
            "score": ro.score,
            "confidence": ro.confidence,
            "data_ok": data_ok,
            "buy_candidate": buy_candidate,
            "intraday_points": intraday_points,
            "intraday_ret": intraday_ret,
        }

        val_id = repo.validations.write(
            decision_id="PENDING",
            symbol=symbol,
            hypothesis=hypothesis,
            request_ids=req_ids,
            evidence=evidence,
            conclusion=concl,
            score=float(ro.confidence),
        )

        # --- DECIDE ---
        decision = "HOLD"
        reason_code = ro.reason_code
        params = dict(ro.params)
        params.update(
            {
                "confidence": float(ro.confidence),
                # UI-friendly alias (currently: use confidence as next-day limit-up probability proxy)
                "p_limit_up": float(ro.confidence),
                "hypothesis": hypothesis,
                "validation_conclusion": concl,
                "validation_id": val_id,
                # UI-friendly explainability buckets
                "features_snapshot": evidence,
                "top_features": [
                    {"k": "momentum_45d", "v": evidence.get("momentum_45d")},
                    {"k": "vol_20d", "v": evidence.get("vol_20d")},
                    {"k": "intraday_ret", "v": evidence.get("intraday_ret")},
                ],
            }
        )

        if concl == "PASS" and ro.score > 0.01:
            decision = "BUY"
            reason_code = "RC_AGENT_PASS_BUY_V2"
        elif ro.score < -0.01:
            decision = "SELL"
            reason_code = "RC_AGENT_SELL_RISK_OFF_V1"

        return AgentDecision(
            decision=decision,
            confidence=float(ro.confidence),
            reason_code=reason_code,
            params=params,
            request_ids=req_ids,
            feature_hash=fxr_final.feature_hash,
            model_hash=self._re.model_hash(),
            validation_id=val_id,
        )

    def _load_response_raw(self, s: Session, request_id: str) -> dict[str, Any] | None:
        req = s.get(models.DataRequest, request_id)
        if req is None or not req.response_id:
            return None
        resp = s.get(models.DataResponse, req.response_id)
        if resp is None:
            return None
        return dict(resp.raw or {})
