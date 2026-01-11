from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, Any
from datetime import datetime

from app.config import settings
from app.utils.time import now_shanghai
from app.utils.crypto import sha256_hex
from app.utils.ids import new_request_id

from app.adapters.ifind_http import IFindHTTPProvider


class THSAdapter(Protocol):
    def fetch_market_event(self, symbol: str) -> dict[str, Any]: ...
    def query_orders(self) -> list[dict[str, Any]]: ...
    def query_fills(self) -> list[dict[str, Any]]: ...

    def fetch_minute_close_int64(self, symbol: str, data_ts: datetime, source: str) -> tuple[str, int]:
        """
        Return (channel_id, close_int64) for given source (DATAFEED/IFIND_HTTP/IFIND_SDK).
        """


def _to_float(x: Any) -> float | None:
    if x is None:
        return None
    if isinstance(x, bool):
        return 1.0 if x else 0.0
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            return float(s.replace(",", ""))
        except Exception:
            return None
    return None


def _ifind_first_table(raw: dict[str, Any]) -> dict[str, Any] | None:
    tables = raw.get("tables")
    if isinstance(tables, list) and tables:
        t0 = tables[0]
        if isinstance(t0, dict):
            return t0
    return None


def _ifind_table_dict(raw: dict[str, Any]) -> dict[str, Any] | None:
    t0 = _ifind_first_table(raw)
    if not t0:
        return None
    tab = t0.get("table")
    if isinstance(tab, dict):
        return tab
    return None


def _ifind_extract_scalar(raw: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    tab = _ifind_table_dict(raw)
    if tab is not None:
        for k in keys:
            if k in tab:
                vals = tab.get(k)
                if isinstance(vals, list) and vals:
                    for x in vals[:5]:
                        n = _to_float(x)
                        if n is not None:
                            return float(n)
                else:
                    n = _to_float(vals)
                    if n is not None:
                        return float(n)

    for k in keys:
        if k in raw:
            n = _to_float(raw.get(k))
            if n is not None:
                return float(n)
    return None


@dataclass
class MockTHSAdapter:
    producer_instance: str = "mock-1"

    def fetch_market_event(self, symbol: str) -> dict[str, Any]:
        ingest_ts = now_shanghai()
        data_ts = ingest_ts

        payload = {"symbol": symbol, "price": 10.0, "ts": ingest_ts.isoformat()}
        payload_bytes = str(payload).encode("utf-8")

        req_id = new_request_id(f"MOCK|{symbol}|{ingest_ts.isoformat()}")

        indicator_set = "PRICE"
        params_canonical = f"symbol={symbol}"

        return {
            "api_schema_version": settings.API_SCHEMA_VERSION,
            "source": "MOCK",
            "ths_product": "MOCK",
            "ths_function": "MOCK_PRICE",
            "ths_indicator_set": indicator_set,
            "ths_params_canonical": params_canonical,
            "ths_errorcode": "0",
            "ths_quota_context": "mock",

            "source_clock_quality": "AGGREGATED",
            "channel_id": f"MOCK:{symbol}",
            "channel_seq": int(ingest_ts.timestamp() * 1000),
            "symbol": symbol,

            "data_ts": data_ts,
            "ingest_ts": ingest_ts,

            "payload": payload,
            "payload_sha256": sha256_hex(payload_bytes),

            "data_status": "VALID",
            "latency_ms": 0,
            "completion_rate": 1.0,

            "request_id": req_id,
            "producer_instance": self.producer_instance,
        }

    def query_orders(self) -> list[dict[str, Any]]:
        return []

    def query_fills(self) -> list[dict[str, Any]]:
        return []

    def fetch_minute_close_int64(self, symbol: str, data_ts: datetime, source: str) -> tuple[str, int]:
        base_price = 10.0
        close_int64 = int(round(base_price * 10000))
        channel_id = f"{source}:{symbol}"
        return channel_id, close_int64


@dataclass
class IFindHTTPTHSAdapter:
    """
    Market event ingestion via iFinD HTTP API.

    Uses real_time_quotation to produce a minimal RawMarketEvent payload:
    - price (latest preferred)
    - optional open/high/low/close/volume/amount if available
    """
    producer_instance: str = "ifind-http-1"
    provider: IFindHTTPProvider = field(default_factory=IFindHTTPProvider)
    _last_seq: dict[str, int] = field(default_factory=dict)

    def _next_seq(self, channel_id: str, base_seq: int) -> int:
        last = int(self._last_seq.get(channel_id, 0))
        seq = int(base_seq)
        if seq <= last:
            seq = last + 1
        self._last_seq[channel_id] = seq
        return seq

    def fetch_market_event(self, symbol: str) -> dict[str, Any]:
        ingest_ts = now_shanghai()
        data_ts = ingest_ts

        indicators = "open,high,low,latest,close,volume,amount"
        req_payload = {"codes": symbol, "indicators": indicators}
        resp = self.provider.call("real_time_quotation", req_payload)

        raw = resp.raw if isinstance(resp.raw, dict) else {}
        price = _ifind_extract_scalar(raw, ("latest", "new", "last", "close", "price"))

        open_p = _ifind_extract_scalar(raw, ("open",))
        high_p = _ifind_extract_scalar(raw, ("high",))
        low_p = _ifind_extract_scalar(raw, ("low",))
        close_p = _ifind_extract_scalar(raw, ("close",))
        volume = _ifind_extract_scalar(raw, ("volume", "vol"))
        amount = _ifind_extract_scalar(raw, ("amount",))

        ok = (str(resp.errorcode) in {"0", ""}) and (price is not None) and (float(price) > 0.0)

        payload = {
            "symbol": symbol,
            "price": float(price) if price is not None else 0.0,
            "open": float(open_p) if open_p is not None else None,
            "high": float(high_p) if high_p is not None else None,
            "low": float(low_p) if low_p is not None else None,
            "close": float(close_p) if close_p is not None else None,
            "volume": float(volume) if volume is not None else None,
            "amount": float(amount) if amount is not None else None,
            "ts": ingest_ts.isoformat(),
            # Keep lineage to the full iFinD response without bloating DB.
            "ifind_raw_sha256": resp.payload_sha256,
        }
        payload_bytes = str(payload).encode("utf-8")

        channel_id = f"IFIND_HTTP:{symbol}"
        base_seq = int(ingest_ts.timestamp() * 1000)
        seq = self._next_seq(channel_id, base_seq)

        req_id = new_request_id(f"IFIND_HTTP|{symbol}|{seq}|{resp.payload_sha256}")

        params_canonical = f"codes={symbol}&indicators={indicators}"

        return {
            "api_schema_version": settings.API_SCHEMA_VERSION,
            "source": "IFIND_HTTP",
            "ths_product": "IFIND_HTTP",
            "ths_function": "real_time_quotation",
            "ths_indicator_set": indicators,
            "ths_params_canonical": params_canonical,
            "ths_errorcode": str(resp.errorcode),
            "ths_quota_context": str(resp.quota_context or ""),

            "source_clock_quality": "AGGREGATED",
            "channel_id": channel_id,
            "channel_seq": seq,
            "symbol": symbol,

            "data_ts": data_ts,
            "ingest_ts": ingest_ts,

            "payload": payload,
            "payload_sha256": sha256_hex(payload_bytes),

            "data_status": "VALID" if ok else "INVALID",
            "latency_ms": 0,
            "completion_rate": 1.0 if ok else 0.0,

            "request_id": req_id,
            "producer_instance": self.producer_instance,
        }

    def query_orders(self) -> list[dict[str, Any]]:
        # Execution plane is out-of-scope for this demo backend.
        return []

    def query_fills(self) -> list[dict[str, Any]]:
        return []

    def fetch_minute_close_int64(self, symbol: str, data_ts: datetime, source: str) -> tuple[str, int]:
        """
        Best-effort minute close via high_frequency (1-minute) around data_ts.
        Not currently used by the main loop, but implemented for future cross-source reconciliation.
        """
        channel_id = f"{source}:{symbol}"

        if source != "IFIND_HTTP":
            return channel_id, 0

        # Query a small window around the minute (avoid strict alignment issues).
        starttime = data_ts.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        endtime = data_ts.replace(second=59, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        req_payload = {
            "codes": symbol,
            "indicators": "close",
            "starttime": starttime,
            "endtime": endtime,
        }
        resp = self.provider.call("high_frequency", req_payload)
        raw = resp.raw if isinstance(resp.raw, dict) else {}
        close = _ifind_extract_scalar(raw, ("close", "latest", "new"))
        if close is None or close <= 0:
            return channel_id, 0
        return channel_id, int(round(float(close) * 10000))


def get_ths_adapter() -> THSAdapter:
    mode = (getattr(settings, "THS_MODE", "") or settings.DATA_PROVIDER or "MOCK").strip().upper()
    if mode == "IFIND_HTTP":
        return IFindHTTPTHSAdapter()
    return MockTHSAdapter()
