from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Any
import json
import random
from datetime import datetime, timedelta

from app.config import settings
from app.utils.crypto import sha256_hex
from app.adapters.ifind_http import IFindHTTPProvider, IFindHTTPResponse


class DataProvider(Protocol):
    def call(self, endpoint: str, payload: dict[str, Any]) -> IFindHTTPResponse:
        raise NotImplementedError


def _mk_ifind_table_payload(table: dict[str, Any]) -> dict[str, Any]:
    # Mirror the common iFinD QuantAPI shape that our extractor expects:
    # {"tables":[{"table": {...}}]}
    return {"tables": [{"table": table}]}


@dataclass
class MockDataProvider:
    """Deterministic-ish mock provider for local/offline runs.

    It returns small, plausible numeric payloads for:
    - real_time_quotation
    - cmd_history_quotation
    - high_frequency

    This keeps the pipeline runnable without tokens/network access.
    """

    seed: int = 42

    def call(self, endpoint: str, payload: dict[str, Any]) -> IFindHTTPResponse:
        endpoint = (endpoint or "").strip()
        symbol = str(payload.get("symbol") or payload.get("ths_code") or "")

        # Stable per-(endpoint,symbol) RNG so repeated calls are reproducible.
        h = sha256_hex(f"{endpoint}|{symbol}|{self.seed}".encode("utf-8"))
        rng = random.Random(int(h[:8], 16))

        now = datetime.utcnow()
        base_px = 5.0 + (int(h[8:12], 16) % 5000) / 1000.0  # ~5-10
        drift = (int(h[12:16], 16) % 2000 - 1000) / 100000.0
        px = max(0.01, base_px * (1.0 + drift))

        if endpoint in {"real_time_quotation", "realtime_quotation", "rtq"}:
            table = {
                "latest": [round(px, 4)],
                "open": [round(px * (1 - 0.003), 4)],
                "high": [round(px * (1 + 0.006), 4)],
                "low": [round(px * (1 - 0.006), 4)],
                "close": [round(px * (1 - 0.001), 4)],
                "volume": [int(100000 * rng.random())],
                "amount": [round(px * int(100000 * rng.random()), 2)],
                "time": [(now + timedelta(hours=8)).isoformat()],
            }
            raw = _mk_ifind_table_payload(table)

        elif endpoint in {"cmd_history_quotation", "history_quotation", "hq"}:
            n = int(payload.get("limit") or 60)
            n = max(5, min(500, n))
            closes, opens, highs, lows, vols = [], [], [], [], []
            base = px
            for _i in range(n):
                shock = (rng.random() - 0.5) * 0.02
                base = max(0.01, base * (1 + shock))
                o = base * (1 + (rng.random() - 0.5) * 0.005)
                c = base * (1 + (rng.random() - 0.5) * 0.005)
                hi = max(o, c) * (1 + rng.random() * 0.01)
                lo = min(o, c) * (1 - rng.random() * 0.01)
                opens.append(round(o, 4))
                closes.append(round(c, 4))
                highs.append(round(hi, 4))
                lows.append(round(lo, 4))
                vols.append(int(100000 * rng.random()))
            table = {"open": opens, "close": closes, "high": highs, "low": lows, "volume": vols}
            raw = _mk_ifind_table_payload(table)

        elif endpoint in {"high_frequency", "hf"}:
            table = {"latest": [round(px, 4)], "close": [round(px, 4)]}
            raw = _mk_ifind_table_payload(table)

        else:
            raw = {"errorcode": "UNSUPPORTED_ENDPOINT", "errmsg": f"mock endpoint not supported: {endpoint}"}

        raw_json = json.dumps(raw, ensure_ascii=False, sort_keys=True)
        payload_sha256 = sha256_hex(raw_json.encode("utf-8"))

        errorcode = str(raw.get("errorcode", "0") or "0")
        errmsg = str(raw.get("errmsg", "") or "")
        return IFindHTTPResponse(
            http_status=200 if errorcode == "0" else 400,
            errorcode=errorcode,
            errmsg=errmsg,
            quota_context="",
            raw=raw,
            payload_sha256=payload_sha256,
        )


def get_data_provider() -> DataProvider:
    if (settings.DATA_PROVIDER or "").upper() == "IFIND_HTTP":
        return IFindHTTPProvider()
    return MockDataProvider()
