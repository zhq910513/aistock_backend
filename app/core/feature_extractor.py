from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import math
import json

from app.utils.crypto import sha256_hex


@dataclass
class FeatureExtractionResult:
    features: dict[str, Any]
    feature_hash: str


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


def _walk_numbers(obj: Any, limit: int = 2000) -> list[float]:
    out: list[float] = []

    def rec(o: Any) -> None:
        if len(out) >= limit:
            return
        if isinstance(o, dict):
            for v in o.values():
                rec(v)
        elif isinstance(o, list):
            for v in o:
                rec(v)
        else:
            n = _to_float(o)
            if n is not None:
                out.append(n)

    rec(obj)
    return out


def _ifind_first_table(raw: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
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

def _first_non_none(nums: list[float | None]) -> float | None:
    for n in nums:
        if n is not None:
            return n
    return None


def _ifind_extract_scalar(raw: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    # Preferred: tables[0].table.<indicator>
    tab = _ifind_table_dict(raw)
    if tab is not None:
        for k in keys:
            if k in tab:
                vals = tab.get(k)
                if isinstance(vals, list):
                    nums = [_to_float(x) for x in vals[:5]]
                    got = _first_non_none(nums)
                    if got is not None:
                        return got
                else:
                    got = _to_float(vals)
                    if got is not None:
                        return got

    # Fallback: root keys
    for k in keys:
        if k in raw:
            got = _to_float(raw.get(k))
            if got is not None:
                return got

    return None


def _ifind_extract_series(raw: dict[str, Any], keys: tuple[str, ...], limit: int = 5000) -> list[float]:
    tab = _ifind_table_dict(raw)
    if tab is not None:
        for k in keys:
            if k in tab:
                vals = tab.get(k)
                out: list[float] = []
                if isinstance(vals, list):
                    for x in vals[:limit]:
                        n = _to_float(x)
                        if n is not None:
                            out.append(float(n))
                else:
                    n = _to_float(vals)
                    if n is not None:
                        out.append(float(n))
                return out

    # no structured table: fallback to numeric walk
    nums = _walk_numbers(raw, limit=limit)
    return [float(x) for x in nums if 0.0 < float(x) < 1e9]


def _extract_price_from_realtime(raw: dict[str, Any]) -> float | None:
    # iFind realtime_quotation usually uses 'latest'
    return _ifind_extract_scalar(
        raw,
        (
            "latest",
            "new",
            "last",
            "close",
            "price",
            "LastPrice",
        ),
    )


def _extract_close_series_from_history(raw: dict[str, Any]) -> list[float]:
    # cmd_history_quotation usually uses 'close'
    return _ifind_extract_series(raw, ("close", "latest", "new"), limit=10000)


def _extract_intraday_metrics(raw: dict[str, Any] | None) -> tuple[float, float, int]:
    if not raw:
        return 0.0, 0.0, 0

    closes = _ifind_extract_series(raw, ("close", "latest", "new"), limit=20000)
    opens = _ifind_extract_series(raw, ("open",), limit=20000)
    vols = _ifind_extract_series(raw, ("volume", "vol"), limit=20000)

    n = int(max(len(closes), len(opens), len(vols)))
    if not closes and not opens:
        return 0.0, float(sum(vols)) if vols else 0.0, n

    # intraday return: last close vs first open (preferred) or first close
    o0 = opens[0] if opens else (closes[0] if closes else 0.0)
    cN = closes[-1] if closes else (opens[-1] if opens else 0.0)
    intraday_ret = (cN / o0) - 1.0 if o0 else 0.0

    return float(intraday_ret), float(sum(vols)) if vols else 0.0, n


class FeatureExtractor:
    """Parse iFind QuantAPI responses into stable, minimal features for 1-3 day holding decisions."""

    def extract(
        self,
        symbol: str,
        realtime_raw: dict[str, Any] | None,
        history_raw: dict[str, Any] | None,
        intraday_raw: dict[str, Any] | None = None,
    ) -> FeatureExtractionResult:
        price_now = _extract_price_from_realtime(realtime_raw or {}) if realtime_raw else None
        series = _extract_close_series_from_history(history_raw or {}) if history_raw else []

        # Guardrail: only use a reasonable tail
        if len(series) > 200:
            series = series[-200:]

        ret_1d = 0.0
        momentum_3d = 0.0
        vol_proxy = 0.0

        if len(series) >= 2:
            c0 = float(series[-1])
            c1 = float(series[-2])
            if c1 != 0:
                ret_1d = (c0 / c1) - 1.0

        if len(series) >= 4:
            c0 = float(series[-1])
            c3 = float(series[-4])
            if c3 != 0:
                momentum_3d = (c0 / c3) - 1.0

        # crude vol proxy: std of daily returns (tail)
        rets: list[float] = []
        for i in range(1, len(series)):
            a = float(series[i - 1])
            b = float(series[i])
            if a > 0:
                rets.append((b / a) - 1.0)
        if len(rets) >= 2:
            mu = sum(rets) / len(rets)
            vol_proxy = math.sqrt(sum((x - mu) ** 2 for x in rets) / (len(rets) - 1))

        intraday_ret, intraday_vol_sum, intraday_points = _extract_intraday_metrics(intraday_raw)

        features = {
            "symbol": symbol,
            "price_now": float(price_now) if price_now is not None else 0.0,
            "ret_1d": float(ret_1d),
            "momentum_3d": float(momentum_3d),
            "vol_proxy": float(vol_proxy),
            "n_hist_points": int(len(series)),
            "intraday_ret": float(intraday_ret),
            "intraday_vol_sum": float(intraday_vol_sum),
            "intraday_points": int(intraday_points),
        }

        feature_hash = sha256_hex(json.dumps(features, sort_keys=True, ensure_ascii=False).encode("utf-8"))
        return FeatureExtractionResult(features=features, feature_hash=feature_hash)
