from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from typing import Any

import requests

from app.config import settings
from app.utils.crypto import sha256_hex
from app.utils.time import now_shanghai


@dataclass(frozen=True)
class PoolFetchResult:
    """Result of pulling the external limit-up candidate pool."""

    raw: Any
    raw_hash: str
    items: list[dict]


def _safe_json_loads(s: str) -> dict:
    try:
        obj = json.loads(s or "{}")
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def fetch_limitup_pool(timeout_sec: int = 20) -> PoolFetchResult:
    """Pull external candidate pool.

    The external API shape is intentionally flexible. We treat the whole payload as raw,
    and try to find a list of items under common keys:
    - {"data": [...]} / {"items": [...]} / [...]

    Each item should be a dict; otherwise it will be ignored.
    """

    mode = str(getattr(settings, "POOL_FETCHER_MODE", "CUSTOM") or "CUSTOM").strip().upper()

    # Backward compatible: keep THS_10JQKA, but now it uses the new limit_up_pool endpoint
    if mode in {"THS_10JQKA", "THS_LIMIT_UP_POOL"}:
        return _fetch_ths_limit_up_pool(timeout_sec=timeout_sec)

    url = str(settings.POOL_FETCH_URL or "").strip()
    if not url:
        raw = {"error": "POOL_FETCH_URL is empty and POOL_FETCHER_MODE is not THS_LIMIT_UP_POOL/THS_10JQKA"}
        raw_hash = sha256_hex(json.dumps(raw, ensure_ascii=False).encode("utf-8"))
        return PoolFetchResult(raw=raw, raw_hash=raw_hash, items=[])

    method = str(settings.POOL_FETCH_METHOD or "GET").strip().upper() or "GET"
    headers = _safe_json_loads(settings.POOL_FETCH_HEADERS_JSON)
    body = _safe_json_loads(settings.POOL_FETCH_BODY_JSON)

    if method not in {"GET", "POST"}:
        method = "GET"

    if method == "GET":
        resp = requests.get(url, headers=headers, timeout=timeout_sec)
    else:
        resp = requests.post(url, headers=headers, json=body, timeout=timeout_sec)

    try:
        raw: Any = resp.json()
    except Exception:
        raw = {"text": (resp.text or "")[:2000], "http_status": resp.status_code}

    raw_hash = sha256_hex(json.dumps(raw, ensure_ascii=False, sort_keys=True).encode("utf-8"))

    items: list[dict] = []
    if isinstance(raw, list):
        for x in raw:
            if isinstance(x, dict):
                items.append(x)
    elif isinstance(raw, dict):
        cand = raw.get("data")
        if isinstance(cand, list):
            items = [x for x in cand if isinstance(x, dict)]
        else:
            cand2 = raw.get("items")
            if isinstance(cand2, list):
                items = [x for x in cand2 if isinstance(x, dict)]

    return PoolFetchResult(raw=raw, raw_hash=raw_hash, items=items)


def _fetch_ths_limit_up_pool(timeout_sec: int = 20) -> PoolFetchResult:
    """Fetch limit-up pool from 10jqka (同花顺 dataapi/limit_up/limit_up_pool), with pagination.

    Expected shape:
    {
      "status_code": 0,
      "status_msg": "success",
      "data": {
        "page": {"limit": 15, "total": 51, "count": 4, "page": 1},
        "info": [ ... ],
        "date": "YYYYMMDD",
        ...
      }
    }
    """

    date = now_shanghai().strftime("%Y%m%d")
    url = "https://data.10jqka.com.cn/dataapi/limit_up/limit_up_pool"

    # ---- config knobs (env/settings) ----
    per_page = int(getattr(settings, "THS_POOL_LIMIT_PER_PAGE", 15) or 15)
    max_items = int(getattr(settings, "THS_POOL_MAX_ITEMS", 200) or 200)

    field = str(
        getattr(
            settings,
            "THS_POOL_FIELDS",
            "199112,10,9001,330323,330324,330325,9002,330329,133971,133970,1968584,3475914,9003,9004",
        )
        or ""
    ).strip()
    filter_val = str(getattr(settings, "THS_POOL_FILTER", "HS") or "HS").strip()
    order_field = str(getattr(settings, "THS_POOL_ORDER_FIELD", "330324") or "330324").strip()
    order_type = str(getattr(settings, "THS_POOL_ORDER_TYPE", "0") or "0").strip()

    # SSL verify: default True; allow override if necessary.
    ssl_verify = bool(getattr(settings, "THS_SSL_VERIFY", True))

    # Minimal headers. This endpoint usually works without special tokens, but does verify UA.
    headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": str(
            getattr(
                settings,
                "THS_USER_AGENT",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            )
        ),
        "Referer": "https://data.10jqka.com.cn/",
    }

    # Pagination loop
    all_items: list[dict] = []
    raw_pages: list[dict] = []

    page_no = 1
    total_pages: int | None = None

    while True:
        if max_items > 0 and len(all_items) >= max_items:
            break

        # "_" param in your sample looks like ms timestamp; keep it dynamic.
        underscore_ms = int(now_shanghai().timestamp() * 1000)

        params = {
            "page": str(page_no),
            "limit": str(per_page),
            "field": field,
            "filter": filter_val,
            "order_field": order_field,
            "order_type": order_type,
            "date": date,
            "_": str(underscore_ms),
        }

        try:
            resp = requests.get(url=url, headers=headers, params=params, timeout=timeout_sec, verify=ssl_verify)
        except Exception as e:
            raw = {"error": f"THS_LIMIT_UP_POOL fetch failed: {type(e).__name__}", "date": date, "page": page_no}
            raw_hash = sha256_hex(json.dumps(raw, ensure_ascii=False, sort_keys=True).encode("utf-8"))
            return PoolFetchResult(raw=raw, raw_hash=raw_hash, items=[])

        try:
            raw_one: Any = resp.json()
        except Exception:
            raw_one = {"text": (resp.text or "")[:2000], "http_status": resp.status_code}

        if isinstance(raw_one, dict):
            raw_pages.append(raw_one)

        # Extract items for this page
        page_items: list[dict] = []
        if isinstance(raw_one, dict):
            data = raw_one.get("data")
            if isinstance(data, dict):
                info = data.get("info")
                if isinstance(info, list):
                    page_items = [x for x in info if isinstance(x, dict)]

                # page meta: {"limit":15,"total":51,"count":4,"page":1}
                page_meta = data.get("page")
                if isinstance(page_meta, dict):
                    if total_pages is None:
                        # count seems to be total page count
                        try:
                            total_pages = int(page_meta.get("count") or 0) or None
                        except Exception:
                            total_pages = None

        # Add audit fields (kept for compatibility with legacy downstream)
        for it in page_items:
            try:
                code = str(it.get("code") or "").strip()
                if code:
                    hash_key = hashlib.md5((date + code).encode("utf8")).hexdigest()
                    it["hash_key"] = hash_key
                    it["date"] = date
            except Exception:
                continue

        all_items.extend(page_items)

        # Stop conditions
        if not page_items:
            break

        if total_pages is not None and page_no >= total_pages:
            break

        # Also stop if next page would be pointless under max_items
        page_no += 1

    # Truncate to max_items if needed
    if 0 < max_items < len(all_items):
        all_items = all_items[:max_items]

    raw_combined = {
        "source": "THS_LIMIT_UP_POOL",
        "date": date,
        "fetched_pages": len(raw_pages),
        "per_page": per_page,
        "max_items": max_items,
        "pages": raw_pages,
    }
    raw_hash = sha256_hex(json.dumps(raw_combined, ensure_ascii=False, sort_keys=True).encode("utf-8"))
    return PoolFetchResult(raw=raw_combined, raw_hash=raw_hash, items=all_items)
