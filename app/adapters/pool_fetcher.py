from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import requests

from app.config import settings
from app.utils.crypto import sha256_hex


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

    url = str(settings.POOL_FETCH_URL or "").strip()
    if not url:
        # Hard-fail: without URL we cannot fetch.
        raw = {"error": "POOL_FETCH_URL is empty"}
        raw_hash = sha256_hex(json.dumps(raw, ensure_ascii=False).encode("utf-8"))
        return PoolFetchResult(raw=raw, raw_hash=raw_hash, items=[])

    method = str(settings.POOL_FETCH_METHOD or "GET").strip().upper() or "GET"
    headers = _safe_json_loads(settings.POOL_FETCH_HEADERS_JSON)
    body = _safe_json_loads(settings.POOL_FETCH_BODY_JSON)

    if method not in {"GET", "POST"}:
        method = "GET"

    resp = None
    if method == "GET":
        resp = requests.get(url, headers=headers, timeout=timeout_sec)
    else:
        resp = requests.post(url, headers=headers, json=body, timeout=timeout_sec)

    # best-effort parse
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
