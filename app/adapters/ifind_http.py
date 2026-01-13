from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
import json
import threading
import time

import httpx

from app.config import settings
from app.utils.crypto import sha256_hex


@dataclass
class IFindHTTPResponse:
    http_status: int | None
    errorcode: str
    errmsg: str
    quota_context: str
    raw: dict[str, Any]
    payload_sha256: str


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, sort_keys=True, ensure_ascii=False)
    except Exception:
        return json.dumps({"_unserializable": True, "repr": repr(obj)}, sort_keys=True, ensure_ascii=False)


def _extract_quota_context(raw: dict[str, Any]) -> str:
    dataVol = raw.get("dataVol")
    if isinstance(dataVol, dict):
        slim = {k: dataVol.get(k) for k in ("quota", "useCount", "remainCount") if k in dataVol}
        if slim:
            return json.dumps(slim, ensure_ascii=False)
    return ""


def _looks_like_auth_failure(http_status: int | None, errorcode: str, errmsg: str) -> bool:
    if http_status in (401, 403):
        return True
    ec = str(errorcode or "").strip()
    em = (errmsg or "").lower()
    if "token" in em or "access_token" in em or "refresh_token" in em:
        return True
    # Unknown errorcode taxonomy; keep conservative.
    if ec in {"401", "403"}:
        return True
    return False


def _pick_access_token(raw: dict[str, Any]) -> str | None:
    # iFinD token APIs are not fully specified in the PDF beyond URL + headers,
    # so we accept multiple common shapes defensively.
    candidates: list[Any] = [
        raw.get("access_token"),
        raw.get("token"),
    ]
    data = raw.get("data")
    if isinstance(data, dict):
        candidates.extend([data.get("access_token"), data.get("token")])
    result = raw.get("result")
    if isinstance(result, dict):
        candidates.extend([result.get("access_token"), result.get("token")])

    for c in candidates:
        if isinstance(c, str) and c.strip():
            return c.strip()
    return None


class IFindTokenManager:
    """
    Minimal token manager for iFinD QuantAPI HTTP.

    PDF summary:
    - refresh_token is long-lived and is used only to fetch/refresh access_token
    - access_token is used in request headers and expires ~7 days
    - get_access_token: returns current valid access_token
    - update_access_token: returns a new access_token (invalidates all old ones)
    """

    def __init__(self, base_url: str, refresh_token: str, initial_access_token: str = "", timeout_sec: float = 8.0) -> None:
        self._base_url = (base_url or "https://quantapi.51ifind.com").rstrip("/")
        self._refresh_token = (refresh_token or "").strip()
        self._access_token = (initial_access_token or "").strip()
        self._timeout = float(timeout_sec)
        self._client = httpx.Client(timeout=self._timeout)
        self._lock = threading.Lock()
        self._token_set_at = time.time() if self._access_token else 0.0

    def has_refresh_token(self) -> bool:
        return bool(self._refresh_token)

    def get_access_token(self, force_refresh: bool = False) -> str | None:
        """
        Return a usable access_token.

        Behavior:
        - If refresh_token exists, we treat access_token as a cached/managed credential:
          * use cached token if still "fresh enough"
          * otherwise call get_access_token (or update_access_token when force_refresh=True)
          * env IFIND_HTTP_TOKEN is treated as an initial seed only (helpful for cold start)
        - If refresh_token does NOT exist, fall back to env IFIND_HTTP_TOKEN.
        """
        with self._lock:
            # No refresh token -> only possible source is explicit IFIND_HTTP_TOKEN.
            if not self._refresh_token:
                explicit = (settings.IFIND_HTTP_TOKEN or "").strip()
                return explicit or None

            # Seed from env IFIND_HTTP_TOKEN once (optional).
            if (not self._access_token) and (settings.IFIND_HTTP_TOKEN or "").strip():
                self._access_token = (settings.IFIND_HTTP_TOKEN or "").strip()
                self._token_set_at = self._token_set_at or time.time()

            now = time.time()
            if self._access_token and self._token_set_at:
                age = now - self._token_set_at
                max_age = float(getattr(settings, "IFIND_HTTP_TOKEN_MAX_AGE_SEC", 6 * 24 * 3600))
                early = float(getattr(settings, "IFIND_HTTP_TOKEN_EARLY_REFRESH_SEC", 15 * 60))
                if (not force_refresh) and age <= max(0.0, max_age - early):
                    return self._access_token

            endpoint = "update_access_token" if force_refresh else "get_access_token"
            url = f"{self._base_url}/api/v1/{endpoint}"
            headers = {"Content-Type": "application/json", "refresh_token": self._refresh_token}

            try:
                r = self._client.post(url, headers=headers)
                raw = r.json() if r.content else {}
                token = _pick_access_token(raw if isinstance(raw, dict) else {})
                if token:
                    self._access_token = token
                    self._token_set_at = time.time()
                    return self._access_token
            except Exception:
                return None

            return None

    def force_update_access_token(self) -> str | None:
        return self.get_access_token(force_refresh=True)


# Module-level singleton so THS adapter + Data provider share token cache.
_token_manager: IFindTokenManager | None = None
_token_manager_lock = threading.Lock()


def get_token_manager(base_url: Optional[str] = None) -> IFindTokenManager:
    global _token_manager
    with _token_manager_lock:
        if _token_manager is None:
            _token_manager = IFindTokenManager(
                base_url=(base_url or settings.IFIND_HTTP_BASE_URL),
                refresh_token=settings.IFIND_HTTP_REFRESH_TOKEN,
                initial_access_token=settings.IFIND_HTTP_TOKEN,
            )
        return _token_manager


class IFindHTTPProvider:
    """
    iFinD QuantAPI HTTP provider.

    Endpoints follow /api/v1/{endpoint}.

    Auth (per PDF examples):
    - access_token is passed via HTTP header:
      {"Content-Type":"application/json","access_token": user_access_token}
    """

    def __init__(self, base_url: Optional[str] = None, token: Optional[str] = None, timeout_sec: float = 8.0) -> None:
        self._base_url = (base_url or settings.IFIND_HTTP_BASE_URL).rstrip("/")
        # token here is optional; if omitted we will resolve via token manager.
        self._explicit_token = (token or "").strip()
        self._timeout = float(timeout_sec)
        self._client = httpx.Client(timeout=self._timeout)
        self._tm = get_token_manager(self._base_url)

    def _resolve_token(self) -> str | None:
        if self._explicit_token:
            return self._explicit_token
        tok = self._tm.get_access_token(force_refresh=False)
        return tok

    def call(self, endpoint: str, payload: dict[str, Any]) -> IFindHTTPResponse:
        base = (self._base_url or "https://quantapi.51ifind.com").rstrip("/")
        url = f"{base}/api/v1/{endpoint}"
        headers = {
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip,deflate",
        }

        token = self._resolve_token()
        if token:
            headers["access_token"] = token

        http_status: int | None = None
        raw: dict[str, Any] = {}
        errorcode = "0"
        errmsg = ""
        quota_context = ""

        def _finalize(_raw: dict[str, Any]) -> IFindHTTPResponse:
            nonlocal http_status, errorcode, errmsg, quota_context
            if isinstance(_raw, dict):
                errorcode = str(_raw.get("errorcode", errorcode))
                errmsg = str(_raw.get("errmsg", errmsg))
                quota_context = _extract_quota_context(_raw)
            payload_sha256 = sha256_hex(_safe_json(_raw).encode("utf-8"))
            return IFindHTTPResponse(
                http_status=http_status,
                errorcode=errorcode,
                errmsg=errmsg,
                quota_context=quota_context,
                raw=_raw,
                payload_sha256=payload_sha256,
            )

        # If we don't have any token, return a structured error early.
        if not token:
            raw = {"errorcode": "NO_TOKEN", "errmsg": "Missing IFIND_HTTP_TOKEN and IFIND_HTTP_REFRESH_TOKEN"}
            http_status = None
            return _finalize(raw)

        # First attempt
        try:
            r = self._client.post(url, headers=headers, json=payload)
            http_status = int(r.status_code)
            raw = r.json() if r.content else {}
            if not isinstance(raw, dict):
                raw = {"errorcode": "NON_JSON", "errmsg": "Response is not JSON", "raw": repr(raw)}

            errorcode = str(raw.get("errorcode", "0"))
            errmsg = str(raw.get("errmsg", ""))

            # On suspected auth failure, try force-refresh token once then retry.
            if self._tm.has_refresh_token() and _looks_like_auth_failure(http_status, errorcode, errmsg):
                new_tok = self._tm.force_update_access_token()
                if new_tok:
                    headers["access_token"] = new_tok
                    r2 = self._client.post(url, headers=headers, json=payload)
                    http_status = int(r2.status_code)
                    raw2 = r2.json() if r2.content else {}
                    if isinstance(raw2, dict):
                        raw = raw2
                    else:
                        raw = {"errorcode": "NON_JSON", "errmsg": "Retry response is not JSON", "raw": repr(raw2)}

            return _finalize(raw)

        except Exception as e:
            http_status = None
            raw = {"errorcode": "HTTP_EXCEPTION", "errmsg": str(e)}
            return _finalize(raw)
