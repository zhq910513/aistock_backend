from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import select

from app.adapters.pool_fetcher import fetch_limitup_pool
from app.config import settings
from app.database.engine import SessionLocal
from app.database import models
from app.core.recommender_v1 import generate_for_batch, persist_decisions
from app.utils.crypto import sha256_hex
from app.utils.symbols import normalize_symbol
from app.utils.time import now_shanghai, trading_day_str


def _parse_hhmm(v: str, default: tuple[int, int]) -> tuple[int, int]:
    s = (v or "").strip()
    try:
        hh, mm = s.split(":", 1)
        h = int(hh)
        m = int(mm)
        if 0 <= h <= 23 and 0 <= m <= 59:
            return h, m
    except Exception:
        pass
    return default


def _allowed_set(csv: str) -> set[str]:
    return {x.strip().upper() for x in (csv or "").split(",") if x.strip()}

def get_pool_filter_rules(session) -> tuple[set[str], set[str], dict]:
    """Resolve pool filter rules.

    Priority is controlled by settings.POOL_RULES_SOURCE:
    - ENV: 仅用环境变量（上线快）
    - DB: 读取 system_settings（可在前端动态改）
    - VERSIONED: 读取 pool_filter_rule_sets（带 effective_ts，可追溯），找不到则 fallback DB/ENV

    Returns (allowed_prefixes_set, allowed_exchanges_set, rules_meta)
    """
    source = (settings.POOL_RULES_SOURCE or "ENV").strip().upper()

    def _coerce_list(v) -> list[str]:
        if v is None:
            return []
        if isinstance(v, list):
            return [str(x).strip().upper() for x in v if str(x).strip()]
        if isinstance(v, str):
            return [x.strip().upper() for x in v.split(",") if x.strip()]
        return [str(v).strip().upper()] if str(v).strip() else []

    prefixes: list[str] = []
    exchanges: list[str] = []
    rule_set_id: str | None = None

    # VERSIONED (方案3)
    if source == "VERSIONED":
        try:
            from app.database.repo import Repo

            repo = Repo(session)
            rs = repo.pool_filter_rules.get_active(now_shanghai())
            if rs is not None:
                prefixes = _coerce_list(rs.allowed_prefixes)
                exchanges = _coerce_list(rs.allowed_exchanges)
                rule_set_id = rs.rule_set_id
        except Exception:
            pass

    # DB (方案2)
    if source in {"DB", "VERSIONED"} and (not prefixes or not exchanges):
        try:
            from app.database.repo import Repo

            repo = Repo(session)
            v1 = repo.system_settings.get("pool.allowed_prefixes")
            v2 = repo.system_settings.get("pool.allowed_exchanges")
            if not prefixes:
                prefixes = _coerce_list(v1)
            if not exchanges:
                exchanges = _coerce_list(v2)
        except Exception:
            pass

    # ENV (方案1)
    if not prefixes:
        prefixes = _coerce_list(settings.POOL_ALLOWED_PREFIXES)
    if not exchanges:
        exchanges = _coerce_list(settings.POOL_ALLOWED_EXCHANGES)

    allowed_prefixes = set(prefixes)
    allowed_exchanges = set(exchanges)

    meta = {
        "source": source,
        "allowed_prefixes": sorted(list(allowed_prefixes)),
        "allowed_exchanges": sorted(list(allowed_exchanges)),
        "symbol_rule": "0/3 -> .SZ, 6 -> .SH",
    }
    if rule_set_id:
        meta["rule_set_id"] = rule_set_id
    return allowed_prefixes, allowed_exchanges, meta


def _filter_and_normalize_items(items: list[dict], allowed_prefixes: set[str], allowed_exchanges: set[str]) -> list[dict]:
    """Apply configured filters and normalize symbols."""
    out: list[dict] = []

    for it in items:
        try:
            code = str(it.get("code") or it.get("symbol") or "").strip()
            # accept vendor formats; normalize_symbol can infer.
            sym = normalize_symbol(code)
            if not sym or len(sym) < 8:
                continue
            code6 = sym.split(".", 1)[0]
            if len(code6) != 6 or not code6.isdigit():
                continue

            prefix = code6[0]
            if allowed_prefixes and prefix not in allowed_prefixes:
                continue

            ex = sym.split(".", 1)[1].upper() if "." in sym else ""
            if allowed_exchanges and ex not in allowed_exchanges:
                continue

            # clone + canonical symbol fields
            obj = dict(it)
            obj["symbol"] = sym
            obj["code"] = code6
            obj["exchange"] = ex
            out.append(obj)
        except Exception:
            continue
    return out


@dataclass
class Orchestrator:
    """Decision orchestrator (P0).

    **Hard boundary**: This orchestrator NEVER executes trades.
    It only:
    - pulls external candidate pool at 16:00 (configurable)
    - persists batch + candidates
    - when batch is committed, generates recommendations (v1)
    """

    running: bool = True

    def stop(self) -> None:
        self.running = False

    async def run(self) -> None:
        # Fast loop: keep cheap; do not busy-spin.
        while self.running:
            try:
                await self._tick()
            except Exception:
                # keep the service alive; errors are visible via logs
                pass
            await asyncio.sleep(1.0)

    async def _tick(self) -> None:
        now = now_shanghai()

        # 1) scheduled pool fetch (best-effort)
        if settings.POOL_FETCH_ENABLED:
            await self._maybe_fetch_pool(now)

        # 2) process committed batches -> decisions
        await self._process_committed_batches()

    async def _maybe_fetch_pool(self, now: datetime) -> None:
        hh, mm = _parse_hhmm(settings.POOL_FETCH_AT_HHMM, default=(16, 0))

        # only fire once per day per trading_day
        if not (now.hour == hh and now.minute == mm):
            return

        td = trading_day_str(now)

        with SessionLocal() as s:
            # already fetched today?
            exists = (
                s.execute(
                    select(models.LimitupPoolBatch)
                    .where(models.LimitupPoolBatch.trading_day == td)
                    .where(models.LimitupPoolBatch.status.in_(["FETCHED", "EDITING", "COMMITTED"]))
                    .order_by(models.LimitupPoolBatch.fetch_ts.desc())
                    .limit(1)
                )
                .scalars()
                .first()
            )
            if exists:
                return

        # fetch from external
        res = fetch_limitup_pool()
        with SessionLocal() as s:
            allowed_prefixes, allowed_exchanges, rules = get_pool_filter_rules(s)
        filtered = _filter_and_normalize_items(res.items, allowed_prefixes, allowed_exchanges)

        batch_id = sha256_hex(f"{td}|{settings.POOL_FETCH_URL}|{res.raw_hash}".encode("utf-8"))[:32]

        with SessionLocal() as s:
            s.add(
                models.LimitupPoolBatch(
                    batch_id=batch_id,
                    trading_day=td,
                    fetch_ts=now,
                    source="EXTERNAL",
                    status="FETCHED",
                    filter_rules=rules,
                    raw_hash=res.raw_hash,
                )
            )
            s.flush()

            for it in filtered:
                sym = str(it.get("symbol") or "").strip()
                name = str(it.get("name") or "").strip()
                s.add(
                    models.LimitupCandidate(
                        batch_id=batch_id,
                        symbol=sym,
                        name=name,
                        p_limit_up=None,
                        p_source="UI",
                        edited_ts=None,
                        candidate_status="PENDING_EDIT",
                        raw_json=it,
                    )
                )

            # after fetch, move to EDITING for UI
            row = s.get(models.LimitupPoolBatch, batch_id)
            if row:
                row.status = "EDITING"

            s.commit()

    async def _process_committed_batches(self) -> None:
        with SessionLocal() as s:
            batches = (
                s.execute(
                    select(models.LimitupPoolBatch)
                    .where(models.LimitupPoolBatch.status == "COMMITTED")
                    .order_by(models.LimitupPoolBatch.fetch_ts.asc())
                    .limit(5)
                )
                .scalars()
                .all()
            )

            for b in batches:
                items = generate_for_batch(s, b)
                persist_decisions(s, b, items)
                # idempotent: once decisions persisted, keep batch in COMMITTED

            s.commit()
