from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import select

from app.config import settings
from app.core.data_dispatcher import DataRequestDispatcher
from app.core.labeling_planner import build_plan, calc_refresh_seconds
from app.database.engine import SessionLocal
from app.database.repo import Repo
from app.database import models
from app.utils.time import now_shanghai, now_shanghai_str


def trading_day_str(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


@dataclass
class PipelineStats:
    last_tick_ts: str | None = None
    last_error: str | None = None
    enqueued_requests: int = 0
    processed_requests: int = 0
    generated_snapshots: int = 0


class LabelingPipeline:
    """
    Background pipeline:
      - consume labeling candidates -> update watchlist
      - planner decides which data to fetch (iFinD)
      - enqueue DataRequests
      - periodically pump dispatcher to execute pending requests
      - generate basic feature snapshots for symbol lab
    """

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._stats = PipelineStats()
        self._dispatcher = DataRequestDispatcher()

    @property
    def stats(self) -> dict:
        return {
            "last_tick_ts": self._stats.last_tick_ts,
            "last_error": self._stats.last_error,
            "enqueued_requests": self._stats.enqueued_requests,
            "processed_requests": self._stats.processed_requests,
            "generated_snapshots": self._stats.generated_snapshots,
        }

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="labeling-pipeline", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def enqueue_for_candidates(self, trading_day: str, symbols: list[str]) -> int:
        if not symbols:
            return 0

        now = now_shanghai()
        td = trading_day.replace("-", "").strip()
        if len(td) == 8 and "-" in trading_day:
            td = td.replace("-", "")
        if len(td) != 8:
            td = trading_day_str(now)

        count = 0
        with SessionLocal() as s:
            repo = Repo(s)
            for sym in symbols:
                sym = (sym or "").strip()
                if not sym:
                    continue

                wl = repo.watchlist.upsert_seen(sym, td)

                # planner decides plan based on hit_count/stage
                plan = build_plan(symbol=sym, trading_day=td, hit_count=int(wl.hit_count or 0))

                # enqueue data requests (dedupe inside repo)
                for req in plan.requests:
                    if repo.data_requests.enqueue(req):
                        count += 1

                # schedule next refresh
                repo.watchlist.set_next_refresh_in(sym, calc_refresh_seconds(int(wl.hit_count or 0)))

            s.commit()

        self._stats.enqueued_requests += count
        return count

    def _run(self) -> None:
        poll_ms = int(getattr(settings, "LABELING_PIPELINE_POLL_MS", 1000))
        poll_ms = max(200, min(60000, poll_ms))

        enabled = str(getattr(settings, "LABELING_AUTO_FETCH_ENABLED", "false")).lower() in ("1", "true", "yes", "y", "on")
        if not enabled:
            # still keep thread alive for stats but do nothing heavy
            while not self._stop.is_set():
                self._stats.last_tick_ts = now_shanghai_str()
                time.sleep(poll_ms / 1000.0)
            return

        while not self._stop.is_set():
            try:
                self._tick()
                self._stats.last_error = None
            except Exception as e:
                self._stats.last_error = f"{type(e).__name__}: {e}"
            finally:
                self._stats.last_tick_ts = now_shanghai_str()
                time.sleep(poll_ms / 1000.0)

    def _tick(self) -> None:
        # 1) periodic refresh watchlist
        self._refresh_watchlist()

        # 2) pump dispatcher to execute pending requests
        processed = self._dispatcher.pump_once()
        if processed:
            self._stats.processed_requests += processed

        # 3) generate snapshots from fresh responses
        self._generate_snapshots()

    def _refresh_watchlist(self) -> None:
        now = now_shanghai()
        td = trading_day_str(now)

        max_symbols = int(getattr(settings, "LABELING_MAX_SYMBOLS_PER_CYCLE", 50))
        max_symbols = max(1, min(500, max_symbols))

        with SessionLocal() as s:
            repo = Repo(s)

            rows = repo.watchlist.list_due(max_symbols=max_symbols)
            if not rows:
                return

            enq = 0
            for row in rows:
                symbol = row.symbol
                if not symbol:
                    continue

                plan = build_plan(symbol=symbol, trading_day=td, hit_count=int(row.hit_count or 0))
                for req in plan.requests:
                    if repo.data_requests.enqueue(req):
                        enq += 1

                repo.watchlist.set_next_refresh_in(symbol, calc_refresh_seconds(int(row.hit_count or 0)))

            s.commit()

        if enq:
            self._stats.enqueued_requests += enq

    def _generate_snapshots(self) -> None:
        """
        Minimal snapshot generator:
          - find recent successful data_responses
          - upsert snapshot rows (versioned by ts)
        """
        now = now_shanghai()
        since = now - timedelta(minutes=30)

        with SessionLocal() as s:
            repo = Repo(s)

            # naive: use last 30 minutes successful responses to create simple snapshots
            q = (
                select(models.DataResponse)
                .where(models.DataResponse.created_at >= since)
                .where(models.DataResponse.status == "OK")
                .order_by(models.DataResponse.created_at.desc())
                .limit(200)
            )
            rows = list(s.execute(q).scalars())
            if not rows:
                return

            created = 0
            for r in rows:
                sym = (r.symbol or "").strip()
                if not sym:
                    continue

                # feature extraction here is intentionally minimal; store response summary for later modeling
                features = {
                    "endpoint": r.endpoint,
                    "params": r.params,
                    "summary": r.summary,
                    "created_at": r.created_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                }
                if repo.feature_snapshots.insert_snapshot(symbol=sym, feature_set="RAW_RESPONSE", features=features):
                    created += 1

            s.commit()

        if created:
            self._stats.generated_snapshots += created
