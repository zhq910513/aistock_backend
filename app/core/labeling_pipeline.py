from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass

from app.config import settings
from app.core.data_dispatcher import DataRequestDispatcher
from app.core.labeling_planner import build_plan
from app.database.engine import SessionLocal
from app.database.repo import Repo
from app.utils.symbols import normalize_symbol
from app.utils.time import now_shanghai, now_shanghai_str


_LOG_PATH = "/tmp/labeling_pipeline.log"


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("labeling_pipeline")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    # stdout
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # file (best-effort)
    try:
        fh = logging.FileHandler(_LOG_PATH)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except Exception:
        # Noisy environments / readonly FS: ignore.
        pass

    return logger


@dataclass
class PipelineStats:
    started_at: str
    last_tick_ts: str | None = None
    last_error: str | None = None
    enqueued_requests: int = 0
    processed_requests: int = 0


class LabelingPipeline:
    """Background pipeline for labeling data factory.

    Responsibilities (minimal, production-safe):
    1) Periodically expand watchlist symbols into DataRequests (planner).
    2) Pump DataRequestDispatcher so PENDING -> SENT/RECEIVED/FAILED progresses.
    """

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._t: threading.Thread | None = None
        self._dispatcher = DataRequestDispatcher()
        self._log = _setup_logger()
        self.stats = PipelineStats(started_at=now_shanghai_str())

    def start(self) -> None:
        if self._t and self._t.is_alive():
            return
        self._t = threading.Thread(target=self._run, name="labeling_pipeline", daemon=True)
        self._t.start()
        self._log.info("labeling_pipeline_started poll_ms=%s", settings.LABELING_PIPELINE_POLL_MS)

    def stop(self) -> None:
        self._stop.set()
        if self._t:
            self._t.join(timeout=2)

    def _run(self) -> None:
        while not self._stop.is_set():
            t0 = time.time()
            try:
                self._tick()
                self.stats.last_error = None
            except Exception as e:
                self.stats.last_error = repr(e)
                self._log.exception("labeling_pipeline_tick_error: %r", e)

            self.stats.last_tick_ts = now_shanghai_str()

            # sleep
            poll_s = max(int(settings.LABELING_PIPELINE_POLL_MS), 50) / 1000.0
            # Maintain roughly poll interval
            dt = time.time() - t0
            if dt < poll_s:
                time.sleep(poll_s - dt)

    def _tick(self) -> None:
        enq = 0
        processed = 0

        with SessionLocal() as s:
            repo = Repo(s)

            # 1) planner: schedule due watchlist symbols (best-effort)
            try:
                due = repo.watchlist.due_for_refresh(max_symbols=int(settings.LABELING_MAX_SYMBOLS_PER_CYCLE))
                for row in due:
                    if not row.active:
                        continue

                    # Canonicalize symbol early; never let invalid/empty symbol spawn requests.
                    raw_sym = str(row.symbol or "").strip()
                    sym = normalize_symbol(raw_sym)
                    if not sym:
                        # should never happen, but if it does: disable this row to stop endless retries
                        row.active = False
                        row.next_refresh_at = None
                        row.updated_at = now_shanghai()
                        self._log.warning("watchlist_row_invalid_symbol_disabled: raw=%r", raw_sym)
                        continue

                    plan = build_plan(
                        symbol=sym,
                        hit_count=int(row.hit_count or 0),
                        planner_state=dict(row.planner_state or {}),
                    )

                    # persist planner state (planner decides next stages)
                    row.symbol = sym
                    row.planner_state = plan.planner_state

                    for pr in plan.requests:
                        # PlannedRequest is frozen; do not mutate.
                        # build_plan() already guarantees pr.symbol is present and payload includes symbol/thscode.
                        _rid, created = repo.data_requests.enqueue_planned(pr, provider=settings.DATA_PROVIDER)
                        if created:
                            enq += 1

                    # refresh policy
                    next_sec = int(plan.planner_state.get("next_refresh_in_sec") or settings.LABELING_REFRESH_ACTIVE_SEC)
                    repo.watchlist.set_next_refresh_in(sym, next_sec)

            except Exception as e:
                # do not block dispatcher on planner failures
                self._log.warning("planner_failed: %r", e)

            # 2) dispatcher: execute pending requests
            res = self._dispatcher.pump_once(s, limit=200)
            processed = (
                int(getattr(res, "sent", 0) or 0)
                + int(getattr(res, "received", 0) or 0)
                + int(getattr(res, "failed", 0) or 0)
            )

            s.commit()

        if enq or processed:
            self.stats.enqueued_requests += enq
            self.stats.processed_requests += processed
            self._log.info("tick enqueued=%s processed=%s", enq, processed)


def main() -> None:
    # Foreground runner for debugging inside container:
    #   cd /app && python -m app.core.labeling_pipeline
    lp = LabelingPipeline()
    lp.start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        lp.stop()


if __name__ == "__main__":
    main()
