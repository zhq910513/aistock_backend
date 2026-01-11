from __future__ import annotations

from dataclasses import dataclass
from sqlalchemy.orm import Session

from app.config import settings
from app.database.repo import Repo


@dataclass
class SourceFidelityEngine:
    """
    S³.1 Cross-Source Reconciliation (skeleton)
    - Phase-1: keep API surface stable; do not change trading behavior.
    - Phase-2: post-market daily reconciliation can write SourceFidelityDaily rows and degrade sources.
    """

    def post_market_reconcile(self, s: Session) -> None:
        """
        Placeholder for post-market reconciliation.
        Intentionally no-op in phase-1 (still logs if enabled).
        """
        if not bool(settings.POST_MARKET_RECONCILE_ENABLED):
            return
        repo = Repo(s)
        repo.system_events.write_event(
            event_type="POST_MARKET_RECONCILE_SKIPPED",
            correlation_id=None,
            severity="INFO",
            payload={"enabled": True, "note": "phase1_skeleton_noop"},
        )
