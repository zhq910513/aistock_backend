from __future__ import annotations

from dataclasses import dataclass
from sqlalchemy.orm import Session

from app.config import settings
from app.database.repo import Repo


@dataclass
class DifferentialAuditEngine:
    """
    S³.1 Differential Audit (skeleton)
    - Phase-1: keep contract; record placeholder event when enabled.
    - Phase-2: implement ShadowLive CONTRA_DECISION_EVENT and comparisons.
    """

    def maybe_record_shadow(self, s: Session, payload: dict) -> None:
        if not bool(settings.SHADOW_LIVE_ENABLED):
            return
        repo = Repo(s)
        repo.system_events.write_event(
            event_type="SHADOW_LIVE_PLACEHOLDER",
            correlation_id=payload.get("correlation_id"),
            severity="INFO",
            payload={"note": "phase1_skeleton", "payload": payload},
        )
