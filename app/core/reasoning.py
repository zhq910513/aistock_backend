from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.config import settings
from app.utils.crypto import sha256_hex


@dataclass
class ReasoningOutput:
    score: float          # higher => more bullish
    confidence: float     # 0..1
    reason_code: str
    params: dict


class ReasoningEngine:
    """
    推理大脑（纯推理/打分）：不直接碰数据、不下单。
    """
    def infer(self, features: dict[str, Any]) -> ReasoningOutput:
        # Minimal deterministic scoring:
        # - expect features contain momentum_3d, ret_1d, vol_proxy
        m3 = float(features.get("momentum_3d", 0.0))
        r1 = float(features.get("ret_1d", 0.0))
        v = float(features.get("vol_proxy", 0.0))

        score = 0.7 * m3 + 0.3 * r1 - 0.05 * v
        confidence = max(0.0, min(1.0, 0.5 + 0.5 * abs(score)))

        reason_code = "RC_SCORE_V1"
        params = {"momentum_3d": m3, "ret_1d": r1, "vol_proxy": v, "score": score}

        return ReasoningOutput(score=score, confidence=confidence, reason_code=reason_code, params=params)

    def model_hash(self) -> str:
        # binds model identity to frozen versions
        material = "|".join(
            [
                settings.RULESET_VERSION_HASH,
                settings.MODEL_SNAPSHOT_UUID,
                settings.STRATEGY_CONTRACT_HASH,
                settings.FEATURE_EXTRACTOR_VERSION,
                settings.COST_MODEL_VERSION,
            ]
        )
        return sha256_hex(material.encode("utf-8"))

    def versions(self) -> dict:
        return {
            "RuleSetVersionHash": settings.RULESET_VERSION_HASH,
            "ModelSnapshotUUID": settings.MODEL_SNAPSHOT_UUID,
            "StrategyContractHash": settings.STRATEGY_CONTRACT_HASH,
            "FeatureExtractorVersion": settings.FEATURE_EXTRACTOR_VERSION,
            "CostModelVersion": settings.COST_MODEL_VERSION,
        }
