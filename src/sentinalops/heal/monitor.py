"""Live model quality from matured predictions (chunk 39) and the degradation signal.

Everything here reads the prediction ledger: only predictions whose outcome is known and not
void count. A window with too few stress events gives no verdict (NaN scores, ``degraded``
False, reason ``too_few_events``) rather than a noisy one.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass

import numpy as np

from sentinalops.config import HealConfig
from sentinalops.labels import average_precision
from sentinalops.serving.ledger import Ledger


@dataclass(frozen=True)
class WindowScores:
    model_version: str | None
    since_ms: int
    until_ms: int
    n: int  # matured, non-void predictions
    positives: int
    ap: float
    brier: float
    base_rate: float
    precision: float
    recall: float

    def to_dict(self) -> dict:
        return asdict(self)


def window_scores(
    ledger: Ledger, since_ms: int, until_ms: int, model_version: str | None = None
) -> WindowScores:
    """Scores of matured, non-void predictions made in ``[since_ms, until_ms)``."""
    rows = [r for r in ledger.predictions(since_ms, until_ms, model_version)
            if r["result"] not in ("pending", "void")]
    y = np.array([r["label"] for r in rows], dtype=float)
    p = np.array([r["probability"] for r in rows], dtype=float)
    alert = np.array([r["alert"] for r in rows], dtype=bool)
    n, pos = len(rows), int(y.sum()) if len(rows) else 0
    hits = int((alert & (y == 1)).sum())
    return WindowScores(
        model_version=model_version, since_ms=since_ms, until_ms=until_ms, n=n, positives=pos,
        ap=average_precision(y, p) if pos else math.nan,
        brier=float(np.mean((p - y) ** 2)) if n else math.nan,
        base_rate=pos / n if n else math.nan,
        precision=hits / int(alert.sum()) if alert.any() else math.nan,
        recall=hits / pos if pos else math.nan,
    )


@dataclass(frozen=True)
class Degradation:
    degraded: bool
    reason: str  # degraded | healthy | too_few_events | no_reference
    current: WindowScores
    reference_ap: float


def degradation(current: WindowScores, reference_ap: float, cfg: HealConfig) -> Degradation:
    """Degraded when live AP falls more than ``degradation_margin`` below the champion's reference.

    The reference is the AP the champion scored on its holdout when it was promoted.
    """
    if current.positives < cfg.min_positives:
        return Degradation(False, "too_few_events", current, reference_ap)
    if math.isnan(reference_ap):
        return Degradation(False, "no_reference", current, reference_ap)
    if current.ap < reference_ap - cfg.degradation_margin:
        return Degradation(True, "degraded", current, reference_ap)
    return Degradation(False, "healthy", current, reference_ap)
