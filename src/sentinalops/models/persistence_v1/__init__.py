"""persistence_v1: "the book is thinning and the spread widening, so stress is coming". FROZEN.

A fixed monotone score from three of the 34 features, with no model fitting: each input is
rank-scaled against its training-data distribution (stored quantiles) and the three are averaged.
Deterministic given the training data. It is the served counterpart of the offline persistence
baseline (which needs the 10-minute depth reference, not a served feature) and the heal gate's bar.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from sentinalops.config import DomainConfig
from sentinalops.models.base import stamp
from sentinalops.models.common.array_model_v1 import ArrayModel

NAME = "persistence_v1"
INPUTS = [("depth_change_30s", -1.0), ("depth_change_60s", -1.0), ("spread_change_60s", 1.0)]
GRID = np.linspace(0, 1, 101)


class _RankScore:
    def __init__(self, feature_names: list[str]) -> None:
        self.cols = [feature_names.index(n) for n, _ in INPUTS]
        self.signs = np.array([s for _, s in INPUTS])
        self.quantiles: np.ndarray | None = None

    def fit(self, X: np.ndarray, y=None) -> _RankScore:
        self.quantiles = np.quantile(X[:, self.cols] * self.signs, GRID, axis=0)  # (101, 3)
        return self

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        V = X[:, self.cols] * self.signs
        p = np.mean([np.interp(V[:, j], self.quantiles[:, j], GRID) for j in range(V.shape[1])], axis=0)
        return np.column_stack([1 - p, p])


class _Recipe:
    name = NAME

    def fit(self, train: pd.DataFrame, cfg: DomainConfig, seed: int = 0,
            params: dict[str, Any] | None = None) -> ArrayModel:
        model = ArrayModel(None, cfg.feature_names, derive=False)
        model.estimator = _RankScore(list(cfg.feature_names)).fit(model.transform(train))
        model.sentinal_fit_info_ = {"calibrated": False, "reason": "rank score, not a probability"}
        return stamp(model, NAME, dict(params or {}))


recipe = _Recipe()
