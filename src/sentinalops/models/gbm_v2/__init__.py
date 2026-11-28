"""gbm_v2: gradient boosting on the 34 + derived features, early stopping, isotonic calibration. FROZEN."""

from __future__ import annotations

from typing import Any

import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

from sentinalops.config import DomainConfig
from sentinalops.models.base import stamp
from sentinalops.models.common.array_model_v1 import ArrayModel, calibrate_or_fallback

NAME = "gbm_v2"
DEFAULTS: dict[str, Any] = {"max_iter": 400, "learning_rate": 0.05, "early_stopping": True,
                            "validation_fraction": 0.15, "l2_regularization": 1.0}


class _Recipe:
    name = NAME

    def fit(self, train: pd.DataFrame, cfg: DomainConfig, seed: int = 0,
            params: dict[str, Any] | None = None) -> ArrayModel:
        params = dict(params or {})
        kw = {**DEFAULTS, "random_state": seed, **params}
        model, info = calibrate_or_fallback(lambda: HistGradientBoostingClassifier(**kw), train, cfg)
        model.sentinal_fit_info_ = info
        return stamp(model, NAME, params)


recipe = _Recipe()
