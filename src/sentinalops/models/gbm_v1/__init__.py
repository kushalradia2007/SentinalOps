"""gbm_v1: the original champion, a scikit-learn HistGradientBoostingClassifier.

Frozen: do not change this package once models trained with it are in a registry. Make
``gbm_v2`` instead. The fitted model is the plain scikit-learn estimator (plus the recipe stamp),
so its pickles depend only on scikit-learn.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

from sentinalops.config import DomainConfig
from sentinalops.models.base import stamp

NAME = "gbm_v1"
DEFAULTS: dict[str, Any] = {"max_iter": 200, "learning_rate": 0.08}


class _Recipe:
    name = NAME

    def fit(
        self, train: pd.DataFrame, cfg: DomainConfig, seed: int = 0, params: dict[str, Any] | None = None
    ) -> HistGradientBoostingClassifier:
        params = dict(params or {})
        model = HistGradientBoostingClassifier(**{**DEFAULTS, **params}, random_state=seed)
        model.fit(train[cfg.feature_names], train["label"].astype(int))
        return stamp(model, NAME, params)


recipe = _Recipe()
