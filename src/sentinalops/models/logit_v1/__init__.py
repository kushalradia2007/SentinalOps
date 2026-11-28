"""logit_v1: standardised, class-weighted logistic regression on the 34 + derived features. FROZEN."""

from __future__ import annotations

from typing import Any

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from sentinalops.config import DomainConfig
from sentinalops.models.base import stamp
from sentinalops.models.common.array_model_v1 import ArrayModel, calibrate_or_fallback

NAME = "logit_v1"
DEFAULTS: dict[str, Any] = {"C": 1.0, "max_iter": 1000}


class _Recipe:
    name = NAME

    def fit(self, train: pd.DataFrame, cfg: DomainConfig, seed: int = 0,
            params: dict[str, Any] | None = None) -> ArrayModel:
        params = dict(params or {})
        kw = {**DEFAULTS, **params}

        def make():
            return make_pipeline(StandardScaler(),
                                 LogisticRegression(class_weight="balanced", random_state=seed, **kw))

        model, info = calibrate_or_fallback(make, train, cfg)
        model.sentinal_fit_info_ = info
        return stamp(model, NAME, params)


recipe = _Recipe()
