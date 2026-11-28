"""lgbm_v1: LightGBM on the 34 + derived features, isotonic calibration. FROZEN.

Threads: training uses ``OMP_NUM_THREADS`` when it is set (the Oracle heal service sets 1, so a
retrain never takes the live pipeline's CPU), otherwise every core. The returned model predicts
single-threaded: on one row, thread start-up costs more than the work.
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
from lightgbm import LGBMClassifier

from sentinalops.config import DomainConfig
from sentinalops.models.base import stamp
from sentinalops.models.common.array_model_v1 import ArrayModel, calibrate_or_fallback

NAME = "lgbm_v1"
DEFAULTS: dict[str, Any] = {"n_estimators": 400, "learning_rate": 0.05, "num_leaves": 31,
                            "min_child_samples": 50, "subsample": 0.8, "subsample_freq": 1,
                            "colsample_bytree": 0.8, "verbose": -1}


def fit_threads() -> int:
    v = os.environ.get("OMP_NUM_THREADS", "").strip()
    return int(v) if v.isdigit() and int(v) > 0 else -1


class _Recipe:
    name = NAME

    def fit(self, train: pd.DataFrame, cfg: DomainConfig, seed: int = 0,
            params: dict[str, Any] | None = None) -> ArrayModel:
        params = dict(params or {})
        kw = {**DEFAULTS, "random_state": seed, "n_jobs": fit_threads(), **params}
        model, info = calibrate_or_fallback(lambda: LGBMClassifier(**kw), train, cfg)
        model.estimator.set_params(n_jobs=1)
        model.sentinal_fit_info_ = info
        return stamp(model, NAME, params)


recipe = _Recipe()
