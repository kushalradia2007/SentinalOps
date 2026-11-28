"""ArrayModel v1: the fitted-model shell for new packages. FROZEN.

Takes the DataFrame the system passes, converts it once to a float64 NumPy array (the 34 inputs in
order), optionally appends ``derived_v1`` columns, runs the estimator and an optional isotonic
calibrator. float64 throughout, so tiny inputs such as ``rel_spread`` (~1e-7) keep their precision.
Estimators are fitted on arrays: no per-call pandas work and no feature-name checks.
"""

from __future__ import annotations

from typing import Any

import numpy as np
from sklearn.isotonic import IsotonicRegression

from sentinalops.models.common import derived_v1
from sentinalops.training.splits import time_holdout

MIN_CAL_POSITIVES = 20


class ArrayModel:
    def __init__(self, estimator: Any, feature_names: list[str], derive: bool = True,
                 calibrator: IsotonicRegression | None = None) -> None:
        self.estimator = estimator
        self.feature_names_in_ = np.array(list(feature_names), dtype=object)
        self.classes_ = np.array([0, 1])
        self.derive = derive
        self.calibrator = calibrator

    def transform(self, X) -> np.ndarray:
        names = list(self.feature_names_in_)
        A = X[names].to_numpy(dtype=np.float64) if hasattr(X, "columns") else np.asarray(X, np.float64)
        return np.hstack([A, derived_v1.compute(A, names)]) if self.derive else A

    def predict_proba(self, X) -> np.ndarray:
        p = self.estimator.predict_proba(self.transform(X))[:, 1]
        if self.calibrator is not None:
            p = self.calibrator.predict(p)
        p = np.clip(p, 0.0, 1.0)
        return np.column_stack([1 - p, p])


def calibrate_or_fallback(make_estimator, train, cfg, derive: bool = True) -> tuple[ArrayModel, dict]:
    """Fit on all but the newest 20% (after the embargo) and isotonic-calibrate on that slice.

    Falls back to an uncalibrated fit on everything when there are no timestamps, when the
    calibration slice has fewer than ``MIN_CAL_POSITIVES`` positives, or the fit part has one class.
    """
    shell = ArrayModel(None, cfg.feature_names, derive)
    X = shell.transform(train)
    y = train["label"].to_numpy().astype(int)
    reason = None
    if "ts_ms" not in train:
        reason = "no ts_ms: cannot hold out a later slice"
    else:
        fit, hold = time_holdout(train["ts_ms"].to_numpy(), 0.2, cfg.split_embargo_seconds)
        if y[hold].sum() < MIN_CAL_POSITIVES:
            reason = f"calibration slice has {int(y[hold].sum())} positives (< {MIN_CAL_POSITIVES})"
        elif len(np.unique(y[fit])) < 2:
            reason = "fit slice has one class only"
    if reason is not None:
        shell.estimator = make_estimator().fit(X, y)
        return shell, {"calibrated": False, "reason": reason}
    shell.estimator = make_estimator().fit(X[fit], y[fit])
    raw = shell.estimator.predict_proba(X[hold])[:, 1]
    shell.calibrator = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip").fit(raw, y[hold])
    return shell, {"calibrated": True, "reason": f"isotonic on the {len(hold)} newest rows"}
