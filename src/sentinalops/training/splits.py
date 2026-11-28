"""Time-ordered splits with an embargo (chunk 13.3 / 26.1).

Rows are one second apart and each label looks 30 s ahead, so neighbouring rows share outcomes.
Random splits leak; these never shuffle, and they drop ``embargo_s`` of data before every test
block so no training label's horizon reaches into it.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class Fold:
    train_idx: np.ndarray  # positions into the input array, in time order
    test_idx: np.ndarray
    train_end_ms: int  # training rows have ts < this
    test_start_ms: int
    test_end_ms: int  # exclusive


def walk_forward(ts_ms: np.ndarray, n_folds: int, min_train_s: int, embargo_s: int) -> list[Fold]:
    """Expanding-window folds: equal contiguous test blocks after ``min_train_s``; train = all before.

    Returns fewer folds (possibly none) when the data is too short, never raises for that.
    """
    ts = np.asarray(ts_ms, np.int64)
    if len(ts) == 0 or n_folds < 1:
        return []
    order = np.argsort(ts, kind="stable")
    t_first, t_end = int(ts.min()), int(ts.max()) + 1000
    start = t_first + min_train_s * 1000
    if start >= t_end:
        return []
    block = (t_end - start) / n_folds
    folds = []
    for k in range(n_folds):
        lo = int(start + k * block)
        hi = t_end if k == n_folds - 1 else int(start + (k + 1) * block)
        train_end = lo - embargo_s * 1000
        sorted_ts = ts[order]
        test = order[(sorted_ts >= lo) & (sorted_ts < hi)]
        train = order[sorted_ts < train_end]
        if len(train) and len(test):
            folds.append(Fold(train, test, train_end, lo, hi))
    return folds


def time_holdout(ts_ms: np.ndarray, fraction: float, embargo_s: int) -> tuple[np.ndarray, np.ndarray]:
    """(fit_idx, holdout_idx): the newest ``fraction`` of rows held out, after an embargo."""
    ts = np.asarray(ts_ms, np.int64)
    if not 0 < fraction < 1:
        raise ValueError("fraction must be between 0 and 1")
    order = np.argsort(ts, kind="stable")
    cut = int(ts[order][int(len(ts) * (1 - fraction))]) if len(ts) else 0
    sorted_ts = ts[order]
    return order[sorted_ts < cut - embargo_s * 1000], order[sorted_ts >= cut]
