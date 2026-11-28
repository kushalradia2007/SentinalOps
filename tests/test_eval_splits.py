"""Walk-forward splits: chronological, expanding, embargoed. A random split would leak."""

import numpy as np
import pytest

from sentinalops.training.splits import time_holdout, walk_forward

TS = 1_763_200_800_000 + np.arange(10 * 3600) * 1000  # ten hours at 1 Hz


def _has_overlap(train_ts, test_ts, horizon_s=30) -> bool:
    """True if a training label's horizon (t, t + H] reaches into the test period."""
    return bool(len(train_ts) and len(test_ts) and train_ts.max() + horizon_s * 1000 >= test_ts.min())


def test_walk_forward_is_chronological_with_embargo():
    folds = walk_forward(TS, n_folds=3, min_train_s=2 * 3600, embargo_s=60)
    assert len(folds) == 3
    for f in folds:
        assert TS[f.train_idx].max() + 60_000 < TS[f.test_idx].min()
    for a, b in zip(folds, folds[1:], strict=False):
        assert TS[a.test_idx].max() < TS[b.test_idx].min()


def test_walk_forward_expands_training():
    folds = walk_forward(TS, n_folds=4, min_train_s=3600, embargo_s=60)
    sizes = [len(f.train_idx) for f in folds]
    assert sizes == sorted(sizes) and sizes[0] < sizes[-1]


def test_walk_forward_unsorted_input_and_too_short_data():
    shuffled = np.random.default_rng(0).permutation(TS)
    folds = walk_forward(shuffled, n_folds=2, min_train_s=3600, embargo_s=60)
    for f in folds:
        assert shuffled[f.train_idx].max() < shuffled[f.test_idx].min()
    assert walk_forward(TS[:600], n_folds=3, min_train_s=3600, embargo_s=60) == []  # 10 min of data


def test_time_holdout_embargo():
    fit, hold = time_holdout(TS, fraction=0.2, embargo_s=60)
    assert TS[fit].max() + 60_000 < TS[hold].min()
    assert len(hold) == pytest.approx(0.2 * len(TS), rel=0.01)


def test_embargo_check_catches_leakage():
    """Mutation guard: without the embargo, training horizons reach into the test block."""
    leaky = walk_forward(TS, n_folds=3, min_train_s=2 * 3600, embargo_s=0)
    safe = walk_forward(TS, n_folds=3, min_train_s=2 * 3600, embargo_s=60)
    assert any(_has_overlap(TS[f.train_idx], TS[f.test_idx]) for f in leaky)
    assert not any(_has_overlap(TS[f.train_idx], TS[f.test_idx]) for f in safe)
