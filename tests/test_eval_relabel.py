"""Per-fold relabelling: thresholds fitted on training time only, features computed once."""

from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from pipeline_helpers import H0, make_events, write_recording
from sentinalops.config import get_domain_config
from sentinalops.labels import apply_thresholds
from sentinalops.training.relabel import fold_params, load_recording, relabel, stress_reference

CFG = get_domain_config()
LP = CFG.label_params()
HOUR = 3_600_000


@pytest.fixture(scope="module")
def recording(tmp_path_factory):
    root = tmp_path_factory.mktemp("rec")
    depth, trades = make_events(H0, 3 * 3600, stress=((1500, 40), (4200, 30), (6600, 50)),
                                holes=((5000, 10),), seed=5)
    write_recording(root / "raw", depth, trades)
    table, depth_read, gaps = load_recording(root / "raw", root / "features", CFG)
    return table, depth_read, gaps, depth


def test_load_recording_excludes_newest_hour(recording):
    table = recording[0]
    assert len(table) > 3000
    assert table["ts_ms"].max() < H0 + 2 * HOUR  # the third (newest) hour is still "in progress"


def test_relabel_with_config_params_reproduces_pipeline_labels(recording):
    table, depth, gaps, _ = recording
    out = relabel(table, stress_reference(depth, LP), LP, gaps)
    pd.testing.assert_series_equal(out["label_status"], table["label_status"], check_names=False)
    pd.testing.assert_series_equal(out["label"].astype("Float64"), table["label"].astype("Float64"),
                                   check_names=False)
    assert (out[CFG.feature_names].to_numpy() == table[CFG.feature_names].to_numpy()).all()


def test_stricter_threshold_marks_fewer_stressed_ticks(recording):
    ref = stress_reference(recording[1], LP)
    loose = apply_thresholds(ref, replace(LP, depth_fraction=0.5))["stressed"].sum()
    strict = apply_thresholds(ref, replace(LP, depth_fraction=0.25))["stressed"].sum()
    assert strict <= loose and loose > 0


def test_gap_void_rows_stay_void(recording):
    table, depth, gaps, _ = recording
    gaps = np.array([[H0 + 1000 * 1000, H0 + 1100 * 1000]], dtype=np.int64)  # a logged outage
    out = relabel(table, stress_reference(depth, LP), LP, gaps)
    in_gap = (out["ts_ms"] > H0 + 970 * 1000) & (out["ts_ms"] < H0 + 1100 * 1000)
    assert in_gap.any() and (out.loc[in_gap, "label_status"] == "void_gap").all()
    assert out.loc[in_gap, "label"].isna().all()


def test_fold_params_use_training_period_only(recording):
    _, depth, _, _ = recording
    train_end = H0 + HOUR + 1800 * 1000
    a = fold_params(stress_reference(depth, LP), LP, train_end, 0.01)
    later = depth.copy()
    after = later["recv_time_ms"] >= train_end
    for c in [c for c in later if c.endswith("_qty_1")]:
        later.loc[after, c] *= 0.1  # the future looks completely different
    b = fold_params(stress_reference(later, LP), LP, train_end, 0.01)
    assert (a.depth_fraction, a.spread_multiple, a.use_spread) == (b.depth_fraction, b.spread_multiple,
                                                                    b.use_spread)


def test_fold_params_need_enough_history(recording):
    _, depth, _, _ = recording
    with pytest.raises(ValueError, match="at least 1000"):
        fold_params(stress_reference(depth, LP), LP, H0 + 600 * 1000, 0.01)
