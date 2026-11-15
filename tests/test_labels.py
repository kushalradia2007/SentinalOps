"""Tests for the liquidity-stress label: semantics, and proof that nothing looks ahead."""

import numpy as np
import pandas as pd
import pytest

from sentinalops.labels import (
    LabelParams,
    add_labels,
    add_stress,
    average_precision,
    base_rate_report,
    book_state,
    build_labels,
    fit_thresholds,
    label_at,
    to_ticks,
    verdict,
)

T0 = 1_763_210_000_000  # a whole second
SMALL = LabelParams(horizon_s=5, reference_window_s=20, min_reference_ticks=10, confirm_ticks=2)
CAUSAL = [
    "mid",
    "rel_spread",
    "depth_top5",
    "ref_depth",
    "ref_spread",
    "depth_ratio",
    "spread_ratio",
    "stressed",
    "confirmed",
]


def make_depth(
    seconds: int, depth=lambda s: 100.0, spread_ticks=lambda s: 1, skip=lambda s: False
) -> pd.DataFrame:
    """Snapshots every 100 ms. ``depth(s)`` is total top-5 quantity at second-offset ``s`` (float)."""
    rows = []
    for k in range(seconds * 10):
        s = k / 10
        if skip(s):
            continue
        bid, ask = 100.00, 100.00 + 0.01 * spread_ticks(s)
        row = {"symbol": "BTCUSDT", "last_update_id": k, "recv_time_ms": T0 + k * 100}
        for i in range(1, 6):
            row[f"bid_px_{i}"], row[f"bid_qty_{i}"] = bid - 0.01 * (i - 1), depth(s) / 10
            row[f"ask_px_{i}"], row[f"ask_qty_{i}"] = ask + 0.01 * (i - 1), depth(s) / 10
        rows.append(row)
    return pd.DataFrame(rows)


def at(labeled: pd.DataFrame, second: int) -> pd.Series:
    return labeled.loc[T0 + second * 1000]


def random_depth(seconds: int, seed: int = 7) -> pd.DataFrame:
    """A noisy book with sudden liquidity withdrawals and a couple of feed gaps."""
    rng = np.random.default_rng(seed)
    level = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, seconds)))
    crash = np.zeros(seconds, dtype=bool)
    for start in rng.choice(np.arange(60, seconds - 20), size=seconds // 60, replace=False):
        crash[start : start + rng.integers(1, 12)] = True
    level = np.where(crash, level * 0.3, level)
    gaps = [(200, 209), (400, 404)] if seconds > 420 else []
    return make_depth(
        seconds, depth=lambda s: float(level[int(s)]), skip=lambda s: any(a <= s < b for a, b in gaps)
    )


# ---- book state and ticks ----------------------------------------------------------------
def test_book_state_values_and_bad_books_dropped():
    depth = make_depth(1, depth=lambda s: 80.0, spread_ticks=lambda s: 2)
    depth.loc[3, "ask_px_1"] = depth.loc[3, "bid_px_1"]  # crossed book
    state = book_state(depth)
    assert len(state) == 9
    assert state["mid"].iloc[0] == pytest.approx(100.01)
    assert state["rel_spread"].iloc[0] == pytest.approx(0.02 / 100.01)
    assert state["depth_top5"].iloc[0] == pytest.approx(80.0)


def test_ticks_never_use_a_snapshot_from_after_the_tick():
    state = pd.DataFrame(
        {
            "recv_time_ms": [T0 - 50, T0 + 1, T0 + 999, T0 + 1000],
            "mid": [1.0, 2.0, 3.0, 4.0],
            "rel_spread": 0.1,
            "depth_top5": 5.0,
        }
    )
    ticks = to_ticks(state.assign(recv_time_ms=state["recv_time_ms"]), LabelParams())
    assert ticks.index.tolist() == [T0, T0 + 1000]
    assert ticks["mid"].tolist() == [1.0, 4.0]  # tick T0 ignores the snapshot 1 ms later
    assert ticks["age_ms"].tolist() == [50, 0]


def test_stale_book_makes_ticks_unknown_and_grid_stays_regular():
    depth = make_depth(20, skip=lambda s: 5 <= s < 12)
    ticks = to_ticks(book_state(depth), LabelParams(stale_ms=3000))
    assert np.all(np.diff(ticks.index) == 1000) and len(ticks) == 20
    unknown = [int((t - T0) / 1000) for t in ticks.index[ticks["depth_top5"].isna()]]
    assert unknown == [8, 9, 10, 11]  # 5-7 still within 3 s of the last snapshot at 4.9 s


# ---- causality ---------------------------------------------------------------------------
def test_reference_excludes_the_current_tick():
    depth = make_depth(40, depth=lambda s: 5.0 if s >= 30 else 100.0)
    stress = add_stress(to_ticks(book_state(depth), SMALL), SMALL)
    assert at(stress, 30)["ref_depth"] == 100.0  # the collapse at 30 s is not in its own reference
    assert at(stress, 30)["depth_ratio"] == pytest.approx(0.05)
    assert np.isnan(at(stress, 5)["ref_depth"])  # warm-up: fewer than min_reference_ticks


@pytest.mark.parametrize("cut", [120, 333, 571])
def test_stress_state_does_not_change_when_the_future_changes(cut):
    """Truncate the data at ``cut``: every causal column at or before the cut is identical."""
    depth = random_depth(600)
    full = add_stress(to_ticks(book_state(depth), SMALL), SMALL)
    past = depth[depth["recv_time_ms"] <= T0 + cut * 1000]
    truncated = add_stress(to_ticks(book_state(past), SMALL), SMALL)
    assert truncated.index[-1] == T0 + cut * 1000
    pd.testing.assert_frame_equal(truncated[CAUSAL], full.loc[truncated.index, CAUSAL])


@pytest.mark.parametrize("cut", [150, 420])
def test_label_depends_only_on_its_own_horizon(cut):
    depth = random_depth(600)
    full = build_labels(depth, SMALL)
    truncated = build_labels(depth[depth["recv_time_ms"] <= T0 + cut * 1000], SMALL)
    settled = truncated.index[: -SMALL.horizon_ticks]  # horizon ends at or before the cut
    pd.testing.assert_series_equal(truncated.loc[settled, "label"], full.loc[settled, "label"])
    assert truncated["label"].iloc[-SMALL.horizon_ticks :].isna().all()  # incomplete horizon is void


def test_vectorised_labels_match_the_slow_reference_everywhere():
    labeled = build_labels(random_depth(600), SMALL)
    assert labeled["label"].sum() > 20 and (labeled["label"] == 0).sum() > 100  # the test has teeth
    for ts, value in labeled["label"].items():
        expected = label_at(labeled, ts, SMALL)
        assert (expected is None and np.isnan(value)) or expected == value, ts


# ---- onset semantics -----------------------------------------------------------------------
def test_onset_label_by_hand():
    depth = make_depth(60, depth=lambda s: 30.0 if 40 <= s < 45 else 100.0)
    lab = build_labels(depth, SMALL)
    stressed = [s for s in range(60) if at(lab, s)["stressed"] == 1]
    confirmed = [s for s in range(60) if at(lab, s)["confirmed"] == 1]
    assert stressed == [40, 41, 42, 43, 44] and confirmed == [41, 42, 43, 44]

    positives = [s for s in range(60) if at(lab, s)["label"] == 1]
    assert positives == [36, 37, 38, 39]  # calm ticks whose next 5 s contain confirmed stress
    assert [at(lab, s)["time_to_onset_s"] for s in positives] == [5, 4, 3, 2]
    for s in range(40, 45):  # already stressed: not a forecast, so not eligible
        assert not at(lab, s)["eligible"] and np.isnan(at(lab, s)["label"])
    assert at(lab, 35)["label"] == 0 and at(lab, 46)["label"] == 0
    assert at(lab, 36)["fwd_min_depth_ratio"] == pytest.approx(0.3)
    assert np.isnan(at(lab, 58)["label"])  # horizon runs past the end of the data


def test_one_tick_flicker_is_not_an_event():
    lab = build_labels(make_depth(60, depth=lambda s: 30.0 if 40 <= s < 41 else 100.0), SMALL)
    assert at(lab, 40)["stressed"] == 1 and (lab["confirmed"] == 1).sum() == 0
    assert lab["label"].sum() == 0


def test_feed_gap_voids_every_label_whose_horizon_touches_it():
    depth = make_depth(80, depth=lambda s: 30.0 if 50 <= s < 55 else 100.0, skip=lambda s: 30 <= s < 40)
    lab = build_labels(depth, SMALL)
    unknown = [s for s in range(20, 80) if np.isnan(at(lab, s)["stressed"])]  # after the warm-up
    assert unknown == [33, 34, 35, 36, 37, 38, 39]  # 30-32 are still within stale_ms of the last snapshot
    for s in range(28, 40):  # horizon (s, s+5] overlaps an unknown tick, or the tick itself is unknown
        assert np.isnan(at(lab, s)["label"]) and not at(lab, s)["eligible"], s
    assert at(lab, 27)["label"] == 0 and at(lab, 40)["label"] == 0 and at(lab, 47)["label"] == 1


def test_spread_rule_is_secondary_and_switchable():
    depth = make_depth(60, spread_ticks=lambda s: 5 if 40 <= s < 45 else 1)
    on = build_labels(depth, SMALL)
    off = build_labels(depth, LabelParams(**{**SMALL.__dict__, "use_spread": False}))
    assert on["label"].sum() == 4 and at(on, 41)["stress_spread"] == 1 and at(on, 41)["stress_depth"] == 0
    assert off["label"].sum() == 0


# ---- thresholds, baseline, report ----------------------------------------------------------
def test_fit_thresholds_targets_the_requested_share_and_drops_a_pinned_spread():
    stress = add_stress(to_ticks(book_state(random_depth(3000, seed=3)), SMALL), SMALL)
    fitted = fit_thresholds(stress, SMALL, stress_share=0.05)
    assert fitted.use_spread is False  # spread never leaves one tick, so it has no tail to use
    refit = add_stress(stress[["mid", "rel_spread", "depth_top5", "age_ms"]], fitted)
    assert refit["stressed"].mean() == pytest.approx(0.05, abs=0.01)
    with pytest.raises(ValueError):
        fit_thresholds(stress.iloc[:200], SMALL)


def test_average_precision():
    assert average_precision(np.array([1, 0, 1, 0]), np.array([0.9, 0.8, 0.7, 0.1])) == pytest.approx(5 / 6)
    assert average_precision(np.array([0, 1, 1]), np.array([0.1, 0.9, 0.8])) == 1.0
    assert np.isnan(average_precision(np.zeros(3), np.arange(3)))


def test_base_rate_report_and_verdict():
    lab = build_labels(make_depth(60, depth=lambda s: 30.0 if 40 <= s < 45 else 100.0), SMALL)
    rep = base_rate_report(lab, SMALL)
    assert rep["episodes"] == 1 and rep["positives"] == 4 and rep["median_episode_s"] == 4
    assert rep["stress_from_spread_share"] == 0 and rep["busiest_hour_share_of_positives"] == 1.0
    notes = " ".join(verdict(rep))
    assert "NO-GO" in notes and "spread rule" in notes and "one hour" in notes

    empty = base_rate_report(build_labels(make_depth(5), SMALL), SMALL)
    assert empty["eligible_ticks"] == 0 and verdict(empty)[0].startswith("NO-GO")


def test_params_validate_and_load_from_the_domain_config():
    with pytest.raises(ValueError):
        LabelParams(depth_fraction=1.2)
    with pytest.raises(ValueError):
        LabelParams(horizon_s=0)
    p = LabelParams.from_domain_yaml("configs/domain_liquidity.yaml")
    assert (p.horizon_s, p.reference_window_s, p.depth_fraction, p.stale_ms) == (30, 600, 0.5, 3000)
    assert p.use_spread is True


def test_labels_accept_rows_exactly_as_the_recorder_writes_them():
    messages = pytest.importorskip("sentinalops.data.recorder.messages")
    from recorder_helpers import depth_frame

    rows = [messages.parse_message(depth_frame(i), T0 + i * 100)[1] for i in range(300)]
    lab = add_labels(add_stress(to_ticks(book_state(pd.DataFrame(rows)), SMALL), SMALL), SMALL)
    assert len(lab) == 30 and lab["stressed"].iloc[-1] == 0
