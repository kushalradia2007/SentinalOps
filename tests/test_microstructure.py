"""Tests for microstructure features: hand-checked values, online/offline agreement, no look-ahead."""

import math

import numpy as np
import pandas as pd
import pytest
import yaml

from sentinalops.features.microstructure import (
    FEATURE_ORDER,
    INSTANT_FEATURES,
    FeatureParams,
    book_features,
    compute_tick,
    compute_ticks,
    feature_names,
)

T0 = 1_763_210_000_000  # a whole second
P = FeatureParams()
SMALL = FeatureParams(windows_s=(1, 5), stale_ms=1500)  # warm-up is 5 s: keeps hand-made cases short


def snapshot(t_ms, bid=100.00, ask=100.01, bid_qty=1.0, ask_qty=1.0, i=0):
    row = {"symbol": "BTCUSDT", "last_update_id": i, "recv_time_ms": int(t_ms)}
    for k in range(1, 6):
        row[f"bid_px_{k}"], row[f"bid_qty_{k}"] = bid - 0.01 * (k - 1), bid_qty
        row[f"ask_px_{k}"], row[f"ask_qty_{k}"] = ask + 0.01 * (k - 1), ask_qty
    return row


def book(seconds, step_ms=100, **fn):
    """Snapshots every ``step_ms``; each keyword is a function of the second offset."""
    rows = []
    for k in range(int(seconds * 1000 / step_ms) + 1):
        s = k * step_ms / 1000
        rows.append(snapshot(T0 + k * step_ms, i=k, **{name: f(s) for name, f in fn.items()}))
    return pd.DataFrame(rows)


def trade_rows(items):
    """items: (second offset, quantity, buyer_is_maker)."""
    return pd.DataFrame(
        [{"recv_time_ms": int(T0 + s * 1000), "quantity": q, "buyer_is_maker": m} for s, q, m in items],
        columns=["recv_time_ms", "quantity", "buyer_is_maker"],
    )


NO_TRADES = trade_rows([])


def random_market(seconds, seed=11, gaps=((200, 206), (410, 412))):
    """Jittered snapshots, a wandering price, random sizes, bursty trades and two feed gaps."""
    rng = np.random.default_rng(seed)
    times = np.cumsum(rng.integers(60, 140, size=seconds * 10)) + T0
    times = times[times <= T0 + seconds * 1000]
    keep = np.ones(len(times), dtype=bool)
    for a, b in gaps:
        keep &= ~((times >= T0 + a * 1000) & (times < T0 + b * 1000))
    times = times[keep]
    mid = 100 * np.exp(np.cumsum(rng.normal(0, 2e-5, len(times))))
    rows = []
    for i, (t, m) in enumerate(zip(times, mid, strict=True)):
        half = 0.005 * rng.integers(1, 4)
        row = snapshot(t, bid=m - half, ask=m + half, i=i)
        for k in range(1, 6):
            row[f"bid_qty_{k}"], row[f"ask_qty_{k}"] = rng.uniform(0.01, 3), rng.uniform(0.01, 3)
        rows.append(row)
    n = seconds * 6
    tt = np.sort(rng.integers(T0, T0 + seconds * 1000, size=n))
    tt = tt[np.isin(tt // 1000, [(T0 // 1000) + s for a, b in gaps for s in range(a, b)], invert=True)]
    trades = pd.DataFrame(
        {
            "recv_time_ms": tt,
            "quantity": rng.exponential(0.05, len(tt)),
            "buyer_is_maker": rng.random(len(tt)) < 0.5,
        }
    )
    return pd.DataFrame(rows), trades


# ---- single snapshot (10.1, 10.2) --------------------------------------------------------------
def test_book_features_by_hand():
    f = book_features(snapshot(T0, bid=100.00, ask=100.02, bid_qty=3.0, ask_qty=1.0))
    assert f["mid"] == pytest.approx(100.01) and f["spread_abs"] == pytest.approx(0.02)
    assert f["rel_spread"] == pytest.approx(0.02 / 100.01)
    assert (f["bid_depth"], f["ask_depth"], f["depth_top5"]) == (15.0, 5.0, 20.0)
    assert f["depth_imbalance"] == pytest.approx(0.5) and f["l1_imbalance"] == pytest.approx(0.5)


def test_imbalance_limits():
    assert book_features(snapshot(T0, ask_qty=0.0))["depth_imbalance"] == 1.0
    assert book_features(snapshot(T0, bid_qty=0.0))["depth_imbalance"] == -1.0
    assert book_features(snapshot(T0))["depth_imbalance"] == 0.0
    assert book_features(snapshot(T0, bid_qty=0.0, ask_qty=0.0))["l1_imbalance"] == 0.0


def test_feature_order_matches_the_domain_config():
    assert len(FEATURE_ORDER) == len(set(FEATURE_ORDER)) == 6 + 7 * 4
    assert FEATURE_ORDER[:6] == INSTANT_FEATURES and feature_names((1, 5)) == SMALL.names
    cfg = yaml.safe_load(open("configs/domain_liquidity.yaml", encoding="utf-8"))
    assert set(cfg["drift_watch"]) <= set(FEATURE_ORDER)
    assert tuple(cfg["feature_windows_seconds"]) == P.windows_s


# ---- windows (10.3 - 10.6) ---------------------------------------------------------------------
def test_trade_features_by_hand_and_window_edges():
    depth = book(12)
    trades = trade_rows(
        [
            (5.0, 9.0, False),  # exactly at t - 5 s: outside the half-open window (t-5, t]
            (5.001, 1.0, False),  # buy-initiated (buyer is taker)
            (8.0, 3.0, True),  # sell-initiated (buyer is maker)
            (9.5, 2.0, False),
            (10.0, 4.0, True),  # exactly at t: inside
            (10.001, 50.0, False),  # after t: must be ignored
        ]
    )
    f = compute_tick(depth, trades, T0 + 10_000, SMALL)
    assert f["trade_intensity_5s"] == pytest.approx(4 / 5) and f["trade_volume_5s"] == pytest.approx(10.0)
    assert f["trade_imbalance_5s"] == pytest.approx((3.0 - 7.0) / 10.0)
    assert f["trade_intensity_1s"] == 2.0 and f["trade_volume_1s"] == pytest.approx(6.0)
    assert f["trade_imbalance_1s"] == pytest.approx((2.0 - 4.0) / 6.0)

    quiet = compute_tick(depth, NO_TRADES, T0 + 10_000, SMALL)
    assert quiet["trade_intensity_5s"] == 0 and quiet["trade_imbalance_5s"] == 0.0


def test_realized_vol_flat_and_by_hand():
    flat = compute_tick(book(12), NO_TRADES, T0 + 10_000, SMALL)
    assert flat["realized_vol_5s"] == 0.0 and flat["mid_return_5s"] == 0.0

    # mid steps 100.005 -> 100.105 at 9.5 s and stays: one non-zero return, inside 1 s and 5 s windows
    depth = book(
        12, bid=lambda s: 100.10 if s >= 9.5 else 100.00, ask=lambda s: 100.11 if s >= 9.5 else 100.01
    )
    f = compute_tick(depth, NO_TRADES, T0 + 10_000, SMALL)
    r = math.log(100.105 / 100.005)
    assert f["realized_vol_1s"] == pytest.approx(abs(r)) and f["realized_vol_5s"] == pytest.approx(abs(r))
    assert f["mid_return_1s"] == pytest.approx(r) and f["mid_return_5s"] == pytest.approx(r)
    later = compute_tick(depth, NO_TRADES, T0 + 11_000, SMALL)  # the step has left the 1 s window
    assert later["realized_vol_1s"] == 0.0 and later["realized_vol_5s"] == pytest.approx(abs(r))


def test_change_features_on_a_step():
    depth = book(12, bid_qty=lambda s: 0.5 if s >= 8 else 1.0, ask=lambda s: 100.03 if s >= 8 else 100.01)
    f = compute_tick(depth, NO_TRADES, T0 + 10_000, SMALL)
    assert f["depth_change_5s"] == pytest.approx(7.5 / 10 - 1)  # bids halved: depth 10 -> 7.5
    assert f["depth_change_1s"] == pytest.approx(0.0)  # the step is older than 1 s
    assert f["spread_change_5s"] == pytest.approx(0.03 / 100.015 - 0.01 / 100.005)
    assert f["depth_imbalance"] == pytest.approx((2.5 - 5.0) / 7.5)


# ---- validity: warm-up, staleness, recovery (10.8) ------------------------------------------------
def test_first_tick_appears_once_the_longest_window_is_full():
    ticks = compute_ticks(book(70), NO_TRADES, P)
    assert ticks.index[ticks["valid"]][0] == T0 + 60_000
    assert compute_tick(book(70), NO_TRADES, T0 + 59_000, P) is None
    assert compute_tick(book(70), NO_TRADES, T0 + 60_000, P) is not None
    assert not ticks.loc[ticks["valid"], FEATURE_ORDER].isna().any().any()  # no NaN after warm-up
    assert ticks.loc[~ticks["valid"], FEATURE_ORDER].isna().all().all()


def test_stale_book_and_full_warm_up_after_a_gap():
    depth = book(30)
    depth = depth[~depth["recv_time_ms"].between(T0 + 12_000, T0 + 15_999)]  # 4 s outage
    ticks = compute_ticks(depth, NO_TRADES, SMALL)
    valid = [int((t - T0) / 1000) for t in ticks.index[ticks["valid"]]]
    # 5 s warm-up; 12 and 13 still within stale_ms of the snapshot at 11.9 s; resumes 5 s after 16 s
    assert valid == [5, 6, 7, 8, 9, 10, 11, 12, 13, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30]


def test_crossed_and_empty_books_are_ignored():
    depth = book(12)
    bad = depth["recv_time_ms"] == T0 + 10_000
    depth.loc[bad, "ask_px_1"] = depth.loc[bad, "bid_px_1"]
    f = compute_tick(depth, NO_TRADES, T0 + 10_000, SMALL)
    assert f["age_ms"] == 100 and f["rel_spread"] == pytest.approx(0.01 / 100.005)
    assert compute_tick(depth.iloc[:0], NO_TRADES, T0, SMALL) is None
    assert len(compute_ticks(depth.iloc[:0], NO_TRADES, SMALL)) == 0


# ---- the two implementations agree ---------------------------------------------------------------
def test_vectorised_ticks_match_the_reference_on_every_tick():
    depth, trades = random_market(480)
    ticks = compute_ticks(depth, trades, P)
    assert 250 < ticks["valid"].sum() < len(ticks)  # warm-up and both gaps produce invalid ticks
    names = [*FEATURE_ORDER, "mid", "spread_abs", "age_ms"]
    for ts, row in ticks.iterrows():
        ref = compute_tick(depth, trades, int(ts), P)
        assert (ref is not None) == bool(row["valid"]), ts
        if ref is not None:
            np.testing.assert_allclose(
                row[names].to_numpy(float), [ref[n] for n in names], rtol=1e-9, atol=1e-12, err_msg=str(ts)
            )


def test_streaming_buffer_gives_the_same_tick_as_full_history():
    depth, trades = random_market(300, gaps=())
    ts = T0 + 250_000
    lo = ts - P.buffer_ms
    buffered = compute_tick(depth[depth["recv_time_ms"] > lo], trades[trades["recv_time_ms"] > lo], ts, P)
    full = compute_tick(depth, trades, ts, P)
    assert buffered is not None and buffered == pytest.approx(full, rel=1e-12)


# ---- no look-ahead ---------------------------------------------------------------------------------
@pytest.mark.parametrize("cut", [150, 333, 444])
def test_ticks_do_not_change_when_the_future_changes(cut):
    depth, trades = random_market(480)
    full = compute_ticks(depth, trades, P)
    limit = T0 + cut * 1000
    past = compute_ticks(depth[depth["recv_time_ms"] <= limit], trades[trades["recv_time_ms"] <= limit], P)
    assert limit - 1000 <= past.index[-1] <= limit and past["valid"].sum() > 50
    pd.testing.assert_frame_equal(past, full.loc[past.index], rtol=1e-9, atol=1e-12)


def test_compute_tick_ignores_everything_received_after_the_tick():
    depth, trades = random_market(300, gaps=())
    ts = T0 + 200_000
    honest = compute_tick(depth[depth["recv_time_ms"] <= ts], trades[trades["recv_time_ms"] <= ts], ts, P)
    future_depth = depth.copy()
    later = future_depth["recv_time_ms"] > ts
    future_depth.loc[later, [f"bid_qty_{k}" for k in range(1, 6)]] = 1e6  # absurd future book
    future_trades = pd.concat([trades, trade_rows([(200.001, 1e6, False)])], ignore_index=True)
    assert compute_tick(future_depth, future_trades, ts, P) == honest


def test_input_order_does_not_matter():
    depth, trades = random_market(200, gaps=())
    shuffled = compute_ticks(depth.sample(frac=1, random_state=1), trades.sample(frac=1, random_state=2), P)
    pd.testing.assert_frame_equal(shuffled, compute_ticks(depth, trades, P))


def test_a_full_day_is_fast_enough_for_the_offline_pipeline():
    import time

    depth, trades = random_market(3600, gaps=())
    start = time.perf_counter()
    ticks = compute_ticks(depth, trades, P)
    elapsed = time.perf_counter() - start
    assert len(ticks) >= 3500 and elapsed < 3.0  # an hour in well under 3 s => a day in about a minute
