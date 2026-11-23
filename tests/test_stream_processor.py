"""Stream processor (chunk 19) and online/offline feature consistency."""

import asyncio
import json

import joblib
import numpy as np
import pandas as pd
import pytest

from pipeline_helpers import H0, make_events
from sentinalops.config import get_domain_config
from sentinalops.data.pipeline import merge_events
from sentinalops.data.recorder.messages import AGG_TRADE, DEPTH5
from sentinalops.features.microstructure import compute_ticks
from sentinalops.serving.model import ChampionModel
from sentinalops.simulator.regimes import make_ticks
from sentinalops.stream.processor import FEATURES_TOPIC, PREDICTIONS_TOPIC, StreamProcessor, run
from sentinalops.training.train_baseline import fit

CFG = get_domain_config()
NAMES = CFG.feature_names
HOLE = (400, 10)  # 10 s outage, about 7 minutes in


@pytest.fixture(scope="module")
def events():
    return make_events(H0, 12 * 60, stress=((250, 20),), holes=(HOLE,), seed=11)


def replay(depth, trades, delay_ms=None):
    """``(kind, row)`` in arrival order. ``delay_ms`` per kind simulates two topics arriving skewed."""
    order = merge_events(trades, depth)
    rows = {DEPTH5: depth.to_dict("records"), AGG_TRADE: trades.to_dict("records")}
    seq = [(k, rows[k][i]) for k, i in zip(order["kind"], order["row"], strict=True)]
    if delay_ms:
        seq.sort(key=lambda e: e[1]["recv_time_ms"] + delay_ms.get(e[0], 0))  # stable: ties keep order
    return seq


def until(events, ms):
    depth, trades = events
    return depth[depth.recv_time_ms < H0 + ms], trades[trades.recv_time_ms < H0 + ms]


def feed(proc, seq):
    out = []
    for kind, row in seq:
        out += proc.on_event(kind, row)
    return out


def ticks_of(out):
    return {m.ts_ms: m.features for topic, m in out if topic == FEATURES_TOPIC}


# ---- online/offline consistency ---------------------------------------------------------------------
@pytest.mark.parametrize("delay_ms", [None, {DEPTH5: 300}, {AGG_TRADE: 450}])
def test_online_features_equal_offline_features(events, delay_ms):
    depth, trades = events
    proc = StreamProcessor(CFG, allowed_lateness_ms=500)
    online = ticks_of(feed(proc, replay(depth, trades, delay_ms)))
    offline = compute_ticks(depth, trades, CFG.feature_params())
    offline = offline[offline["valid"] & (offline.index <= proc.stats.last_tick_ms)]

    assert proc.stats.late == {AGG_TRADE: 0, DEPTH5: 0}
    assert sorted(online) == offline.index.tolist()
    got = np.array([[online[t][n] for n in NAMES] for t in offline.index])
    np.testing.assert_allclose(got, offline[NAMES].to_numpy(), rtol=1e-9, atol=1e-12)
    assert len(online) > 500


def test_too_little_lateness_drops_late_events_and_counts_them(events):
    depth, trades = events
    proc = StreamProcessor(CFG, allowed_lateness_ms=100)
    feed(proc, replay(depth, trades, {DEPTH5: 400}))
    assert proc.stats.late[DEPTH5] > 0 and proc.stats.late[AGG_TRADE] == 0


def test_buffers_stay_bounded(events):
    depth, trades = events
    proc = StreamProcessor(CFG)
    p = CFG.feature_params()
    for kind, row in replay(depth, trades):
        proc.on_event(kind, row)
        last = proc.stats.last_tick_ms
        if last is None:
            continue
        if proc.trades.times:
            assert proc.trades.times[0] > last - p.max_window_ms
        if len(proc.depth.times) > 1:
            assert proc.depth.times[1] > last - p.buffer_ms


# ---- suspension (task 18.5) ---------------------------------------------------------------------------
def test_stale_feed_suspends_ticks_until_a_full_warm_up(events):
    depth, trades = events
    proc = StreamProcessor(CFG)
    online = ticks_of(feed(proc, replay(depth, trades)))
    hole_start, hole_end = H0 + HOLE[0] * 1000, H0 + sum(HOLE) * 1000
    suspended = [t for t in range(hole_start + CFG.stale_ms + 1000, hole_end + 60_000, 1000)]
    assert not set(suspended) & set(online)
    assert hole_end + 62_000 in online  # back after a full 60 s window
    assert proc.stats.suspended_ticks >= len(suspended)


def test_wall_clock_advance_emits_suspended_ticks_when_silent():
    depth, trades = make_events(H0, 120, seed=2)
    proc = StreamProcessor(CFG)
    feed(proc, replay(depth, trades))
    before = proc.stats.ticks
    out = proc.advance(proc.watermark + 30_000)  # 30 s of silence
    stale_after = max(m.ts_ms for _, m in out) if out else proc.watermark - 30_000
    assert len(out) <= CFG.stale_data_seconds + 1  # only ticks still within the stale limit
    assert proc.stats.ticks == before + 30 and stale_after < proc.stats.last_tick_ms


# ---- predictions (task 19.4) --------------------------------------------------------------------------
@pytest.fixture(scope="module")
def champion_path(tmp_path_factory):
    path = tmp_path_factory.mktemp("m") / "champion.joblib"
    joblib.dump(fit(make_ticks("calm", 4_000, seed=1)), path)
    return path


def test_every_feature_tick_is_scored(events, champion_path):
    depth, trades = events
    proc = StreamProcessor(CFG, model=ChampionModel(champion_path, NAMES))
    out = feed(proc, replay(*until(events, 180_000)))
    feats = [m for t, m in out if t == FEATURES_TOPIC]
    preds = [m for t, m in out if t == PREDICTIONS_TOPIC]
    assert len(preds) == len(feats) > 60 and proc.stats.predictions == len(preds)
    assert [p.ts_ms for p in preds] == [f.ts_ms for f in feats]
    p = preds[0]
    assert len(p.prediction_id) == 32 and p.horizon_s == 30 and p.model_version.startswith("champion-")
    assert 0 <= p.probability <= 1 and p.alert == (p.probability >= CFG.alert_threshold)
    assert len({p.prediction_id for p in preds}) == len(preds)


def test_missing_model_still_publishes_features(events, tmp_path):
    depth, trades = events
    proc = StreamProcessor(CFG, model=ChampionModel(tmp_path / "none.joblib", NAMES))
    out = feed(proc, replay(*until(events, 90_000)))
    assert out and all(t == FEATURES_TOPIC for t, _ in out)
    assert proc.stats.scoring_errors == len(out) and proc.stats.predictions == 0


# ---- input hygiene and wiring -------------------------------------------------------------------------
def test_other_symbols_are_ignored_and_unknown_kinds_raise():
    proc = StreamProcessor(CFG)
    depth, _ = make_events(H0, 5, symbol="ETHUSDT")
    assert proc.on_depth(depth.to_dict("records")[0]) == [] and proc.stats.other_symbol == 1
    with pytest.raises(ValueError):
        proc.on_event("bookTicker", {})
    with pytest.raises(ValueError):
        StreamProcessor(CFG, allowed_lateness_ms=-1)


def test_run_publishes_json_keyed_by_symbol(events, champion_path):
    depth, trades = events
    seq = replay(*until(events, 75_000))
    published: list[tuple[str, str, dict]] = []

    async def source():
        for e in seq:
            yield e

    async def publish(topic, key, value):
        published.append((topic, key, json.loads(json.dumps(value))))

    proc = StreamProcessor(CFG, model=ChampionModel(champion_path, NAMES))
    stats = asyncio.run(run(proc, source(), publish))
    topics = pd.Series([t for t, _, _ in published]).value_counts().to_dict()
    assert topics == {FEATURES_TOPIC: stats.feature_ticks, PREDICTIONS_TOPIC: stats.predictions}
    assert stats.feature_ticks > 5 and all(k == "BTCUSDT" for _, k, _ in published)
    assert set(published[0][2]["features"]) == set(NAMES)
