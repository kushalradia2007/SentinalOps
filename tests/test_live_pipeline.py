"""Live pipeline over the in-memory bus: raw events -> features/predictions/outcomes -> ledger."""

import asyncio
import math

import joblib
import numpy as np
import pandas as pd
import pytest

from pipeline_helpers import H0, make_events
from sentinalops.config import get_domain_config
from sentinalops.data.pipeline import merge_events
from sentinalops.data.recorder.messages import AGG_TRADE, DEPTH5
from sentinalops.features.microstructure import compute_ticks
from sentinalops.labels import build_labels
from sentinalops.live.bus import TOPIC_OF_KIND, InMemoryBus
from sentinalops.live.pipeline import clock_step, start_pipeline
from sentinalops.serving.ledger import Ledger
from sentinalops.serving.model import ChampionModel
from sentinalops.simulator.regimes import make_ticks
from sentinalops.stream.processor import StreamProcessor
from sentinalops.stream.verifier import OutcomeVerifier
from sentinalops.training.train_baseline import fit

CFG = get_domain_config()
NAMES = CFG.feature_names


@pytest.fixture(scope="module")
def events():
    return make_events(H0, 1800, stress=((900, 40),), holes=((1300, 10),), seed=21)


@pytest.fixture(scope="module")
def model_file(tmp_path_factory):
    path = tmp_path_factory.mktemp("m") / "champion.joblib"
    joblib.dump(fit(make_ticks("calm", 4000, seed=2)), path)
    return path


def arrival_order(depth, trades):
    order = merge_events(trades, depth)
    rows = {DEPTH5: depth.to_dict("records"), AGG_TRADE: trades.to_dict("records")}
    return [(k, rows[k][i]) for k, i in zip(order["kind"], order["row"], strict=True)]


async def replay(seq, ledger, model_path, seconds=None, shadow_path=None):
    """Deterministic event time: publish one second of events, drain, then step the clock."""
    bus = InMemoryBus()
    proc = StreamProcessor(CFG, model=ChampionModel(model_path, NAMES),
                           shadow=ChampionModel(shadow_path, NAMES) if shadow_path else None)
    verifier = OutcomeVerifier(CFG)
    tasks, stats = await start_pipeline(bus, proc, verifier, ledger)
    by_second: dict[int, list] = {}
    for kind, row in seq:
        by_second.setdefault(row["recv_time_ms"] // 1000, []).append((kind, row))
    first, last = min(by_second), max(by_second)
    end = first + seconds if seconds else last + 1
    for s in range(first, end):
        for kind, row in by_second.get(s, []):
            await bus.publish(TOPIC_OF_KIND[kind], row["symbol"], row)
        await bus.drain()
        await clock_step(bus, proc, verifier, (s + 1) * 1000, stats)
        await bus.drain()
    await bus.close()
    await asyncio.gather(*tasks)
    return proc, verifier, stats


def test_replay_matches_offline(events, model_file, tmp_path):
    depth, trades = events
    led = Ledger(tmp_path / "l.sqlite")
    proc, verifier, stats = asyncio.run(replay(arrival_order(depth, trades), led, model_file))
    rows = led.predictions(0)
    assert len(rows) == proc.stats.predictions > 1000 and stats.ledger_errors == 0

    offline = compute_ticks(depth, trades, CFG.feature_params())
    offline = offline[offline["valid"]]
    online = pd.DataFrame([{**r["features"], "ts_ms": r["ts_ms"]} for r in rows])
    online = online.set_index("ts_ms").sort_index()
    assert set(online.index) <= set(offline.index)
    np.testing.assert_allclose(online[NAMES].to_numpy(), offline.loc[online.index, NAMES].to_numpy(),
                               rtol=1e-9, atol=1e-12)

    labels = build_labels(depth, CFG.label_params())["label"]
    matured = [r for r in rows if r["matured"]]
    assert len(matured) > 1000
    for r in matured:
        want = labels.get(r["ts_ms"], math.nan)
        got = math.nan if r["label"] is None else float(r["label"])
        assert (math.isnan(want) and math.isnan(got)) or want == got, r["ts_ms"]
    assert any(r["void_reason"] == "gap" for r in matured)
    assert proc.stats.late == {AGG_TRADE: 0, DEPTH5: 0} and verifier.stats.late == 0


def test_silent_feed_suspends_ticks(events, model_file, tmp_path):
    depth, trades = events
    cut = H0 + 120_000
    seq = arrival_order(depth[depth.recv_time_ms < cut], trades[trades.recv_time_ms < cut])

    async def main():
        bus = InMemoryBus()
        proc = StreamProcessor(CFG, model=ChampionModel(model_file, NAMES))
        verifier = OutcomeVerifier(CFG)
        tasks, stats = await start_pipeline(bus, proc, verifier, Ledger(tmp_path / "l.sqlite"))
        for kind, row in seq:
            await bus.publish(TOPIC_OF_KIND[kind], row["symbol"], row)
        await bus.drain()
        before = proc.stats.predictions
        # The feed goes silent; only the wall clock moves. The book stays fresh for
        # stale_data_seconds (3 s), so a few more ticks are scored, then scoring must stop.
        for s in range(1, 6):
            await clock_step(bus, proc, verifier, cut + s * 1000, stats)
        after_5s, suspended_5s = proc.stats.predictions, proc.stats.suspended_ticks
        for s in range(6, 16):
            await clock_step(bus, proc, verifier, cut + s * 1000, stats)
        await bus.drain()
        await bus.close()
        await asyncio.gather(*tasks)
        return proc, before, after_5s, suspended_5s

    proc, before, after_5s, suspended_5s = asyncio.run(main())
    assert before > 30 and after_5s - before <= CFG.stale_data_seconds + 1
    assert proc.stats.predictions == after_5s, "no predictions once the book is stale"
    assert proc.stats.suspended_ticks - suspended_5s == 10


def test_no_model_then_model_appears(events, model_file, tmp_path):
    depth, trades = events
    path = tmp_path / "late" / "champion.joblib"
    seq = arrival_order(*[d[d.recv_time_ms < H0 + 150_000] for d in events])
    first = [e for e in seq if e[1]["recv_time_ms"] < H0 + 90_000]
    rest = [e for e in seq if e[1]["recv_time_ms"] >= H0 + 90_000]

    async def main():
        bus = InMemoryBus()
        proc = StreamProcessor(CFG, model=ChampionModel(path, NAMES))
        verifier = OutcomeVerifier(CFG)
        tasks, stats = await start_pipeline(bus, proc, verifier, Ledger(tmp_path / "l.sqlite"))
        for kind, row in first:
            await bus.publish(TOPIC_OF_KIND[kind], row["symbol"], row)
        await bus.drain()
        feats_before, preds_before = proc.stats.feature_ticks, proc.stats.predictions
        path.parent.mkdir(parents=True)
        joblib.dump(joblib.load(model_file), path)
        for kind, row in rest:
            await bus.publish(TOPIC_OF_KIND[kind], row["symbol"], row)
        await bus.drain()
        await bus.close()
        await asyncio.gather(*tasks)
        return proc, feats_before, preds_before

    proc, feats_before, preds_before = asyncio.run(main())
    assert feats_before > 20 and preds_before == 0 and proc.stats.scoring_errors == feats_before
    assert proc.stats.predictions > 30


def test_restart_does_not_duplicate_predictions(events, model_file, tmp_path):
    seq = arrival_order(*[d[d.recv_time_ms < H0 + 120_000] for d in events])
    led = Ledger(tmp_path / "l.sqlite")
    asyncio.run(replay(seq, led, model_file))
    n = len(led.predictions(0))
    asyncio.run(replay(seq, led, model_file))  # a restarted pipeline re-scores the same ticks
    assert n > 30 and len(led.predictions(0)) == n
