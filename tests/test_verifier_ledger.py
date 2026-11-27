"""Outcome verifier and prediction ledger (chunk 34)."""

import math
import sqlite3

import joblib
import numpy as np
import pandas as pd
import pytest

from pipeline_helpers import H0, make_events
from sentinalops.config import DomainConfig, get_domain_config
from sentinalops.labels import add_stress, book_state, build_labels, label_at, to_ticks
from sentinalops.schemas import OutcomeLabel, StressPrediction
from sentinalops.serving.ledger import Ledger, classify
from sentinalops.serving.model import ChampionModel
from sentinalops.simulator.regimes import make_ticks
from sentinalops.stream.processor import FEATURES_TOPIC, PREDICTIONS_TOPIC, StreamProcessor
from sentinalops.stream.verifier import OutcomeVerifier
from sentinalops.training.train_baseline import fit

CFG = get_domain_config()
P = CFG.label_params()
H = P.horizon_s * 1000
HOLE = (1000, 8)


@pytest.fixture(scope="module")
def depth():
    d, _ = make_events(H0, 30 * 60, stress=((700, 20), (1300, 15)), holes=(HOLE,), seed=4)
    return d


def verify(cfg, rows, lateness_ms=500):
    v = OutcomeVerifier(cfg, allowed_lateness_ms=lateness_ms)
    out = []
    for r in rows:
        out += v.on_depth(r)
    return v, {o.ts_ms: o for o in out}


def as_label(o: OutcomeLabel) -> float:
    return math.nan if o.label is None else float(o.label)


# ---- 34.7 agreement with the offline label ----------------------------------------------------------
def test_verifier_agrees_with_offline_labels_on_every_tick(depth):
    v, online = verify(CFG, depth.to_dict("records"))
    offline = build_labels(depth, P)
    assert len(online) == len(offline) - P.horizon_ticks  # everything but the unresolved last 30 s
    on = pd.Series({t: as_label(o) for t, o in online.items()})
    off = offline["label"].reindex(on.index)
    assert ((on == off) | (on.isna() & off.isna())).all()
    assert (on == 1).sum() > 30 and v.stats.late == 0


def test_verifier_agrees_with_label_at(depth):
    _, online = verify(CFG, depth.to_dict("records"))
    stress = add_stress(to_ticks(book_state(depth), P), P)
    for t in np.random.default_rng(1).choice(sorted(online), 200, replace=False):
        assert online[int(t)].label == label_at(stress, int(t), P)


def test_spread_rule_off_still_matches(depth):
    cfg = DomainConfig(**{**CFG.model_dump(), "label": {**CFG.label.model_dump(), "secondary": None}})
    _, online = verify(cfg, depth.to_dict("records"))
    off = build_labels(depth, cfg.label_params())["label"]
    on = pd.Series({t: as_label(o) for t, o in online.items()})
    assert ((on == off.reindex(on.index)) | (on.isna() & off.reindex(on.index).isna())).all()


def test_out_of_order_snapshots_within_lateness_change_nothing(depth):
    rows = depth.to_dict("records")
    for i in range(0, len(rows) - 1, 7):  # swap neighbours: up to about 540 ms apart
        rows[i], rows[i + 1] = rows[i + 1], rows[i]
    v, shuffled = verify(CFG, rows, lateness_ms=1000)
    _, ordered = verify(CFG, depth.to_dict("records"))
    assert v.stats.late == 0
    assert len(ordered) - len(shuffled) <= 1  # the larger lateness holds back the final tick
    assert all(shuffled[t].label == ordered[t].label for t in shuffled)


# ---- void reasons (34.4) -------------------------------------------------------------------------
def test_void_reasons(depth):
    _, online = verify(CFG, depth.to_dict("records"))
    stress = add_stress(to_ticks(book_state(depth), P), P)
    hole_lo, hole_hi = H0 + HOLE[0] * 1000, H0 + sum(HOLE) * 1000
    for t, o in online.items():
        if o.void_reason == "already_stressed":
            assert stress.at[t, "stressed"] == 1
        elif o.void_reason == "gap":
            assert hole_lo - H - 5_000 <= t <= hole_hi + 5_000
        elif o.void_reason == "warmup":
            assert t < H0 + (P.min_reference_ticks + 5) * 1000
    reasons = pd.Series([o.void_reason for o in online.values()]).value_counts()
    assert {"already_stressed", "gap", "warmup"} <= set(reasons.index)


# ---- causality (34.2) -----------------------------------------------------------------------------
def test_outcome_is_only_emitted_after_its_horizon_and_ignores_later_data(depth):
    v = OutcomeVerifier(CFG)
    for r in depth.to_dict("records"):
        for o in v.on_depth(r):
            assert v.watermark >= o.ts_ms + H + v.lateness  # never resolved early
    _, full = verify(CFG, depth.to_dict("records"))
    cut = H0 + 1200 * 1000
    _, early = verify(CFG, depth[depth.recv_time_ms < cut].to_dict("records"))
    common = [t for t in early if t + H + P.stale_ms < cut]
    assert len(common) > 1000
    assert all((early[t].label, early[t].void_reason) == (full[t].label, full[t].void_reason) for t in common)


def test_other_symbols_and_crossed_books():
    v = OutcomeVerifier(CFG)
    d, _ = make_events(H0, 3, symbol="ETHUSDT")
    assert v.on_depth(d.to_dict("records")[0]) == [] and v.stats.other_symbol == 1
    d, _ = make_events(H0, 3)
    bad = {**d.to_dict("records")[0], "bid_px_1": 1e9}
    v.on_depth(bad)
    assert v.stats.rejected_books == 1 and v.next_tick is None


# ---- ledger ---------------------------------------------------------------------------------------
def pred(ts, p=0.8, alert=True, version="v1", pid=None):
    kw = dict(symbol="BTCUSDT", ts_ms=ts, horizon_s=30, probability=p, alert=alert, threshold=0.5,
              model_version=version)
    return StressPrediction(**kw, **({"prediction_id": pid} if pid else {}))


def outcome(ts, label, reason=None):
    return OutcomeLabel(symbol="BTCUSDT", ts_ms=ts, horizon_s=30, label=label, void_reason=reason)


def test_ledger_is_append_only_idempotent_and_durable(tmp_path):
    path = tmp_path / "l.sqlite"
    led = Ledger(path)
    p = pred(1_000, pid="a" * 32)
    assert led.add_prediction(p, {"rel_spread": 1e-7}) is True
    assert led.add_prediction(p) is False  # replay ignored
    assert led.add_outcome(outcome(1_000, 1)) is True
    assert led.add_outcome(outcome(1_000, 0)) is False  # first outcome wins
    with sqlite3.connect(path) as db, pytest.raises(sqlite3.DatabaseError, match="append-only"):
        db.execute("UPDATE predictions SET probability = 0")
    with sqlite3.connect(path) as db, pytest.raises(sqlite3.DatabaseError, match="append-only"):
        db.execute("DELETE FROM outcomes")
    row = Ledger(path).get("a" * 32)  # reopened: survives restarts
    assert row["result"] == "hit" and row["features"] == {"rel_spread": 1e-7} and row["label"] == 1
    assert Ledger(path).get("missing") is None


def test_classification_table():
    assert classify(True, 1, True) == "hit"
    assert classify(False, 1, True) == "miss"
    assert classify(True, 0, True) == "false_alarm"
    assert classify(False, 0, True) == "correct_negative"
    assert classify(True, None, True) == "void"
    assert classify(True, None, False) == "pending"


def test_scores_for_a_window_and_model_version(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    cases = [  # ts, probability, alert, version, label (or reason)
        (1, 0.9, True, "v1", 1), (2, 0.7, True, "v1", 0), (3, 0.2, False, "v1", 1),
        (4, 0.1, False, "v1", 0), (5, 0.6, True, "v1", None), (6, 0.3, False, "v1", "pending"),
        (7, 0.9, True, "v2", 1),
    ]
    for ts, p, alert, version, label in cases:
        led.add_prediction(pred(ts * 1000, p, alert, version))
        if label != "pending":
            led.add_outcome(outcome(ts * 1000, label, "gap" if label is None else None))
    s = led.scores(model_version="v1")
    assert (s.hit, s.false_alarm, s.miss, s.correct_negative, s.void, s.pending) == (1, 1, 1, 1, 1, 1)
    assert s.brier == pytest.approx(((0.9 - 1) ** 2 + 0.7**2 + (0.2 - 1) ** 2 + 0.1**2) / 4)
    assert s.precision == 0.5 and s.recall == 0.5 and s.base_rate == 0.5
    assert led.scores(since_ms=3000, until_ms=5000).scored == 2
    assert led.scores().hit == 2
    empty = led.scores(since_ms=10**9)
    assert empty.scored == 0 and math.isnan(empty.brier) and math.isnan(empty.precision)
    assert set(empty.to_dict()) >= {"precision", "recall", "brier", "scored"}


# ---- the whole loop -------------------------------------------------------------------------------
def test_processor_verifier_and_ledger_close_the_loop(tmp_path):
    d, t = make_events(H0, 7 * 60, stress=((320, 20), (380, 15)), seed=8)
    model_path = tmp_path / "m.joblib"
    joblib.dump(fit(make_ticks("calm", 4_000, seed=2)), model_path)
    proc = StreamProcessor(CFG, model=ChampionModel(model_path, CFG.feature_names))
    verifier = OutcomeVerifier(CFG)
    led = Ledger(tmp_path / "ledger.sqlite")

    events = [("depth5", r) for r in d.to_dict("records")] + [("aggTrade", r) for r in t.to_dict("records")]
    events.sort(key=lambda e: e[1]["recv_time_ms"])
    features: dict[int, dict] = {}
    for kind, row in events:
        for topic, msg in proc.on_event(kind, row):
            if topic == FEATURES_TOPIC:
                features[msg.ts_ms] = msg.features
            elif topic == PREDICTIONS_TOPIC:
                led.add_prediction(msg, features[msg.ts_ms])
        led.add_outcomes(verifier.on_event(kind, row))

    rows = led.predictions()
    assert len(rows) == proc.stats.predictions > 300
    results = pd.Series([r["result"] for r in rows]).value_counts().to_dict()
    assert set(results) <= {"hit", "miss", "false_alarm", "correct_negative", "void", "pending"}
    assert results.get("pending", 0) <= P.horizon_ticks + 1  # only the last 30 s are unresolved
    s = led.scores()
    assert s.scored + s.void + s.pending == len(rows) and s.hit + s.miss > 0
    assert 0 <= s.brier <= 1


def test_ledger_wal_enabled(tmp_path):
    import sqlite3

    Ledger(tmp_path / "l.sqlite")
    with sqlite3.connect(tmp_path / "l.sqlite") as db:
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


def test_ledger_table_filters_void_and_dedupes_shadow(tmp_path):
    from sentinalops.heal.ledger_table import ledger_table

    led = Ledger(tmp_path / "l.sqlite")
    names = CFG.feature_names
    t0 = 1_763_200_800_000
    for i in range(4):
        ts = t0 + i * 1000
        for version, scored, value in (("v002", 20, 2.0), ("v001", 10, 1.0)):  # shadow scored first
            led.add_prediction(
                StressPrediction(symbol="BTCUSDT", ts_ms=ts, horizon_s=30, probability=0.1, alert=False,
                                 threshold=0.5, model_version=version, scored_at_ms=ts + scored),
                dict.fromkeys(names, value + i))
        label, reason = ((None, "gap") if i == 2 else (i % 2, None))
        led.add_outcome(OutcomeLabel(symbol="BTCUSDT", ts_ms=ts, horizon_s=30, label=label,
                                     void_reason=reason))
    led.add_prediction(StressPrediction(symbol="BTCUSDT", ts_ms=t0 + 9000, horizon_s=30, probability=0.1,
                                        alert=False, threshold=0.5, model_version="v001"),
                       dict.fromkeys(names, 0.0))  # not matured yet
    df = ledger_table(led, t0, t0 + 60_000, names)
    assert list(df["ts_ms"]) == [t0, t0 + 1000, t0 + 3000]  # void and pending rows are gone
    assert list(df["label"]) == [0, 1, 1]
    assert list(df[names[0]]) == [1.0, 2.0, 4.0]  # the earlier-scored (v001) features
    assert (df["label_status"] == "ok").all() and (df["regime"] == "untagged").all()
    assert ledger_table(led, t0 + 1000, t0 + 3000, names)["ts_ms"].tolist() == [t0 + 1000]


def test_ledger_add_many_is_one_idempotent_batch(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    t0 = 1_763_200_800_000
    preds = [(StressPrediction(prediction_id=f"{i:032x}", symbol="BTCUSDT", ts_ms=t0 + i * 1000,
                               horizon_s=30, probability=0.2, alert=False, threshold=0.5,
                               model_version="v1"), {"a": float(i)})
             for i in range(3)]
    outs = [OutcomeLabel(symbol="BTCUSDT", ts_ms=t0 + i * 1000, horizon_s=30, label=i % 2) for i in range(2)]
    assert led.add_many(preds, outs) == (3, 2)
    assert led.add_many(preds, outs) == (0, 0)  # replayed batch: nothing new
    rows = led.predictions(0)
    assert len(rows) == 3 and rows[0]["features"] == {"a": 0.0} and rows[1]["label"] == 1


def test_predictions_can_skip_features_and_window_scores_does(tmp_path, monkeypatch):
    """Heal checks run every 5 min over everything since promotion: they must not parse features."""
    import json as json_mod

    from sentinalops.heal.monitor import window_scores

    led = Ledger(tmp_path / "l.sqlite")
    t0 = 1_763_200_800_000
    led.add_many([(StressPrediction(symbol="BTCUSDT", ts_ms=t0 + i * 1000, horizon_s=30, probability=0.3,
                                    alert=False, threshold=0.5, model_version="v1"), {"a": 1.0})
                   for i in range(3)],
                  [OutcomeLabel(symbol="BTCUSDT", ts_ms=t0 + i * 1000, horizon_s=30, label=i % 2)
                   for i in range(3)])
    assert all(r["features"] is None for r in led.predictions(0, with_features=False))
    assert led.predictions(0)[0]["features"] == {"a": 1.0}  # the default is unchanged

    def no_parsing(*a, **k):
        raise AssertionError("features were parsed")

    monkeypatch.setattr(json_mod, "loads", no_parsing)
    w = window_scores(led, t0, t0 + 10_000, "v1")
    assert (w.n, w.positives) == (3, 1)
