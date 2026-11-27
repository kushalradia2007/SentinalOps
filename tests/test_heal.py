"""The self-healing loop (chunks 39, 42-45)."""

import math
import sys

import joblib
import pandas as pd
import pytest

from sentinalops.config import DomainConfig, get_domain_config
from sentinalops.heal import controller as C
from sentinalops.heal.monitor import WindowScores, degradation, window_scores
from sentinalops.heal.registry import ModelRegistry
from sentinalops.heal.retrain import assemble, evaluate_gates, train_challenger
from sentinalops.schemas import OutcomeLabel, StressPrediction
from sentinalops.serving.ledger import Ledger
from sentinalops.serving.model import ChampionModel
from sentinalops.simulator import heal_sim as S
from sentinalops.simulator.regimes import make_ticks
from sentinalops.training.train_baseline import train

T0 = 1_763_200_800_000
MIN = 60_000
BASE = get_domain_config()


def config(**heal) -> DomainConfig:
    h = {**BASE.heal.model_dump(), "metrics_window_minutes": 20, "min_matured_labels": 1500,
         "cooldown_minutes": 20, "shadow_minutes": 20, **heal}
    return DomainConfig(**{**BASE.model_dump(), "retrain_window_hours": 1, "heal": h})


CFG = config()


@pytest.fixture(scope="module")
def calm_model():
    return train(make_ticks("calm", 20_000, seed=7))


@pytest.fixture(scope="module")
def volatile_model():
    return train(make_ticks("volatile", 20_000, seed=8))


# ---- 39 monitoring --------------------------------------------------------------------------------
def scores(ap=0.5, positives=50):
    return WindowScores(None, 0, 1, 1000, positives, ap, 0.05, 0.05, 0.5, 0.5)


@pytest.mark.parametrize(
    "ap, positives, reference, expected",
    [
        (0.20, 50, 0.60, (True, "degraded")),
        (0.50, 50, 0.60, (False, "healthy")),  # within the 0.15 margin
        (0.05, 5, 0.60, (False, "too_few_events")),  # no verdict on thin evidence
        (0.20, 50, math.nan, (False, "no_reference")),
    ],
)
def test_degradation_truth_table(ap, positives, reference, expected):
    d = degradation(scores(ap, positives), reference, CFG.heal)
    assert (d.degraded, d.reason) == expected


def test_window_scores_from_the_ledger(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    rows = [(0.9, 1), (0.8, 0), (0.3, 1), (0.1, 0), (0.2, 0)]
    for i, (p, y) in enumerate(rows):
        ts = T0 + i * 1000
        led.add_prediction(StressPrediction(symbol="BTCUSDT", ts_ms=ts, horizon_s=30, probability=p,
                                            alert=p >= 0.5, threshold=0.5, model_version="v1"))
        led.add_outcome(OutcomeLabel(symbol="BTCUSDT", ts_ms=ts, horizon_s=30, label=y))
    led.add_prediction(StressPrediction(symbol="BTCUSDT", ts_ms=T0 + 9000, horizon_s=30, probability=0.5,
                                        alert=True, threshold=0.5, model_version="v1"))  # pending
    w = window_scores(led, T0, T0 + 10_000, "v1")
    assert (w.n, w.positives) == (5, 2)
    assert w.ap == pytest.approx((1 / 1 + 2 / 3) / 2)
    assert w.precision == 0.5 and w.recall == 0.5
    assert w.brier == pytest.approx(sum((p - y) ** 2 for p, y in rows) / 5)
    empty = window_scores(led, T0 + 60_000, T0 + 70_000)
    assert empty.n == 0 and math.isnan(empty.ap) and math.isnan(empty.brier)


# ---- 45 registry ----------------------------------------------------------------------------------
def test_registry_promote_shadow_rollback(tmp_path, calm_model, volatile_model):
    reg = ModelRegistry(tmp_path)
    v1 = reg.register(calm_model[0], {"holdout_ap": 0.6}, T0)
    v2 = reg.register(volatile_model[0], {"holdout_ap": 0.7}, T0 + 1)
    assert (v1, v2) == ("v001", "v002")
    reg.promote(v1, T0, "bootstrap")
    served = ChampionModel(reg.champion_path, BASE.feature_names)
    assert served.current()[1] == "v001" and not reg.shadow_path.exists()

    reg.promote(v2, T0 + MIN, "heal", shadow_ms=10 * MIN)
    s = reg.state()
    assert (s.champion, s.shadow, s.shadow_until_ms) == ("v002", "v001", T0 + 11 * MIN)
    assert served.current()[1] == "v002"  # hot reload picks up the swap
    assert ChampionModel(reg.shadow_path, BASE.feature_names).current()[1] == "v001"

    assert reg.rollback(T0 + 5 * MIN, "test") == "v001"
    assert served.current()[1] == "v001" and not reg.shadow_path.exists()
    assert (reg.count("promote"), reg.count("rollback")) == (2, 1)
    assert joblib.load(reg.version_path("v002")).sentinal_version_ == "v002"  # versions never change
    with pytest.raises(RuntimeError):
        reg.rollback(T0 + 6 * MIN, "nothing to roll back to")
    with pytest.raises(KeyError):
        reg.promote("v999", T0, "missing")


# ---- 43 training data -----------------------------------------------------------------------------
def test_assemble_respects_embargo_window_and_regimes():
    table = S.market([("calm", 60), ("volatile", 30), ("calm", 60)], T0)
    table.loc[table.index[::50], "label_status"] = "void_gap"
    now = T0 + 150 * MIN
    ts = assemble(table, now, CFG, seed=1)
    start = now - 60 * MIN
    assert (ts.holdout["ts_ms"] >= start).all() and (ts.holdout["ts_ms"] < now).all()
    recent_train = ts.train[ts.train["ts_ms"] >= start]
    assert recent_train["ts_ms"].max() + CFG.split_embargo_seconds * 1000 < ts.holdout["ts_ms"].min()
    replay = ts.train[ts.train["ts_ms"] < start]
    assert set(replay["regime"]) == {"calm", "volatile"}  # older regimes are kept in the mix
    assert len(replay) == pytest.approx(len(recent_train) * CFG.heal.replay_fraction, rel=0.05)
    assert set(replay.index).isdisjoint(ts.older_eval.index)
    for part in (ts.train, ts.holdout, ts.older_eval):
        assert (part["label_status"] == "ok").all()
    assert ts.counts["holdout"] == len(ts.holdout) and ts.counts["regime_mix"]


# ---- 44 gates -------------------------------------------------------------------------------------
def test_gates_pass_for_a_real_improvement_and_fail_for_none(calm_model):
    table = S.market([("calm", 60), ("volatile", 70)], T0)
    ts = assemble(table, T0 + 130 * MIN, CFG)
    challenger = train_challenger(ts, CFG)
    good = evaluate_gates(calm_model[0], challenger, ts, CFG)
    assert good.passed, good.to_markdown()
    same = evaluate_gates(calm_model[0], calm_model[0], ts, CFG)
    assert not same.passed and not same.gates["improves_current_regime"]["passed"]
    assert "FAILED" in same.to_markdown() and '"passed": false' in same.to_json()


def test_regression_gate_blocks_a_model_that_forgot_the_old_regime(calm_model, volatile_model):
    table = S.market([("calm", 60), ("volatile", 70)], T0)
    ts = assemble(table, T0 + 130 * MIN, config(max_regression=0.05))
    report = evaluate_gates(calm_model[0], volatile_model[0], ts, config(max_regression=0.05))
    assert report.gates["improves_current_regime"]["passed"]
    assert not report.gates["no_regression_on_older"]["passed"] and not report.passed


# ---- 42 trigger rule --------------------------------------------------------------------------------
@pytest.fixture
def ctl(tmp_path, calm_model, monkeypatch):
    reg = ModelRegistry(tmp_path / "reg")
    c = C.HealController(CFG, Ledger(tmp_path / "l.sqlite"), reg, lambda now: pd.DataFrame())
    c.bootstrap(calm_model[0], 0.6, T0)
    state = {"ap": 0.6, "n": 5000}

    def fake_scores(ledger, since, until, version=None):
        return WindowScores(version, since, until, state["n"], 100, state["ap"], 0.05, 0.05, 0.5, 0.5)

    monkeypatch.setattr(C, "window_scores", fake_scores)
    return c, state


def test_trigger_needs_degradation_and_enough_labels(ctl):
    c, state = ctl
    assert c.decide(T0 + 60 * MIN).reason == "healthy"
    state["ap"] = 0.2
    state["n"] = 100
    assert c.decide(T0 + 60 * MIN).reason == "too_few_matured_labels"
    state["n"] = 5000
    assert c.decide(T0 + 60 * MIN).fire


def test_cooldown_and_idempotency(ctl):
    c, state = ctl
    state["ap"] = 0.2
    c.registry.log_event(T0 + 60 * MIN, "heal_attempt", champion="v001", matured=5000)
    assert c.decide(T0 + 70 * MIN).reason == "cooldown"  # 20 min cool-down
    assert c.decide(T0 + 90 * MIN).reason == "already_attempted"  # same evidence: no second retrain
    state["n"] = 5000 + CFG.heal.min_matured_labels
    assert c.decide(T0 + 90 * MIN).fire  # enough new evidence


def test_no_trigger_without_a_champion_or_during_shadow(tmp_path, calm_model):
    reg = ModelRegistry(tmp_path)
    c = C.HealController(CFG, Ledger(tmp_path / "l.sqlite"), reg, lambda now: pd.DataFrame())
    assert c.decide(T0).reason == "no_champion"
    v1 = c.bootstrap(calm_model[0], 0.6, T0)
    v2 = reg.register(calm_model[0], {"holdout_ap": 0.6}, T0)
    reg.promote(v2, T0, "test", shadow_ms=MIN)
    assert c.decide(T0).reason == "shadow_period" and reg.state().shadow == v1
    assert c.tick(T0 + 1000).action == "waiting"


# ---- the whole loop ---------------------------------------------------------------------------------
def test_heal_loop_recovers_from_a_regime_shift(tmp_path, calm_model):
    """Calm, then volatile: the champion degrades, a challenger passes the gates, the score recovers."""
    table = S.market([("calm", 90), ("volatile", 150)], T0)
    reg, led = ModelRegistry(tmp_path / "reg"), Ledger(tmp_path / "l.sqlite")
    c = C.HealController(CFG, led, reg, S.matured_table(table, 30))
    c.bootstrap(calm_model[0], calm_model[1], T0)
    steps = S.run(CFG, table, reg, led, c, step_minutes=5)
    actions = [(int((s.now_ms - T0) / MIN), s.result.action) for s in steps]
    shift = 90

    assert all(a == "idle" for m, a in actions if m <= shift), "no retraining while the market is calm"
    promoted = [m for m, a in actions if a == "retrained_promoted"]
    assert len(promoted) == 1 and shift < promoted[0] <= shift + 60
    assert any(a == "shadow_kept" for _, a in actions) and reg.count("rollback") == 0

    worst = min(s.live_ap for s in steps if shift + 10 <= (s.now_ms - T0) / MIN < promoted[0])
    healed = [s.live_ap for s in steps if (s.now_ms - T0) / MIN >= promoted[0] + 25]
    assert worst < 0.25, "the champion should degrade after the shift"
    assert min(healed) > 0.45 and sum(healed) / len(healed) > worst + 0.35, "and recover after healing"
    report = (reg.root / "reports" / f"{reg.state().champion}.md").read_text()
    assert "PASSED" in report
    healed_versions = [v for v, m in reg.state().versions.items() if m.get("source") == "heal"]
    assert healed_versions and all(reg.meta(v)["model_name"] == "gbm_v1" for v in healed_versions)


# ---- the model slot: heal retrains the champion's own recipe ---------------------------------------
class _LogitRecipe:
    name = "logit_test"

    def fit(self, train, cfg, seed=0, params=None):
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import StandardScaler

        from sentinalops.models.base import stamp

        model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=500))
        model.fit(train[cfg.feature_names], train["label"].astype(int))
        return stamp(model, self.name, params or {})


LOGIT = _LogitRecipe()


def test_heal_retrains_champion_recipe_not_config(monkeypatch):
    from sentinalops import models
    from sentinalops.heal.retrain import resolve_recipe

    monkeypatch.setitem(models._RECIPES, "logit_test", f"{__name__}:LOGIT")
    table = S.market([("calm", 60)], T0)
    ts = assemble(table, T0 + 60 * MIN, CFG)
    champion = LOGIT.fit(ts.train, CFG)
    assert CFG.model.name == "gbm_v1"  # the config names another recipe
    assert resolve_recipe(champion, CFG) == ("logit_test", {})
    challenger = train_challenger(ts, CFG, champion=champion)
    assert models.model_name(challenger) == "logit_test"


def test_unstamped_champion_falls_back_to_config():
    from sklearn.ensemble import HistGradientBoostingClassifier

    from sentinalops.heal.retrain import resolve_recipe

    assert resolve_recipe(HistGradientBoostingClassifier(), CFG) == ("gbm_v1", {})
    assert resolve_recipe(None, CFG) == ("gbm_v1", {})


def test_bad_promotion_is_rolled_back_after_the_shadow_period(tmp_path, calm_model, volatile_model):
    table = S.market([("volatile", 40)], T0)
    reg, led = ModelRegistry(tmp_path / "reg"), Ledger(tmp_path / "l.sqlite")
    c = C.HealController(CFG, led, reg, S.matured_table(table, 30))
    good = c.bootstrap(volatile_model[0], volatile_model[1], T0)
    bad = reg.register(calm_model[0], {"holdout_ap": calm_model[1]}, T0)
    reg.promote(bad, T0, "forced, to test rollback", shadow_ms=20 * MIN)  # skips the gates on purpose
    steps = S.run(CFG, table, reg, led, c, step_minutes=5)
    rolled = [s for s in steps if s.result.action == "rolled_back"]
    assert len(rolled) == 1 and rolled[0].result.version == good
    assert reg.state().champion == good and reg.count("rollback") == 1
    assert rolled[0].result.details["new"]["ap"] < rolled[0].result.details["old"]["ap"]


def test_processor_scores_with_the_shadow_model_too(tmp_path, calm_model, volatile_model):
    from pipeline_helpers import H0, make_events
    from sentinalops.stream.processor import PREDICTIONS_TOPIC, StreamProcessor

    reg = ModelRegistry(tmp_path)
    reg.promote(reg.register(calm_model[0], {}, T0), T0, "a")
    reg.promote(reg.register(volatile_model[0], {}, T0), T0, "b", shadow_ms=MIN)
    names = BASE.feature_names
    proc = StreamProcessor(BASE, model=ChampionModel(reg.champion_path, names),
                           shadow=ChampionModel(reg.shadow_path, names))
    d, t = make_events(H0, 75, seed=3)
    events = [("depth5", r) for r in d.to_dict("records")] + [("aggTrade", r) for r in t.to_dict("records")]
    events.sort(key=lambda e: e[1]["recv_time_ms"])
    preds = [m for k, r in events for topic, m in proc.on_event(k, r) if topic == PREDICTIONS_TOPIC]
    assert preds and {p.model_version for p in preds} == {"v001", "v002"}
    assert proc.stats.shadow_predictions == proc.stats.predictions


# ---- review fixes ---------------------------------------------------------------------------------
def test_average_precision_handles_ties_like_sklearn():
    import numpy as np
    from sklearn.metrics import average_precision_score

    from sentinalops.labels import average_precision

    const = np.full(4, 0.5)
    assert average_precision(np.array([1, 1, 0, 0]), const) == pytest.approx(0.5)
    assert average_precision(np.array([0, 0, 1, 1]), const) == pytest.approx(0.5)  # order-independent
    rng = np.random.default_rng(0)
    y = rng.binomial(1, 0.2, 500)
    s = rng.integers(0, 5, 500) / 4  # heavy ties, like a step-function calibrator
    assert average_precision(y, s) == pytest.approx(average_precision_score(y, s))
    assert math.isnan(average_precision(np.zeros(5), np.ones(5)))


def test_replay_and_older_eval_are_separated_in_time():
    """Neighbouring seconds share their 30 s outcome window, so a random split would leak."""
    import numpy as np

    table = S.market([("calm", 60), ("volatile", 60), ("calm", 60)], T0)
    ts = assemble(table, T0 + 180 * MIN, CFG, seed=1)
    replay = ts.train[ts.train["ts_ms"] < T0 + 120 * MIN]["ts_ms"].to_numpy()
    held = np.sort(ts.older_eval["ts_ms"].to_numpy())
    assert len(replay) and len(held)
    i = np.clip(np.searchsorted(held, replay), 1, len(held) - 1)
    nearest = np.minimum(np.abs(held[i] - replay), np.abs(held[i - 1] - replay))
    assert nearest.min() >= CFG.split_embargo_seconds * 1000
    assert set(ts.older_eval["regime"]) == {"calm", "volatile"}


def test_train_challenger_gives_recipes_the_timestamps(monkeypatch):
    from sentinalops import models

    seen = {}

    class Spy(_LogitRecipe):
        name = "spy_test"

        def fit(self, train, cfg, seed=0, params=None):
            seen["columns"] = list(train.columns)
            return super().fit(train, cfg, seed, params)

    monkeypatch.setattr(sys.modules[__name__], "SPY", Spy(), raising=False)
    monkeypatch.setitem(models._RECIPES, "spy_test", f"{__name__}:SPY")
    table = S.market([("calm", 60)], T0)
    ts = assemble(table, T0 + 60 * MIN, CFG)
    champion = models.base.stamp(LOGIT.fit(ts.train, CFG), "spy_test", {})
    train_challenger(ts, CFG, champion=champion)
    assert "ts_ms" in seen["columns"] and "regime" in seen["columns"]
