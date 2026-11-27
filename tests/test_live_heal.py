"""The heal service: bootstraps the served champion, trains from the ledger, idles without evidence."""

import math

import joblib
import pytest

from sentinalops.config import DomainConfig, get_domain_config
from sentinalops.heal.registry import ModelRegistry
from sentinalops.live.heal_service import HealService
from sentinalops.schemas import OutcomeLabel, StressPrediction
from sentinalops.serving.ledger import Ledger
from sentinalops.serving.model import ChampionModel
from sentinalops.simulator import heal_sim as S
from sentinalops.simulator.regimes import make_ticks
from sentinalops.training.train import train

T0 = 1_763_200_800_000
MIN = 60_000
BASE = get_domain_config()
H = {**BASE.heal.model_dump(), "metrics_window_minutes": 20, "min_matured_labels": 1500,
     "cooldown_minutes": 20, "shadow_minutes": 20}
CFG = DomainConfig(**{**BASE.model_dump(), "retrain_window_hours": 1, "heal": H})


@pytest.fixture(scope="module")
def calm_model():
    model, _, _ = train(make_ticks("calm", 20_000, seed=7), CFG, "gbm_v1", {})
    return model


def test_heal_idles_on_empty_ledger(tmp_path, calm_model):
    reg, led = ModelRegistry(tmp_path / "models"), Ledger(tmp_path / "l.sqlite")
    svc = HealService(CFG, led, reg)
    r = svc.tick(T0)
    assert (r.action, r.decision.reason) == ("idle", "no_champion")
    joblib.dump(calm_model, reg.champion_path)
    r = svc.tick(T0 + MIN)
    assert (r.action, r.decision.reason) == ("idle", "too_few_events")


def test_bootstrap_registers_existing_champion_with_holdout_ap(tmp_path, calm_model):
    reg = ModelRegistry(tmp_path / "models")
    joblib.dump(calm_model, reg.champion_path)
    svc = HealService(CFG, Ledger(tmp_path / "l.sqlite"), reg)
    assert svc.bootstrap_if_needed(T0) == "v001"
    assert reg.state().champion == "v001"
    assert reg.meta("v001")["holdout_ap"] == pytest.approx(calm_model.sentinal_holdout_ap_)
    assert reg.meta("v001")["model_name"] == "gbm_v1"
    assert svc.bootstrap_if_needed(T0 + 1) is None  # already registered: nothing to do


def test_bootstrap_without_holdout_ap_is_nan(tmp_path, calm_model):
    reg = ModelRegistry(tmp_path / "models")
    m = joblib.load(joblib.dump(calm_model, tmp_path / "x.joblib")[0])
    del m.sentinal_holdout_ap_
    joblib.dump(m, reg.champion_path)
    HealService(CFG, Ledger(tmp_path / "l.sqlite"), reg).bootstrap_if_needed(T0)
    assert math.isnan(reg.meta("v001")["holdout_ap"])


def test_heal_fires_from_ledger_data(tmp_path, calm_model):
    """Calm then volatile, scored into the ledger with features; the service retrains from it."""
    table = S.market([("calm", 90), ("volatile", 150)], T0)
    names, h = CFG.feature_names, CFG.label_horizon_seconds * 1000
    reg, led = ModelRegistry(tmp_path / "models"), Ledger(tmp_path / "l.sqlite")
    joblib.dump(calm_model, reg.champion_path)
    svc = HealService(CFG, led, reg)
    served = {"champion": ChampionModel(reg.champion_path, names),
              "shadow": ChampionModel(reg.shadow_path, names)}
    actions, matured_until = [], T0
    for lo in range(T0, T0 + 240 * MIN, 5 * MIN):
        hi = lo + 5 * MIN
        chunk = table[(table["ts_ms"] >= lo) & (table["ts_ms"] < hi)]
        preds = []
        for model_ref in served.values():
            if not model_ref.available:
                continue
            model, version = model_ref.current()
            probs = model.predict_proba(chunk[names])[:, 1]
            for (_, row), p in zip(chunk.iterrows(), probs, strict=True):
                preds.append((StressPrediction(symbol="BTCUSDT", ts_ms=int(row.ts_ms), horizon_s=30,
                                               probability=float(p), alert=bool(p >= 0.5), threshold=0.5,
                                               model_version=version),
                              {n: float(row[n]) for n in names}))
        matured = table[(table["ts_ms"] >= matured_until) & (table["ts_ms"] + h < hi)]
        led.add_many(preds, [OutcomeLabel(symbol="BTCUSDT", ts_ms=int(t), horizon_s=30, label=int(y))
                             for t, y in zip(matured["ts_ms"], matured["label"], strict=True)])
        matured_until = hi - h
        actions.append(((hi - T0) // MIN, svc.tick(hi).action))

    assert all(a == "idle" for m, a in actions if m <= 90), actions
    assert any(a.startswith("retrained") for m, a in actions if m > 90), actions
    assert reg.count("promote") >= 2  # the bootstrap, then a healed model
