"""New model packages: contract, derived features, calibration fallback, numeric precision."""

import joblib
import numpy as np
import pandas as pd
import pytest

from sentinalops.config import get_domain_config
from sentinalops.models import get_recipe, model_name
from sentinalops.models.base import check_fitted
from sentinalops.models.common import derived_v1
from sentinalops.serving.model import ChampionModel
from sentinalops.simulator.heal_sim import market
from sentinalops.simulator.regimes import make_ticks

CFG = get_domain_config()
NAMES = CFG.feature_names
T0 = 1_763_200_800_000
NEW = ["persistence_v1", "logit_v1", "gbm_v2", "lgbm_v1"]


@pytest.fixture(scope="module")
def timed():
    return market([("calm", 90)], T0, seed=3)  # 5400 rows with ts_ms, ~3% positives


@pytest.mark.parametrize("name", NEW)
def test_new_recipes_satisfy_contract_and_load_in_champion_model(name, timed, tmp_path):
    model = get_recipe(name).fit(timed, CFG, seed=0)
    check_fitted(model, CFG)
    assert model_name(model) == name
    path = tmp_path / "champion.joblib"
    joblib.dump(model, path)
    row = {k: float(timed.iloc[0][k]) for k in NAMES}
    p, _ = ChampionModel(path, NAMES).predict(row)
    assert 0.0 <= p <= 1.0
    probs = model.predict_proba(timed[NAMES])
    assert probs.shape == (len(timed), 2) and np.allclose(probs.sum(axis=1), 1)


def test_persistence_ranks_thinning_books_higher(timed):
    model = get_recipe("persistence_v1").fit(timed, CFG)
    p = model.predict_proba(timed[NAMES])[:, 1]
    assert p[timed["label"] == 1].mean() > p[timed["label"] == 0].mean()


def test_derived_features_have_no_inf_with_zero_inputs():
    X = np.zeros((3, len(NAMES)))
    Z = derived_v1.compute(X, NAMES)
    assert Z.shape == (3, len(derived_v1.NAMES)) and np.isfinite(Z).all()
    assert len(derived_v1.NAMES) <= 12


def test_fallback_without_timestamps_or_positives(timed):
    no_ts = make_ticks("calm", 3000, seed=4)  # no ts_ms column
    m = get_recipe("logit_v1").fit(no_ts, CFG)
    assert m.sentinal_fit_info_["calibrated"] is False and "ts_ms" in m.sentinal_fit_info_["reason"]
    few = timed.copy()
    few.loc[few.index[int(len(few) * 0.7):], "label"] = 0  # no positives in the newest slice
    m = get_recipe("logit_v1").fit(few, CFG)
    assert m.sentinal_fit_info_["calibrated"] is False and "positives" in m.sentinal_fit_info_["reason"]
    m = get_recipe("logit_v1").fit(timed, CFG)
    assert m.sentinal_fit_info_["calibrated"] is True
    assert "calibrated" not in m.sentinal_model_params_  # params stay re-usable by the heal loop


def test_float32_matches_float64(timed):
    model = get_recipe("logit_v1").fit(timed, CFG)
    X = timed[NAMES].head(200)
    assert (X["rel_spread"] < 1e-6).all()
    p64 = model.predict_proba(X.astype("float64"))[:, 1]
    p32 = model.predict_proba(X.astype("float32"))[:, 1]
    np.testing.assert_allclose(p32, p64, atol=1e-6)


def test_predict_proba_accepts_one_row_dataframe(timed):
    model = get_recipe("logit_v1").fit(timed, CFG)
    one = pd.DataFrame([timed.iloc[5][NAMES].to_dict()])
    assert model.predict_proba(one).shape == (1, 2)


def test_lgbm_predicts_single_threaded(timed):
    model = get_recipe("lgbm_v1").fit(timed, CFG)
    assert model.estimator.get_params()["n_jobs"] == 1


def test_lgbm_fit_threads_follow_omp_num_threads(monkeypatch):
    from sentinalops.models.lgbm_v1 import fit_threads

    monkeypatch.setenv("OMP_NUM_THREADS", "1")
    assert fit_threads() == 1
    monkeypatch.delenv("OMP_NUM_THREADS")
    assert fit_threads() == -1


@pytest.mark.parametrize("name", ["logit_v1", "gbm_v2", "lgbm_v1", "persistence_v1"])
def test_latency_is_reasonable(name, timed, tmp_path):
    from sentinalops.training.metrics import latency_ms

    path = tmp_path / "m.joblib"
    joblib.dump(get_recipe(name).fit(timed, CFG), path)
    rows = [{k: float(v) for k, v in r.items()} for r in timed[NAMES].head(50).to_dict("records")]
    lat = latency_ms(ChampionModel(path, NAMES), rows, n=200, warmup=20)
    assert lat["p99"] < 25, lat  # buffer for shared CI runners; evaluate.py flags > 10 ms


def test_dockerfile_and_setup_install_libgomp():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    assert "libgomp1" in (root / "Dockerfile").read_text(encoding="utf-8")
    assert "libgomp1" in (root / "deploy" / "oracle" / "setup.sh").read_text(encoding="utf-8")
    deps = (root / "pyproject.toml").read_text(encoding="utf-8")
    assert '"lightgbm>=4.3"' in deps
