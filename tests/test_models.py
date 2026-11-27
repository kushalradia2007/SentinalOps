"""The model slot (Part A): recipe contract, registry and the gbm_v1 recipe."""

import pickle

import joblib
import numpy as np
import pytest
from sklearn.ensemble import HistGradientBoostingClassifier

from sentinalops.config import get_domain_config
from sentinalops.models import get_recipe, list_recipes, model_name
from sentinalops.models.base import check_fitted, stamp
from sentinalops.serving.model import ChampionModel
from sentinalops.simulator.regimes import make_ticks

BASE = get_domain_config()


@pytest.fixture(scope="module")
def ticks():
    return make_ticks("calm", 4000, seed=3)


def test_gbm_v1_is_default_and_loads_in_champion_model(ticks, tmp_path):
    assert "gbm_v1" in list_recipes()
    model = get_recipe("gbm_v1").fit(ticks, BASE, seed=0)
    check_fitted(model, BASE)
    assert model_name(model) == "gbm_v1"
    path = tmp_path / "champion.joblib"
    joblib.dump(model, path)
    row = {k: float(ticks.iloc[0][k]) for k in BASE.feature_names}
    p, _ = ChampionModel(path, BASE.feature_names).predict(row)
    assert 0.0 <= p <= 1.0


def test_gbm_v1_matches_old_baseline(ticks):
    model = get_recipe("gbm_v1").fit(ticks, BASE, seed=0)
    old = HistGradientBoostingClassifier(max_iter=200, learning_rate=0.08, random_state=0)
    old.fit(ticks[BASE.feature_names], ticks["label"])
    X = ticks[BASE.feature_names]
    np.testing.assert_array_equal(model.predict_proba(X), old.predict_proba(X))


def test_gbm_v1_params_override(ticks):
    model = get_recipe("gbm_v1").fit(ticks, BASE, seed=0, params={"max_iter": 20})
    assert model.max_iter == 20
    assert model.sentinal_model_params_ == {"max_iter": 20}


def test_unknown_recipe_lists_known():
    with pytest.raises(KeyError, match="gbm_v1"):
        get_recipe("no_such_model")


def test_stamp_survives_pickle():
    m = stamp(HistGradientBoostingClassifier(), "x_v1", {"a": 1})
    back = pickle.loads(pickle.dumps(m))
    assert back.sentinal_model_name_ == "x_v1" and back.sentinal_model_params_ == {"a": 1}


def test_check_fitted_rejects_wrong_inputs(ticks):
    m = HistGradientBoostingClassifier(max_iter=5).fit(ticks[BASE.feature_names[:5]], ticks["label"])
    with pytest.raises(ValueError, match="feature"):
        check_fitted(m, BASE)
