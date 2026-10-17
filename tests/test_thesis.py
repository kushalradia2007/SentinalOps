"""Sanity tests for the demo thesis: champion misses the zero-day, healed model catches it."""
import pandas as pd

from sentinalops.features.extract import FEATURE_ORDER, extract_package_features
from sentinalops.simulator.zero_day import benign_package, evil_package
from sentinalops.training.synthetic import make_dataset
from sentinalops.training.train_baseline import train


def _score(model, pkg):
    feats = extract_package_features(*pkg)
    return model.predict_proba(pd.DataFrame([feats])[FEATURE_ORDER])[:, 1][0]


def test_features_fire_on_evil_not_benign():
    b = extract_package_features(*benign_package())
    e = extract_package_features(*evil_package())
    assert e["n_exec_eval"] >= 1 and e["has_install_hook"] == 1
    assert e["base64_entropy"] > 4.5 > b["base64_entropy"]
    assert b["has_install_hook"] == 0


def test_champion_misses_then_challenger_catches():
    champion, _ = train(make_dataset("classic"))
    assert _score(champion, benign_package()) < 0.1
    assert _score(champion, evil_package()) < 0.5, "champion should miss the zero-day"

    retrain = pd.concat([make_dataset("classic", seed=1), make_dataset("zero_day", 400, 400, seed=2)])
    challenger, _ = train(retrain)
    assert _score(challenger, evil_package()) > 0.9, "challenger should catch the zero-day"
    assert _score(challenger, benign_package()) < 0.1
