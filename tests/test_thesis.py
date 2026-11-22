"""The project thesis on synthetic regimes (task 3.5).

A champion trained on the calm regime degrades when the market turns volatile, because stress
there has different precursors; a challenger retrained on recent volatile data recovers.
"""

import numpy as np
import pandas as pd
import pytest

from sentinalops.features.microstructure import FEATURE_ORDER
from sentinalops.simulator.regimes import BASE_RATE, make_ticks
from sentinalops.training.train_baseline import evaluate, fit, train


@pytest.fixture(scope="module")
def data():
    return {
        "calm_train": make_ticks("calm", 20_000, seed=0),
        "calm_test": make_ticks("calm", 20_000, seed=1),
        "vol_train": make_ticks("volatile", 20_000, seed=2),
        "vol_test": make_ticks("volatile", 20_000, seed=3),
    }


def test_generator_shape_and_determinism():
    a, b = make_ticks("calm", 500, seed=9), make_ticks("calm", 500, seed=9)
    assert list(a.columns) == [*FEATURE_ORDER, "label"]
    pd.testing.assert_frame_equal(a, b)
    assert np.isfinite(a[FEATURE_ORDER].to_numpy()).all()
    assert (a["depth_top5"] > 0).all() and (a["rel_spread"] > 0).all()
    np.testing.assert_allclose(a["bid_depth"] + a["ask_depth"], a["depth_top5"])
    with pytest.raises(ValueError):
        make_ticks("sideways")


def test_regimes_differ_in_base_rate_and_precursors(data):
    calm, vol = data["calm_train"], data["vol_train"]
    assert calm["label"].mean() == pytest.approx(BASE_RATE["calm"], abs=0.005)
    assert vol["label"].mean() == pytest.approx(BASE_RATE["volatile"], abs=0.008)

    def gap(df, col):  # how much a feature moves before stress, in noise units
        return (df.loc[df.label == 1, col].mean() - df.loc[df.label == 0, col].mean()) / df[col].std()

    # Calm: the book thins out before stress. Volatile: trading bursts instead.
    assert gap(calm, "depth_change_60s") < -0.5 and abs(gap(vol, "depth_change_60s")) < 0.2
    assert gap(vol, "trade_intensity_1s") > 0.5 and abs(gap(calm, "trade_intensity_1s")) < 0.1


def test_champion_degrades_then_challenger_recovers(data):
    champion, calm_ap = train(data["calm_train"])
    vol_ap = evaluate(champion, data["vol_test"])
    assert calm_ap > 0.4, "champion should learn the calm regime"
    assert vol_ap < 0.2 and vol_ap < calm_ap / 2, "champion should fail when the regime shifts"

    recent = pd.concat([data["calm_train"].sample(5_000, random_state=0), data["vol_train"]])
    challenger = fit(recent)
    healed_ap = evaluate(challenger, data["vol_test"])
    assert healed_ap > 0.5 and healed_ap > vol_ap + 0.3, "retraining on recent data should recover"
    assert evaluate(challenger, data["calm_test"]) > 0.3, "and not forget the calm regime entirely"
