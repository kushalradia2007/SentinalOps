import json
import os

import joblib
import pytest
from fastapi.testclient import TestClient

from sentinalops.config import Settings
from sentinalops.serving.app import create_app
from sentinalops.simulator.regimes import example_ticks, make_ticks
from sentinalops.training.train_baseline import fit

TS = 1_763_000_000_000


@pytest.fixture(scope="module")
def champion():
    return fit(make_ticks("calm", 8_000, seed=3))


@pytest.fixture
def client(tmp_path, champion):
    joblib.dump(champion, tmp_path / "m.joblib")
    settings = Settings(model_path=tmp_path / "m.joblib", data_dir=tmp_path / "data")
    return TestClient(create_app(settings)), tmp_path


def _body(features, ts_ms=TS, symbol="BTCUSDT"):
    return {"symbol": symbol, "ts_ms": ts_ms, "features": features}


def test_health(client):
    c, _ = client
    r = c.get("/health").json()
    assert r["status"] == "ok" and r["model_loaded"] is True
    assert r["domain"] == "market_liquidity" and r["symbol"] == "BTCUSDT" and r["horizon_s"] == 30


def test_score_calm_and_stressed_ticks_and_ledger(client):
    c, tmp = client
    calm, stressed = example_ticks()
    r_calm = c.post("/score", json=_body(calm)).json()
    r_stress = c.post("/score", json=_body(stressed, ts_ms=TS + 1000)).json()
    assert r_calm["probability"] < 0.2 and r_calm["alert"] is False
    assert r_stress["probability"] > 0.5 and r_stress["alert"] is True
    assert r_calm["horizon_s"] == 30 and r_calm["model_version"].startswith("champion-")
    rows = [json.loads(line) for line in (tmp / "data" / "predictions.jsonl").read_text().splitlines()]
    assert [r["ts_ms"] for r in rows] == [TS, TS + 1000]
    assert rows[0]["prediction_id"] == r_calm["prediction_id"] and rows[0]["features"] == calm


@pytest.mark.parametrize(
    "mutate",
    [
        lambda f: {k: v for k, v in f.items() if k != "rel_spread"},  # missing feature
        lambda f: {**f, "bogus": 1.0},  # unexpected feature
        lambda f: {},
    ],
)
def test_score_rejects_wrong_feature_sets(client, mutate):
    c, _ = client
    calm, _ = example_ticks()
    assert c.post("/score", json=_body(mutate(calm))).status_code == 422


def test_score_rejects_other_symbols_and_bad_bodies(client):
    c, _ = client
    calm, _ = example_ticks()
    assert c.post("/score", json=_body(calm, symbol="ETHUSDT")).status_code == 422
    assert c.post("/score", json={"files": {}, "meta": {"name": "x"}}).status_code == 422


def test_outcomes_and_metrics(client):
    c, tmp = client
    _, stressed = example_ticks()
    c.post("/score", json=_body(stressed))
    ok = {"symbol": "BTCUSDT", "ts_ms": TS, "horizon_s": 30, "label": 1}
    assert c.post("/outcomes", json=ok).status_code == 200
    void = {"symbol": "BTCUSDT", "ts_ms": TS + 1000, "horizon_s": 30, "label": None, "void_reason": "gap"}
    assert c.post("/outcomes", json=void).status_code == 200
    assert c.post("/outcomes", json={**ok, "label": None}).status_code == 422
    rows = [json.loads(line) for line in (tmp / "data" / "outcomes.jsonl").read_text().splitlines()]
    assert [r["label"] for r in rows] == [1, None]
    text = c.get("/metrics/").text
    assert "sentinalops_stress_probability_count 1.0" in text
    assert "sentinalops_alerts_total 1.0" in text
    assert 'sentinalops_outcomes_total{label="1"} 1.0' in text
    assert 'sentinalops_outcomes_total{label="void"} 1.0' in text


def test_missing_model_returns_503(tmp_path):
    c = TestClient(create_app(Settings(model_path=tmp_path / "nope.joblib", data_dir=tmp_path)))
    assert c.get("/health").json()["model_loaded"] is False
    assert c.post("/score", json=_body(example_ticks()[0])).status_code == 503


def test_model_with_other_inputs_is_refused(tmp_path, champion):
    """An old champion (e.g. the supply-chain model) must not be served against liquidity features."""
    df = make_ticks("calm", 500, seed=4).rename(columns={"rel_spread": "base64_entropy"})
    from sklearn.linear_model import LogisticRegression

    wrong = LogisticRegression().fit(df.drop(columns="label"), df["label"])
    joblib.dump(wrong, tmp_path / "m.joblib")
    c = TestClient(create_app(Settings(model_path=tmp_path / "m.joblib", data_dir=tmp_path)))
    r = c.post("/score", json=_body(example_ticks()[0]))
    assert r.status_code == 503 and "do not match" in r.json()["detail"]


def test_hot_reload_picks_up_new_champion(tmp_path, champion):
    path = tmp_path / "m.joblib"
    joblib.dump(champion, path)
    c = TestClient(create_app(Settings(model_path=path, data_dir=tmp_path)))
    calm, _ = example_ticks()
    v1 = c.post("/score", json=_body(calm)).json()["model_version"]
    os.utime(path, (path.stat().st_atime, path.stat().st_mtime + 10))
    v2 = c.post("/score", json=_body(calm)).json()["model_version"]
    assert v1 != v2
