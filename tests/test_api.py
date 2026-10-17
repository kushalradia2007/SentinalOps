import json

import joblib
import pytest
from fastapi.testclient import TestClient

from sentinalops.config import Settings
from sentinalops.serving.app import create_app
from sentinalops.simulator.zero_day import benign_package, evil_package
from sentinalops.training.synthetic import make_dataset
from sentinalops.training.train_baseline import train


@pytest.fixture(scope="module")
def champion():
    model, _ = train(make_dataset("classic", n_benign=1500, n_mal=200))
    return model


@pytest.fixture
def client(tmp_path, champion):
    joblib.dump(champion, tmp_path / "m.joblib")
    settings = Settings(model_path=tmp_path / "m.joblib", data_dir=tmp_path / "data")
    return TestClient(create_app(settings)), tmp_path


def _body(pkg):
    files, meta = pkg
    return {"files": files, "meta": meta}


def test_health(client):
    c, _ = client
    r = c.get("/health").json()
    assert r["status"] == "ok" and r["model_loaded"] is True


def test_score_benign_and_logs_features(client):
    c, tmp = client
    r = c.post("/score", json=_body(benign_package())).json()
    assert r["blocked"] is False and r["model_version"].startswith("champion-")
    rows = [json.loads(line) for line in (tmp / "data" / "features.jsonl").read_text().splitlines()]
    assert rows[0]["package"] == "friendly-utils" and "base64_entropy" in rows[0]["features"]


def test_score_validation_error(client):
    c, _ = client
    assert c.post("/score", json={"files": {}, "meta": {}}).status_code == 422


def test_labels_and_metrics(client):
    c, tmp = client
    c.post("/score", json=_body(evil_package()))
    assert c.post("/labels", json={"package": "trusted-core-lib", "is_malicious": True}).status_code == 200
    row = json.loads((tmp / "data" / "labels.jsonl").read_text().splitlines()[0])
    assert row["package"] == "trusted-core-lib" and row["is_malicious"] is True
    text = c.get("/metrics/").text
    assert "sentinalops_score_count" in text and 'sentinalops_labels_total{is_malicious="true"} 1.0' in text


def test_missing_model_returns_503(tmp_path):
    c = TestClient(create_app(Settings(model_path=tmp_path / "nope.joblib", data_dir=tmp_path)))
    assert c.get("/health").json()["model_loaded"] is False
    assert c.post("/score", json=_body(benign_package())).status_code == 503


def test_hot_reload_picks_up_new_champion(tmp_path, champion):
    path = tmp_path / "m.joblib"
    joblib.dump(champion, path)
    c = TestClient(create_app(Settings(model_path=path, data_dir=tmp_path)))
    v1 = c.post("/score", json=_body(benign_package())).json()["model_version"]
    import os
    os.utime(path, (path.stat().st_atime, path.stat().st_mtime + 10))
    v2 = c.post("/score", json=_body(benign_package())).json()["model_version"]
    assert v1 != v2
