import os
import subprocess
import sys
from pathlib import Path

import joblib
import pytest

from sentinalops.features.microstructure import FEATURE_ORDER
from sentinalops.training import train_baseline

ROOT = Path(__file__).resolve().parents[1]


def test_train_main_writes_loadable_model(tmp_path):
    out = tmp_path / "m" / "champion.joblib"
    train_baseline.main(["--out", str(out), "--rows", "4000"])
    model = joblib.load(out)
    assert hasattr(model, "predict_proba") and list(model.feature_names_in_) == FEATURE_ORDER


def test_importing_the_api_module_has_no_cwd_dependent_side_effects(tmp_path):
    """Regression: a module-level create_app() crashed when imported outside the repo root."""
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    r = subprocess.run([sys.executable, "-c", "import sentinalops.serving.app"], cwd=tmp_path,
                       env=env, capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-400:]


T0 = 1_763_200_800_000


def test_train_cli_synthetic_writes_stamped_model(tmp_path, capsys):
    from sentinalops.training import train

    out = tmp_path / "champion.joblib"
    train.main(["--data", "synthetic", "--rows", "4000", "--out", str(out)])
    model = joblib.load(out)
    assert model.sentinal_model_name_ == "gbm_v1"
    assert isinstance(model.sentinal_holdout_ap_, float) and 0 < model.sentinal_holdout_ap_ <= 1
    assert "model=gbm_v1" in capsys.readouterr().out
    assert not list(tmp_path.glob("*.tmp"))


def test_time_split_has_embargo_and_drops_void_rows():
    from sentinalops.config import get_domain_config
    from sentinalops.simulator.heal_sim import market
    from sentinalops.training.train import time_split

    cfg = get_domain_config()
    table = market([("calm", 30)], T0)
    table.loc[table.index[::7], "label_status"] = "void_gap"
    tr, ho = time_split(table, cfg)
    assert tr["ts_ms"].max() + cfg.split_embargo_seconds * 1000 < ho["ts_ms"].min()
    assert (tr["label_status"] == "ok").all() and (ho["label_status"] == "ok").all()
    assert len(ho) == pytest.approx(0.2 * (table["label_status"] == "ok").sum(), rel=0.1)


def test_train_cli_on_table_dir(tmp_path):
    from sentinalops.simulator.heal_sim import market
    from sentinalops.training import train

    table = market([("calm", 60)], T0)
    part = tmp_path / "features" / "date=2025-11-15"
    part.mkdir(parents=True)
    table.to_parquet(part / "hour=10.parquet")
    out = tmp_path / "champion.joblib"
    train.main(["--data", str(tmp_path / "features"), "--out", str(out)])
    assert joblib.load(out).sentinal_model_name_ == "gbm_v1"
