import os
import subprocess
import sys
from pathlib import Path

import joblib

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
