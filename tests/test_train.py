import subprocess
import sys
from pathlib import Path

import joblib

from sentinalops.training import train_baseline

ROOT = Path(__file__).resolve().parents[1]


def test_train_main_writes_loadable_model(tmp_path, monkeypatch):
    out = tmp_path / "m" / "champion.joblib"
    monkeypatch.setattr(sys, "argv", ["train_baseline", "--out", str(out)])
    train_baseline.main()
    assert hasattr(joblib.load(out), "predict_proba")


def test_importing_the_api_module_has_no_cwd_dependent_side_effects(tmp_path):
    """Regression: a module-level create_app() crashed when imported outside the repo root."""
    r = subprocess.run([sys.executable, "-c", "import sentinalops.serving.app"], cwd=tmp_path,
                       env={"PYTHONPATH": str(ROOT / "src"), "PATH": "/usr/bin:/bin"},
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-400:]
