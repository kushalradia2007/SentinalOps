"""The promoted champion, shared by the API and the stream processor.

Reloads whenever the model file changes (the 'heal' step: promotion swaps the file) and
refuses a model whose inputs differ from the configured features.
"""

from __future__ import annotations

import threading
from pathlib import Path

import joblib
import pandas as pd


class ModelUnavailable(RuntimeError):
    """No servable model: the file is missing or expects different inputs."""


class ChampionModel:
    def __init__(self, path: str | Path, feature_order: list[str]) -> None:
        self.path = Path(path)
        self.feature_order = list(feature_order)
        self._model = None
        self._mtime = 0.0
        self._lock = threading.Lock()

    @property
    def available(self) -> bool:
        return self.path.exists()

    def current(self) -> tuple[object, str]:
        """(model, version), reloading if the file changed. Raises ``ModelUnavailable``."""
        if not self.path.exists():
            raise ModelUnavailable("model not available")
        mtime = self.path.stat().st_mtime
        with self._lock:
            if self._model is None or mtime != self._mtime:
                model = joblib.load(self.path)
                if list(getattr(model, "feature_names_in_", [])) != self.feature_order:
                    raise ModelUnavailable("model inputs do not match the configured features")
                self._model, self._mtime = model, mtime
            return self._model, f"champion-{int(self._mtime)}"

    def predict(self, features: dict[str, float]) -> tuple[float, str]:
        """Stress probability for one feature dict, and the model version that produced it."""
        model, version = self.current()
        row = pd.DataFrame([[features[k] for k in self.feature_order]], columns=self.feature_order)
        return float(model.predict_proba(row)[:, 1][0]), version
