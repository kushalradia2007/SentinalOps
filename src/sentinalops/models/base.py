"""The model contract: what every model package must provide (see ``docs/models.md``).

A **recipe** knows how to train one kind of model. Its ``fit`` returns a **fitted model** that
the rest of the system uses without knowing what is inside:

- ``predict_proba(X: DataFrame) -> ndarray (n, 2)``; column 1 is the stress probability.
- ``classes_ == [0, 1]`` and ``feature_names_in_ == cfg.feature_names`` (the 34 raw features,
  in order). The API and stream processor refuse a model with other inputs.
- a writable ``sentinal_version_`` (the registry stamps it), and the recipe's own stamp:
  ``sentinal_model_name_`` and ``sentinal_model_params_``. The heal loop reads these to retrain
  the champion with its own recipe.

Released packages are frozen: pickles refer to their classes by import path, so changing a
released package can break loading older registry versions. An upgrade is a new package.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    import pandas as pd

    from sentinalops.config import DomainConfig


class Recipe(Protocol):
    name: str

    def fit(
        self, train: pd.DataFrame, cfg: DomainConfig, seed: int = 0, params: dict[str, Any] | None = None
    ) -> Any: ...


def stamp(model: Any, name: str, params: dict[str, Any]) -> Any:
    """Record which recipe (and which parameters) produced ``model``. Returns ``model``."""
    model.sentinal_model_name_ = name
    model.sentinal_model_params_ = dict(params)
    return model


def check_fitted(model: Any, cfg: DomainConfig) -> None:
    """Raise ``ValueError`` unless ``model`` satisfies the contract for ``cfg``."""
    if not callable(getattr(model, "predict_proba", None)):
        raise ValueError("model has no predict_proba")
    names = list(getattr(model, "feature_names_in_", []))
    if names != list(cfg.feature_names):
        raise ValueError("model feature_names_in_ do not match the configured features")
    classes = [int(c) for c in getattr(model, "classes_", [])]
    if classes != [0, 1]:
        raise ValueError(f"model classes_ must be [0, 1], got {classes}")
