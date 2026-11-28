"""Model packages and the recipe registry: the one place a model is plugged in.

Add a model: create ``sentinalops/models/<name>/`` exposing ``recipe`` (see ``base.Recipe``),
then add one line to ``_RECIPES``. Names map to import paths and are imported only when used,
so listing them (the config validator does) stays cheap.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from sentinalops.models.base import Recipe

_RECIPES: dict[str, str] = {
    "gbm_v1": "sentinalops.models.gbm_v1:recipe",
    "persistence_v1": "sentinalops.models.persistence_v1:recipe",
    "logit_v1": "sentinalops.models.logit_v1:recipe",
    "gbm_v2": "sentinalops.models.gbm_v2:recipe",
    "lgbm_v1": "sentinalops.models.lgbm_v1:recipe",
}


def list_recipes() -> list[str]:
    return sorted(_RECIPES)


def register_recipe(name: str, import_path: str) -> None:
    """Make ``name`` resolvable to ``"module:attribute"`` (for plug-ins outside this package)."""
    if ":" not in import_path:
        raise ValueError("import_path must look like 'package.module:attribute'")
    _RECIPES[name] = import_path


def get_recipe(name: str) -> Recipe:
    try:
        module, _, attr = _RECIPES[name].partition(":")
    except KeyError:
        raise KeyError(f"unknown model {name!r}; known models: {list_recipes()}") from None
    return getattr(import_module(module), attr)


def model_name(model: Any) -> str | None:
    """The recipe that produced ``model``, or None for models trained before the model slot."""
    return getattr(model, "sentinal_model_name_", None)
