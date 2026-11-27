"""Typed configuration: the domain YAML plus environment settings.

``DomainConfig`` is the single typed view of ``configs/domain_liquidity.yaml``. Unknown keys
are rejected, so a typo in the YAML fails loudly instead of silently using a default. The
label and feature modules get their parameters from here (``label_params``,
``feature_params``), so training, serving and the outcome verifier all read one file.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

if TYPE_CHECKING:  # imported lazily at runtime to keep config import cheap
    from sentinalops.features.microstructure import FeatureParams
    from sentinalops.labels import LabelParams

DEFAULT_DOMAIN_CONFIG = Path("configs/domain_liquidity.yaml")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Sources(_Strict):
    offline: list[str] = Field(default_factory=list)
    live: list[str] = Field(default_factory=list)
    delayed_labels: list[str] = Field(default_factory=list)


class LabelConfig(_Strict):
    """The stress-label block. See ``docs/labels.md`` for the definition."""

    primary: Literal["depth_depletion"] = "depth_depletion"
    depth_fraction: float = Field(0.5, gt=0, lt=1)
    secondary: Literal["spread_expansion"] | None = "spread_expansion"
    spread_multiple: float = Field(3.0, gt=1)
    reference_window_seconds: int = Field(600, ge=1)
    min_reference_seconds: int = Field(300, ge=1)
    confirm_ticks: int = Field(2, ge=1)
    onset_only: Literal[True] = True  # persistence labels are not implemented, on purpose

    @model_validator(mode="after")
    def _reference_fits(self) -> LabelConfig:
        if self.min_reference_seconds > self.reference_window_seconds:
            raise ValueError("min_reference_seconds cannot exceed reference_window_seconds")
        return self


class ModelConfig(_Strict):
    """Which model package to train by hand (``python -m sentinalops.training.train``).

    The heal loop does not read this: it retrains the champion with the champion's own recipe.
    """

    name: str = "gbm_v1"
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _known(self) -> ModelConfig:
        from sentinalops.models import list_recipes

        if self.name not in list_recipes():
            raise ValueError(f"unknown model {self.name!r}; known models: {list_recipes()}")
        return self


class HealConfig(_Strict):
    """The self-healing loop: when to retrain, how, and what a challenger must prove."""

    # monitoring (chunk 39)
    metrics_window_minutes: int = Field(60, ge=1)  # rolling window for live scores
    min_positives: int = Field(20, ge=1)  # fewer stress events in the window -> no verdict
    degradation_margin: float = Field(0.15, gt=0, lt=1)  # AP drop vs the champion's reference
    # trigger (chunk 42)
    min_matured_labels: int = Field(2000, ge=1)  # since the champion was promoted
    cooldown_minutes: int = Field(60, ge=0)
    # retraining (chunk 43)
    replay_fraction: float = Field(0.3, ge=0, le=2)  # older rows added, relative to recent rows
    holdout_fraction: float = Field(0.2, gt=0, lt=1)  # newest slice of the window, for the gates
    # gates (chunk 44)
    min_gain: float = Field(0.05, ge=0)  # challenger AP - champion AP on the current regime
    max_regression: float = Field(0.10, ge=0)  # allowed AP loss on older regimes
    min_lift_over_baseline: float = Field(1.5, ge=1)  # challenger AP / base rate
    max_brier_increase: float = Field(0.01, ge=0)
    # promotion (chunk 45)
    shadow_minutes: int = Field(30, ge=0)
    rollback_margin: float = Field(0.05, ge=0)  # new champion worse than the old by this -> rollback


class DomainConfig(_Strict):
    """Everything that defines the domain lives in one YAML file."""

    domain: str
    thesis: str = ""
    sources: Sources = Field(default_factory=Sources)
    features_module: str
    drift_watch: list[str] = Field(min_length=1)
    drift_threshold: float = Field(0.3, ge=0, le=1)
    retrain_window_hours: int = Field(6, gt=0)
    promotion_metric: Literal["avg_precision", "brier", "roc_auc"] = "avg_precision"
    alert_threshold: float = Field(0.5, ge=0, le=1)
    model: ModelConfig = Field(default_factory=ModelConfig)

    symbol: str = Field("BTCUSDT", pattern=r"^[A-Z0-9]{5,20}$")
    prediction_cadence_seconds: int = Field(1, ge=1, le=60)
    label_horizon_seconds: int = Field(30, ge=1, le=3600)
    feature_windows_seconds: tuple[int, ...] = Field((1, 5, 30, 60), min_length=1)
    label: LabelConfig = Field(default_factory=LabelConfig)
    split_embargo_seconds: int = Field(60, ge=0)
    stale_data_seconds: float = Field(3.0, gt=0, le=60)
    heal: HealConfig = Field(default_factory=HealConfig)

    @model_validator(mode="after")
    def _consistent(self) -> DomainConfig:
        cadence = self.prediction_cadence_seconds
        windows = self.feature_windows_seconds
        if len(set(windows)) != len(windows) or list(windows) != sorted(windows) or windows[0] < 1:
            raise ValueError("feature_windows_seconds must be positive, unique and ascending")
        if self.label_horizon_seconds % cadence or self.label.reference_window_seconds % cadence:
            raise ValueError("label horizon and reference window must be multiples of the cadence")
        if self.split_embargo_seconds < self.label_horizon_seconds:
            raise ValueError("split_embargo_seconds must be at least label_horizon_seconds")
        unknown = sorted(set(self.drift_watch) - set(self.feature_names))
        if unknown:
            raise ValueError(f"drift_watch names unknown features: {unknown}")
        return self

    @property
    def feature_names(self) -> list[str]:
        from sentinalops.features.microstructure import feature_names

        return feature_names(self.feature_windows_seconds)

    @property
    def stale_ms(self) -> int:
        return int(round(self.stale_data_seconds * 1000))

    def feature_params(self) -> FeatureParams:
        from sentinalops.features.microstructure import FeatureParams

        return FeatureParams(
            windows_s=self.feature_windows_seconds,
            cadence_s=self.prediction_cadence_seconds,
            stale_ms=self.stale_ms,
        )

    def label_params(self) -> LabelParams:
        from sentinalops.labels import LabelParams

        lab = self.label
        return LabelParams(
            cadence_s=self.prediction_cadence_seconds,
            horizon_s=self.label_horizon_seconds,
            reference_window_s=lab.reference_window_seconds,
            min_reference_ticks=lab.min_reference_seconds // self.prediction_cadence_seconds,
            depth_fraction=lab.depth_fraction,
            spread_multiple=lab.spread_multiple,
            use_spread=lab.secondary is not None,
            confirm_ticks=lab.confirm_ticks,
            stale_ms=self.stale_ms,
        )


class Settings(BaseModel):
    """Runtime settings, read from environment variables."""

    model_path: Path = Path("models/champion.joblib")
    data_dir: Path = Path("data")
    domain_config: Path = DEFAULT_DOMAIN_CONFIG
    log_level: str = "INFO"
    alert_threshold: float | None = Field(None, ge=0, le=1)  # overrides the domain config
    kafka_broker: str = "localhost:9092"
    mlflow_tracking_uri: str = "http://localhost:5000"
    redis_url: str = "redis://localhost:6379"

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Settings:
        e = os.environ if env is None else env
        raw = {
            "model_path": e.get("MODEL_PATH"),
            "data_dir": e.get("DATA_DIR"),
            "domain_config": e.get("DOMAIN_CONFIG"),
            "log_level": e.get("LOG_LEVEL"),
            "alert_threshold": e.get("ALERT_THRESHOLD"),
            "kafka_broker": e.get("KAFKA_BROKER"),
            "mlflow_tracking_uri": e.get("MLFLOW_TRACKING_URI"),
            "redis_url": e.get("REDIS_URL"),
        }
        return cls(**{k: v for k, v in raw.items() if v not in (None, "")})

    def effective_threshold(self, cfg: DomainConfig) -> float:
        return cfg.alert_threshold if self.alert_threshold is None else self.alert_threshold


def load_domain_config(path: str | Path) -> DomainConfig:
    with Path(path).open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: expected a mapping at the top level")
    return DomainConfig(**raw)


@lru_cache(maxsize=8)
def get_domain_config(path: str | Path = DEFAULT_DOMAIN_CONFIG) -> DomainConfig:
    return load_domain_config(path)
