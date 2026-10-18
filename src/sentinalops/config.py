"""Typed configuration: per-domain YAML + environment settings."""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class DomainConfig(BaseModel):
    """Everything that changes between domains lives in one YAML file."""

    domain: str
    thesis: str = ""
    sources: dict[str, list[str]] = Field(default_factory=dict)
    features_module: str
    drift_watch: list[str]
    drift_threshold: float = Field(0.3, ge=0, le=1)
    retrain_window_hours: int = Field(48, gt=0)
    promotion_metric: str = "avg_precision"
    block_threshold: float = Field(0.5, ge=0, le=1)
    label_delay_minutes: int = Field(60, ge=0)


class Settings(BaseModel):
    """Runtime settings, read from environment variables."""

    model_path: Path = Path("models/champion.joblib")
    data_dir: Path = Path("data")
    domain_config: Path = Path("configs/domain_supply_chain.yaml")
    log_level: str = "INFO"
    block_threshold: float | None = Field(None, ge=0, le=1)  # overrides domain config
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
            "block_threshold": e.get("BLOCK_THRESHOLD"),
            "kafka_broker": e.get("KAFKA_BROKER"),
            "mlflow_tracking_uri": e.get("MLFLOW_TRACKING_URI"),
            "redis_url": e.get("REDIS_URL"),
        }
        return cls(**{k: v for k, v in raw.items() if v not in (None, "")})

    def effective_threshold(self, cfg: DomainConfig) -> float:
        return cfg.block_threshold if self.block_threshold is None else self.block_threshold


def load_domain_config(path: str | Path) -> DomainConfig:
    with Path(path).open() as f:
        return DomainConfig(**yaml.safe_load(f))


@lru_cache(maxsize=8)
def get_domain_config(path: str | Path = "configs/domain_supply_chain.yaml") -> DomainConfig:
    return load_domain_config(path)
