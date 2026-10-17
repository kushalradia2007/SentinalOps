"""Shared event and API schemas, used by the stream, store, API and retraining flows."""
from __future__ import annotations

import time
from typing import Literal

from pydantic import BaseModel, Field, field_validator


def _now() -> float:
    return time.time()


class PackageEvent(BaseModel):
    """A newly published package version (the raw stream topic)."""

    registry: Literal["pypi", "npm", "github"]
    name: str
    version: str = "0"
    files: dict[str, str] = Field(default_factory=dict)
    meta: dict = Field(default_factory=dict)
    published_at: float = Field(default_factory=_now)


class FeatureRecord(BaseModel):
    """Extracted features for one package (the features topic / feature store row)."""

    package: str
    version: str = "0"
    features: dict[str, float]
    ts: float = Field(default_factory=_now)


class Prediction(BaseModel):
    package: str
    score: float = Field(ge=0, le=1)
    blocked: bool
    model_version: str
    ts: float = Field(default_factory=_now)


class LabelEvent(BaseModel):
    """Delayed ground truth (OSV.dev / ReversingLabs / simulated drop)."""

    package: str
    is_malicious: bool
    source: str = "manual"
    ts: float = Field(default_factory=_now)


class ScoreRequest(BaseModel):
    files: dict[str, str]
    meta: dict

    @field_validator("meta")
    @classmethod
    def _needs_name(cls, v: dict) -> dict:
        if not v.get("name"):
            raise ValueError("meta.name is required")
        return v


class ScoreResponse(BaseModel):
    package: str
    score: float
    blocked: bool
    model_version: str


class LabelRequest(BaseModel):
    package: str
    is_malicious: bool
    source: str = "api"
