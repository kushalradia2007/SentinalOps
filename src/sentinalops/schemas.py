"""Event and API schemas shared by the recorder, stream processor, API and outcome verifier.

Conventions (same as the recorder's Parquet files):

- Timestamps are UTC epoch milliseconds (``int``).
- Prices and quantities are floats.
- ``TradeEvent`` and ``DepthSnapshot`` round-trip with the recorder's flat row dicts through
  ``from_row`` / ``to_row``, so the stream and the offline files share one shape.

Feature dictionaries are checked for finite values here; whether they carry the right
feature *names* depends on the domain config, so the API checks that against
``DomainConfig.feature_names``.
"""

from __future__ import annotations

import math
import time
import uuid
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator, model_validator

DEPTH_LEVELS = 5
Symbol = Annotated[str, StringConstraints(pattern=r"^[A-Z0-9]{5,20}$")]


def _now_ms() -> int:
    return int(time.time() * 1000)


class _Event(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class TradeEvent(_Event):
    """One aggregate trade (``<symbol>@aggTrade``). Field names match the recorder's trade files."""

    symbol: Symbol
    event_time_ms: int = Field(gt=0)
    trade_time_ms: int = Field(gt=0)
    agg_trade_id: int = Field(ge=0)
    price: float = Field(gt=0)
    quantity: float = Field(gt=0)
    first_trade_id: int = Field(ge=0)
    last_trade_id: int = Field(ge=0)
    buyer_is_maker: bool
    recv_time_ms: int = Field(gt=0)

    @model_validator(mode="after")
    def _ids_ordered(self) -> TradeEvent:
        if self.last_trade_id < self.first_trade_id:
            raise ValueError("last_trade_id must be >= first_trade_id")
        if not (math.isfinite(self.price) and math.isfinite(self.quantity)):
            raise ValueError("price and quantity must be finite")
        return self

    @property
    def is_buy(self) -> bool:
        """Buyer-initiated: the buyer took liquidity, so the seller was the maker."""
        return not self.buyer_is_maker

    @classmethod
    def from_row(cls, row: dict) -> TradeEvent:
        return cls(**row)

    def to_row(self) -> dict:
        return self.model_dump()


class Level(_Event):
    px: float = Field(gt=0)
    qty: float = Field(ge=0)


class DepthSnapshot(_Event):
    """Top-of-book snapshot (``<symbol>@depth5@100ms``), best level first on each side."""

    symbol: Symbol
    last_update_id: int = Field(ge=0)
    recv_time_ms: int = Field(gt=0)
    bids: tuple[Level, ...] = Field(min_length=DEPTH_LEVELS, max_length=DEPTH_LEVELS)
    asks: tuple[Level, ...] = Field(min_length=DEPTH_LEVELS, max_length=DEPTH_LEVELS)

    @model_validator(mode="after")
    def _book_is_sane(self) -> DepthSnapshot:
        bid_px = [lv.px for lv in self.bids]
        ask_px = [lv.px for lv in self.asks]
        if any(b <= c for b, c in zip(bid_px, bid_px[1:], strict=False)):
            raise ValueError("bid prices must strictly decrease from the best level")
        if any(a >= c for a, c in zip(ask_px, ask_px[1:], strict=False)):
            raise ValueError("ask prices must strictly increase from the best level")
        if bid_px[0] >= ask_px[0]:
            raise ValueError("crossed book: best bid must be below best ask")
        return self

    @property
    def mid(self) -> float:
        return (self.bids[0].px + self.asks[0].px) / 2

    @classmethod
    def from_row(cls, row: dict) -> DepthSnapshot:
        def side(name: str) -> tuple[dict, ...]:
            return tuple(
                {"px": row[f"{name}_px_{i}"], "qty": row[f"{name}_qty_{i}"]}
                for i in range(1, DEPTH_LEVELS + 1)
            )

        return cls(
            symbol=row["symbol"],
            last_update_id=row["last_update_id"],
            recv_time_ms=row["recv_time_ms"],
            bids=side("bid"),
            asks=side("ask"),
        )

    def to_row(self) -> dict:
        row: dict = {
            "symbol": self.symbol,
            "last_update_id": self.last_update_id,
            "recv_time_ms": self.recv_time_ms,
        }
        for name, levels in (("bid", self.bids), ("ask", self.asks)):
            for i, lv in enumerate(levels, start=1):
                row[f"{name}_px_{i}"] = lv.px
                row[f"{name}_qty_{i}"] = lv.qty
        return row


def _finite_features(v: dict[str, float]) -> dict[str, float]:
    if not v:
        raise ValueError("features must not be empty")
    bad = sorted(k for k, x in v.items() if not math.isfinite(x))
    if bad:
        raise ValueError(f"features must be finite; bad: {bad}")
    return v


class FeatureTick(_Event):
    """One valid feature tick: the model input for ``symbol`` at ``ts_ms``."""

    symbol: Symbol
    ts_ms: int = Field(gt=0)
    features: dict[str, float]

    @field_validator("features")
    @classmethod
    def check_features(cls, v: dict[str, float]) -> dict[str, float]:
        return _finite_features(v)


class StressPrediction(_Event):
    """A forecast that stress starts in ``(ts_ms, ts_ms + horizon_s]``. Goes to the prediction ledger."""

    prediction_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    symbol: Symbol
    ts_ms: int = Field(gt=0)
    horizon_s: int = Field(gt=0)
    probability: float = Field(ge=0, le=1)
    alert: bool
    threshold: float = Field(ge=0, le=1)
    model_version: str
    scored_at_ms: int = Field(default_factory=_now_ms)

    @property
    def matures_at_ms(self) -> int:
        return self.ts_ms + self.horizon_s * 1000


class OutcomeLabel(_Event):
    """What actually happened in the horizon after ``ts_ms``, produced once the horizon has passed.

    ``label`` is ``None`` when the outcome is unknowable (a feed gap inside the horizon, or the
    market was already stressed at ``ts_ms``); such outcomes are recorded but never scored.
    """

    symbol: Symbol
    ts_ms: int = Field(gt=0)
    horizon_s: int = Field(gt=0)
    label: Literal[0, 1] | None
    void_reason: Literal["gap", "already_stressed", "warmup"] | None = None
    source: str = "outcome_verifier"
    verified_at_ms: int = Field(default_factory=_now_ms)

    @model_validator(mode="after")
    def _void_has_reason(self) -> OutcomeLabel:
        if (self.label is None) != (self.void_reason is not None):
            raise ValueError("void_reason must be set exactly when label is None")
        return self


class ScoreRequest(BaseModel):
    """Body of ``POST /score``: one feature tick."""

    model_config = ConfigDict(extra="forbid")

    symbol: Symbol
    ts_ms: int = Field(gt=0)
    features: dict[str, float]

    @field_validator("features")
    @classmethod
    def check_features(cls, v: dict[str, float]) -> dict[str, float]:
        return _finite_features(v)
