"""Stream processor (chunk 19): raw trades and depth in, feature ticks and stress predictions out.

The processor is transport-agnostic: feed it events with ``on_trade`` / ``on_depth`` (rows in
the recorder schema) and it returns what to publish. ``run`` wires it to any async event source.

Event time is ``recv_time_ms``, the same clock the offline pipeline uses.

**When a tick is emitted.** Tick ``t`` (a whole multiple of the cadence) is computed once the
watermark, the newest receive time seen on either stream, reaches ``t + allowed_lateness_ms``.
Waiting a little lets the two streams, which arrive through separate topics, catch up with
each other. ``advance(now_ms)`` moves the watermark with the wall clock, so ticks keep
coming (and come out invalid) when the feed goes silent.

**Late events (task 19.5).** An event whose receive time is at or before the last emitted
tick arrived too late to be counted in it. It is dropped and counted in ``stats.late``;
offline it would have been included, so ``late`` must stay at zero for online and offline
features to agree. Out-of-order events that are not late are inserted in order.

**Suspension (task 18.5).** ``compute_tick`` returns nothing while the book is stale and
until the feed has been continuous for the longest window again, so a stale feed yields no
feature ticks and no predictions, and recovery needs a full warm-up. No extra logic.

**Buffers (task 19.2).** Trades are kept for the longest window. Depth is kept for the longest
window plus the one snapshot just before it, which is exactly what ``compute_tick`` needs to
give the same answer as on the full history (tested; dropping that snapshot breaks the test).
A further ``stale_ms`` of depth is kept as a safety margin.
"""

from __future__ import annotations

import bisect
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field

import pandas as pd

from sentinalops.config import DomainConfig
from sentinalops.data.recorder.messages import AGG_TRADE, DEPTH5, DEPTH_SCHEMA, TRADE_SCHEMA
from sentinalops.features.microstructure import compute_tick
from sentinalops.logging import get_logger
from sentinalops.schemas import FeatureTick, StressPrediction
from sentinalops.serving.model import ChampionModel, ModelUnavailable

log = get_logger("sentinalops.stream")

FEATURES_TOPIC = "features"
PREDICTIONS_TOPIC = "predictions"
Output = tuple[str, FeatureTick | StressPrediction]


@dataclass
class ProcessorStats:
    events: dict[str, int] = field(default_factory=lambda: {AGG_TRADE: 0, DEPTH5: 0})
    late: dict[str, int] = field(default_factory=lambda: {AGG_TRADE: 0, DEPTH5: 0})
    other_symbol: int = 0
    ticks: int = 0
    feature_ticks: int = 0
    suspended_ticks: int = 0  # ticks with no valid features: stale feed or warm-up
    predictions: int = 0
    shadow_predictions: int = 0
    scoring_errors: int = 0
    last_tick_ms: int | None = None


class _Buffer:
    """Rows of one stream ordered by receive time."""

    def __init__(self) -> None:
        self.times: list[int] = []
        self.rows: list[dict] = []

    def insert(self, row: dict) -> None:
        t = row["recv_time_ms"]
        if not self.times or t >= self.times[-1]:
            self.times.append(t)
            self.rows.append(row)
        else:
            i = bisect.bisect_right(self.times, t)
            self.times.insert(i, t)
            self.rows.insert(i, row)

    def drop_through(self, cutoff_ms: int, keep_one_before: bool) -> None:
        """Forget rows with time <= cutoff (keeping the newest such row if asked)."""
        n = bisect.bisect_right(self.times, cutoff_ms)
        if keep_one_before:
            n = max(n - 1, 0)
        if n:
            del self.times[:n]
            del self.rows[:n]

    def frame(self, columns: list[str]) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=columns)


class StreamProcessor:
    def __init__(
        self,
        cfg: DomainConfig,
        model: ChampionModel | None = None,
        allowed_lateness_ms: int = 500,
        shadow: ChampionModel | None = None,
        alert_threshold: float | None = None,
    ) -> None:
        if allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms must be >= 0")
        self.cfg = cfg
        self.params = cfg.feature_params()
        self.model = model
        self.shadow = shadow  # the previous champion during a shadow period (chunk 45)
        self.lateness = allowed_lateness_ms
        self.threshold = cfg.alert_threshold if alert_threshold is None else alert_threshold
        self.step = self.params.cadence_s * 1000
        self.trades, self.depth = _Buffer(), _Buffer()
        self.watermark: int | None = None
        self.next_tick: int | None = None
        self.stats = ProcessorStats()

    # ---- input -------------------------------------------------------------------------------
    def on_trade(self, row: dict) -> list[Output]:
        return self._on_event(AGG_TRADE, row)

    def on_depth(self, row: dict) -> list[Output]:
        return self._on_event(DEPTH5, row)

    def on_event(self, kind: str, row: dict) -> list[Output]:
        if kind not in (AGG_TRADE, DEPTH5):
            raise ValueError(f"unknown stream kind {kind!r}")
        return self._on_event(kind, row)

    def _on_event(self, kind: str, row: dict) -> list[Output]:
        if row.get("symbol") != self.cfg.symbol:
            self.stats.other_symbol += 1
            return []
        t = int(row["recv_time_ms"])
        self.stats.events[kind] += 1
        if self.stats.last_tick_ms is not None and t <= self.stats.last_tick_ms:
            self.stats.late[kind] += 1
            return []
        (self.trades if kind == AGG_TRADE else self.depth).insert(row)
        if self.next_tick is None:
            self.next_tick = -(-t // self.step) * self.step  # first whole tick at or after the first event
        return self.advance(t)

    # ---- clock -------------------------------------------------------------------------------
    def advance(self, now_ms: int) -> list[Output]:
        """Move the watermark to ``now_ms`` (if later) and emit every tick that is now due."""
        if self.watermark is None or now_ms > self.watermark:
            self.watermark = now_ms
        out: list[Output] = []
        while self.next_tick is not None and self.next_tick + self.lateness <= self.watermark:
            out += self._emit(self.next_tick)
            self.next_tick += self.step
        return out

    def _emit(self, ts_ms: int) -> list[Output]:
        self.stats.ticks += 1
        self.stats.last_tick_ms = ts_ms
        # Forget what this tick (and so every later one) can no longer use.
        self.trades.drop_through(ts_ms - self.params.max_window_ms, keep_one_before=False)
        self.depth.drop_through(ts_ms - self.params.buffer_ms, keep_one_before=True)

        feats = self._features(ts_ms)
        if feats is None:
            self.stats.suspended_ticks += 1
            return []
        tick = FeatureTick(symbol=self.cfg.symbol, ts_ms=ts_ms, features=feats)
        self.stats.feature_ticks += 1
        out: list[Output] = [(FEATURES_TOPIC, tick)]
        if self.model is not None:
            try:
                p, version = self.model.predict(feats)
            except ModelUnavailable as e:
                self.stats.scoring_errors += 1
                log.warning("cannot score", extra={"ts_ms": ts_ms, "error": str(e)})
                return out
            out.append((PREDICTIONS_TOPIC, self._prediction(ts_ms, p, version)))
            self.stats.predictions += 1
        if self.shadow is not None and self.shadow.available:
            try:  # shadow scores go to the ledger under their own version; alerting uses the champion's
                p, version = self.shadow.predict(feats)
                out.append((PREDICTIONS_TOPIC, self._prediction(ts_ms, p, version)))
                self.stats.shadow_predictions += 1
            except ModelUnavailable:
                pass
        return out

    def _prediction(self, ts_ms: int, p: float, version: str) -> StressPrediction:
        return StressPrediction(
            symbol=self.cfg.symbol, ts_ms=ts_ms, horizon_s=self.cfg.label_horizon_seconds,
            probability=p, alert=p >= self.threshold, threshold=self.threshold, model_version=version,
        )

    def _features(self, ts_ms: int) -> dict[str, float] | None:
        if not self.depth.rows:
            return None
        depth = self.depth.frame(DEPTH_SCHEMA.names)
        trades = self.trades.frame(TRADE_SCHEMA.names)
        tick = compute_tick(depth, trades, ts_ms, self.params)
        if tick is None:
            return None
        return {name: tick[name] for name in self.params.names}


# ---- wiring ----------------------------------------------------------------------------------------
Publish = Callable[[str, str, dict], Awaitable[None]]  # (topic, key, value)


async def run(
    processor: StreamProcessor, events: AsyncIterator[tuple[str, dict]], publish: Publish
) -> ProcessorStats:
    """Consume ``(kind, row)`` events until the source ends and publish every output keyed by symbol."""
    async for kind, row in events:
        for topic, msg in processor.on_event(kind, row):
            await publish(topic, msg.symbol, msg.model_dump(mode="json"))
    return processor.stats
