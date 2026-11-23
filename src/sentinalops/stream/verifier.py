"""Outcome verifier (chunk 34): the live version of the chunk 9 label.

Fed the raw depth stream, it decides for every tick ``t`` whether liquidity stress started in
``(t, t + H]``, once tick ``t + H`` has been seen, and returns an ``OutcomeLabel``. It
re-implements ``labels.build_labels`` incrementally, step for step:

1. as-of book state per tick: newest snapshot with ``recv_time_ms <= t``; unknown if older
   than ``stale_ms`` (``labels.to_ticks``),
2. trailing reference: median of the previous ``reference_ticks`` ticks, ignoring unknown
   ticks, trusted once ``min_reference_ticks`` are known (``labels.add_stress``),
3. ``stressed`` and ``confirmed`` (stressed for ``confirm_ticks`` ticks in a row),
4. the onset label at ``t`` from ticks ``t+1 .. t+H`` (``labels.add_labels``).

The replay test checks it against ``build_labels`` and ``label_at`` tick by tick.

Void outcomes (``label=None``) carry a reason:

- ``already_stressed``: the market was stressed at ``t``; onset is not defined.
- ``gap``: the book was stale somewhere in ``[t - confirm_ticks + 1, t + H]``.
- ``warmup``: otherwise unknown, i.e. the trailing reference was not established yet.

Clock, watermark and late events follow the stream processor (``docs/streaming.md``).
"""

from __future__ import annotations

import bisect
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import numpy as np

from sentinalops.config import DomainConfig
from sentinalops.data.recorder.messages import DEPTH5
from sentinalops.schemas import OutcomeLabel
from sentinalops.stream.processor import Publish

OUTCOMES_TOPIC = "outcomes"
NAN = float("nan")


@dataclass
class VerifierStats:
    snapshots: int = 0
    late: int = 0
    other_symbol: int = 0
    rejected_books: int = 0  # crossed or empty, ignored like labels.book_state does
    ticks: int = 0
    outcomes: dict[str, int] = field(
        default_factory=lambda: {"1": 0, "0": 0, "already_stressed": 0, "gap": 0, "warmup": 0}
    )
    last_tick_ms: int | None = None


@dataclass
class _Tick:
    ts_ms: int
    stale: bool
    stressed: float  # 0, 1 or NaN
    confirmed: float  # 0, 1 or NaN


class OutcomeVerifier:
    def __init__(self, cfg: DomainConfig, allowed_lateness_ms: int = 500) -> None:
        if allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms must be >= 0")
        self.cfg = cfg
        self.p = cfg.label_params()
        self.lateness = allowed_lateness_ms
        self.step = self.p.cadence_s * 1000
        self.min_ref = min(self.p.min_reference_ticks, self.p.reference_ticks)
        # snapshot buffer: (recv_time_ms, depth_top5, rel_spread)
        self._snap_t: list[int] = []
        self._snap_v: list[tuple[float, float]] = []
        self._ref_depth: deque[float] = deque(maxlen=self.p.reference_ticks)
        self._ref_spread: deque[float] = deque(maxlen=self.p.reference_ticks)
        self._recent_stressed: deque[float] = deque(maxlen=self.p.confirm_ticks)
        # ticks still needed: t - H (to resolve) .. now, plus confirm_ticks before for the gap reason
        self._history: deque[_Tick] = deque(maxlen=self.p.horizon_ticks + self.p.confirm_ticks + 1)
        self.watermark: int | None = None
        self.next_tick: int | None = None
        self.stats = VerifierStats()

    # ---- input -------------------------------------------------------------------------------
    def on_depth(self, row: dict) -> list[OutcomeLabel]:
        if row.get("symbol") != self.cfg.symbol:
            self.stats.other_symbol += 1
            return []
        t = int(row["recv_time_ms"])
        self.stats.snapshots += 1
        if self.stats.last_tick_ms is not None and t <= self.stats.last_tick_ms:
            self.stats.late += 1
            return []
        bid, ask = float(row["bid_px_1"]), float(row["ask_px_1"])
        total = sum(float(row[f"{s}_qty_{i}"]) for s in ("bid", "ask") for i in range(1, 6))
        if not (bid > 0 and ask > bid and total > 0):
            self.stats.rejected_books += 1
            return self.advance(t)
        mid = (bid + ask) / 2
        i = bisect.bisect_right(self._snap_t, t)
        self._snap_t.insert(i, t)
        self._snap_v.insert(i, (total, (ask - bid) / mid))
        if self.next_tick is None:
            self.next_tick = -(-t // self.step) * self.step
        return self.advance(t)

    def on_event(self, kind: str, row: dict) -> list[OutcomeLabel]:
        """Accepts the combined stream; only depth matters for the label."""
        return self.on_depth(row) if kind == DEPTH5 else []

    # ---- clock -------------------------------------------------------------------------------
    def advance(self, now_ms: int) -> list[OutcomeLabel]:
        if self.watermark is None or now_ms > self.watermark:
            self.watermark = now_ms
        out: list[OutcomeLabel] = []
        while self.next_tick is not None and self.next_tick + self.lateness <= self.watermark:
            resolved = self._tick(self.next_tick)
            if resolved is not None:
                out.append(resolved)
            self.next_tick += self.step
        return out

    # ---- one tick ----------------------------------------------------------------------------
    def _state(self, ts_ms: int) -> tuple[float, float, bool]:
        """(depth_top5, rel_spread, stale) as of ``ts_ms``; drops snapshots no later tick needs."""
        i = bisect.bisect_right(self._snap_t, ts_ms) - 1
        if i < 0:
            return NAN, NAN, True
        snap_t, (depth, spread) = self._snap_t[i], self._snap_v[i]
        del self._snap_t[:i], self._snap_v[:i]  # keep the as-of snapshot for the next tick
        if ts_ms - snap_t > self.p.stale_ms:
            return NAN, NAN, True
        return depth, spread, False

    def _reference(self, values: deque[float]) -> float:
        arr = np.fromiter(values, float, len(values))
        known = arr[~np.isnan(arr)]
        return float(np.median(known)) if len(known) >= self.min_ref else NAN

    def _tick(self, ts_ms: int) -> OutcomeLabel | None:
        self.stats.ticks += 1
        self.stats.last_tick_ms = ts_ms
        depth, spread, stale = self._state(ts_ms)

        ref_depth, ref_spread = self._reference(self._ref_depth), self._reference(self._ref_spread)
        self._ref_depth.append(depth)  # the reference for later ticks includes this one
        self._ref_spread.append(spread)
        depth_ratio, spread_ratio = depth / ref_depth, spread / ref_spread
        if np.isnan(depth_ratio) or np.isnan(spread_ratio):
            stressed = NAN
        else:
            hit = depth_ratio < self.p.depth_fraction or (
                self.p.use_spread and spread_ratio > self.p.spread_multiple
            )
            stressed = float(hit)
        self._recent_stressed.append(stressed)
        recent = list(self._recent_stressed)
        if len(recent) < self.p.confirm_ticks or any(np.isnan(recent)):
            confirmed = NAN
        else:
            confirmed = min(recent)
        self._history.append(_Tick(ts_ms, stale, stressed, confirmed))
        return self._resolve(ts_ms - self.p.horizon_ticks * self.step)

    def _resolve(self, t: int) -> OutcomeLabel | None:
        """Outcome of tick ``t`` now that tick ``t + H`` is known, or None if ``t`` predates the stream."""
        hist = list(self._history)
        idx = next((i for i, h in enumerate(hist) if h.ts_ms == t), None)
        if idx is None:
            return None
        at, future = hist[idx], hist[idx + 1 :]
        label: int | None
        if at.stressed == 1:
            label, reason = None, "already_stressed"
        elif np.isnan(at.stressed) or any(np.isnan(h.confirmed) for h in future):
            label = None
            window = hist[max(idx - self.p.confirm_ticks + 1, 0) :]
            reason = "gap" if any(h.stale for h in window) else "warmup"
        else:
            label, reason = int(any(h.confirmed == 1 for h in future)), None
        self.stats.outcomes[str(label) if label is not None else reason] += 1
        return OutcomeLabel(
            symbol=self.cfg.symbol, ts_ms=t, horizon_s=self.p.horizon_s, label=label, void_reason=reason
        )


async def run_verifier(
    verifier: OutcomeVerifier, events: AsyncIterator[tuple[str, dict]], publish: Publish
) -> VerifierStats:
    """Consume ``(kind, row)`` events and publish every outcome to ``outcomes`` keyed by symbol."""
    async for kind, row in events:
        for outcome in verifier.on_event(kind, row):
            await publish(OUTCOMES_TOPIC, outcome.symbol, outcome.model_dump(mode="json"))
    return verifier.stats
