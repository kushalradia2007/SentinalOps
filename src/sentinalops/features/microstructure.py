"""Microstructure features (chunk 10), shared by the offline pipeline, stream processor and API.

Two implementations of the same definition:

- ``compute_tick(depth, trades, ts_ms)``  one tick, written to be obviously correct. The stream
  processor calls it on its rolling buffers.
- ``compute_ticks(depth, trades)``        every tick of a recording at once, vectorised. The
  offline pipeline calls it.

Tests assert they agree on every tick, which is what keeps online and offline features
consistent.

Definitions, for a tick at time ``t`` (UTC epoch ms) and a window of ``w`` seconds:

- The clock is ``recv_time_ms`` for both streams: an event exists for us when we received it.
- "Now" is the newest depth snapshot with ``recv_time_ms <= t``.
- A window is the half-open interval ``(t - w, t]``.
- "Then" (for change features) is the newest snapshot with ``recv_time_ms <= t - w``.
- A tick is **valid** only if the book is fresh (newest snapshot at most ``stale_ms`` old) and
  the feed has been continuous (no gap above ``stale_ms`` between snapshots) for at least the
  longest window. This one rule covers warm-up and recovery after an outage. Invalid ticks
  have no features.

Nothing here reads an event received after ``t``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import numpy as np
import pandas as pd

DEPTH_LEVELS = 5
WINDOWS_S = (1, 5, 30, 60)

INSTANT_FEATURES = ["rel_spread", "bid_depth", "ask_depth", "depth_top5", "depth_imbalance", "l1_imbalance"]
WINDOW_FEATURES = [
    "trade_intensity",  # trades per second
    "trade_volume",  # base-asset quantity traded
    "trade_imbalance",  # (buy - sell) / (buy + sell) volume, 0 when there were no trades
    "realized_vol",  # sqrt of summed squared log mid returns between snapshots
    "mid_return",  # log(mid now / mid then)
    "depth_change",  # depth now / depth then - 1
    "spread_change",  # relative spread now - then
]
CONTEXT_COLUMNS = ["mid", "spread_abs", "age_ms"]  # reported with each tick, not model inputs


def feature_names(windows_s: tuple[int, ...] = WINDOWS_S) -> list[str]:
    return INSTANT_FEATURES + [f"{name}_{w}s" for name in WINDOW_FEATURES for w in windows_s]


FEATURE_ORDER = feature_names()


@dataclass(frozen=True)
class FeatureParams:
    windows_s: tuple[int, ...] = WINDOWS_S
    cadence_s: int = 1
    stale_ms: int = 3000

    def __post_init__(self) -> None:
        if not self.windows_s or min(self.windows_s) < 1 or self.cadence_s < 1 or self.stale_ms < 1:
            raise ValueError("windows, cadence and stale_ms must be positive")

    @property
    def max_window_ms(self) -> int:
        return max(self.windows_s) * 1000

    @property
    def buffer_ms(self) -> int:
        """How much history a streaming caller must keep for ``compute_tick`` to be valid."""
        return self.max_window_ms + self.stale_ms

    @property
    def names(self) -> list[str]:
        return feature_names(self.windows_s)


# ---- one snapshot (tasks 10.1, 10.2) -------------------------------------------------------
def book_features(snapshot: Mapping) -> dict[str, float]:
    """Features of a single ``depth5`` snapshot (a row in the recorder's schema)."""
    bid, ask = float(snapshot["bid_px_1"]), float(snapshot["ask_px_1"])
    bid_depth = sum(float(snapshot[f"bid_qty_{i}"]) for i in range(1, DEPTH_LEVELS + 1))
    ask_depth = sum(float(snapshot[f"ask_qty_{i}"]) for i in range(1, DEPTH_LEVELS + 1))
    b1, a1 = float(snapshot["bid_qty_1"]), float(snapshot["ask_qty_1"])
    mid = (bid + ask) / 2
    total = bid_depth + ask_depth
    return {
        "mid": mid,
        "spread_abs": ask - bid,
        "rel_spread": (ask - bid) / mid,
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
        "depth_top5": total,
        "depth_imbalance": (bid_depth - ask_depth) / total if total > 0 else 0.0,
        "l1_imbalance": (b1 - a1) / (b1 + a1) if b1 + a1 > 0 else 0.0,
    }


# ---- shared preparation ---------------------------------------------------------------------
@dataclass(frozen=True)
class _Book:
    """Clean, time-ordered snapshot arrays. ``sq_ret[i]`` is the squared log mid return into snapshot i."""

    t: np.ndarray
    cols: dict[str, np.ndarray]
    sq_ret: np.ndarray
    run_start: np.ndarray  # receive time of the first snapshot of the gap-free run containing i


def _prepare_book(depth: pd.DataFrame, stale_ms: int) -> _Book:
    d = depth.sort_values("recv_time_ms", kind="stable")
    bid, ask = d["bid_px_1"].to_numpy(float), d["ask_px_1"].to_numpy(float)
    bq = d[[f"bid_qty_{i}" for i in range(1, DEPTH_LEVELS + 1)]].to_numpy(float)
    aq = d[[f"ask_qty_{i}" for i in range(1, DEPTH_LEVELS + 1)]].to_numpy(float)
    bid_depth, ask_depth = bq.sum(axis=1), aq.sum(axis=1)
    total = bid_depth + ask_depth
    ok = (bid > 0) & (ask > bid) & (total > 0)  # drop crossed or empty books

    t = d["recv_time_ms"].to_numpy(np.int64)[ok]
    bid, ask, bid_depth, ask_depth, total = bid[ok], ask[ok], bid_depth[ok], ask_depth[ok], total[ok]
    b1, a1 = bq[ok, 0], aq[ok, 0]
    mid = (bid + ask) / 2
    l1 = np.divide(b1 - a1, b1 + a1, out=np.zeros_like(b1), where=(b1 + a1) > 0)
    cols = {
        "mid": mid,
        "spread_abs": ask - bid,
        "rel_spread": (ask - bid) / mid,
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
        "depth_top5": total,
        "depth_imbalance": (bid_depth - ask_depth) / total,
        "l1_imbalance": l1,
    }
    sq_ret = np.zeros(len(t))
    if len(t) > 1:
        sq_ret[1:] = np.diff(np.log(mid)) ** 2
    new_run = np.ones(len(t), dtype=bool)
    if len(t) > 1:
        new_run[1:] = np.diff(t) > stale_ms
    run_start = np.maximum.accumulate(np.where(new_run, t, np.iinfo(np.int64).min)) if len(t) else t
    return _Book(t, cols, sq_ret, run_start)


def _prepare_trades(trades: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(receive time, quantity, buy-initiated quantity), time-ordered."""
    if trades is None or len(trades) == 0:
        return np.empty(0, np.int64), np.empty(0), np.empty(0)
    tr = trades.sort_values("recv_time_ms", kind="stable")
    qty = tr["quantity"].to_numpy(float)
    # buyer_is_maker = True means the seller crossed the spread: a sell-initiated trade
    buy = np.where(tr["buyer_is_maker"].to_numpy(bool), 0.0, qty)
    return tr["recv_time_ms"].to_numpy(np.int64), qty, buy


# ---- one tick, the reference (task 10.7) ------------------------------------------------------
def compute_tick(
    depth: pd.DataFrame, trades: pd.DataFrame, ts_ms: int, p: FeatureParams = FeatureParams()
) -> dict[str, float] | None:
    """Features at ``ts_ms`` from events received at or before it. ``None`` if the tick is not valid."""
    depth = depth[depth["recv_time_ms"] <= ts_ms]
    if trades is not None and len(trades):
        trades = trades[trades["recv_time_ms"] <= ts_ms]
    book = _prepare_book(depth, p.stale_ms)
    if len(book.t) == 0:
        return None
    now = len(book.t) - 1
    age = int(ts_ms - book.t[now])
    if age > p.stale_ms or ts_ms - book.run_start[now] < p.max_window_ms:
        return None

    out = {name: float(book.cols[name][now]) for name in INSTANT_FEATURES}
    tt, qty, buy = _prepare_trades(trades)
    for w in p.windows_s:
        lo = ts_ms - w * 1000
        in_window = tt > lo
        volume, buy_volume = float(qty[in_window].sum()), float(buy[in_window].sum())
        then = int(np.nonzero(book.t <= lo)[0][-1])  # exists: the run started at least max_window ago
        out[f"trade_intensity_{w}s"] = float(in_window.sum()) / w
        out[f"trade_volume_{w}s"] = volume
        out[f"trade_imbalance_{w}s"] = (2 * buy_volume - volume) / volume if volume > 0 else 0.0
        out[f"realized_vol_{w}s"] = float(np.sqrt(book.sq_ret[book.t > lo].sum()))
        out[f"mid_return_{w}s"] = float(np.log(book.cols["mid"][now] / book.cols["mid"][then]))
        out[f"depth_change_{w}s"] = float(book.cols["depth_top5"][now] / book.cols["depth_top5"][then] - 1)
        out[f"spread_change_{w}s"] = float(book.cols["rel_spread"][now] - book.cols["rel_spread"][then])
    for name in ("mid", "spread_abs"):
        out[name] = float(book.cols[name][now])
    out["age_ms"] = float(age)
    return out


# ---- every tick at once (offline) ----------------------------------------------------------------
def tick_grid(first_ms: int, last_ms: int, cadence_s: int = 1) -> np.ndarray:
    """Whole-cadence tick times from the first to the last event, inclusive."""
    step = cadence_s * 1000
    return np.arange(-(-first_ms // step) * step, (last_ms // step) * step + step, step, dtype=np.int64)


def compute_ticks(
    depth: pd.DataFrame, trades: pd.DataFrame, p: FeatureParams = FeatureParams()
) -> pd.DataFrame:
    """One row per tick, indexed by ``ts_ms``: ``valid``, the features, then the context columns.

    Features and context are NaN where ``valid`` is False.
    """
    columns = ["valid", *p.names, *CONTEXT_COLUMNS]
    book = _prepare_book(depth, p.stale_ms)
    if len(book.t) == 0:
        return pd.DataFrame(columns=columns, index=pd.Index([], name="ts_ms", dtype="int64"))
    grid = tick_grid(int(book.t[0]), int(book.t[-1]), p.cadence_s)
    now = np.searchsorted(book.t, grid, side="right") - 1  # newest snapshot at or before each tick
    age = grid - book.t[now]
    valid = (age <= p.stale_ms) & (grid - book.run_start[now] >= p.max_window_ms)

    data: dict[str, np.ndarray] = {name: book.cols[name][now] for name in INSTANT_FEATURES}
    tt, qty, buy = _prepare_trades(trades)
    cum_qty = np.concatenate(([0.0], np.cumsum(qty)))
    cum_buy = np.concatenate(([0.0], np.cumsum(buy)))
    cum_sq = np.concatenate(([0.0], np.cumsum(book.sq_ret)))
    hi = np.searchsorted(tt, grid, side="right")
    for w in p.windows_s:
        lo_time = grid - w * 1000
        lo = np.searchsorted(tt, lo_time, side="right")
        then_raw = np.searchsorted(book.t, lo_time, side="right") - 1
        then = np.clip(then_raw, 0, None)  # then_raw < 0 only on invalid (warm-up) ticks
        volume, buy_volume = cum_qty[hi] - cum_qty[lo], cum_buy[hi] - cum_buy[lo]
        data[f"trade_intensity_{w}s"] = (hi - lo) / w
        data[f"trade_volume_{w}s"] = volume
        data[f"trade_imbalance_{w}s"] = np.divide(
            2 * buy_volume - volume, volume, out=np.zeros(len(grid)), where=(hi > lo) & (volume > 0)
        )
        # snapshots with recv in (lo_time, t] are positions then_raw+1 .. now
        data[f"realized_vol_{w}s"] = np.sqrt(np.clip(cum_sq[now + 1] - cum_sq[then_raw + 1], 0, None))
        data[f"mid_return_{w}s"] = np.log(book.cols["mid"][now] / book.cols["mid"][then])
        data[f"depth_change_{w}s"] = book.cols["depth_top5"][now] / book.cols["depth_top5"][then] - 1
        data[f"spread_change_{w}s"] = book.cols["rel_spread"][now] - book.cols["rel_spread"][then]
    data["mid"], data["spread_abs"] = book.cols["mid"][now], book.cols["spread_abs"][now]
    data["age_ms"] = age.astype(float)

    out = pd.DataFrame(data, index=pd.Index(grid, name="ts_ms"))[[*p.names, *CONTEXT_COLUMNS]]
    out[~valid] = np.nan
    out.insert(0, "valid", valid)
    return out
