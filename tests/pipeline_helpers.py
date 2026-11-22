"""Synthetic recordings in the recorder's on-disk layout, for pipeline tests and the speed check."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from sentinalops.data.recorder.messages import AGG_TRADE, DEPTH5
from sentinalops.data.recorder.sink import ParquetSink

H0 = 1_763_200_800_000  # 2025-11-15T10:00:00Z, an hour boundary


def make_events(
    start_ms: int,
    seconds: int,
    *,
    symbol: str = "BTCUSDT",
    snapshot_ms: int = 500,
    trades_per_s: float = 2.0,
    stress: tuple[tuple[int, int], ...] = (),
    holes: tuple[tuple[int, int], ...] = (),
    seed: int = 0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(depth, trades) in the recorder schema.

    ``stress`` is a list of ``(offset_s, duration_s)`` episodes where the book thins to 20%.
    ``holes`` are ``(offset_s, duration_s)`` stretches with no events of either kind.
    """
    rng = np.random.default_rng(seed)
    t = start_ms + np.arange(0, seconds * 1000, snapshot_ms, dtype=np.int64)
    t = t + rng.integers(0, 40, len(t))  # receive jitter
    n = len(t)
    thin = np.ones(n)
    for off, dur in stress:
        thin[(t >= start_ms + off * 1000) & (t < start_ms + (off + dur) * 1000)] = 0.2
    mid = 90_000 + np.cumsum(rng.normal(0, 0.5, n)).round(2)
    depth = {"symbol": symbol, "last_update_id": np.arange(n, dtype=np.int64) + 1_000, "recv_time_ms": t}
    for k in range(1, 6):
        depth[f"bid_px_{k}"] = mid - 0.005 - (k - 1) * 0.01
        depth[f"bid_qty_{k}"] = rng.uniform(1.0, 3.0, n) * thin
        depth[f"ask_px_{k}"] = mid + 0.005 + (k - 1) * 0.01
        depth[f"ask_qty_{k}"] = rng.uniform(1.0, 3.0, n) * thin
    depth = pd.DataFrame(depth)

    m = rng.poisson(trades_per_s * seconds)
    tt = np.sort(start_ms + rng.integers(0, seconds * 1000, m)).astype(np.int64)
    trades = pd.DataFrame(
        {
            "symbol": symbol,
            "event_time_ms": tt - 5,
            "trade_time_ms": tt - 6,
            "agg_trade_id": np.arange(m, dtype=np.int64) + 50_000,
            "price": 90_000.0 + rng.normal(0, 5, m).round(2),
            "quantity": rng.uniform(0.001, 0.2, m),
            "first_trade_id": np.arange(m, dtype=np.int64) * 3,
            "last_trade_id": np.arange(m, dtype=np.int64) * 3 + 2,
            "buyer_is_maker": rng.random(m) < 0.5,
            "recv_time_ms": tt,
        }
    )
    for off, dur in holes:
        lo, hi = start_ms + off * 1000, start_ms + (off + dur) * 1000
        depth = depth[(depth.recv_time_ms < lo) | (depth.recv_time_ms >= hi)]
        trades = trades[(trades.recv_time_ms < lo) | (trades.recv_time_ms >= hi)]
    return depth.reset_index(drop=True), trades.reset_index(drop=True)


def write_recording(root: Path, depth: pd.DataFrame, trades: pd.DataFrame, chunk_s: int = 600) -> None:
    """Write through the real ``ParquetSink``, several files per hour like a live recorder."""
    sink = ParquetSink(root)
    for kind, df in ((DEPTH5, depth), (AGG_TRADE, trades)):
        if len(df) == 0:
            continue
        bucket = df["recv_time_ms"] // (chunk_s * 1000)
        for _, part in df.groupby(bucket, sort=True):
            for row in part.to_dict("records"):
                sink.add(kind, row)
            res = sink.flush()
            assert not res.errors, res.errors
