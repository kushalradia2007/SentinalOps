"""Derived features v1: extra columns computed from the 34 raw inputs of the same tick. FROZEN.

Short-versus-long window contrasts and book shape. Every ratio adds ``EPS`` to its denominator,
so zero inputs (a silent minute, an empty side) give finite values.
"""

from __future__ import annotations

import numpy as np

EPS = 1e-12

# (name, numerator, denominator) ratio contrasts; a few transforms follow in compute()
_RATIOS = [
    ("intensity_1s_vs_60s", "trade_intensity_1s", "trade_intensity_60s"),
    ("intensity_5s_vs_60s", "trade_intensity_5s", "trade_intensity_60s"),
    ("vol_5s_vs_60s", "realized_vol_5s", "realized_vol_60s"),
    ("vol_1s_vs_30s", "realized_vol_1s", "realized_vol_30s"),
    ("return_5s_vs_vol_60s", "mid_return_5s", "realized_vol_60s"),
]
NAMES = [r[0] for r in _RATIOS] + [
    "abs_trade_imbalance_5s", "depth_change_5s_minus_60s", "log_bid_ask_depth", "log_depth_top5",
]


def compute(X: np.ndarray, names: list[str]) -> np.ndarray:
    """Derived columns (n, len(NAMES)) for raw features ``X`` whose columns are ``names``."""
    X = np.asarray(X, dtype=np.float64)
    col = {n: X[:, i] for i, n in enumerate(names)}
    out = [col[a] / (np.abs(col[b]) + EPS) for _, a, b in _RATIOS]
    out += [
        np.abs(col["trade_imbalance_5s"]),
        col["depth_change_5s"] - col["depth_change_60s"],
        np.log((col["bid_depth"] + EPS) / (col["ask_depth"] + EPS)),
        np.log(col["depth_top5"] + EPS),
    ]
    return np.column_stack(out)
