"""Synthetic feature ticks for two market regimes (chunk 3): the stand-in for recorded data.

Each row is one feature tick with the columns of ``FEATURE_ORDER`` plus ``label`` (1 if
liquidity stress starts within the horizon). The two regimes differ in *why* stress happens,
which is the shift the self-healing loop has to survive:

- **calm**: stress is preceded by the book thinning out. Top-5 depth falls over the last
  30-60 s and the spread widens; trading is quiet and balanced.
- **volatile**: stress is preceded by aggressive one-sided trading. Trade count and volume
  burst in the last 1-5 s, trade imbalance goes to one side and realized volatility jumps,
  while depth changes carry no information (they are noisy, and the book thins out often
  without stress following).

A model trained only on calm data watches the wrong features in the volatile regime. That is
the thesis test in ``tests/test_thesis.py``. The numbers are plausible for BTCUSDT but not
fitted to anything; real recorded data replaces this once chunk 12 exists.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from sentinalops.features.microstructure import FEATURE_ORDER, WINDOWS_S

REGIMES = ("calm", "volatile")
BASE_RATE = {"calm": 0.03, "volatile": 0.07}


def _window_frac(w: int, scale: float) -> float:
    """How much of a slow build-up a window of ``w`` seconds sees (0 for short, ~1 for long)."""
    return 1 - np.exp(-w / scale)


def make_ticks(regime: str = "calm", n: int = 20_000, seed: int = 0) -> pd.DataFrame:
    """``n`` shuffled feature ticks from ``regime``, with columns ``FEATURE_ORDER + ['label']``."""
    if regime not in REGIMES:
        raise ValueError(f"regime must be one of {REGIMES}, got {regime!r}")
    if n < 1:
        raise ValueError("n must be positive")
    rng = np.random.default_rng(seed)
    volatile = regime == "volatile"

    y = rng.binomial(1, BASE_RATE[regime], n)
    # Precursor strength. Markets are noisy: a fifth of stress onsets come with no warning,
    # warnings vary in strength, and a few calm-ahead ticks show a warning that fizzles out.
    warned = rng.binomial(1, 0.8, n) * rng.uniform(0.2, 1.4, n)
    false_alarm = rng.binomial(1, 0.03, n) * rng.uniform(0.2, 1.0, n)
    s = np.where(y == 1, warned, false_alarm)
    side = rng.choice([-1.0, 1.0], n)  # direction of a one-sided burst

    # --- book precursors (calm regime) ------------------------------------------------
    if volatile:
        # The book thins out often and for no reason; stress does not depend on it.
        thinning = rng.binomial(1, 0.25, n) * rng.uniform(0.1, 0.5, n)
    else:
        thinning = 0.4 * s
    depth_noise = 0.25 if volatile else 0.08

    depth_ref = rng.lognormal(np.log(12.0), 0.35, n)  # BTC on the top five levels
    depth_top5 = depth_ref * np.clip(1 - thinning + rng.normal(0, depth_noise / 2, n), 0.05, None)
    book_imb = np.clip(rng.normal(0, 0.25, n) + (0.4 * side * s if volatile else 0), -0.99, 0.99)
    bid_depth = depth_top5 * (1 + book_imb) / 2
    ask_depth = depth_top5 - bid_depth
    l1_imb = np.clip(book_imb + rng.normal(0, 0.3, n), -1, 1)

    spread_ref = 1.2e-7 * (3.0 if volatile else 1.0)
    spread_up = (0.0 if volatile else 1.2 * s) + (0.3 * thinning if volatile else 0.0)
    rel_spread = spread_ref * rng.lognormal(0, 0.25, n) * (1 + spread_up)

    cols: dict[str, np.ndarray] = {
        "rel_spread": rel_spread,
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
        "depth_top5": depth_top5,
        "depth_imbalance": book_imb,
        "l1_imbalance": l1_imb,
    }

    # --- trade precursors (volatile regime) -------------------------------------------
    base_rate = 9.0 if volatile else 3.0  # trades per second
    burst = 3.0 * s if volatile else 0.0
    base_vol = 0.004 if volatile else 0.0015  # per-sqrt-second mid volatility (log units)
    for w in WINDOWS_S:
        recent = 1 - _window_frac(w, 6.0)  # a burst dominates short windows, washes out of long ones
        build = _window_frac(w, 25.0)  # thinning shows up in longer windows
        rate = base_rate * (1 + burst * (0.3 + 0.7 * recent)) * rng.lognormal(0, 0.3, n)
        count = rng.poisson(rate * w)
        cols[f"trade_intensity_{w}s"] = count / w
        cols[f"trade_volume_{w}s"] = count * rng.lognormal(np.log(0.05), 0.5, n)
        imb = rng.normal(0, 0.3 / np.sqrt(w), n) + (0.7 * side * s * (0.4 + 0.6 * recent) if volatile else 0)
        cols[f"trade_imbalance_{w}s"] = np.where(count > 0, np.clip(imb, -1, 1), 0.0)
        vol_mult = 1 + (2.5 * s * (0.3 + 0.7 * recent) if volatile else 0)
        cols[f"realized_vol_{w}s"] = base_vol * np.sqrt(w) * vol_mult * rng.lognormal(0, 0.3, n)
        cols[f"mid_return_{w}s"] = rng.normal(0, base_vol * np.sqrt(w), n) + (
            side * s * base_vol * 3 * np.sqrt(w) if volatile else 0
        )
        cols[f"depth_change_{w}s"] = -thinning * build + rng.normal(0, depth_noise * np.sqrt(build), n)
        cols[f"spread_change_{w}s"] = rel_spread - rel_spread / (1 + spread_up * build) + rng.normal(
            0, spread_ref * 0.1, n
        )

    df = pd.DataFrame(cols)[FEATURE_ORDER]
    df["label"] = y
    return df


def example_ticks(seed: int = 0) -> tuple[dict[str, float], dict[str, float]]:
    """One clearly calm tick and one clearly pre-stress tick from the calm regime, for demos."""
    df = make_ticks("calm", 4_000, seed)
    calm = df[df["label"] == 0].iloc[0]
    stressed = df[df["label"] == 1].sort_values("depth_change_60s").iloc[0]
    return (
        {k: float(calm[k]) for k in FEATURE_ORDER},
        {k: float(stressed[k]) for k in FEATURE_ORDER},
    )
