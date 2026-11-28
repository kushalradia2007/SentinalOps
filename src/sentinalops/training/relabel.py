"""Per-fold labels for the evaluation (design: features once, thresholds per fold).

Features do not depend on the stress thresholds, and neither does the trailing reference
(``depth_ratio``, ``spread_ratio``). So the feature table is built once by the offline pipeline,
the reference once from the raw depth, and each fold only refits the thresholds on its training
period and re-applies them. Gap voids use the pipeline's own rules, so they stay voids.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from sentinalops.config import DomainConfig
from sentinalops.data import pipeline as P
from sentinalops.data.recorder.messages import DEPTH5
from sentinalops.labels import (
    LabelParams,
    add_labels,
    add_reference,
    apply_thresholds,
    book_state,
    fit_thresholds,
    to_ticks,
)


def stress_reference(depth: pd.DataFrame, lp: LabelParams) -> pd.DataFrame:
    """One-second ticks with the trailing reference and ratios (no thresholds applied)."""
    return add_reference(to_ticks(book_state(depth), lp), lp)


def fold_params(ref: pd.DataFrame, lp: LabelParams, train_end_ms: int, stress_share: float) -> LabelParams:
    """Thresholds fitted on ticks before ``train_end_ms`` only. Raises ValueError if too little history."""
    return fit_thresholds(ref[ref.index < train_end_ms], lp, stress_share)


def relabel(table: pd.DataFrame, ref: pd.DataFrame, lp: LabelParams, gaps: np.ndarray) -> pd.DataFrame:
    """``table`` with ``label``, ``label_status`` and ``time_to_onset_s`` recomputed under ``lp``."""
    lab = add_labels(apply_thresholds(ref, lp), lp)
    ts = table["ts_ms"].to_numpy(np.int64)
    lab = lab.reindex(ts)
    gap_in_horizon = P.overlaps_gap(ts, ts + lp.horizon_s * 1000, gaps)
    status = P.label_status(lab, gap_in_horizon)
    ok = status == "ok"
    out = table.copy()
    out["label"] = pd.array(np.where(ok, lab["label"].to_numpy(float), np.nan), dtype="Int8")
    out["label_status"] = status
    out["time_to_onset_s"] = np.where(ok, lab["time_to_onset_s"].to_numpy(float), np.nan)
    return out


def load_recording(data_dir: str | Path, table_dir: str | Path, cfg: DomainConfig):
    """(feature table, raw depth covering it, gap array). Builds missing hours; skips the newest."""
    P.run(data_dir, table_dir, cfg)
    table = P.read_table(table_dir)
    table = table[table["symbol"] == cfg.symbol].reset_index(drop=True) if len(table) else table
    gaps = P.load_gaps(data_dir, cfg.symbol)
    if table.empty:
        return table, pd.DataFrame(), gaps
    pad = P.Padding.for_config(cfg.feature_params(), cfg.label_params())
    lo, hi = int(table["ts_ms"].min()) - pad.before_ms, int(table["ts_ms"].max()) + pad.after_ms
    depth = P.read_events(data_dir, DEPTH5, cfg.symbol, lo, hi)
    return table, depth, gaps
