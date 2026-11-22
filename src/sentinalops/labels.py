"""Liquidity-stress labels (chunk 9): the single definition of what the model predicts.

Shared by the offline pipeline (chunk 12) and the outcome verifier (chunk 34), so both
always agree. The pipeline, on a regular one-second grid:

1. ``book_state``   depth snapshots -> mid, relative spread, top-5 depth
2. ``to_ticks``     as-of sample once per second; a tick is unknown if the book is stale
3. ``add_stress``   trailing reference (rolling median of *earlier* ticks), then
                    ``stressed`` and ``confirmed`` (stressed for ``confirm_ticks`` in a row)
4. ``add_labels``   for a calm tick t: label = 1 if confirmed stress occurs in (t, t+H]

Causality rules, each enforced by a test:

- Everything from steps 1-3 at tick t uses only data at or before t.
- The label at t uses only ticks in (t, t+H], so features and labels never overlap.
- Ticks already in stress are not eligible (we forecast onset, not persistence).
- If any tick in the horizon is unknown (feed gap, warm-up, end of data), the label is
  void, never guessed.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path

import numpy as np
import pandas as pd

DEPTH_LEVELS = 5
STATE_COLUMNS = ["mid", "rel_spread", "depth_top5"]


@dataclass(frozen=True)
class LabelParams:
    cadence_s: int = 1
    horizon_s: int = 30
    reference_window_s: int = 600
    min_reference_ticks: int = 300  # ticks needed before the reference is trusted
    depth_fraction: float = 0.5  # stress if depth < fraction * reference depth (primary)
    spread_multiple: float = 3.0  # stress if spread > multiple * reference spread (secondary)
    use_spread: bool = True
    confirm_ticks: int = 2  # consecutive stressed ticks before stress counts
    stale_ms: int = 3000  # a tick is unknown if the newest snapshot is older than this

    def __post_init__(self) -> None:
        if min(self.cadence_s, self.horizon_s, self.reference_window_s, self.confirm_ticks) < 1:
            raise ValueError("cadence, horizon, reference window and confirm_ticks must be >= 1")
        if not 0 < self.depth_fraction < 1 or self.spread_multiple <= 1:
            raise ValueError("need 0 < depth_fraction < 1 and spread_multiple > 1")
        if self.horizon_s % self.cadence_s or self.reference_window_s % self.cadence_s:
            raise ValueError("horizon and reference window must be multiples of the cadence")

    @property
    def horizon_ticks(self) -> int:
        return self.horizon_s // self.cadence_s

    @property
    def reference_ticks(self) -> int:
        return self.reference_window_s // self.cadence_s

    @classmethod
    def from_domain_yaml(cls, path: str | Path) -> LabelParams:
        from sentinalops.config import load_domain_config

        return load_domain_config(path).label_params()


# ---- 1. book state -----------------------------------------------------------------
def book_state(depth: pd.DataFrame) -> pd.DataFrame:
    """Per snapshot: mid, relative spread, top-5 depth. Drops crossed or empty books."""
    bid, ask = depth["bid_px_1"].to_numpy(float), depth["ask_px_1"].to_numpy(float)
    qty_cols = [f"{s}_qty_{i}" for s in ("bid", "ask") for i in range(1, DEPTH_LEVELS + 1)]
    total = depth[qty_cols].to_numpy(float).sum(axis=1)
    mid = (bid + ask) / 2
    out = pd.DataFrame(
        {
            "recv_time_ms": depth["recv_time_ms"].to_numpy(np.int64),
            "mid": mid,
            "rel_spread": (ask - bid) / mid,
            "depth_top5": total,
        }
    )
    ok = (bid > 0) & (ask > bid) & (total > 0)
    return out[ok].sort_values("recv_time_ms", kind="stable").reset_index(drop=True)


# ---- 2. one-second ticks -------------------------------------------------------------
def to_ticks(state: pd.DataFrame, p: LabelParams = LabelParams()) -> pd.DataFrame:
    """As-of sample: tick t takes the newest snapshot with ``recv_time_ms <= t``.

    Returns a frame indexed by ``ts_ms`` on a gap-free grid. Ticks whose newest snapshot
    is older than ``stale_ms`` are NaN ("unknown").
    """
    step = p.cadence_s * 1000
    times = state["recv_time_ms"].to_numpy(np.int64)
    if len(times) == 0:
        return pd.DataFrame(
            columns=[*STATE_COLUMNS, "age_ms"], index=pd.Index([], name="ts_ms", dtype="int64")
        )
    grid = np.arange(-(-times[0] // step) * step, (times[-1] // step) * step + step, step, dtype=np.int64)
    idx = np.searchsorted(times, grid, side="right") - 1  # newest snapshot at or before each tick
    age = grid - times[idx]
    ticks = pd.DataFrame(
        {c: state[c].to_numpy(float)[idx] for c in STATE_COLUMNS}, index=pd.Index(grid, name="ts_ms")
    )
    ticks["age_ms"] = age
    ticks.loc[age > p.stale_ms, STATE_COLUMNS] = np.nan
    return ticks


# ---- 3. stress state (causal) ----------------------------------------------------------
def add_stress(ticks: pd.DataFrame, p: LabelParams = LabelParams()) -> pd.DataFrame:
    """Add the trailing reference and the stress state. Uses only ticks at or before each row."""
    out = ticks.copy()
    roll = dict(window=p.reference_ticks, min_periods=min(p.min_reference_ticks, p.reference_ticks))
    # shift(1): the reference for tick t is built from ticks strictly before t
    out["ref_depth"] = out["depth_top5"].rolling(**roll).median().shift(1)
    out["ref_spread"] = out["rel_spread"].rolling(**roll).median().shift(1)
    out["depth_ratio"] = out["depth_top5"] / out["ref_depth"]
    out["spread_ratio"] = out["rel_spread"] / out["ref_spread"]

    known = out["depth_ratio"].notna() & out["spread_ratio"].notna()
    depth_hit = out["depth_ratio"] < p.depth_fraction
    spread_hit = (out["spread_ratio"] > p.spread_multiple) & p.use_spread
    out["stress_depth"] = depth_hit.astype(float).where(known)
    out["stress_spread"] = spread_hit.astype(float).where(known)
    out["stressed"] = (depth_hit | spread_hit).astype(float).where(known)
    # confirmed at t = stressed at each of the last `confirm_ticks` ticks (NaN if any is unknown)
    out["confirmed"] = out["stressed"].rolling(p.confirm_ticks, min_periods=p.confirm_ticks).min()
    return out


# ---- 4. forward labels -----------------------------------------------------------------
def _forward(series: pd.Series, horizon_ticks: int) -> np.ndarray:
    """Rows j=1..H of the values at t+j, shape (H, N). NaN beyond the end of the data."""
    return np.stack([series.shift(-j).to_numpy(float) for j in range(1, horizon_ticks + 1)])


def add_labels(ticks: pd.DataFrame, p: LabelParams = LabelParams()) -> pd.DataFrame:
    """Add the onset label and regression targets. Looks only at ticks in (t, t+H]."""
    out = ticks.copy()
    h = p.horizon_ticks
    fwd = _forward(out["confirmed"], h)
    horizon_known = ~np.isnan(fwd).any(axis=0)
    hit = (fwd == 1).any(axis=0)
    calm_now = (out["stressed"] == 0).to_numpy()

    out["eligible"] = calm_now & horizon_known
    out["label"] = np.where(out["eligible"], hit.astype(float), np.nan)
    first = (fwd == 1).argmax(axis=0) + 1
    out["time_to_onset_s"] = np.where(out["eligible"] & hit, first * p.cadence_s, np.nan)

    for name, col, agg in (
        ("fwd_max_rel_spread", "rel_spread", np.max),
        ("fwd_min_depth_ratio", "depth_ratio", np.min),
    ):
        f = _forward(out[col], h)
        full = ~np.isnan(f).any(axis=0)
        out[name] = np.where(full, agg(np.where(np.isnan(f), 0.0, f), axis=0), np.nan)
    return out


def build_labels(depth: pd.DataFrame, p: LabelParams = LabelParams()) -> pd.DataFrame:
    """Depth snapshots (recorder ``depth5`` schema) -> labelled one-second ticks."""
    return add_labels(add_stress(to_ticks(book_state(depth), p), p), p)


def label_at(stress: pd.DataFrame, ts_ms: int, p: LabelParams = LabelParams()) -> int | None:
    """Slow, obvious reference for one prediction time. ``None`` means void or not eligible.

    The outcome verifier must give this answer; tests check it against ``add_labels``.
    """
    step = p.cadence_s * 1000
    if ts_ms not in stress.index or stress.at[ts_ms, "stressed"] != 0:
        return None
    future = [ts_ms + j * step for j in range(1, p.horizon_ticks + 1)]
    if any(t not in stress.index or pd.isna(stress.at[t, "confirmed"]) for t in future):
        return None
    return int(any(stress.at[t, "confirmed"] == 1 for t in future))


# ---- thresholds and base rates (tasks 9.3 and 9.6) --------------------------------------
def fit_thresholds(
    stress: pd.DataFrame, p: LabelParams = LabelParams(), stress_share: float = 0.01
) -> LabelParams:
    """Pick thresholds from *training-period* ticks so each rule fires on about ``stress_share`` of them.

    Call once on the training period, then freeze the result in the domain config. If the
    spread is pinned at one tick (the usual case on BTCUSDT) its ratio has no usable tail,
    and the spread rule is switched off.
    """
    depth = stress["depth_ratio"].dropna().to_numpy()
    spread = stress["spread_ratio"].dropna().to_numpy()
    if len(depth) < 1000:
        raise ValueError(f"need at least 1000 ticks with a reference to fit thresholds, got {len(depth)}")
    depth_fraction = float(np.clip(np.quantile(depth, stress_share), 0.05, 0.95))
    spread_multiple = float(np.quantile(spread, 1 - stress_share))
    use_spread = spread_multiple > 1.5
    return replace(
        p,
        depth_fraction=round(depth_fraction, 4),
        use_spread=use_spread,
        spread_multiple=round(spread_multiple, 4) if use_spread else p.spread_multiple,
    )


def average_precision(y: np.ndarray, score: np.ndarray) -> float:
    """Area under the precision-recall curve (step-wise), ties broken pessimistically by order."""
    y = np.asarray(y, dtype=float)
    if y.sum() == 0:
        return float("nan")
    order = np.argsort(-np.asarray(score, dtype=float), kind="stable")
    hits = y[order]
    precision = np.cumsum(hits) / np.arange(1, len(hits) + 1)
    return float((precision * hits).sum() / hits.sum())


def _episodes(confirmed: pd.Series) -> list[tuple[int, int]]:
    """(start, end) positions of each run of confirmed stress."""
    c = (confirmed.to_numpy(float) == 1).astype(np.int8)
    edges = np.diff(np.concatenate(([0], c, [0])))
    return list(zip(np.nonzero(edges == 1)[0].tolist(), np.nonzero(edges == -1)[0].tolist(), strict=True))


def base_rate_report(labeled: pd.DataFrame, p: LabelParams = LabelParams()) -> dict:
    """How often the label fires, how bursty it is, and how strong the lazy baseline is."""
    n = len(labeled)
    known = labeled["stressed"].notna()
    elig = labeled["eligible"].to_numpy(bool)
    y = labeled.loc[elig, "label"].to_numpy(float)
    hours = n * p.cadence_s / 3600
    eps = _episodes(labeled["confirmed"])
    durations = [(b - a) * p.cadence_s for a, b in eps]
    # positives belonging to the single busiest hour: 1.0 means every positive came from one burst
    pos_idx = labeled.index[elig][y == 1]
    by_hour = pd.Series(pos_idx // 3_600_000).value_counts() if len(pos_idx) else pd.Series(dtype=float)
    # persistence baseline: "the thinner the book is right now, the likelier stress is"
    score = -labeled.loc[elig, "depth_ratio"].to_numpy(float)
    prevalence = float(y.mean()) if len(y) else float("nan")
    ap = average_precision(y, score) if len(y) else float("nan")
    return {
        "params": asdict(p),
        "ticks": n,
        "hours": round(hours, 3),
        "known_share": round(float(known.mean()), 4) if n else None,
        "stressed_share": round(float(labeled.loc[known, "stressed"].mean()), 5) if known.any() else None,
        "stress_from_depth_share": round(float(labeled.loc[known, "stress_depth"].mean()), 5)
        if known.any()
        else None,
        "stress_from_spread_share": round(float(labeled.loc[known, "stress_spread"].mean()), 5)
        if known.any()
        else None,
        "eligible_ticks": int(elig.sum()),
        "positives": int(y.sum()),
        "positive_rate": round(prevalence, 5) if len(y) else None,
        "episodes": len(eps),
        "episodes_per_hour": round(len(eps) / hours, 3) if hours else None,
        "median_episode_s": float(np.median(durations)) if durations else None,
        "busiest_hour_share_of_positives": round(float(by_hour.iloc[0] / by_hour.sum()), 3)
        if len(by_hour)
        else None,
        "persistence_baseline_ap": round(ap, 4) if ap == ap else None,
        "persistence_lift_over_prevalence": round(ap / prevalence, 2)
        if ap == ap and prevalence > 0
        else None,
    }


def verdict(report: dict) -> list[str]:
    """Plain-language go/no-go notes for task 9.6."""
    notes = []
    if not report["eligible_ticks"]:
        return ["NO-GO: no eligible ticks. Record more data (the reference needs a warm-up) and rerun."]
    rate, episodes = report["positive_rate"] or 0, report["episodes"]
    if episodes < 50:
        notes.append(f"NO-GO for training: only {episodes} stress episodes; aim for a few hundred.")
    if rate < 0.002:
        notes.append(
            "Label is very rare (<0.2%). Try a longer horizon or a softer depth_fraction (task 9.7)."
        )
    elif rate > 0.2:
        notes.append("Label fires on more than 20% of ticks: the threshold is too soft to mean 'stress'.")
    if (report["stress_from_spread_share"] or 0) < 0.1 * (report["stressed_share"] or 0):
        notes.append("The spread rule contributes almost nothing; depth depletion carries the label.")
    if (report["busiest_hour_share_of_positives"] or 0) > 0.5:
        notes.append("More than half of all positives come from one hour: results will hinge on that burst.")
    lift = report["persistence_lift_over_prevalence"]
    if lift is not None:
        notes.append(
            f"Persistence baseline AP is {lift}x the prevalence. A model must beat that, not chance."
        )
    return notes or ["GO: base rate, episode count and burstiness look workable."]


# ---- CLI ---------------------------------------------------------------------------------
def load_recorded_depth(data_dir: str | Path, symbol: str) -> pd.DataFrame:
    import pyarrow.parquet as pq

    files = sorted((Path(data_dir) / "stream=depth5" / f"symbol={symbol}").rglob("part-*.parquet"))
    if not files:
        raise FileNotFoundError(f"no depth5 files for {symbol} under {data_dir}")
    return pd.concat([pq.read_table(f).to_pandas() for f in files], ignore_index=True)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Build liquidity-stress labels and the base-rate report")
    ap.add_argument("--data-dir", default="data/raw/binance")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--config", default="configs/domain_liquidity.yaml")
    ap.add_argument("--fit", action="store_true", help="fit thresholds on this data (training period only!)")
    ap.add_argument("--stress-share", type=float, default=0.01)
    ap.add_argument("--out", default=None, help="write labelled ticks to this Parquet file")
    args = ap.parse_args(argv)

    p = LabelParams.from_domain_yaml(args.config)
    stress = add_stress(to_ticks(book_state(load_recorded_depth(args.data_dir, args.symbol)), p), p)
    if args.fit:
        p = fit_thresholds(stress, p, args.stress_share)
        stress = add_stress(stress[[*STATE_COLUMNS, "age_ms"]], p)
    labeled = add_labels(stress, p)
    report = base_rate_report(labeled, p)
    print(json.dumps(report, indent=2))
    for note in verdict(report):
        print("-", note)
    if args.out:
        labeled.to_parquet(args.out)


if __name__ == "__main__":
    main()
