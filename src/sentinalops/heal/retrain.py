"""Challenger training data and the challenger itself (chunk 43), and the gates (chunk 44).

Input is the training table from chunk 12 (``ts_ms``, the features, ``label``,
``label_status``, ``regime``). Only rows with ``label_status == 'ok'`` are used.

Split, for a retrain at ``now_ms``::

    ... older rows ...  | recent window (retrain_window_hours)                      |
    replay buffer        | train                      | embargo | holdout (newest)   |

- **Holdout:** the newest ``holdout_fraction`` of the window. The gates judge on it.
- **Embargo:** ``split_embargo_seconds`` (>= the label horizon) between train and holdout, so
  no training label looks into the holdout period.
- **Replay buffer:** a sample of rows older than the window, spread evenly over regimes, so
  the challenger does not forget regimes it has not seen lately. ``older_eval`` is a disjoint
  sample of the same, used by the no-regression gate.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field

import numpy as np
import pandas as pd

from sentinalops.config import DomainConfig
from sentinalops.labels import average_precision
from sentinalops.models import get_recipe, model_name


@dataclass
class TrainingSet:
    train: pd.DataFrame
    holdout: pd.DataFrame
    older_eval: pd.DataFrame
    window: tuple[int, int]  # [start, end) of the recent window
    counts: dict = field(default_factory=dict)


def _sample_by_regime(df: pd.DataFrame, n: int, rng: np.random.Generator) -> pd.DataFrame:
    if n <= 0 or df.empty:
        return df.iloc[:0]
    groups = list(df.groupby("regime", sort=True))
    per = max(n // len(groups), 1)
    parts = [g.sample(min(per, len(g)), random_state=int(rng.integers(1 << 31))) for _, g in groups]
    return pd.concat(parts)


def assemble(table: pd.DataFrame, now_ms: int, cfg: DomainConfig, seed: int = 0) -> TrainingSet:
    """Recent window + replay buffer for training, the newest slice for the gates."""
    h = cfg.heal
    ok = table[(table["label_status"] == "ok") & (table["ts_ms"] < now_ms)].sort_values("ts_ms")
    start = now_ms - cfg.retrain_window_hours * 3_600_000
    recent, older = ok[ok["ts_ms"] >= start], ok[ok["ts_ms"] < start]
    holdout_start = now_ms - int((now_ms - start) * h.holdout_fraction)
    holdout = recent[recent["ts_ms"] >= holdout_start]
    train_recent = recent[recent["ts_ms"] < holdout_start - cfg.split_embargo_seconds * 1000]

    rng = np.random.default_rng(seed)
    n_replay = int(len(train_recent) * h.replay_fraction)
    older_shuffled = older.sample(frac=1.0, random_state=seed) if len(older) else older
    half = len(older_shuffled) // 2
    replay = _sample_by_regime(older_shuffled.iloc[:half], n_replay, rng)
    older_eval = older_shuffled.iloc[half:]
    train = pd.concat([train_recent, replay]).sort_values("ts_ms")
    counts = {
        "train_recent": len(train_recent), "replay": len(replay), "holdout": len(holdout),
        "older_eval": len(older_eval), "train_positives": int(train["label"].sum()),
        "holdout_positives": int(holdout["label"].sum()),
        "regime_mix": {str(k): int(v) for k, v in train["regime"].value_counts().items()},
    }
    return TrainingSet(train, holdout, older_eval, (start, now_ms), counts)


def resolve_recipe(champion, cfg: DomainConfig) -> tuple[str, dict]:
    """The recipe to retrain with: the champion's own, else (older models) the configured one."""
    name = model_name(champion) if champion is not None else None
    if name is None:
        return cfg.model.name, dict(cfg.model.params)
    return name, dict(getattr(champion, "sentinal_model_params_", {}) or {})


def train_challenger(ts: TrainingSet, cfg: DomainConfig, seed: int = 0, champion=None):
    """Retrain the champion's model type on the new window (heal never switches model types)."""
    if ts.train.empty or ts.train["label"].nunique() < 2:
        raise ValueError("training window has no positive or no negative labels")
    data = ts.train.assign(label=ts.train["label"].astype(int))
    name, params = resolve_recipe(champion, cfg)
    return get_recipe(name).fit(data[[*cfg.feature_names, "label"]], cfg, seed, params)


# ---- gates (chunk 44) -----------------------------------------------------------------------------
def _score(model, df: pd.DataFrame, names: list[str]) -> dict:
    if df.empty or df["label"].sum() == 0:
        return {"ap": math.nan, "brier": math.nan, "n": len(df), "positives": 0}
    y = df["label"].to_numpy(float)
    p = model.predict_proba(df[names])[:, 1]
    return {"ap": average_precision(y, p), "brier": float(np.mean((p - y) ** 2)), "n": len(df),
            "positives": int(y.sum())}


@dataclass
class GateReport:
    passed: bool
    gates: dict[str, dict]
    champion: dict
    challenger: dict
    baseline_ap: float
    counts: dict

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=float)

    def to_markdown(self) -> str:
        lines = ["| Gate | Passed | Value | Needed |", "| --- | --- | --- | --- |"]
        for name, g in self.gates.items():
            lines.append(f"| {name} | {'yes' if g['passed'] else 'no'} | {g['value']:.4f} | {g['needed']} |")
        verdict = "PASSED: promote" if self.passed else "FAILED: keep the champion"
        return f"**{verdict}**\n\n" + "\n".join(lines)


def evaluate_gates(champion, challenger, ts: TrainingSet, cfg: DomainConfig) -> GateReport:
    """Champion and challenger on the same holdout (current regime) and on older regimes."""
    h, names = cfg.heal, cfg.feature_names
    champ = {"current": _score(champion, ts.holdout, names),
             "older": _score(champion, ts.older_eval, names)}
    chall = {"current": _score(challenger, ts.holdout, names),
             "older": _score(challenger, ts.older_eval, names)}
    base_rate = float(ts.holdout["label"].mean()) if len(ts.holdout) else math.nan
    gain = chall["current"]["ap"] - champ["current"]["ap"]
    regression = champ["older"]["ap"] - chall["older"]["ap"]
    lift = chall["current"]["ap"] / base_rate if base_rate else math.nan
    brier_up = chall["current"]["brier"] - champ["current"]["brier"]
    older_known = not math.isnan(regression)
    gates = {
        "improves_current_regime": {"value": gain, "needed": f">= {h.min_gain}",
                                    "passed": gain >= h.min_gain},
        "no_regression_on_older": {"value": regression if older_known else 0.0,
                                   "needed": f"<= {h.max_regression}",
                                   "passed": (not older_known) or regression <= h.max_regression},
        "beats_base_rate_baseline": {"value": lift, "needed": f">= {h.min_lift_over_baseline}x",
                                     "passed": lift >= h.min_lift_over_baseline},
        "calibration_not_worse": {"value": brier_up, "needed": f"<= {h.max_brier_increase}",
                                  "passed": brier_up <= h.max_brier_increase},
    }
    # NaN comparisons are False, so a gate with no data fails closed (except older regimes: none yet)
    passed = all(g["passed"] for g in gates.values())
    return GateReport(passed, gates, champ, chall, base_rate, ts.counts)
