"""Run the heal loop against a simulated market (synthetic regimes, one tick per second).

Stands in for the live stack in tests and the demo: each step scores the step's ticks with
the served champion (and the shadow model, if any) exactly as the stream processor would,
records predictions in the ledger, records outcomes once their horizon has passed, then calls
``HealController.tick``. The result is the timeline of what the loop did.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from sentinalops.config import DomainConfig
from sentinalops.heal.controller import HealController, TickResult
from sentinalops.heal.monitor import window_scores
from sentinalops.heal.registry import ModelRegistry
from sentinalops.schemas import OutcomeLabel, StressPrediction
from sentinalops.serving.ledger import Ledger
from sentinalops.serving.model import ChampionModel
from sentinalops.simulator.regimes import make_ticks

MINUTE_MS = 60_000


def market(segments: list[tuple[str, int]], start_ms: int, seed: int = 0) -> pd.DataFrame:
    """A training-table-shaped timeline: ``[(regime, minutes), ...]`` at one tick per second."""
    parts, t = [], start_ms
    for i, (regime, minutes) in enumerate(segments):
        n = minutes * 60
        df = make_ticks(regime, n, seed=seed + 101 * i)
        df.insert(0, "ts_ms", t + 1000 * pd.RangeIndex(n).to_numpy())
        df.insert(0, "symbol", "BTCUSDT")
        df["label_status"], df["regime"], df["source"] = "ok", regime, "synthetic"
        parts.append(df)
        t += n * 1000
    return pd.concat(parts, ignore_index=True)


@dataclass
class Step:
    now_ms: int
    regime: str
    champion: str | None
    live_ap: float
    result: TickResult


def run(
    cfg: DomainConfig,
    table: pd.DataFrame,
    registry: ModelRegistry,
    ledger: Ledger,
    controller: HealController,
    step_minutes: int = 5,
) -> list[Step]:
    names, h = cfg.feature_names, cfg.label_horizon_seconds * 1000
    champion = ChampionModel(registry.champion_path, names)
    shadow = ChampionModel(registry.shadow_path, names)
    threshold = cfg.alert_threshold
    start, end = int(table["ts_ms"].min()), int(table["ts_ms"].max()) + 1000
    matured_until = start
    steps: list[Step] = []
    for lo in range(start, end, step_minutes * MINUTE_MS):
        hi = min(lo + step_minutes * MINUTE_MS, end)
        chunk = table[(table["ts_ms"] >= lo) & (table["ts_ms"] < hi)]
        preds = []
        for served in (champion, shadow):
            if not served.available:
                continue
            model, version = served.current()
            probs = model.predict_proba(chunk[names])[:, 1]
            preds += [
                StressPrediction(symbol="BTCUSDT", ts_ms=int(ts), horizon_s=h // 1000, probability=float(p),
                                 alert=bool(p >= threshold), threshold=threshold, model_version=version)
                for ts, p in zip(chunk["ts_ms"], probs, strict=True)
            ]
        ledger.add_predictions(preds)
        # outcomes are known once the horizon has passed
        matured = table[(table["ts_ms"] >= matured_until) & (table["ts_ms"] + h < hi)]
        ledger.add_outcomes([
            OutcomeLabel(symbol="BTCUSDT", ts_ms=int(ts), horizon_s=h // 1000, label=int(y))
            for ts, y in zip(matured["ts_ms"], matured["label"], strict=True)
        ])
        matured_until = hi - h
        result = controller.tick(hi)
        state = registry.state()
        live = window_scores(ledger, hi - cfg.heal.metrics_window_minutes * MINUTE_MS, hi, state.champion)
        steps.append(Step(hi, str(chunk["regime"].iloc[-1]), state.champion, live.ap, result))
    return steps


def matured_table(table: pd.DataFrame, horizon_s: int):
    """The ``load_table`` callable for the controller: rows whose label has matured by ``now_ms``."""
    return lambda now_ms: table[table["ts_ms"] + horizon_s * 1000 < now_ms]
