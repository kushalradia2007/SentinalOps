"""Evaluate every model on recorded data: ``python -m sentinalops.training.evaluate`` (chunks 26, 30, 31).

For each walk-forward fold:

1. Fit the stress thresholds on the fold's **training period only** (``--stress-share``) and relabel
   the whole recording with them (features are computed once; see ``training/relabel.py``).
2. Report the fold's base rate and the go/no-go notes from ``labels.verdict``.
3. Train every model on the training period's usable rows; score the test period's usable rows.

Baselines always included: ``base_rate`` (constant training prevalence) and ``persistence_offline``
(the 10-minute depth ratio, rank-scaled on training data; needs a reference no served model has).

Writes a Markdown report (``docs/results.md``) and a strict-JSON report. With less than 14 days of
data every number is marked PRELIMINARY.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from sentinalops.config import DEFAULT_DOMAIN_CONFIG, DomainConfig, load_domain_config
from sentinalops.labels import add_labels, apply_thresholds, base_rate_report, verdict
from sentinalops.models import get_recipe, list_recipes
from sentinalops.serving.model import ChampionModel
from sentinalops.training.metrics import latency_ms, summarize
from sentinalops.training.relabel import fold_params, load_recording, relabel, stress_reference
from sentinalops.training.splits import walk_forward

BASELINES = ["base_rate", "persistence_offline"]
PRELIMINARY_DAYS = 14
LATENCY_LIMIT_MS = 10.0
_BASE_RATE_KEYS = ["eligible_ticks", "positives", "positive_rate", "episodes", "episodes_per_hour",
                   "busiest_hour_share_of_positives", "persistence_baseline_ap"]


def _clean(obj):
    """Strict JSON: NaN/inf -> None, numpy scalars -> Python."""
    if isinstance(obj, dict):
        return {str(k): _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    if isinstance(obj, (np.floating, float)):
        return None if math.isnan(obj) or math.isinf(obj) else float(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    return obj


def _rank_scale(train_values: np.ndarray, values: np.ndarray) -> np.ndarray:
    grid = np.linspace(0, 1, 101)
    q = np.quantile(train_values[~np.isnan(train_values)], grid)
    return np.interp(np.nan_to_num(values, nan=np.nanmedian(train_values)), q, grid)


def evaluate(table: pd.DataFrame, ref: pd.DataFrame, gaps: np.ndarray, cfg: DomainConfig,
             models: list[str], n_folds: int = 4, stress_share: float = 0.01,
             min_train_s: int = 6 * 3600, measure_latency: bool = True) -> dict:
    lp, names = cfg.label_params(), cfg.feature_names
    ts = table["ts_ms"].to_numpy(np.int64)
    span_h = (ts.max() - ts.min()) / 3.6e6 if len(ts) else 0.0
    report: dict = {"generated": datetime.now(UTC).isoformat(timespec="seconds"), "rows": len(table),
                    "span_hours": span_h, "preliminary": span_h < PRELIMINARY_DAYS * 24,
                    "stress_share": stress_share, "n_folds": n_folds, "min_train_hours": min_train_s / 3600,
                    "models": [*BASELINES, *models], "folds": [], "latency": {}}
    last_fitted: dict = {}
    for k, fold in enumerate(walk_forward(ts, n_folds, min_train_s, cfg.split_embargo_seconds)):
        entry: dict = {"fold": k, "train_end": _iso(fold.train_end_ms),
                       "test": [_iso(fold.test_start_ms), _iso(fold.test_end_ms)]}
        try:
            params = fold_params(ref, lp, fold.train_end_ms, stress_share)
        except ValueError as e:
            report["folds"].append({**entry, "skipped": str(e)})
            continue
        entry["label_params"] = {"depth_fraction": params.depth_fraction,
                                 "spread_multiple": params.spread_multiple, "use_spread": params.use_spread}
        lab = relabel(table, ref, params, gaps)
        labeled = add_labels(apply_thresholds(ref, params), params)
        test_window = labeled[(labeled.index >= fold.test_start_ms) & (labeled.index < fold.test_end_ms)]
        brr = base_rate_report(test_window, params)
        entry["base_rate"] = {k2: brr[k2] for k2 in _BASE_RATE_KEYS}
        entry["verdict"] = verdict(brr)

        train = lab.iloc[fold.train_idx]
        train = train[train["label_status"] == "ok"].assign(label=lambda d: d["label"].astype(int))
        test = lab.iloc[fold.test_idx]
        test = test[test["label_status"] == "ok"]
        entry["train_rows"], entry["test_rows"] = len(train), len(test)
        entry["test_positives"] = int(test["label"].astype(int).sum()) if len(test) else 0
        if len(test) == 0 or train["label"].nunique() < 2:
            report["folds"].append({**entry, "skipped": "no usable test rows or one class in training"})
            continue

        y = test["label"].astype(int).to_numpy()
        t_ms, tto = test["ts_ms"].to_numpy(np.int64), test["time_to_onset_s"].to_numpy(float)
        scores = {"base_rate": np.full(len(test), train["label"].mean())}
        depth_ratio = ref["depth_ratio"]
        scores["persistence_offline"] = _rank_scale(
            -depth_ratio.reindex(train["ts_ms"]).to_numpy(float), -depth_ratio.reindex(t_ms).to_numpy(float))
        results, fit_info = {}, {}
        for name in models:
            model = get_recipe(name).fit(train, cfg, seed=0)
            scores[name] = model.predict_proba(test[names])[:, 1]
            fit_info[name] = getattr(model, "sentinal_fit_info_", None)
            last_fitted[name] = model
        for name, p in scores.items():
            summary = summarize(y, p, t_ms, tto, cfg.alert_threshold)
            results[name] = {**summary, "fit_info": fit_info.get(name)}
            if name in BASELINES:  # rank scores, not probabilities: a reliability table means nothing
                results[name].pop("reliability")
        entry["results"] = results
        report["folds"].append(entry)

    if measure_latency and last_fitted:
        rows = table[names].head(100).to_dict("records")
        with tempfile.TemporaryDirectory() as d:
            for name, model in last_fitted.items():
                path = Path(d) / f"{name}.joblib"
                joblib.dump(model, path)
                lat = latency_ms(ChampionModel(path, names), rows)
                report["latency"][name] = {**lat, "over_limit": lat["p99"] > LATENCY_LIMIT_MS}
    report["summary"] = _summary(report)
    return _clean(report)


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d %H:%M")


def _summary(report: dict) -> dict:
    out = {}
    scored = [f for f in report["folds"] if "results" in f]
    for name in report["models"]:
        vals = {"ap": [], "lift": [], "brier": [], "recall_1pct": [], "caught_1pct": []}
        for f in scored:
            r = f["results"].get(name)
            if not r:
                continue
            for key, v in (("ap", r["ap"]), ("lift", r["lift"]), ("brier", r["brier"]),
                           ("recall_1pct", r["at_1pct_fpr"]["recall"]),
                           ("caught_1pct", r["at_1pct_fpr"]["episodes_caught_share"])):
                if v is not None and not (isinstance(v, float) and math.isnan(v)):
                    vals[key].append(v)
        out[name] = {k: {"mean": float(np.mean(v)), "std": float(np.std(v)), "folds": len(v)} if v else None
                     for k, v in vals.items()}
    return out


def _f(x, digits=3) -> str:
    return "–" if x is None else f"{x:.{digits}f}"


def render_markdown(report: dict) -> str:
    lines = ["# Model evaluation results", ""]
    if report["preliminary"]:
        lines += [f"> **PRELIMINARY:** {report['span_hours']:.1f} hours of data "
                  f"(under {PRELIMINARY_DAYS} days). Rankings can change with more data and more "
                  "market regimes.", ""]
    lines += [f"Generated {report['generated']} · {report['rows']:,} rows · stress share "
              f"{report['stress_share']} · {report['n_folds']} walk-forward folds "
              f"(minimum {report['min_train_hours']:.1f} h training, 60 s embargo).", "",
              "**Summary** (mean ± std over scored folds). The 1% FPR operating point is an *oracle* "
              "threshold chosen on the test fold: it compares ranking, not an achievable live setting.", "",
              "| Model | AP | Lift | Brier | Recall @1% FPR | Episodes caught @1% FPR | p99 ms |",
              "|---|---|---|---|---|---|---|"]
    def ms(s: dict, key: str) -> str:
        v = s.get(key)
        return "–" if not v else f"{v['mean']:.3f} ± {v['std']:.3f}"

    for name in report["models"]:
        s = report["summary"].get(name) or {}

        lat = report["latency"].get(name)
        p99 = "–" if not lat else f"{lat['p99']:.2f}{' ⚠' if lat['over_limit'] else ''}"
        lines.append(f"| {name} | {ms(s, 'ap')} | {ms(s, 'lift')} | {ms(s, 'brier')} | "
                     f"{ms(s, 'recall_1pct')} | {ms(s, 'caught_1pct')} | {p99} |")
    for f in report["folds"]:
        lines += ["", f"## Fold {f['fold']}: test {f['test'][0]} → {f['test'][1]} UTC"]
        if "skipped" in f:
            lines.append(f"Skipped: {f['skipped']}")
            continue
        lp, br = f["label_params"], f["base_rate"]
        lines += [f"Thresholds fitted on training data: depth_fraction {lp['depth_fraction']}, "
                  f"spread rule {'on (' + str(lp['spread_multiple']) + ')' if lp['use_spread'] else 'off'}. "
                  f"Test: {f['test_rows']:,} usable rows, {f['test_positives']} positives, "
                  f"{br['episodes']} stress episodes.", ""]
        lines += [f"- {n}" for n in f["verdict"]]
        lines += ["", "| Model | AP | Lift | ROC-AUC | Brier | @alert P / R / FA per h | "
                  "@1% FPR (oracle) recall / caught / median lead s | Calibrated |",
                  "|---|---|---|---|---|---|---|---|"]
        for name, r in f["results"].items():
            a, o = r["at_alert"], r["at_1pct_fpr"]
            cal = "–" if not r.get("fit_info") else ("yes" if r["fit_info"]["calibrated"] else "no")
            at_alert = f"{_f(a['precision'], 2)} / {_f(a['recall'], 2)} / {_f(a['false_alarms_per_hour'], 1)}"
            at_fpr = (f"{_f(o['recall'], 2)} / {_f(o['episodes_caught_share'], 2)} / "
                      f"{_f(o['median_lead_s'], 0)}")
            lines.append(f"| {name} | {_f(r['ap'])} | {_f(r['lift'], 2)} | {_f(r['roc_auc'])} | "
                         f"{_f(r['brier'], 4)} | {at_alert} | {at_fpr} | {cal} |")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Evaluate models on recorded data (walk-forward, embargoed)")
    ap.add_argument("--data-dir", default="data/raw/binance")
    ap.add_argument("--table-dir", default="data/processed/features")
    ap.add_argument("--config", default=os.environ.get("DOMAIN_CONFIG", str(DEFAULT_DOMAIN_CONFIG)))
    ap.add_argument("--models", default="all", help="comma-separated registered models, or 'all'")
    ap.add_argument("--folds", type=int, default=4)
    ap.add_argument("--stress-share", type=float, default=0.01)
    ap.add_argument("--min-train-hours", type=float, default=6.0)
    ap.add_argument("--report", default="docs/results.md")
    ap.add_argument("--json-dir", default="reports")
    args = ap.parse_args(argv)

    cfg = load_domain_config(args.config)
    models = (list_recipes() if args.models == "all"
              else [m.strip() for m in args.models.split(",") if m.strip()])
    for m in models:
        get_recipe(m)  # fail fast on a typo
    table, depth, gaps = load_recording(args.data_dir, args.table_dir, cfg)
    if table.empty:
        raise SystemExit(f"no finished hours under {args.data_dir} yet (the newest hour is always left out)")
    ref = stress_reference(depth, cfg.label_params())
    report = evaluate(table, ref, gaps, cfg, models, args.folds, args.stress_share,
                      int(args.min_train_hours * 3600))
    md = render_markdown(report)
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(md, encoding="utf-8")
    out = Path(args.json_dir) / f"eval-{datetime.now(UTC):%Y%m%dT%H%M%SZ}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, allow_nan=False), encoding="utf-8")
    print(md)
    print(f"wrote {args.report} and {out}")


if __name__ == "__main__":
    main()
