"""Train any registered model by hand: ``python -m sentinalops.training.train``.

This is the manual upgrade path (``docs/models.md``). The model comes from ``--model`` or the
config's ``model.name``; the data is either synthetic regimes or a training table built by the
offline pipeline (``data/processed/features``).

- **Table data:** only ``label_status == 'ok'`` rows; the newest 20% is held out, after a gap of
  ``split_embargo_seconds`` so no training label looks into the holdout.
- **Synthetic data:** rows are independent draws, so a stratified random split is fine.

The holdout average precision is stamped on the model as ``sentinal_holdout_ap_``; the heal loop
uses it as the reference when this model becomes the champion.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import joblib
import pandas as pd
from sklearn.metrics import average_precision_score
from sklearn.model_selection import train_test_split

from sentinalops.config import DEFAULT_DOMAIN_CONFIG, DomainConfig, load_domain_config
from sentinalops.models import get_recipe
from sentinalops.models.base import check_fitted

DEFAULT_OUT = Path("models/champion.joblib")


def time_split(table: pd.DataFrame, cfg: DomainConfig, holdout_fraction: float = 0.2):
    """(train, holdout): chronological, usable rows only, with the embargo between them."""
    ok = table[table["label_status"] == "ok"].sort_values("ts_ms")
    if ok.empty:
        raise ValueError("no usable rows (label_status == 'ok') in the table")
    cut = int(ok["ts_ms"].iloc[int(len(ok) * (1 - holdout_fraction))])
    train = ok[ok["ts_ms"] < cut - cfg.split_embargo_seconds * 1000]
    return train, ok[ok["ts_ms"] >= cut]


def load_data(data: str, rows: int) -> pd.DataFrame:
    if data == "synthetic":
        from sentinalops.simulator.regimes import make_ticks

        return make_ticks("calm", rows)
    from sentinalops.data.pipeline import read_table

    table = read_table(data)
    if table.empty:
        raise SystemExit(f"no training table under {data}")
    return table


def train(data: pd.DataFrame, cfg: DomainConfig, name: str, params: dict, seed: int = 0):
    """Fit ``name`` and return (model, holdout AP, training rows)."""
    if "ts_ms" in data and "label_status" in data:
        tr, ho = time_split(data, cfg)
    else:
        tr, ho = train_test_split(data, test_size=0.2, stratify=data["label"], random_state=seed)
    model = get_recipe(name).fit(tr, cfg, seed, params)
    check_fitted(model, cfg)
    y = ho["label"].astype(int)
    p = model.predict_proba(ho[cfg.feature_names])[:, 1]
    ap = float(average_precision_score(y, p)) if y.any() else float("nan")
    model.sentinal_holdout_ap_ = ap
    return model, ap, len(tr)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Train a registered model and save it as the champion file")
    ap.add_argument("--config", default=os.environ.get("DOMAIN_CONFIG", str(DEFAULT_DOMAIN_CONFIG)))
    ap.add_argument("--model", default=None, help="registered model name (default: config model.name)")
    ap.add_argument("--params", default=None, help="JSON object overriding the recipe's parameters")
    ap.add_argument("--data", default="synthetic", help="'synthetic' or a training-table directory")
    ap.add_argument("--rows", type=int, default=20_000, help="synthetic rows")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out", default=os.environ.get("MODEL_PATH", str(DEFAULT_OUT)))
    args = ap.parse_args(argv)

    cfg = load_domain_config(args.config)
    name = args.model or cfg.model.name
    params = json.loads(args.params) if args.params else dict(cfg.model.params)
    model, holdout_ap, n = train(load_data(args.data, args.rows), cfg, name, params, args.seed)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".tmp")
    joblib.dump(model, tmp)
    os.replace(tmp, out)
    print(f"model={name} holdout_ap={holdout_ap:.4f} rows={n} out={out}")


if __name__ == "__main__":
    main()
