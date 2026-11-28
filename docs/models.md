# Models: the plug-in slot

The system around the model is fixed. A model is a package in `src/sentinalops/models/`, and the rest of the system only knows the contract below.

## The contract (`models/base.py`)

A package exposes a **recipe**: `recipe.fit(train_df, cfg, seed=0, params=None)`. It returns a **fitted model** with:

| Requirement | Why |
|---|---|
| `predict_proba(X: DataFrame) -> (n, 2)`, column 1 = stress probability | The API, stream processor and gates call it |
| `classes_ == [0, 1]` and `feature_names_in_ == cfg.feature_names` (the 34 raw features) | The API refuses a model with other inputs |
| A writable `sentinal_version_` | The registry stamps the version, so every prediction names its model |
| `sentinal_model_name_` and `sentinal_model_params_` (use `base.stamp`) | The heal loop retrains the champion with **its own** recipe |

The recipe owns everything about training: preprocessing, derived features (computed from the 34 inputs only), class weights, tuning, calibration. `base.check_fitted(model, cfg)` checks the contract.

## Frozen packages

Pickles refer to classes by import path. Never change a package once models trained with it are in a registry, because old versions and rollbacks must keep loading. An upgrade is a **new package** (`gbm_v2`), never an edit of `gbm_v1`.

## Upgrade a model in 3 steps

1. Add `src/sentinalops/models/<new_name>/__init__.py` exposing `recipe`, and add one line to `_RECIPES` in `src/sentinalops/models/__init__.py`.
2. Set `model.name: <new_name>` (and `params`) in `configs/domain_liquidity.yaml`.
3. Train and install it:

   ```bash
   python -m sentinalops.training.train --data data/processed/features   # or --data synthetic
   ```

   This writes `models/champion.joblib` atomically. The API and stream processor hot-reload it. Under the live system, stop the heal service, replace the champion, and start the heal service: it registers the new file as the champion.

## How the heal loop chooses the recipe

`heal/retrain.py::resolve_recipe` uses the champion's `sentinal_model_name_` and `sentinal_model_params_`. Editing the config never changes what a heal cycle trains. Only champions trained before the model slot existed (no stamp) fall back to `model.name` from the config.

## Models

| Name | What | Notes |
|---|---|---|
| `gbm_v1` | scikit-learn `HistGradientBoostingClassifier(max_iter=200, learning_rate=0.08)` | The original champion; pickles depend on scikit-learn only |
| `persistence_v1` | fixed rank score from `depth_change_30s`, `depth_change_60s`, `spread_change_60s` | No learning; the heal gate's bar. Not a probability |
| `logit_v1` | standardised, class-weighted logistic regression on 34 + derived features | Isotonic-calibrated when the data allows |
| `gbm_v2` | `HistGradientBoostingClassifier` (400 iterations, early stopping) on 34 + derived features | Isotonic-calibrated when the data allows |
| `lgbm_v1` | LightGBM on 34 + derived features | Trains on `OMP_NUM_THREADS` threads (all cores if unset), predicts single-threaded; needs `libgomp1` |

Shared building blocks in `models/common/` (`derived_v1.py`, `array_model_v1.py`) are **frozen too**: pickles import them by path. New derived features go in `derived_v2.py`, never in an edit of `derived_v1.py`.

## Comparing models

```bash
python -m sentinalops.training.evaluate --data-dir data/raw/binance --folds 4 --stress-share 0.01
```

The command walks forward through the recording with a 60 s embargo, fits the label thresholds on each training period only, and scores every model against the `base_rate` and `persistence_offline` baselines. It writes `docs/results.md` and `reports/eval-*.json`. With less than 14 days of data the results are marked PRELIMINARY. Record the champion in `docs/model-card.md`.
