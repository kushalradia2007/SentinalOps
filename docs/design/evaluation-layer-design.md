# Design: evaluation layer (chunks 26, 30, 31)

Date: 2026-11-28. Status: approved, including the six review adjustments.

## Intent

One command gives a fair, leakage-free verdict on which model forecasts liquidity stress best. It works on real recorded data, compares against baselines that are hard to beat, and uses metrics that matter for a rare event. Any model package plugged into the model slot is scored the same way. Today there are 26 hours of real data, so every result is labelled **preliminary** until at least 14 days exist.

## Goals, non-goals, anti-goals

- **Goals:**
  - a metrics module and walk-forward time splits with an embargo;
  - per-fold label thresholds;
  - baselines and two new candidates (`gbm_v2`, `lgbm_v1`);
  - an evaluation command that writes `docs/results.md` and a JSON report;
  - a persistence gate in the heal loop;
  - a model card template.
- **Non-goals:** anomaly detectors (27–29), MLflow logging, regime tagging (14; the breakdown uses the `regime` column once it exists), and hyperparameter search beyond a small fixed grid.
- **Anti-goals:**
  - any random split;
  - thresholds fitted on test data;
  - tuning on synthetic data;
  - claiming accuracy from 26 hours of data;
  - a served model whose single-tick p99 exceeds 10 ms.

## Approach

A plain Python harness. MLflow would need a tracking server that isn't running, so it can be added later as a logging layer. This was chosen by the owner.

## Data flow

1. **Features once.** `data.pipeline.run` builds (or reuses) the 1 Hz feature table from the raw recording. The newest hour is left out, as always.
2. **Stress reference once.** `labels.add_stress(to_ticks(book_state(depth)))` gives `depth_ratio` and `spread_ratio`. These don't depend on the thresholds.
3. **Per fold:**
   - `fit_thresholds` runs on the **training period's** ticks only, with `--stress-share`, default 0.01;
   - the whole series is relabelled with the fitted thresholds (`add_stress` → `add_labels`);
   - the labels are joined onto the feature rows by `ts_ms`;
   - `label_status` is recomputed with the pipeline's own `label_status` and gap-log overlap, so gap voids stay voids.
4. **Report per fold:** the `labels.base_rate_report` for the fold, and `verdict` notes such as too sparse or one dominant burst.

## Splits (`training/splits.py`)

- `walk_forward(ts, n_folds, min_train_s, embargo_s)`: expanding train window, contiguous test blocks, and `embargo_s` (at least 60 s) removed between them.
- Calibration slices inside recipes use `time_holdout(ts, fraction, embargo_s)`.
- A leakage mutation test proves the split test fails if the embargo is removed.

## Metrics (`training/metrics.py`)

All take `y` and `p` arrays (plus `ts_ms` and `time_to_onset_s` where needed):

- **Ranking:** tie-aware average precision (the same function as `labels.average_precision`, already fixed in `main`) and ROC-AUC.
- **Calibration:** Brier score and a 10-bin reliability table.
- **Lift** over the base rate.
- **Operating points:** at `cfg.alert_threshold` **and** at the threshold giving 1% false-positive rate. At each:
  - precision and recall;
  - false alarms per hour (alerts on label-0 ticks ÷ hours covered);
  - **episode-level lead time:** each stress onset is an episode, made of the ticks whose `time_to_onset_s` points at it. An episode is *caught* if any of its ticks alerted, and its lead time is the largest `time_to_onset_s` among alerted ticks. Reported as the share of episodes caught and the median lead.
  - Empty cases give `None`, never a crash or a NaN in JSON.
- **Latency:** p50, p95 and p99 of single-row `ChampionModel.predict` calls.

## Models

| Name | Where | What |
|---|---|---|
| `base_rate` | harness only | constant training prevalence |
| `persistence_offline` | harness only | `-depth_ratio` (needs the 10-minute reference, which isn't one of the 34 features) |
| `persistence_v1` | model package | fixed monotone score from the 34 features: thinning depth (`-depth_change_30s`, `-depth_change_60s`) and widening spread (`spread_change_60s`), rank-scaled; no fitting except optional Platt calibration |
| `logit_v1` | model package | standardised logistic regression, class-weighted |
| `gbm_v1` | existing | baseline gradient boosting, unchanged |
| `gbm_v2` | model package | HistGradientBoosting with derived ratios, early stopping, isotonic calibration on a later embargoed slice |
| `lgbm_v1` | model package | LightGBM with the same derived ratios and calibration; `n_jobs=-1` while fitting, set to `n_jobs=1` before returning (single-row latency) |

Every new package:
- **Derived features:** computed in NumPy from the 34 inputs, with an `eps` in every ratio, plus short-vs-long contrasts such as `trade_intensity_1s / (trade_intensity_60s + eps)`.
- **Inference:** converts the input to an array once per call.
- **Small-data fallback:** if `ts_ms` is missing, there are fewer than 20 positives in the calibration slice, or a class is missing, it falls back to uncalibrated fitting and records `calibrated: false` in its params.
- **Precision check:** a test compares float32 and float64 predictions on a sample with tiny `rel_spread` values (about 1e-7).

LightGBM becomes a core dependency, since served models need it in the API and live images.

## Heal loop

`evaluate_gates` adds a `beats_persistence` gate: the challenger's AP on the holdout must be at least the AP of `persistence_v1`, which is deterministic and needs no fitting in the gate. The heal story test (`simulator/heal_sim.py`) runs with `gbm_v2`, `lgbm_v1` and `logit_v1` installed. Each must detect the shift, retrain its own recipe, pass the gates and promote.

Already in `main` from the code-review fixes (PR #14), so not redone:
- tie-aware `labels.average_precision`;
- `train_challenger` passing `ts_ms` and `regime`;
- the embargoed chronological replay/older-eval split.

## Command

```
python -m sentinalops.training.evaluate --data-dir <raw recording> --out data/processed/features \
    [--stress-share 0.01] [--folds 4] [--models all] [--report docs/results.md]
```

It prints:
- one table per fold and a summary table (mean ± spread across folds);
- the per-fold base-rate report and verdict notes;
- a PRELIMINARY banner when there are fewer than 14 days of data.

It writes `docs/results.md` and `reports/eval-<time>.json`. The real run happens on the IITM server, in its own folder next to the recorder's, with the recorder left untouched.

## Model card

`docs/model-card.md` is a template covering: data period, threshold fit, metrics versus baselines per operating point, per-regime results, latency, and known limits.

## Left to the builder

Exact feature-contrast list, the small fixed hyperparameter grid, fold sizes when data is short (minimum 6 h of training), and table formatting.
