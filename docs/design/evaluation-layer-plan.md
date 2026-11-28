# Evaluation Layer Implementation Plan

**Goal:** One command scores every registered model against strong baselines, on recorded data, with per-fold label thresholds and leakage-free walk-forward splits.

**Architecture:**
- Pure functions for metrics, splits and per-fold relabelling under `src/sentinalops/training/`.
- New model packages behind the existing model slot.
- A harness (`training/evaluate.py`) that composes them and writes a Markdown and a JSON report.
- The heal loop gains a deterministic persistence gate.

**Tech Stack:** Python 3.11+, NumPy, pandas, scikit-learn, LightGBM 4.x (new core dependency).

**Spec:** `docs/design/evaluation-layer-design.md`

## Global Constraints

- No random splits anywhere; embargo ≥ `split_embargo_seconds` (60 s).
- Thresholds are fitted on each fold's training period only (`labels.fit_thresholds`, `--stress-share` default 0.01).
- Served models: single-row p99 < 10 ms through `ChampionModel.predict`; inputs are exactly `cfg.feature_names`; `predict_proba` converts to a NumPy array once.
- Released model packages are frozen; shared helpers live in a frozen, versioned module (`models/common/derived_v1.py`).
- Empty or undefined metrics are `None` (strict JSON, no NaN).
- Every existing test still passes; `ruff check . --no-cache` is clean.
- No AI attribution in commits, docs or the PR.

## Review Focus

1. A test fold with no positives (a quiet market): metrics are `None`, no crash. Test in Task 1 and Task 6.
2. Too little reference history to fit thresholds (< 1000 ticks): the fold is skipped with a stated reason. Test in Task 6.
3. A calibration slice without enough positives: the recipe falls back to uncalibrated and records it. Test in Task 4.
4. Constant scores (base rate): AP equals prevalence, and the 1%-FPR operating point copes with ties. Test in Task 1.
5. Gap-voided rows stay void after relabelling; the recorder's newest in-progress hour is never used. Test in Task 3.

---

### Task 1: Metrics module

**Files:** Create `src/sentinalops/training/metrics.py`; Test `tests/test_eval_metrics.py`

**Produces:**
- `average_precision(y, p) -> float | None`, re-exporting the tie-aware `labels.average_precision` with NaN mapped to `None`.
- `roc_auc(y, p) -> float | None`.
- `brier(y, p) -> float | None`.
- `reliability(y, p, bins=10) -> list[dict]`, with keys `lo`, `hi`, `n`, `mean_p`, `frac_pos`.
- `threshold_at_fpr(y, p, fpr=0.01) -> float | None`: the smallest threshold whose FPR is ≤ `fpr`. In reports this is computed on the **test** fold and labelled an **oracle** operating point: it compares models' ranking fairly, but isn't an achievable live setting. The realistic point is `cfg.alert_threshold`.
- `operating_point(y, p, threshold, ts_ms, time_to_onset_s) -> dict`, with keys `threshold`, `alerts`, `precision`, `recall`, `false_alarms_per_hour`, `episodes`, `episodes_caught_share`, `median_lead_s`. An episode is keyed by onset time = `ts_ms + time_to_onset_s*1000` over positive rows; it is caught if any of its rows alerted; its lead is the max `time_to_onset_s` among alerted rows.
- `latency_ms(champion_model, rows: list[dict], n=200) -> dict` with keys `p50`, `p95`, `p99`.
- `summarize(y, p, ts_ms, tto, alert_threshold) -> dict` with `ap`, `roc_auc`, `brier`, `base_rate`, `lift`, `at_alert`, `at_1pct_fpr` and `reliability`.

- [ ] Tests:
  - `test_ap_constant_scores_equals_prevalence` (`[1,0,0,0]` constant → 0.25);
  - `test_metrics_none_without_positives` (`summarize` with all-zero y gives `ap`/`lift`/`roc_auc` = `None` and dumps with `json.dumps(..., allow_nan=False)`);
  - `test_threshold_at_fpr_with_ties`: 100 negatives with scores 0..99/100 and 10 positives at 0.995. `threshold_at_fpr(y, p, 0.01)` gives FPR ≤ 0.01 and recall 1.0;
  - `test_episode_lead_time`: two episodes, one caught at lead 20 s and 10 s (median of maxima), one missed. `episodes_caught_share == 0.5` and `median_lead_s == 20`;
  - `test_false_alarms_per_hour` (3 false alerts over 3600 rows at 1 s → 3.0);
  - `test_brier_and_reliability_bins_sum_to_n`.
- [ ] Run, watch the tests fail, implement, run, then run the full suite and `ruff`.

### Task 2: Walk-forward splits

**Files:** Create `src/sentinalops/training/splits.py`; Test `tests/test_eval_splits.py`

**Produces:**
- `walk_forward(ts_ms: np.ndarray, n_folds: int, min_train_s: int, embargo_s: int) -> list[Fold]`. `Fold` is a dataclass with `train_idx`, `test_idx`, `train_end_ms`, `test_start_ms` and `test_end_ms`. The test blocks are equal contiguous slices of the span after `min_train_s`; train is everything earlier minus the embargo.
- `time_holdout(ts_ms, fraction: float, embargo_s: int) -> tuple[np.ndarray, np.ndarray]`: (fit_idx, holdout_idx), with the newest `fraction` held out.

- [ ] Tests:
  - `test_walk_forward_is_chronological_with_embargo`: for every fold, max train ts + embargo·1000 < min test ts, and the test blocks don't overlap;
  - `test_walk_forward_expands_training` (train sizes increase);
  - `test_time_holdout_embargo`;
  - `test_embargo_check_catches_leakage`: the mutation guard. Call `walk_forward(..., embargo_s=0)` and assert that `_has_overlap(train_ts, test_ts, horizon_s=30)` (a test helper) is True for at least one fold, while with 60 it's False for all.
- [ ] Red, implement, green, full suite.

### Task 3: Per-fold relabelling

**Files:** Create `src/sentinalops/training/relabel.py`; Test `tests/test_eval_relabel.py`

**Consumes:** `labels.book_state`, `to_ticks`, `add_stress`, `add_labels`, `fit_thresholds`, `base_rate_report`; `data.pipeline.read_events`, `load_gaps`, `overlaps_gap`, `label_status`, `read_table`, `recorded_hours`.

**Produces:**
- `stress_reference(depth: pd.DataFrame, lp: LabelParams) -> pd.DataFrame`: `add_stress(to_ticks(book_state(depth), lp), lp)`, computed once.
- `fold_params(ref: pd.DataFrame, lp, train_end_ms: int, stress_share: float) -> LabelParams`: fits on `ref` rows with index < `train_end_ms`; raises `ValueError` from `fit_thresholds` if there are too few rows.
- `relabel(table: pd.DataFrame, ref: pd.DataFrame, lp: LabelParams, gaps: np.ndarray) -> pd.DataFrame`: the table with `label`, `label_status` and `time_to_onset_s` recomputed (via `add_stress` + `add_labels` on `ref[STATE + reference columns]`, and `pipeline.label_status` with `overlaps_gap` over the horizon). Features are untouched.
- `load_recording(data_dir, table_dir, cfg) -> tuple[pd.DataFrame, pd.DataFrame, np.ndarray]`: (feature table, depth, gaps). It runs `pipeline.run` (resumable; the newest hour is excluded by its default) and reads depth for the table's time span plus padding.

- [ ] Tests, using `tests/pipeline_helpers.make_events` + `write_recording` (2 h, a stress episode, a 10 s hole):
  - `test_relabel_with_config_params_reproduces_pipeline_labels` (equal `label`/`label_status` columns);
  - `test_stricter_threshold_never_adds_positives`;
  - `test_gap_void_rows_stay_void`;
  - `test_fold_params_use_training_period_only` (changing the depth after `train_end_ms` doesn't change the fitted params);
  - `test_load_recording_excludes_newest_hour`.
- [ ] Red, implement, green, full suite.

### Task 4: Shared derived features, `persistence_v1`, `logit_v1`

**Files:** Create `src/sentinalops/models/common/__init__.py`, `models/common/derived_v1.py`, `models/common/wrapper.py`, `models/persistence_v1/__init__.py`, `models/logit_v1/__init__.py`; Modify `models/__init__.py` (`_RECIPES`); Test `tests/test_models_v2.py`

**Produces:**
- `derived_v1.NAMES: list[str]` and `derived_v1.compute(X: np.ndarray, names: list[str]) -> np.ndarray` (float64; every ratio divides by `+ 1e-12` relative terms). It includes `depth_imbalance`, `trade_intensity_1s/(trade_intensity_60s+eps)`, `realized_vol_5s/(realized_vol_60s+eps)` and `bid_depth/(ask_depth+eps)` style contrasts, at most 12 extra columns.
- `wrapper.ArrayModel` (picklable): holds `estimator`, `feature_names_in_`, `classes_ = np.array([0, 1])`, an optional `calibrator`, and `derive: bool`. `predict_proba(X: DataFrame) -> (n, 2)` builds one NumPy array, then derived features, then the estimator, then the calibrator.
- `wrapper.calibrate_or_fallback(estimator_factory, X, y, ts, cfg, seed) -> (ArrayModel, info)`:
  - with `ts` and ≥ 20 positives in the newest 20% after the embargo, it fits on the rest and isotonic-calibrates on the holdout;
  - otherwise it fits on everything without calibration;
  - `info = {"calibrated": bool, "reason": str}` goes into `sentinal_model_params_`.
- `persistence_v1`: score = mean rank-scaled (`-depth_change_30s`, `-depth_change_60s`, `spread_change_60s`), where the rank-scaling reference quantiles are taken from the training data (stored arrays); optionally Platt-calibrated. It's deterministic given the training data.
- `logit_v1`: StandardScaler + `LogisticRegression(class_weight="balanced", max_iter=1000)` on 34 + derived features, through `calibrate_or_fallback`.

- [ ] Tests:
  - `test_new_recipes_satisfy_contract_and_load_in_champion_model` (parametrized over `persistence_v1`, `logit_v1`; `check_fitted`, `ChampionModel` round trip);
  - `test_derived_features_have_no_inf_with_zero_inputs`;
  - `test_fallback_without_timestamps_or_positives` (`info["calibrated"] is False` and the reason is recorded);
  - `test_float32_matches_float64` (predictions on `rel_spread ~1e-7` rows agree to 1e-6).
- [ ] Red, implement, green, full suite.

### Task 5: `gbm_v2` and `lgbm_v1`

**Files:** Create `models/gbm_v2/__init__.py`, `models/lgbm_v1/__init__.py`; Modify `pyproject.toml` (core `lightgbm>=4.3`), `models/__init__.py`; Test `tests/test_models_v2.py`

**Produces:**
- `gbm_v2`: `HistGradientBoostingClassifier(max_iter=400, learning_rate=0.05, early_stopping=True, validation_fraction=0.15, random_state=seed)` on 34 + derived features, through `calibrate_or_fallback`.
- `lgbm_v1`: `LGBMClassifier(n_estimators=400, learning_rate=0.05, num_leaves=31, min_child_samples=50, subsample=0.8, subsample_freq=1, colsample_bytree=0.8, n_jobs=<fit threads>, verbose=-1, random_state=seed)`. The fit threads are `int(OMP_NUM_THREADS)` if that is set (the Oracle heal unit sets 1), else -1 for all cores. It calls `set_params(n_jobs=1)` before it's returned and goes through `calibrate_or_fallback`.
- LightGBM needs the OpenMP runtime: add `libgomp1` to the `Dockerfile` (an `apt-get install` step) and to `deploy/oracle/setup.sh`'s package list, and add a `python -c "import lightgbm"` check to the Docker CI job's build.
- `params` override the defaults for both.

- [ ] Tests:
  - extend the contract test to `gbm_v2` and `lgbm_v1`;
  - `test_lgbm_predicts_single_threaded` (`model.estimator.get_params()["n_jobs"] == 1`);
  - `test_lgbm_fit_threads_follow_omp_num_threads` (with `OMP_NUM_THREADS=1` set via monkeypatch, the recipe's fit-thread helper returns 1; unset, it returns -1);
  - `test_latency_is_reasonable`: `metrics.latency_ms` after 20 warm-up calls, p99 < 25 ms for each new package. That's a buffer against noisy CI runners; the strict 10 ms limit is reported and flagged by `evaluate.py` on the real machine;
  - `test_dockerfile_and_setup_install_libgomp`.
- [ ] Red, implement, green, full suite (`pip install -e .` first to get LightGBM).

### Task 6: Evaluation harness and CLI

**Files:** Create `src/sentinalops/training/evaluate.py`, `docs/model-card.md`; Test `tests/test_evaluate.py`

**Consumes:** Tasks 1–5.

**Produces:**
- `evaluate(table, ref, gaps, cfg, models: list[str], n_folds=4, stress_share=0.01, min_train_s=6*3600) -> dict`:
  - fold loop: fit params → relabel → per-fold `base_rate_report` + `verdict` → train each model on `ok` train rows → score test `ok` rows;
  - each fold records its fitted `depth_fraction`, `spread_multiple`, `use_spread` and test positive rate, and the report shows them;
  - fold count and minimum training hours are flags, so 25 hours today (for example 3 folds, 6 h minimum training) and 14+ days later both work. Too-short data gives fewer scored folds, never a crash;
  - latency p99 above 10 ms is flagged in the report;
  - the `base_rate` and `persistence_offline` baselines are added automatically;
  - folds that fail threshold fitting are recorded as `{"skipped": reason}`.
- `render_markdown(report) -> str`: one table per fold plus a summary (mean ± std across scored folds). The PRELIMINARY banner appears when the span is under 14 days.
- `main(argv)`: flags `--data-dir`, `--table-dir` (default `data/processed/features`), `--config`, `--models` (default `all`), `--folds 4`, `--stress-share 0.01`, `--min-train-hours 6`, `--report docs/results.md`, `--json-dir reports`.

- [ ] Tests (a synthetic 10 h recording via `make_events` with several stress episodes; `--min-train-hours 2`, 3 folds):
  - `test_evaluate_end_to_end_writes_reports` (Markdown contains `PRELIMINARY`, every model name and `persistence_offline`; the JSON is strict);
  - `test_fold_skipped_when_reference_too_short` (`min_train_s` = 600 → the first fold is skipped with its reason);
  - `test_quiet_fold_has_none_metrics` (a recording without stress → `ap is None`, no crash).
- [ ] Red, implement, green, full suite.

### Task 7: Persistence gate and the heal story for every candidate

**Files:** Modify `src/sentinalops/heal/retrain.py` (`evaluate_gates`), `docs/healing.md`; Test `tests/test_heal.py`

**Produces:** In `evaluate_gates`, the gate `beats_persistence`: `value` = challenger AP − AP of `persistence_v1` fitted on `ts.train`, `needed` = `>= 0`; it fails closed on NaN.

- [ ] Tests:
  - `test_gate_fails_when_challenger_loses_to_persistence` (a constant-score challenger fails it);
  - parametrize the existing `test_heal_loop_recovers_from_a_regime_shift` over the `gbm_v1`, `gbm_v2`, `lgbm_v1` and `logit_v1` champions. The champion is trained with its recipe on calm data and stamped; assert idle before the shift, at least one `retrained_promoted` after it, and that the promoted version's `model_name` equals the champion's. The `gbm_v1` case keeps today's exact assertions.
- [ ] Red, implement, green, full suite.

### Task 8: Docs, a real run, commit

- [ ] `docs/models.md`: the models table gains the new packages, and the frozen rule now covers `models/common/derived_v1.py` too (new derived features go in `derived_v2.py`, never edits). `docs/model-card.md` is the template. `README`/`ROADMAP` progress updated (26, 30 and 31 in part).
- [ ] Run on the IITM server: clone the branch into `~/eval-run` (separate from `Project 1`), point `--data-dir` at `~/Project 1/data/raw/binance` (read-only), and keep `--table-dir` and the reports in `~/eval-run`. Run under `nice`. Copy back `docs/results.md`, marked PRELIMINARY.
- [ ] Full suite plus `ruff check . --no-cache`. Commit without attribution, push the branch, open the PR description file.
