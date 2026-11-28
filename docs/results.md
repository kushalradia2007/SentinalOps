# Model evaluation results

> **PRELIMINARY:** 25.8 hours of data (under 14 days). Rankings can change with more data and more market regimes.

Generated 2026-11-28T12:30:12+00:00 · 92,204 rows · stress share 0.01 · 4 walk-forward folds (minimum 6.0 h training, 60 s embargo).

**Summary** (mean ± std over scored folds). The 1% FPR operating point is an *oracle* threshold chosen on the test fold: it compares ranking, not an achievable live setting.

| Model | AP | Lift | Brier | Recall @1% FPR | Episodes caught @1% FPR | p99 ms |
|---|---|---|---|---|---|---|
| base_rate | 0.026 ± 0.009 | 1.000 ± 0.000 | 0.026 ± 0.009 | – | – | – |
| persistence_offline | 0.065 ± 0.010 | 2.833 ± 1.089 | 0.333 ± 0.010 | 0.078 ± 0.024 | 0.599 ± 0.030 | – |
| gbm_v1 | 0.086 ± 0.043 | 3.394 ± 1.319 | 0.027 ± 0.009 | 0.087 ± 0.045 | 0.253 ± 0.123 | 3.91 |
| gbm_v2 | 0.047 ± 0.015 | 2.104 ± 1.329 | 0.026 ± 0.009 | – | – | 6.11 |
| lgbm_v1 | 0.040 ± 0.010 | 1.777 ± 0.940 | 0.026 ± 0.009 | – | – | 1.11 |
| logit_v1 | 0.041 ± 0.010 | 1.701 ± 0.603 | 0.027 ± 0.008 | 0.095 ± 0.000 | 0.364 ± 0.000 | 0.86 |
| persistence_v1 | 0.041 ± 0.007 | 1.735 ± 0.638 | 0.282 ± 0.005 | 0.040 ± 0.020 | 0.271 ± 0.143 | 0.53 |

## Fold 0: test 2026-11-27 16:13 → 2026-11-27 21:09 UTC
Thresholds fitted on training data: depth_fraction 0.3293, spread rule off. Test: 17,385 usable rows, 633 positives, 30 stress episodes.

- NO-GO for training: only 30 stress episodes; aim for a few hundred.
- The spread rule contributes almost nothing; depth depletion carries the label.
- Persistence baseline AP is 1.39x the prevalence. A model must beat that, not chance.

| Model | AP | Lift | ROC-AUC | Brier | @alert P / R / FA per h | @1% FPR (oracle) recall / caught / median lead s | Calibrated |
|---|---|---|---|---|---|---|---|
| base_rate | 0.036 | 1.00 | 0.500 | 0.0354 | – / 0.00 / 0.0 | – / – / – | – |
| persistence_offline | 0.050 | 1.37 | 0.537 | 0.3498 | 0.04 / 0.55 / 1701.8 | 0.04 / 0.57 / 11 | – |
| gbm_v1 | 0.158 | 4.33 | 0.770 | 0.0364 | – / 0.00 / 0.0 | 0.13 / 0.37 / 20 | – |
| gbm_v2 | 0.042 | 1.16 | 0.564 | 0.0356 | – / 0.00 / 0.0 | – / – / – | yes |
| lgbm_v1 | 0.034 | 0.93 | 0.464 | 0.0356 | – / 0.00 / 0.0 | – / – / – | yes |
| logit_v1 | 0.042 | 1.14 | 0.564 | 0.0355 | – / 0.00 / 0.0 | – / – / – | yes |
| persistence_v1 | 0.050 | 1.38 | 0.540 | 0.2809 | 0.04 / 0.54 / 1680.6 | 0.03 / 0.40 / 16 | no |

## Fold 1: test 2026-11-27 21:09 → 2026-11-28 02:06 UTC
Thresholds fitted on training data: depth_fraction 0.3175, spread rule off. Test: 17,674 usable rows, 284 positives, 11 stress episodes.

- NO-GO for training: only 11 stress episodes; aim for a few hundred.
- The spread rule contributes almost nothing; depth depletion carries the label.
- More than half of all positives come from one hour: results will hinge on that burst.
- Persistence baseline AP is 3.84x the prevalence. A model must beat that, not chance.

| Model | AP | Lift | ROC-AUC | Brier | @alert P / R / FA per h | @1% FPR (oracle) recall / caught / median lead s | Calibrated |
|---|---|---|---|---|---|---|---|
| base_rate | 0.016 | 1.00 | 0.500 | 0.0158 | – / 0.00 / 0.0 | – / – / – | – |
| persistence_offline | 0.062 | 3.83 | 0.559 | 0.3268 | 0.02 / 0.55 / 1789.4 | 0.10 / 0.64 / 24 | – |
| gbm_v1 | 0.081 | 5.02 | 0.751 | 0.0159 | – / 0.00 / 0.0 | 0.12 / 0.36 / 26 | – |
| gbm_v2 | 0.071 | 4.39 | 0.822 | 0.0156 | – / 0.00 / 0.0 | – / – / – | yes |
| lgbm_v1 | 0.054 | 3.37 | 0.766 | 0.0157 | – / 0.00 / 0.0 | – / – / – | yes |
| logit_v1 | 0.044 | 2.71 | 0.678 | 0.0174 | 0.14 / 0.05 / 19.2 | 0.10 / 0.36 / 23 | yes |
| persistence_v1 | 0.044 | 2.75 | 0.548 | 0.2751 | 0.02 / 0.58 / 1738.8 | 0.07 / 0.36 / 13 | no |

## Fold 2: test 2026-11-28 02:06 → 2026-11-28 07:03 UTC
Thresholds fitted on training data: depth_fraction 0.3293, spread rule off. Test: 17,663 usable rows, 598 positives, 29 stress episodes.

- NO-GO for training: only 29 stress episodes; aim for a few hundred.
- The spread rule contributes almost nothing; depth depletion carries the label.
- More than half of all positives come from one hour: results will hinge on that burst.
- Persistence baseline AP is 2.2x the prevalence. A model must beat that, not chance.

| Model | AP | Lift | ROC-AUC | Brier | @alert P / R / FA per h | @1% FPR (oracle) recall / caught / median lead s | Calibrated |
|---|---|---|---|---|---|---|---|
| base_rate | 0.034 | 1.00 | 0.500 | 0.0328 | – / 0.00 / 0.0 | – / – / – | – |
| persistence_offline | 0.074 | 2.20 | 0.597 | 0.3306 | 0.04 / 0.60 / 1754.4 | 0.07 / 0.62 / 14 | – |
| gbm_v1 | 0.061 | 1.81 | 0.671 | 0.0343 | 0.00 / 0.00 / 4.2 | 0.02 / 0.07 / 18 | – |
| gbm_v2 | 0.044 | 1.30 | 0.579 | 0.0334 | – / 0.00 / 0.0 | – / – / – | yes |
| lgbm_v1 | 0.044 | 1.31 | 0.594 | 0.0333 | – / 0.00 / 0.0 | – / – / – | yes |
| logit_v1 | 0.053 | 1.58 | 0.608 | 0.0333 | – / 0.00 / 0.0 | – / – / – | yes |
| persistence_v1 | 0.036 | 1.05 | 0.500 | 0.2878 | 0.03 / 0.52 / 1770.0 | 0.02 / 0.03 / 27 | no |

## Fold 3: test 2026-11-28 07:03 → 2026-11-28 12:00 UTC
Thresholds fitted on training data: depth_fraction 0.3326, spread rule off. Test: 17,479 usable rows, 328 positives, 14 stress episodes.

- NO-GO for training: only 14 stress episodes; aim for a few hundred.
- The spread rule contributes almost nothing; depth depletion carries the label.
- Persistence baseline AP is 3.96x the prevalence. A model must beat that, not chance.

| Model | AP | Lift | ROC-AUC | Brier | @alert P / R / FA per h | @1% FPR (oracle) recall / caught / median lead s | Calibrated |
|---|---|---|---|---|---|---|---|
| base_rate | 0.019 | 1.00 | 0.500 | 0.0185 | – / 0.00 / 0.0 | – / – / – | – |
| persistence_offline | 0.074 | 3.93 | 0.682 | 0.3231 | 0.03 / 0.68 / 1731.8 | 0.10 / 0.57 / 14 | – |
| gbm_v1 | 0.045 | 2.42 | 0.566 | 0.0200 | 0.18 / 0.03 / 8.5 | 0.07 / 0.21 / 17 | – |
| gbm_v2 | 0.029 | 1.56 | 0.509 | 0.0198 | – / 0.00 / 0.0 | – / – / – | yes |
| lgbm_v1 | 0.028 | 1.49 | 0.531 | 0.0198 | – / 0.00 / 0.0 | – / – / – | yes |
| logit_v1 | 0.026 | 1.37 | 0.531 | 0.0200 | – / 0.00 / 0.0 | – / – / – | yes |
| persistence_v1 | 0.033 | 1.76 | 0.596 | 0.2853 | 0.02 / 0.66 / 1736.2 | 0.04 / 0.29 / 14 | no |
