# Model card: `<model name>` (`<version>`)

Fill this in when a model becomes the champion. Numbers come from `python -m sentinalops.training.evaluate` (see `docs/results.md` and `reports/eval-*.json`).

## What it predicts

The probability that liquidity stress (top-5 depth collapse, or spread blow-out when that rule is on) **starts** within the next 30 seconds, on BTCUSDT, once a second. Only calm ticks are scored. See `docs/labels.md`.

## Model

| | |
|---|---|
| Package | `src/sentinalops/models/<name>/` (frozen) |
| Inputs | the 34 features in `FEATURE_ORDER`, plus derived features if listed |
| Parameters | `<params>` |
| Calibration | `<isotonic on the newest 20% of training data / none, and why>` |
| Single-tick latency (p50 / p99) | `<ms>` / `<ms>` (limit 10 ms) |

## Data

| | |
|---|---|
| Recording period | `<first UTC>` → `<last UTC>` (`<days>` days, `<gaps>` logged outages) |
| Label thresholds | `depth_fraction=<x>`, spread rule `<on/off>`, fitted on training periods only at stress share `<s>` |
| Evaluation | `<k>` walk-forward folds, minimum `<h>` h training, 60 s embargo |
| Positive rate (test) | `<rate>` over `<episodes>` stress episodes |

## Results (mean ± std over folds)

| Model | AP | Lift | Brier | Recall @1% FPR (oracle) | Episodes caught | Median lead (s) |
|---|---|---|---|---|---|---|
| base rate | | | | | | |
| persistence (offline) | | | | | | |
| persistence_v1 | | | | | | |
| **this model** | | | | | | |

At the configured alert threshold `<t>`: precision `<p>`, recall `<r>`, `<n>` false alarms per hour.

Per regime (once regime tagging exists): `<table>`.

## Known limits

- Trained and evaluated on `<days>` days of one market (BTCUSDT on Binance spot); regimes not seen in that period are untested.
- The label is relative to the last 10 minutes: long-lasting thin books stop counting as stress after a few minutes.
- The oracle 1% FPR point is chosen on test data; the live alert threshold is `<t>`.
- `<anything else: data gaps, single burst dominating positives, calibration drift>`

## Approval

Promoted on `<date>` by `<who>` (manual install) / by the heal loop (gate report `reports/<version>.md`).
