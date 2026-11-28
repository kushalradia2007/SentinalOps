# The self-healing loop (chunks 39, 42-45)

`src/sentinalops/heal/`. Watches the champion's live quality in the prediction ledger, retrains when it degrades, promotes a challenger only if it proves itself, and rolls back if the promotion turns out worse. Settings are the `heal:` block of `configs/domain_liquidity.yaml`.

```
ledger ──► monitor (39) ──► trigger (42) ──► retrain (43) ──► gates (44) ──► promote + shadow (45) ──► keep or roll back (45)
   ▲                                                                                                          │
   └──────────── the API and stream processor hot-reload champion.joblib and score with it ◄──────────────────┘
```

## Monitor (chunk 39), `monitor.py`

- `window_scores(ledger, since, until, model_version)`: AP, Brier, precision, recall and base rate over matured, non-void predictions.
- `degradation(...)`: degraded when live AP over the last `metrics_window_minutes` is more than `degradation_margin` below the AP the champion scored on its holdout at promotion. Fewer than `min_positives` stress events gives no verdict (`too_few_events`), never a noisy one.

## Trigger (chunk 42), `controller.py`

`HealController.tick(now_ms)` is the whole loop, safe to run on a schedule. A retrain starts only when **all** hold:

| Rule | Why |
| --- | --- |
| Live AP degraded | Drift alone never retrains |
| At least `min_matured_labels` outcomes since promotion | Enough evidence |
| No attempt in the last `cooldown_minutes` | No thrashing |
| Not already attempted for this champion on the same evidence | Idempotent: a rejected challenger is not retrained again until `min_matured_labels` new outcomes have arrived |
| Not in a shadow period | One change at a time |

## Retrain (chunk 43), `retrain.py`

From the chunk 12 training table (`label_status == 'ok'` rows only):

- **Recent window:** the last `retrain_window_hours`.
- **Holdout:** the newest `holdout_fraction` of the window, after a `split_embargo_seconds` gap (at least the label horizon) so no training label looks into it.
- **Replay buffer:** older rows, `replay_fraction` of the recent training rows, spread evenly over regimes so the challenger does not forget them. The regression gate uses older rows from *other* 10-minute blocks, with the last `split_embargo_seconds` of every block dropped, because neighbouring seconds share their 30 s outcome window and a random split would leak.

## Gates (chunk 44)

Champion and challenger are scored on the same holdout. Every gate must pass, and a gate without data fails closed.

| Gate | Default |
| --- | --- |
| Challenger AP on the current regime beats the champion's by | 0.05 |
| AP loss on older regimes at most | 0.10 |
| Challenger AP at least this multiple of the base rate (the constant baseline until chunk 26) | 1.5 |
| Brier score worse by at most | 0.01 |
| Challenger AP at least that of `persistence_v1` (a fixed, fit-free "the book is thinning" score on the same holdout) | 0 |

The report is saved as `reports/<version>.json` and `.md` next to the registry.

## Promote, shadow, roll back (chunk 45), `registry.py`

A file-based stand-in for the MLflow registry (chunk 32): immutable `versions/`, and `champion.joblib` / `shadow.joblib` swapped atomically. Each model carries its version, so every prediction in the ledger names the model that made it.

- On promotion the old champion becomes the **shadow** for `shadow_minutes`: the stream processor scores every tick with both, and both go to the ledger under their own versions. Alerts use the champion only.
- When the shadow period ends, both are compared on the same ticks. If the new champion's AP is worse by `rollback_margin`, the old one is restored automatically; otherwise the shadow is retired.
- `registry.count("promote")` and `count("rollback")` back the `model_promotions_total` / `model_rollbacks_total` counters; every event is in `registry.json`.

## Tests (`tests/test_heal.py`)

The main test runs the whole story on a simulated market (`simulator/heal_sim.py`): 90 minutes calm, then 150 minutes volatile, scored tick by tick through the ledger.

| Minutes | What the loop did |
| --- | --- |
| 0-90 calm | Idle; live AP about 0.5-0.7 |
| 95-100 | Shift: live AP falls to about 0.1-0.2; degradation detected |
| 100 | First challenger **rejected**: only about 10 minutes of volatile data, so it failed the gain and baseline gates |
| 105-120 | Cool-down, then "already attempted" |
| 125 | Second challenger **promoted** (+0.49 AP, all gates pass); old champion goes to shadow |
| 145 | Shadow review: new model better, kept |
| 150-240 | Healthy again; live AP about 0.6-0.69 |

A second test force-promotes a bad model and checks it is rolled back after the shadow period. Mutation check: promoting without the gates, never rolling back, or dropping the embargo each fail a test.

## Not done yet

- Prefect flow and schedule around `tick()` (42.1, 42.6), MLflow logging and registry (43.5, 32), Prometheus counters (45.4), drift scores as a warning input (42.2 with chunk 38).
- MLflow logging of gate reports.
