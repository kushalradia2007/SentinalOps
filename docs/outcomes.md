# Prediction ledger and outcome verifier (chunk 34)

The loop that lets the platform grade itself: every prediction is stored, the verifier works out what actually happened once the 30 s horizon has passed, and the ledger joins the two.

## Outcome verifier

`src/sentinalops/stream/verifier.py`. Consumes the raw depth stream (labels depend only on the book) and, for every tick `t`, emits an `OutcomeLabel` to `outcomes` once tick `t + 30 s` has been seen.

It is the chunk 9 label computed incrementally: as-of book state per tick, the trailing 600 s median reference (previous ticks only, at least 300 known), `stressed`, `confirmed` (two stressed ticks in a row), then the onset label from ticks `t+1 .. t+30`.

| `label` | `void_reason` | Meaning |
| --- | --- | --- |
| 1 | - | stress started within 30 s |
| 0 | - | it did not |
| null | `already_stressed` | the market was stressed at `t`; onset is undefined |
| null | `gap` | the book went stale in the horizon (or just before `t`) |
| null | `warmup` | the trailing reference was not established yet |

Watermark, allowed lateness and late-event counting work like the stream processor (`docs/streaming.md`).

**Tests:**

- Replaying 30 minutes of depth (two stress episodes and an 8 s outage) gives the same label as offline `build_labels` on every one of 1,769 ticks, and matches the slow reference `label_at` on a 200-tick sample. Mutation check: letting the reference include the current tick, or dropping the confirmation rule, fails the test.
- An outcome is never emitted before the watermark passes `t + 30 s`; deleting data after time T never changes an outcome whose horizon ended before T.

The offline training table also voids labels whose horizon overlaps an outage in the recorder's gap log; the live verifier relies on staleness only.

## Prediction ledger

`src/sentinalops/serving/ledger.py`, an SQLite file (`data_dir/ledger.sqlite` for the API).

- **Append-only:** `predictions` and `outcomes` are insert-only; SQLite triggers reject any update or delete. A prediction gets its outcome by joining on `(symbol, ts_ms, horizon_s)`, so nothing is written back into a prediction row.
- **Idempotent:** a replayed prediction (same ID) or a second outcome for the same tick is ignored, so it can consume an at-least-once topic.
- **Feature snapshot:** stored with each prediction for explanations and drift checks.

Each prediction ends as `hit`, `miss`, `false_alarm`, `correct_negative` or `void`, and is `pending` until its outcome arrives. Void rows are excluded from every metric.

`Ledger.scores(since_ms, until_ms, model_version)` returns the confusion table, precision, recall, base rate and Brier score for a time window; chunk 39 builds the rolling monitoring metrics on it.

## Still to do

- `GET /predictions/{id}` and `GET /predictions?since=` (task 34.6, Tanuj): `Ledger.get` and `Ledger.predictions` already return what they need.
- Wiring the verifier and the ledger to Redpanda topics (chunk 15).
