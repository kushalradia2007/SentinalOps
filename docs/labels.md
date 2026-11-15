# The liquidity-stress label (chunk 9)

This is the definition of what the model predicts. The offline pipeline (chunk 12) and the outcome verifier (chunk 34) both call `src/sentinalops/labels.py`, so training labels and live verification cannot drift apart.

**Status:** the code tasks (9.1, 9.2, 9.4, 9.5) are done and tested on synthetic data. The thresholds below are still placeholders. Tasks 9.3, 9.6 and 9.7 need at least a few days of recorded depth data and are not done.

## Definition

Everything happens on a one-second grid.

1. **Book state.** From each `depth5` snapshot: mid-price, relative spread, and top-5 depth (the sum of quantities across five bid and five ask levels). Crossed or empty books are dropped.
2. **Tick.** Tick `t` takes the newest snapshot received at or before `t`. If that snapshot is more than 3 s old, the tick is **unknown**.
3. **Reference.** The rolling median of depth and of spread over the previous 600 s, excluding the current tick. It needs 300 known ticks before it is trusted.
4. **Stressed.** `depth < depth_fraction × reference depth` (primary), or `spread > spread_multiple × reference spread` (secondary).
5. **Confirmed.** Stressed for 2 ticks in a row. A single-snapshot flicker is not an event.
6. **Label at `t`.** For a tick that is calm at `t`: 1 if confirmed stress occurs anywhere in `(t, t + 30 s]`, otherwise 0.

| Case | Label |
| --- | --- |
| Calm now, confirmed stress within 30 s | 1 |
| Calm now, no confirmed stress within 30 s | 0 |
| Already stressed at `t` | not eligible |
| Any tick in the horizon is unknown (feed gap, warm-up, end of data) | void |

The module also produces `time_to_onset_s` (for the lead-time metric) and two regression targets, `fwd_max_rel_spread` and `fwd_min_depth_ratio`.

## Why it is built this way

- **Onset, not state.** Stress persists. If stressed ticks were eligible, "stressed now" would predict "stressed in 30 s" almost perfectly and every model would look good. Excluding them makes the task a forecast.
- **Void, never guessed.** A label whose horizon crosses a feed gap is dropped. Treating missing data as "no stress" would teach the model that outages are calm.
- **Reference excludes the present.** A collapse at `t` must not lower the bar it is measured against.
- **Long-lived stress fades from the label.** The reference is a 10-minute median, so if depth stays low for more than about five minutes the reference adapts and the market counts as calm again. This is intended: the label means "abnormal relative to recent conditions".

## What the tests prove

`tests/test_labels.py` checks the rules above, including:

- Truncating the data at any point leaves every causal column at earlier ticks unchanged.
- A label is identical whether or not data beyond its own horizon exists.
- The fast vectorised labels equal a slow, obvious per-tick implementation (`label_at`) on every tick of a noisy series with gaps. The verifier in chunk 34 has to match `label_at`.

Four deliberate bugs were injected to confirm the tests catch them: a reference that sees one tick ahead, a horizon that includes the current tick, stressed ticks made eligible, and ticks sampling the next snapshot. Each one failed between 3 and 7 tests.

## Running it on recorded data

```bash
python -m sentinalops.labels --data-dir data/raw/binance --symbol BTCUSDT
python -m sentinalops.labels --data-dir data/raw/binance --fit --stress-share 0.01
```

The first command uses the thresholds in `configs/domain_liquidity.yaml`. It prints the base-rate report followed by go/no-go notes. The report covers:

- how often each rule fires;
- eligible ticks and positives;
- stress episodes per hour and their median length;
- the share of positives that came from the single busiest hour;
- how well a lazy baseline ("the thinner the book is now, the likelier stress is") already ranks the positives.

`--fit` picks thresholds so each rule fires on about that share of ticks. If the spread never leaves one tick it switches the spread rule off.

## Still to decide with real data

| Task | Question | Needs |
| --- | --- | --- |
| 9.3 | Final `depth_fraction` and `spread_multiple`, fitted on the training period only, then frozen in the config with the date range used | Several days of recording |
| 9.6 | Is the base rate workable? Aim for a few hundred episodes, a positive rate between 0.2% and 20%, and positives not concentrated in one burst | Same |
| 9.6 | Does the spread rule contribute anything on BTCUSDT, or is depth the whole label? | Same |
| 9.7 | If the label is too rare: longer horizon, softer threshold, or a second symbol | The 9.6 report |

Fit thresholds only on the training period. Fitting on all data and then testing on part of it is leakage.
