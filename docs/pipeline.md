# Offline feature and label pipeline (chunk 12)

`python -m sentinalops.data.pipeline` turns the recorder's Parquet files into the training table: one row per second with the 34 features, the stress label and the regression targets.

## Output

```
data/processed/features/date=2026-11-15/hour=13.parquet   one file per recorded UTC hour
data/processed/features/_runs/run-<time>.json              summary of each run
```

Read it all with `sentinalops.data.pipeline.read_table(out_dir)`, or `pd.read_parquet` on the directory.

| Column | Meaning |
| --- | --- |
| `symbol`, `ts_ms` | the tick (UTC epoch ms, whole seconds) |
| 34 features | `FEATURE_ORDER` from `features/microstructure.py`; never null |
| `label` | 1 if stress starts in `(t, t+30 s]`, 0 if not, null when `label_status` is not `ok` |
| `label_status` | `ok`, `not_calm` (already stressed at t), `void_unknown` (stale feed or warm-up in the horizon), `void_gap` (a recorded outage in the horizon) |
| `time_to_onset_s`, `fwd_max_rel_spread`, `fwd_min_depth_ratio` | regression targets from chunk 9 |
| `regime` | `untagged` until chunk 14 |
| `source` | `recorded_binance_stream` |

The schema is `table_schema()`; `validate_table()` checks it and runs before every write.

## Rules

- **Rows:** a tick is kept only if its features are valid (fresh book, 60 s of continuous feed) and no outage in the recorder's gap log overlaps `(t - 60 s, t]`. An outage can lose trades while the book still looks fresh, so the gap log is checked as well as staleness.
- **Labels:** a label is kept only if no logged outage overlaps `(t, t + 30 s]`. Gaps scoped to another symbol are ignored.
- **Hours are independent:** each hour is built from the previous, the same and the next hour's files with enough padding for the 600 s label reference, the 60 s windows and the 30 s horizon. A test checks that the hour files equal one pass over the whole recording.
- **Resumable:** finished hours are skipped (`--force` rebuilds). Files are written to `.tmp` then renamed.
- **The newest hour is left out** because its last labels need the next hour; pass `--include-last` once the recording has stopped.

## Leakage tests

- Deleting all data after time T leaves every row whose horizon ends before T unchanged.
- Sampled rows match the slow reference `compute_tick`.
- Mutation check: removing the feature-window gap check, the horizon gap check, or the padding each makes a test fail.

## Speed (task 12.6)

`scripts/bench_pipeline.py` on a synthetic day at real rates (864,000 snapshots, about 1.7 million trades): **6.7 s per day** on a Windows laptop, against a 10-minute target.
