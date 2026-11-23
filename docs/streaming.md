# Streaming path

## Stream processor (chunk 19)

`src/sentinalops/stream/processor.py`. Raw trades and depth snapshots in (rows in the recorder schema); `FeatureTick` and `StressPrediction` messages out, keyed by symbol, for the `features` and `predictions` topics.

### Decision: no stream framework (task 19.1)

The processor is a plain class with `on_trade` / `on_depth` / `advance`, plus an async `run(processor, events, publish)` that works with any event source. Kafka wiring (chunk 15's aiokafka wrappers) plugs into `run` without touching the processor. Reasons:

- faust-streaming adds a dependency and its own runtime, and we need none of its tables or windows: `compute_tick` already does the windowing.
- A plain class runs in unit tests with no broker, which is how the consistency test replays a recording.

### Event time and when a tick is emitted

- The clock is `recv_time_ms`, the same as the recorder and the offline pipeline.
- The **watermark** is the newest receive time seen on either stream. Tick `t` (a whole second) is computed once the watermark reaches `t + allowed_lateness_ms` (default 500 ms), so the trade and depth topics can arrive slightly skewed from each other.
- `advance(now_ms)` moves the watermark with the wall clock, so ticks keep being produced (and come out invalid) while the feed is silent.

### Late and out-of-order events (task 19.5)

| Arrival | What happens |
| --- | --- |
| Out of order but after the last emitted tick | Inserted in order; no effect on correctness |
| At or before the last emitted tick ("late") | Dropped and counted in `stats.late[kind]` |

Offline, a late event would have been counted, so **late events are the only way online and offline features can differ**. `stats.late` should stay at zero; if it does not, raise `allowed_lateness_ms`. The test replays a recording with one stream delayed by 300-450 ms and gets identical features with the default lateness, and late drops with too little lateness.

### Suspension while the feed is stale (task 18.5)

`compute_tick` returns nothing when the newest snapshot is older than `stale_data_seconds`, and until the feed has been continuous for the longest window again. So a stale feed produces no feature ticks and no predictions, and recovery needs a full 60 s warm-up. These ticks are counted in `stats.suspended_ticks`.

### Buffers (task 19.2)

Trades are kept for the longest window; depth for the longest window plus the one snapshot before it (plus a `stale_ms` safety margin). Memory is bounded by the feed rate times about 63 seconds.

### Online/offline consistency

`tests/test_stream_processor.py` replays a synthetic recording (with a stress episode and a 10 s outage) through the processor in arrival order and compares every feature tick with `compute_ticks` on the whole recording: same ticks, values equal to 1e-9. Shrinking the depth buffer below what `compute_tick` needs makes the test fail.

One intended difference from the training table: the offline pipeline also drops ticks whose window overlaps an outage in the recorder's gap log. The live processor has no gap log; it relies on staleness only.

### Not done yet

- Kafka consumer/producer wiring and topic creation (chunk 15).
- Prometheus metrics for `ProcessorStats` and consumer lag (task 19.6).
- Speed: each tick rebuilds small DataFrames from the buffers, about 8 ms per tick on a laptop, fine at one tick per second. Replaying a day for chunk 21 at high speed will want an incremental version.
