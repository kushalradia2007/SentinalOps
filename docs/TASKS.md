# Task breakdown: Liquidity Intelligence

Every open chunk from `docs/ROADMAP.md` is split into small tasks. Pick the next unticked task for your name. Task IDs are `chunk.number` (for example `16.3`). Chunks 1, 4, 5 and 6 are finished.

## Summary

- **276 tasks** across 46 chunks.
- Sizes: S=183, M=92, L=1.
- Tasks per person: Jaswanth 53, Kushal 71, Tanuj 35, Vaibhav 71, Yogya 46.

## How to use this file

- **Sizes:** S is about 1-2 hours, M is about half a day, L is about a day. Nothing is bigger than L.
- **Needs:** tasks or chunks that must be finished first. `-` means you can start now.
- **One task = one small pull request** (branch name `chunk-NN-task-M`). Open it as soon as the task is done, do not wait to finish the whole chunk.
- **Done when:** the check that the reviewer will make. If your task meets it and has a test, it is done.
- **Owner `Yogya (rev. Vaibhav)`** means a junior does the task and the named senior reviews it.
- **Stuck for 30 minutes?** Ask in the group chat. Juniors: ask your mentor first, then anyone.

## Words used in the tasks

- **Trade event:** one message from the `btcusdt@aggTrade` stream (price, quantity, trade time, and whether the buyer was the maker).
- **Depth snapshot:** one message from `btcusdt@depth5@100ms` (top five bid and ask levels). It carries no event time, so we stamp it with our receive time.
- **Tick:** one row of features computed once per second.
- **Stress label:** 1 if liquidity stress starts within the next 30 seconds, else 0. Defined in chunk 9.
- **Matured:** a prediction whose 30-second horizon has passed, so its label is known.

## Chunk 2: Domain switch

Phase: Foundation | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 2.1 | Extend `DomainConfig` with the liquidity keys in `configs/domain_liquidity.yaml` (symbol, cadence, horizon, windows, label block, embargo, stale limit) | Kushal | M | - | Config loads with every key typed and range-checked; bad values raise |
| 2.2 | New event schemas: `TradeEvent`, `DepthSnapshot`, `FeatureTick`, `StressPrediction`, `OutcomeLabel` | Kushal | M | - | Round-trip and validation tests for each |
| 2.3 | Make `domain_liquidity.yaml` the default in `config.py`, `.env.example` and compose | Kushal | S | 2.1 | `get_domain_config()` returns `market_liquidity`; tests updated |
| 2.4 | Update `pyproject.toml` description and add `websockets` to dependencies | Kushal | S | - | `pip install -e ".[dev]"` works in CI |
| 2.5 | Change `/score` request and response to the new schemas (features in, stress probability out) | Kushal | M | 2.2, 3.3 | API tests pass with the new payload |
| 2.6 | Delete the supply-chain code and tests once replacements exist | Kushal | S | 2.5, 3.5, 10 | No `malwarebench`, `qut_dv25`, `fetcher`, `extract`, `zero_day` modules; CI green |

## Chunk 3: Regime-shift simulator, synthetic data, champion, thesis test

Phase: Foundation | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 3.1 | Synthetic tick generator for a calm regime (stable depth, one-tick spread, rare stress) | Kushal | M | 2.2 | Seeded; returns a DataFrame with the feature columns and a label |
| 3.2 | Synthetic volatile regime where stress has different precursors (trade bursts, one-sided depth) | Kushal | M | 3.1 | Stress base rate and precursor pattern differ measurably from calm |
| 3.3 | Train the baseline champion on calm data and save `models/champion.joblib` | Kushal | S | 3.1 | `make train` writes the model |
| 3.4 | Update `scripts/smoke.py` to score one calm tick and one stressed tick | Kushal | S | 2.5, 3.3 | `python scripts/smoke.py` prints two scores |
| 3.5 | Thesis test: champion trained on calm degrades on volatile; retrained challenger recovers | Kushal | M | 3.2, 3.3 | `tests/test_thesis.py` asserts the PR-AUC drop and the recovery |

## Chunk 7: Binance stream recorder

Phase: Data | Owner: **Vaibhav**

> **Why first:** there is no public archive of spot order-book depth. Every hour the recorder is not running is training data we never get.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 7.1 | Connect to the public market-data WebSocket for `btcusdt@aggTrade` and print parsed messages | Vaibhav | S | - | Prints price, quantity, trade time, maker flag |
| 7.2 | Same for `btcusdt@depth5@100ms`, stamping each snapshot with local receive time | Vaibhav | S | 7.1 | Prints five bid and five ask levels with a timestamp |
| 7.3 | Host allowlist: the recorder only connects to the documented Binance market-data hosts | Vaibhav | S | 7.1 | Any other host raises; tested |
| 7.4 | Buffer messages and flush to Parquet every minute, partitioned by `stream/date/hour` | Vaibhav | M | 7.1, 7.2 | Files appear under `data/raw/binance/`; schema asserted in a test |
| 7.5 | Reconnect with backoff on disconnect; answer server pings; reconnect before the 24-hour limit | Vaibhav | M | 7.4 | Killing the network for 30 s loses no more than that window |
| 7.6 | Write a gap log (start, end, reason) whenever data is missing | Vaibhav | S | 7.5 | `gaps.parquet` has one row per outage |
| 7.7 | `python -m sentinalops.data.recorder` CLI plus a compose service with a data volume | Vaibhav | S | 7.4 | `docker compose up recorder` records and survives a restart |
| 7.8 | Save 200 real messages of each stream as test fixtures | Tanuj (rev. Vaibhav) | S | 7.1, 7.2 | `tests/fixtures/binance/*.jsonl` committed; parser test uses them |
| 7.9 | Run for 24 hours and report row counts, file sizes and gaps | Vaibhav | M | 7.5-7.7 | Numbers in the PR; disk use per day known |

## Chunk 8: Historical trade archive loader

Phase: Data | Owner: **Yogya** | Mentor: **Vaibhav**

> **How to start:** New file `src/sentinalops/data/archive.py`. Open https://data.binance.vision in your browser, go to spot, daily, aggTrades, BTCUSDT, and download one day by hand first. Look inside the zip before writing code.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 8.1 | By hand: download one day of BTCUSDT `aggTrades`, write down the column names | Yogya | S | - | Column list in the PR description |
| 8.2 | `download_day(date)`: fetch the zip and its checksum file (host allowlist) | Yogya | S | 8.1 | File saved under `data/raw/archive/`; other hosts refused |
| 8.3 | Verify the checksum; raise if it does not match | Yogya | S | 8.2 | Test with a corrupted file |
| 8.4 | Read the CSV into a DataFrame with the same columns as the recorder's trade files | Yogya | M | 8.2, 7.4 | Schema test shared with the recorder |
| 8.5 | CLI `python -m sentinalops.data.archive --from DATE --to DATE` that skips days already done | Yogya | S | 8.4 | Second run downloads nothing |
| 8.6 | Test using a 50-row sample CSV | Yogya | S | 8.4 | Passes offline in CI |

## Chunk 9: Label definition

Phase: Data | Owner: **Vaibhav**

> **This chunk decides what the model predicts.** Nothing in Phase E starts until 9.6 is reviewed by Kushal.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 9.1 | Trailing reference: rolling median of top-5 depth and relative spread over 600 s, using past data only | Vaibhav | M | 7.4 | Test proves a future spike does not change the past reference |
| 9.2 | `is_stressed(t)`: depth below a fraction of reference (primary) or spread above a multiple (secondary) | Vaibhav | S | 9.1 | Pure function; unit-tested on hand-made series |
| 9.3 | Choose the depth fraction and spread multiple from training-period data, then freeze them in the config | Vaibhav | M | 9.2, 7.9 | Values and the date range used are written in `docs/labels.md` |
| 9.4 | Onset label: 1 if the market is calm at `t` and becomes stressed within `(t, t+30s]` | Vaibhav | M | 9.2 | Test: already-stressed ticks are excluded, not labelled 1 |
| 9.5 | Regression targets: max relative spread and min depth in the next 30 s | Vaibhav | S | 9.1 | Columns present; tested |
| 9.6 | Base-rate report: how often each label fires per hour, per day, and how bursty it is | Vaibhav | M | 9.4 | Table in `docs/labels.md`; go/no-go on the spread label recorded |
| 9.7 | Fallback decision if the base rate is too low: longer horizon, softer threshold, or a second symbol | Vaibhav | S | 9.6 | One paragraph in `docs/labels.md`, agreed with Kushal |

## Chunk 10: Microstructure feature library

Phase: Data | Owner: **Vaibhav**

> New module `src/sentinalops/features/microstructure.py`. Every function is pure, takes events up to time `t`, and is shared by the offline pipeline, the stream processor and the API.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 10.1 | Book features from one snapshot: mid-price, absolute spread, relative spread | Vaibhav | S | 2.2 | Hand-checked values in tests |
| 10.2 | Depth features: top-5 bid depth, ask depth, total depth, depth imbalance | Vaibhav | S | 2.2 | Imbalance is +1 with no asks, -1 with no bids, 0 when equal |
| 10.3 | Rolling-window helper that never reads past `t` (windows 1, 5, 30, 60 s) | Vaibhav | M | - | Test fails if a future event is included |
| 10.4 | Trade features per window: count, volume, buy-initiated share, trade imbalance | Vaibhav | M | 10.3 | Maker flag mapped correctly to buy or sell; tested |
| 10.5 | Realized volatility of mid-price returns per window | Vaibhav | S | 10.3 | Zero for a flat series; matches a hand calculation |
| 10.6 | Change features: depth change and spread change over each window | Vaibhav | S | 10.2, 10.3 | Tested on a step series |
| 10.7 | `FEATURE_ORDER` list and `compute_tick(snapshots, trades, t)` returning one feature dict | Vaibhav | M | 10.1-10.6 | Returns every name in `FEATURE_ORDER`; no NaN after warm-up |
| 10.8 | Warm-up rule: emit no tick until the longest window is full | Vaibhav | S | 10.7 | First tick appears after 60 s of data (tested) |
| 10.9 | Write a feature dictionary page (name, formula, unit) | Yogya (rev. Vaibhav) | S | 10.7 | `docs/features.md`, one row per feature |

## Chunk 11: Feed data quality

Phase: Data | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 11.1 | Trade continuity check: aggregate trade IDs must increase by one | Jaswanth | S | 7.4 | Returns the missing ID ranges; tested |
| 11.2 | Snapshot checks: best bid below best ask, positive quantities, `lastUpdateId` never decreases | Jaswanth | S | 7.4 | Bad fixture rows are flagged |
| 11.3 | Staleness: time since the last message per stream | Jaswanth | S | - | Function with a fake clock test |
| 11.4 | Clock skew: compare trade event time with local receive time, report the distribution | Jaswanth | S | 7.9 | Median and p99 lag in the PR |
| 11.5 | Quality report CLI over a recorded day | Jaswanth | S | 11.1-11.4 | Prints four sections and an overall pass or fail |
| 11.6 | Tests for 11.1-11.3 using saved fixtures | Yogya (rev. Jaswanth) | S | 11.1-11.3, 7.8 | Run offline in CI |

## Chunk 12: Offline feature and label pipeline

Phase: Data | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 12.1 | Offline table schema: `symbol, ts, <features>, label, targets, regime, source` plus a validator | Vaibhav | S | 10.7 | Schema in code with a test |
| 12.2 | Merge recorded trades and snapshots into one time-ordered event stream | Vaibhav | M | 7.4 | Order asserted; ties broken deterministically |
| 12.3 | Build one feature tick per second by calling `compute_tick` | Vaibhav | L | 10.7, 12.2 | One hour of data processed; resumable per hour partition |
| 12.4 | Attach labels and targets from chunk 9; drop ticks whose horizon crosses a recorded gap | Vaibhav | M | 9.4, 7.6, 12.3 | No label uses data across a gap (tested) |
| 12.5 | Write `features.parquet` partitioned by date, plus a run summary | Vaibhav | S | 12.4 | Row counts and label rate printed |
| 12.6 | Speed check: one recorded day processes in under 10 minutes | Vaibhav | M | 12.3 | Timing in the PR; vectorise if slower |

## Chunk 13: Dataset QA

Phase: Data | Owner: **Yogya** | Mentor: **Vaibhav**

> **How to start:** Work in a new file `src/sentinalops/data/qa.py`. Each task is one small function that takes a pandas DataFrame. Write a tiny 6-row DataFrame in the test first, then make the function pass it.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 13.1 | `find_duplicates(df)`: rows with the same `symbol` and `ts` | Yogya | S | - | Test with a 6-row DataFrame containing 1 duplicate |
| 13.2 | `label_balance(df)`: count and share of stress labels per day | Yogya | S | - | Returns a DataFrame; tested |
| 13.3 | `temporal_split(df, test_fraction, embargo_seconds)`: older rows train, newer rows test, with a gap between | Yogya | M | - | No test row is within the embargo of any train row (asserted) |
| 13.4 | `check_embargo(train, test, horizon)`: fail if any train label window overlaps the test period | Yogya | M | 13.3 | Returns the offending rows; test with 1 overlap |
| 13.5 | `find_time_gaps(df, max_gap_seconds)`: list the holes in the tick series | Yogya | S | - | Finds a 10-second hole in a test series |
| 13.6 | CLI `python -m sentinalops.data.qa <parquet>` printing a one-page report | Yogya | S | 13.1-13.5 | Runs on the chunk 12 output and prints five sections |

## Chunk 14: Market regime tagging

Phase: Data | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 14.1 | Define the regimes in words and numbers: calm, volatile, thin, directional, recovery | Vaibhav | S | 9.6 | Table in `docs/regimes.md` |
| 14.2 | Rule-based tagger from 5-minute volatility, depth and trade-imbalance quantiles (past data only) | Vaibhav | M | 12.5, 14.1 | Every tick has a regime; quantiles frozen from training data |
| 14.3 | Regime timeline plot for a recorded week | Vaibhav | S | 14.2 | Image in `docs/regimes.md` |
| 14.4 | Per-regime label rate and row counts | Vaibhav | S | 14.2 | Table in the PR; shows which regimes have enough data |
| 14.5 | Tests for the tagger on synthetic calm and volatile series from chunk 3 | Vaibhav | S | 14.2, 3.2 | CI green |

## Chunk 15: Redpanda topics and event schemas

Phase: Streaming | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 15.1 | Define topics: `raw_trades`, `raw_depth`, `features`, `predictions`, `outcomes`, `dlq` (partitions, retention, key = symbol) | Jaswanth | S | 6 | Table in `docs/streaming.md` |
| 15.2 | Idempotent `scripts/create_topics.py` | Jaswanth | S | 15.1 | Running it twice does not fail |
| 15.3 | JSON (de)serialiser helpers for each event schema from 2.2 | Jaswanth | M | 15.1, 2.2 | Round-trip test for every schema |
| 15.4 | Producer/consumer wrappers (aiokafka) plus an in-memory fake for tests | Jaswanth | M | 15.3 | Fake used in tests, real one used in compose |
| 15.5 | Compose `topic-init` service; check with `rpk topic list` | Jaswanth | S | 15.2 | `docker compose up` creates the six topics |
| 15.6 | Retention sizing: `raw_depth` gets about 10 messages per second, so set retention by size | Jaswanth | S | 15.1, 7.9 | Disk estimate in `docs/streaming.md` |

## Chunk 16: Live trade producer

Phase: Streaming | Owner: **Yogya** | Mentor: **Jaswanth**

> **How to start:** New file `src/sentinalops/stream/trade_producer.py`. Read the recorder from chunk 7 first: the WebSocket connection code is shared, so import it instead of copying. Do tasks in order; each one prints something so you can see it working.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 16.1 | Using the saved fixture, parse one `aggTrade` message into a `TradeEvent` | Yogya | S | 2.2, 7.8 | Test passes offline |
| 16.2 | Map the maker flag to `side` (buyer is maker means a sell-initiated trade) | Yogya | S | 16.1 | Two-row test, one of each side |
| 16.3 | Connect live with the shared client and print `TradeEvent` objects | Yogya | S | 16.1, 7.1 | Prints events for one minute |
| 16.4 | Publish events to `raw_trades` keyed by symbol | Yogya | S | 15.4, 16.3 | Visible with `rpk topic consume raw_trades` |
| 16.5 | Count published messages and expose the counter on `/metrics` | Yogya | S | 16.4 | Counter increases in Prometheus |
| 16.6 | Run for 10 minutes; Ctrl-C exits cleanly without losing buffered messages | Yogya | S | 16.4 | No errors in the log |

## Chunk 17: Live depth producer

Phase: Streaming | Owner: **Tanuj** | Mentor: **Jaswanth**

> **How to start:** Copy the structure of `trade_producer.py` (chunk 16) into `depth_producer.py`. Do 17.1 with the saved fixture before connecting to anything.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 17.1 | Using the saved fixture, parse one depth message into a `DepthSnapshot` (prices and quantities as floats) | Tanuj | S | 2.2, 7.8 | Test passes offline |
| 17.2 | Stamp each snapshot with the local receive time | Tanuj | S | 17.1 | Timestamp present and increasing in a test |
| 17.3 | Reject bad snapshots (best bid at or above best ask, fewer than five levels) | Tanuj | S | 17.1 | Bad fixture rows are dropped and counted |
| 17.4 | Connect live and print snapshots | Tanuj | S | 17.1, 7.2 | Prints about 10 per second |
| 17.5 | Publish to `raw_depth` keyed by symbol | Tanuj | S | 15.4, 17.4 | Visible on the topic |
| 17.6 | Compose services for both producers | Tanuj (rev. Jaswanth) | S | 16.4, 17.5 | `docker compose up` starts both and they stay up |

## Chunk 18: Feed health

Phase: Streaming | Owner: **Tanuj** | Mentor: **Jaswanth**

> **How to start:** New file `src/sentinalops/stream/health.py`. This is the data-pipeline half of self-healing. Each task is one small function with a fake clock in its test, so nothing here needs a live connection.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 18.1 | `FeedHealth` class that records the last message time per stream | Tanuj | S | - | Unit test with a fake clock |
| 18.2 | `is_stale()`: true when either stream is older than `stale_data_seconds` from the config | Tanuj | S | 18.1, 2.1 | Truth-table test |
| 18.3 | Publish a `feed_healthy` gauge (1 or 0) and `feed_age_seconds` per stream | Tanuj | S | 18.1 | Visible on `/metrics` |
| 18.4 | Hook the producers: update `FeedHealth` on every message | Tanuj | S | 16.4, 17.5 | Gauge is 1 while the producers run |
| 18.5 | Suspension: the processor emits no prediction while the feed is stale, and needs a full warm-up after it recovers | Jaswanth | M | 18.2, 19.4 | Test: stale period produces zero predictions |
| 18.6 | Count reconnects and suspended seconds as metrics | Tanuj | S | 18.3 | Counters visible |
| 18.7 | Stop the depth producer on purpose and screenshot the gauge going to 0 and back | Tanuj | S | 18.4 | Screenshot in the PR |

## Chunk 19: Stream processor

Phase: Streaming | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 19.1 | Spike: confirm faust-streaming runs on our Python; if not, use a plain aiokafka consumer | Jaswanth | S | 15.4 | Decision recorded in `docs/streaming.md` |
| 19.2 | Consume `raw_trades` and `raw_depth` into in-memory rolling buffers (60 s) | Jaswanth | M | 19.1 | Buffers never hold events older than the longest window |
| 19.3 | Every second, call `compute_tick` and publish a `FeatureTick` to `features` | Jaswanth | M | 19.2, 10.7 | Tick on the topic matches the offline value for the same input |
| 19.4 | Score each tick with the champion and publish a `StressPrediction` to `predictions` | Jaswanth | M | 19.3, 3.3 | Prediction has an ID, timestamp, horizon and model version |
| 19.5 | Late and out-of-order events: define the allowed lateness and what happens beyond it | Jaswanth | M | 19.2 | Rule in `docs/streaming.md`; tested |
| 19.6 | Prometheus counters: ticks, predictions, errors, consumer lag | Jaswanth | S | 19.3 | Visible on `/metrics` |
| 19.7 | Integration test with in-memory fake topics and the saved fixtures | Jaswanth | M | 19.4 | Runs in CI without Redpanda |

## Chunk 20: Dead-letter queue, retries, idempotency, backpressure

Phase: Streaming | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 20.1 | Classify errors: retryable (broker, network) vs permanent (unparseable message, schema error) | Jaswanth | S | - | Function with unit tests |
| 20.2 | Retry helper: exponential backoff with jitter | Jaswanth | S | - | Tested with a fake clock |
| 20.3 | Send permanently failed messages to `dlq` with the reason | Jaswanth | S | 20.1, 15.4 | DLQ message contains the original payload + error |
| 20.4 | Idempotency: one prediction per (symbol, tick second), even after a restart or replay | Jaswanth | M | 19.4 | Reprocessing the same minute produces no duplicate prediction IDs |
| 20.5 | Backpressure: if the processor falls behind, drop to the newest snapshot instead of queueing | Jaswanth | M | 19.2 | Lag recovers in a slow-consumer test; dropped count is a metric |
| 20.6 | Tests for 20.1-20.5 | Jaswanth | M | 20.1-20.5 | CI green |

## Chunk 21: Replay simulator with speed control

Phase: Streaming | Owner: **Tanuj** | Mentor: **Jaswanth**

> **How to start:** New file `src/sentinalops/simulator/replay.py`. Start with 21.1 in a Python shell: `pd.read_parquet(...)`, look at the columns, then build up one flag at a time. The demo depends on this chunk: if the live market is calm on demo day, we replay a recorded volatile period instead.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 21.1 | Read one hour of recorded trades and depth and print the first 5 rows of each | Tanuj | S | 7.4 | Prints rows |
| 21.2 | Merge both into one stream in timestamp order | Tanuj | S | 21.1 | Order asserted in a test |
| 21.3 | `--speed N`: sleep (gap between timestamps / N) | Tanuj | M | 21.2 | `--speed 60` replays one hour in one minute |
| 21.4 | Publish to `raw_trades` and `raw_depth` exactly as the live producers do | Tanuj | S | 15.4, 21.2 | The processor cannot tell replay from live |
| 21.5 | `--from`, `--to` and `--loop` flags | Tanuj | S | 21.2 | Flags documented in `--help` |
| 21.6 | Replay clock: the processor and verifier use event time, not wall time, during replay | Jaswanth | M | 19.3, 21.4 | A 60x replay produces the same ticks as a 1x replay |
| 21.7 | Test: high speed finishes fast and preserves order | Tanuj | S | 21.3 | Test passes in under 2 s |

## Chunk 22: Feast repo: entities, feature views, offline store

Phase: Feature store | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 22.1 | Create `feature_repo/` with `feature_store.yaml` (local offline Parquet) | Jaswanth | S | 12 | `feast apply` succeeds |
| 22.2 | Entity `symbol` and a Parquet file source with `ts` as the event timestamp | Jaswanth | S | 22.1 | Registered in Feast |
| 22.3 | Feature view with every feature in `FEATURE_ORDER`, with a short TTL | Jaswanth | M | 22.2 | `feast apply` lists the view |
| 22.4 | Run `feast apply` in CI/compose | Jaswanth | S | 22.3 | Step green in CI |
| 22.5 | Test: `get_historical_features` for 10 timestamps returns the right columns | Jaswanth | M | 22.3 | Test passes |
| 22.6 | Write a short "how features flow" page in `docs/` with a Mermaid diagram | Yogya (rev. Jaswanth) | S | 22.3 | Page reviewed by Jaswanth |

## Chunk 23: Redis online materialization

Phase: Feature store | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 23.1 | Point the online store at Redis in `feature_store.yaml` | Jaswanth | S | 22.1 | `feast apply` still succeeds |
| 23.2 | Push each `FeatureTick` from the processor to the online store | Jaswanth | M | 23.1, 19.3 | Latest tick for BTCUSDT readable from Redis |
| 23.3 | Helper `get_online_features(symbol)` returning the feature dict and its age | Jaswanth | M | 23.2 | Matches the last published tick |
| 23.4 | Measure p50/p99 lookup latency over 1000 calls | Jaswanth | S | 23.3 | Numbers in the PR |
| 23.5 | CI test with a Redis service container | Jaswanth | M | 23.3 | Test passes in GitHub Actions |

## Chunk 24: Point-in-time-correct training sets

Phase: Feature store | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 24.1 | Write 10 lines explaining point-in-time correctness with a market example | Vaibhav | S | - | In `docs/pit.md` |
| 24.2 | `build_training_set(entity_df)` using Feast historical features | Vaibhav | M | 22.5 | Returns features as of each row's time |
| 24.3 | Leakage test: a feature computed after the prediction time never appears | Vaibhav | M | 24.2 | Test fails if you remove the time filter |
| 24.4 | Combine with `temporal_split` and the embargo from chunk 13 | Vaibhav | S | 13.3, 24.2 | One function returns train and test sets |

## Chunk 25: Online/offline consistency test

Phase: Feature store | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 25.1 | Replay one recorded hour through the stream path and compute the same hour offline | Vaibhav | M | 12, 21, 23 | Two DataFrames keyed by tick time |
| 25.2 | Assert equality within tolerance and print any differences | Vaibhav | S | 25.1 | Zero diffs, or each diff explained |
| 25.3 | Run it nightly in CI on a small fixture hour | Vaibhav | S | 25.2 | Workflow file added |

## Chunk 26: Stress-forecast champion, persistence baseline, metrics suite

Phase: Models | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 26.1 | Load `features.parquet` and apply the embargoed temporal split | Vaibhav | S | 12, 13.3 | Train/test shapes and label rates printed |
| 26.2 | Metrics module: PR-AUC, precision, recall, recall at 1% false-positive rate, Brier score | Vaibhav | M | - | Unit-tested on a toy example |
| 26.3 | Lead-time metric: seconds between the first alert and the stress onset | Vaibhav | M | 26.2 | Unit-tested on a synthetic series |
| 26.4 | Persistence baseline: score = current stress indicator and current depth ratio | Vaibhav | S | 26.2, 9.2 | Baseline metrics printed |
| 26.5 | Train the GBM stress forecaster on the training period | Vaibhav | M | 26.1, 26.2 | PR-AUC printed next to the persistence baseline |
| 26.6 | Log params, metrics and model to MLflow | Vaibhav | S | 26.5 | Run visible in the MLflow UI |
| 26.7 | Save the model as the API champion file | Vaibhav | S | 26.5 | API loads it and `python scripts/smoke.py` passes |
| 26.8 | Write a one-page model card (data period, metrics, known limits) | Yogya (rev. Vaibhav) | S | 26.5 | In `docs/model-card.md`, reviewed by Vaibhav |

## Chunk 27: Anomaly detectors

Phase: Models | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 27.1 | Define a common `Detector` interface (`fit`, `score`) | Vaibhav | S | - | Protocol + a dummy implementation |
| 27.2 | Rolling z-score detector on spread, depth and imbalance (the simple statistical baseline) | Vaibhav | S | 27.1 | Scores in [0,1]; unit test |
| 27.3 | IsolationForest detector | Vaibhav | S | 27.1 | Scores in [0,1]; unit test |
| 27.4 | PyOD ECOD and COPOD detectors | Vaibhav | S | 27.1 | Scores in [0,1]; unit tests |
| 27.5 | Run all on the real features; report how well the anomaly score ranks upcoming stress | Vaibhav | M | 26.1, 27.2-27.4 | Table in the PR |

## Chunk 28: PyTorch autoencoder

Phase: Models | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 28.1 | Dataset/DataLoader for calm-regime ticks (scaled with training-period statistics) | Kushal | S | 26.1, 14.2 | Batches have the right shape |
| 28.2 | MLP autoencoder with a bottleneck about a quarter of the input width | Kushal | M | - | Forward pass test |
| 28.3 | Training loop with early stopping, CPU only | Kushal | M | 28.1, 28.2 | Loss decreases on synthetic data |
| 28.4 | Anomaly score = reconstruction error; threshold at the 99th percentile of calm ticks | Kushal | S | 28.3 | Implements the `Detector` interface |
| 28.5 | CPU smoke test | Kushal | S | 28.4 | Runs in under 10 s in CI |

## Chunk 29: Streaming learners (River)

Phase: Models | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 29.1 | HalfSpaceTrees streaming detector (River) | Jaswanth | M | 27.1 | Learns one tick at a time; test |
| 29.2 | ADWIN drift detector on the score stream | Jaswanth | S | 29.1 | Fires on synthetic drift |
| 29.3 | Wrap both in the `Detector` interface | Jaswanth | S | 29.1 | Same interface as the others |
| 29.4 | Test on the calm-to-volatile synthetic shift from chunk 3 | Jaswanth | S | 29.2, 3.2 | Drift detected within N ticks |

## Chunk 30: Comparison harness and MLflow experiments

Phase: Models | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 30.1 | Evaluation runner over the four approaches: rolling thresholds, batch detectors, online detectors, supervised forecaster | Kushal | M | 26, 27, 28, 29 | One command prints a table |
| 30.2 | Per-regime breakdown of every metric | Kushal | M | 30.1, 14.2 | Table has one block per regime |
| 30.3 | MLflow experiment naming and param logging convention | Yogya (rev. Kushal) | S | 26.6 | Documented in `docs/mlflow.md` |
| 30.4 | Markdown results-table generator | Yogya (rev. Kushal) | S | 30.1 | Writes `docs/results.md` |
| 30.5 | Inference latency per model (single tick, CPU) | Kushal | S | 30.1 | Column added to the table |

## Chunk 31: Calibration and cost-sensitive alert threshold

Phase: Models | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 31.1 | Probability calibration (isotonic or Platt) on a validation period after the training period | Kushal | S | 26.5 | Brier score improves; reliability table printed |
| 31.2 | Reliability diagram saved with the MLflow run | Kushal | S | 31.1 | Image attached to the run |
| 31.3 | Cost matrix in the domain config (a missed stress event costs more than a false alarm) | Tanuj (rev. Kushal) | S | 2.1 | Config field validated |
| 31.4 | Pick the alert threshold that minimises expected cost | Kushal | M | 31.1, 31.3 | Chosen threshold printed with its cost |
| 31.5 | Write the threshold where the API reads it (`block_threshold`) | Kushal | S | 31.4 | API uses it after restart |

## Chunk 32: MLflow registry: champion/challenger aliases

Phase: Models | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 32.1 | Register the model in the MLflow registry | Jaswanth | S | 26.6 | Version 1 exists |
| 32.2 | Set `champion` and `challenger` aliases | Jaswanth | S | 32.1 | Aliases visible in the UI |
| 32.3 | `promote()` and `rollback()` helpers | Jaswanth | M | 32.2 | Tested against a local SQLite MLflow |
| 32.4 | API and stream processor load `models:/<name>@champion` and reload when the alias moves | Jaswanth | M | 32.2 | Promoting changes the served `model_version` without restart |
| 32.5 | Tests for 32.1-32.4 | Jaswanth | M | 32.1-32.4 | CI green |

## Chunk 33: /score with online feature lookup and alias hot-reload

Phase: Serving | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 33.1 | Load the model by MLflow alias in the API (uses 32.4) | Kushal | M | 32.4 | `/health` shows the alias version |
| 33.2 | `GET /score/{symbol}`: read the latest tick from the online store and return the stress probability | Kushal | M | 23.3 | Response includes probability, horizon, feature age, model version |
| 33.3 | Refuse to score when the features are older than `stale_data_seconds` | Kushal | S | 33.2 | Returns 503 with a reason; tested with a fake store |
| 33.4 | `GET /state/{symbol}`: latest features, anomaly score, regime and feed health for the dashboard | Kushal | S | 33.2 | Schema documented in the API docs |
| 33.5 | Tests for 33.1-33.4 | Yogya (rev. Kushal) | S | 33.1-33.4 | CI green |

## Chunk 34: Prediction ledger and outcome verifier

Phase: Serving | Owner: **Kushal**

> This replaces the old `/labels` endpoint. Labels are no longer posted by anyone: the verifier computes them from the stream once each horizon has passed.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 34.1 | Append-only prediction ledger (ID, timestamp, horizon, probability, model version, feature snapshot) | Kushal | M | 2.2 | Every prediction is stored and survives restarts |
| 34.2 | Verifier: for each prediction older than its horizon, compute the outcome with the chunk 9 label code | Kushal | M | 34.1, 9.4 | Uses only events after the prediction time (tested) |
| 34.3 | Publish an `OutcomeLabel` to `outcomes` and write it back to the ledger | Kushal | S | 34.2, 15.4 | Each prediction ends as hit, miss, false alarm or correct negative |
| 34.4 | Unresolvable predictions: mark as void when the horizon crosses a feed gap | Kushal | S | 34.2, 18.2 | Void rows are excluded from every metric |
| 34.5 | Query: confusion table and Brier score for a time window and model version | Kushal | M | 34.3 | Returns numbers for the last N minutes |
| 34.6 | `GET /predictions/{id}` and `GET /predictions?since=` for the dashboard | Tanuj (rev. Kushal) | S | 34.1 | Returns the prediction with its outcome once matured |
| 34.7 | Property test: the verifier and the offline label pipeline agree on a recorded hour | Kushal | M | 34.2, 12.4 | Zero disagreements |

## Chunk 35: Alert policy, severity tiers, SHAP explanations

Phase: Serving | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 35.1 | Three-tier severity: normal / watch / alert, from the calibrated probability | Vaibhav | S | 31.5 | Pure function with tests |
| 35.2 | Alert de-duplication: one alert per stress episode, with a cool-down | Vaibhav | M | 35.1 | A 60-second stress burst raises one alert (tested) |
| 35.3 | SHAP top-3 reasons for the tree model | Vaibhav | M | 26.5 | Reasons for a stressed fixture mention depth or imbalance |
| 35.4 | Return `severity` and `reasons` in the `/score` response | Vaibhav | S | 35.1, 35.3 | Schema updated; API test |
| 35.5 | Optional webhook on alert (URL from `.env`, off by default) | Vaibhav | S | 35.2 | Fake receiver gets one POST in a test |
| 35.6 | Tests for 35.1-35.5 | Vaibhav | S | 35.1-35.5 | CI green |

## Chunk 36: Load testing and p99 latency targets

Phase: Serving | Owner: **Yogya** | Mentor: **Kushal**

> **How to start:** Install locust (`pip install locust`) and follow its "Quick start" page. Start the API with `make api`; the request to send is in `scripts/smoke.py`.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 36.1 | Install locust and write `locustfile.py` that calls `/score/BTCUSDT` | Yogya | S | 33.2 | Locust UI shows requests succeeding |
| 36.2 | Run 1 minute with 20 users and save the results | Yogya | S | 36.1 | Screenshot or CSV in `docs/perf/` |
| 36.3 | Add `/state/BTCUSDT` and `/predictions?since=` to the mix | Yogya | S | 36.1 | All three endpoints appear in the stats |
| 36.4 | Write the p50/p95/p99 table | Yogya | S | 36.2 | In `docs/perf/README.md` |
| 36.5 | Measure end-to-end delay: market event to published prediction | Jaswanth (rev. Kushal) | M | 19.4 | p50 and p99 in `docs/perf/README.md` |
| 36.6 | Open a GitHub issue for anything slower than 100 ms at p99 | Yogya | S | 36.4 | Issue linked in the PR |

## Chunk 37: Prometheus metrics

Phase: Monitoring | Owner: **Jaswanth**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 37.1 | Request latency histogram and request counter on the API | Jaswanth | S | - | Visible on `/metrics` |
| 37.2 | Gauges for the live market: relative spread, top-5 depth, imbalance, trades per second | Jaswanth | S | 19.3 | Values move in Prometheus |
| 37.3 | Histograms for the stress probability and the anomaly score | Jaswanth | S | 19.4 | Drift jobs can read them |
| 37.4 | Verify Prometheus targets are UP (API, processor, producers, recorder) | Jaswanth | S | 37.1 | Screenshot in the PR |
| 37.5 | Metric naming guide | Jaswanth | S | 37.1-37.3 | Short page in `docs/` |

## Chunk 38: Evidently drift jobs

Phase: Monitoring | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 38.1 | Build reference (training period) and current (last N minutes) datasets from the feature log | Kushal | M | 12.5, 19.3 | Two DataFrames with the same columns |
| 38.2 | Evidently data-drift report on the `drift_watch` features | Kushal | M | 38.1 | HTML report generated |
| 38.3 | Prediction drift: compare the stress-probability distribution with the reference | Kushal | S | 38.2 | Second score returned |
| 38.4 | Export both drift scores as Prometheus gauges | Kushal | S | 38.2, 38.3 | Gauges visible in Prometheus |
| 38.5 | Run the job on a schedule (every N minutes) | Jaswanth (rev. Kushal) | S | 38.4 | Gauges update by themselves |
| 38.6 | Test: calm to volatile synthetic data triggers high drift | Kushal | M | 38.2, 3.2 | Test passes |

## Chunk 39: Matured-label rolling precision, recall and Brier score

Phase: Monitoring | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 39.1 | Rolling-window precision, recall, false alarms per hour and Brier score from the ledger | Kushal | M | 34.5 | Unit-tested |
| 39.2 | The same metrics for the persistence baseline, so the gap is always visible | Kushal | S | 39.1, 26.4 | Two series per metric |
| 39.3 | Expose them as Prometheus gauges labelled by model version | Kushal | S | 39.1 | Visible in Prometheus |
| 39.4 | Handle "too few stress events in the window" without errors | Kushal | S | 39.1 | Gauges report NaN plus a sample-count gauge |
| 39.5 | Degradation signal: current window worse than the reference by a configured margin | Kushal | M | 39.1 | Returns true on the synthetic regime shift, false on calm data |
| 39.6 | Tests for 39.1-39.5 | Yogya (rev. Kushal) | S | 39.1-39.5 | CI green |

## Chunk 40: Alert rules

Phase: Monitoring | Owner: **Tanuj** | Mentor: **Kushal**

> **How to start:** Alert rules are one small YAML file. Read the Prometheus "Alerting rules" docs page (10 min), then do 40.1. Files go in `infra/`; the Prometheus UI is on :9090 after `docker compose up`.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 40.1 | Write rule `ModelDriftHigh`: drift gauge above 0.3 for 2 minutes | Tanuj | S | 38.4 | File in `infra/` |
| 40.2 | Mount the rules file in compose and load it in Prometheus | Tanuj | S | 40.1 | Rule listed under Alerts in the UI |
| 40.3 | Write rule `ReliabilityDropped` on the degradation signal | Tanuj | S | 39.5 | Rule listed in the UI |
| 40.4 | Write rule `FeedStale` on the `feed_healthy` gauge | Tanuj | S | 18.3 | Rule listed in the UI |
| 40.5 | Add `promtool check rules` to CI | Tanuj | S | 40.1 | CI step green |
| 40.6 | Trigger each alert on purpose and take a screenshot | Tanuj | S | 40.2-40.4 | Three screenshots in the PR |

## Chunk 41: Grafana dashboards and "Model Healed" annotations

Phase: Monitoring | Owner: **Yogya** | Mentor: **Kushal**

> **How to start:** Open Grafana on :3000 (admin/admin) after `docker compose up`. Build each panel with the UI first, then export its JSON. One dashboard row per task.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 41.1 | Row "Live Market": trades per second, relative spread, bid and ask depth, imbalance | Yogya | S | 37.2 | Shows live data |
| 41.2 | Row "Anomaly Intelligence": anomaly score over time and current severity | Yogya | S | 37.3 | Shows live data |
| 41.3 | Row "Predictive Intelligence": stress probability with the alert threshold line | Yogya | S | 37.3 | Line visible |
| 41.4 | Row "Model Reliability": rolling precision, recall, Brier, false alarms, with the persistence baseline | Yogya | M | 39.3 | Both series visible |
| 41.5 | Row "Self-Healing": drift gauges, feed health, model version, retrain runs | Yogya | S | 38.4, 18.3 | Shows live data |
| 41.6 | "Model Healed" annotation driven by `model_promotions_total` | Yogya | M | 45.4 | Marker appears after a promotion |
| 41.7 | Export JSON into `infra/grafana/dashboards/` and provision it | Yogya | S | 41.1-41.5 | Dashboard appears after a fresh `docker compose up` |

## Chunk 42: Prefect flow triggered by performance drop plus label count

Phase: Self-healing | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 42.1 | Prefect flow skeleton `heal_flow` | Kushal | S | - | Runs locally and logs a line |
| 42.2 | Task: read the degradation signal and the drift scores | Kushal | S | 39.5, 38.4 | Returns typed values; mocked in tests |
| 42.3 | Task: count matured labels and stress events since the last retrain | Kushal | S | 34.5 | Returns two ints; mocked in tests |
| 42.4 | Trigger rule: performance degraded AND enough matured labels; drift alone only logs a warning | Kushal | S | 42.2, 42.3 | Truth-table test |
| 42.5 | Cool-down: no second retrain within a configured interval | Kushal | S | 42.4 | Tested with a fake clock |
| 42.6 | Prefect server and schedule in compose | Jaswanth (rev. Kushal) | M | 42.1 | Flow visible in the Prefect UI |
| 42.7 | Tests for the flow with mocks | Kushal | S | 42.4, 42.5 | CI green |

## Chunk 43: Retrain DAG: recent window plus replay buffer

Phase: Self-healing | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 43.1 | Assemble the last `retrain_window_hours` of features plus matured labels | Kushal | M | 34.3, 12.5 | Returns a labelled DataFrame |
| 43.2 | Replay buffer: sample older data from each regime so the model does not forget them | Kushal | M | 43.1, 14.2 | Mix ratio configurable; tested |
| 43.3 | Hold out the most recent slice (after an embargo) for the gates | Kushal | S | 43.1, 13.3 | Holdout never overlaps the training rows |
| 43.4 | Train and calibrate the challenger | Kushal | S | 26.5, 31.1 | Model object returned |
| 43.5 | Log the challenger to MLflow with tags (window, regime mix, data counts) | Kushal | S | 43.4 | Run visible |
| 43.6 | Idempotency: never retrain twice for the same degradation event | Kushal | S | 43.1-43.5 | Second trigger is skipped (tested) |
| 43.7 | End-to-end test on synthetic calm to volatile data | Kushal | M | 43.6, 3.2 | Challenger recovers the lost PR-AUC; passes in CI |

## Chunk 44: Challenger vs champion gates

Phase: Self-healing | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 44.1 | Evaluate champion and challenger on the same holdout | Kushal | S | 43.3, 43.4 | Metrics for both returned |
| 44.2 | Gate: challenger improves PR-AUC on the current regime by at least X | Kushal | S | 44.1 | Configurable X; tested |
| 44.3 | Gate: no regression beyond a tolerance on older regimes | Kushal | S | 44.1, 14.2 | Configurable tolerance; tested |
| 44.4 | Gate: challenger beats the persistence baseline and is not worse calibrated | Kushal | S | 44.1, 26.4 | Tested |
| 44.5 | Gate report (JSON + Markdown) saved with the run | Kushal | S | 44.2-44.4 | Report attached to the MLflow run |
| 44.6 | Tests for 44.1-44.5 | Yogya (rev. Kushal) | S | 44.1-44.5 | CI green |

## Chunk 45: Auto-promotion and rollback safety

Phase: Self-healing | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 45.1 | Promote the challenger to champion through the registry | Kushal | S | 32.3, 44.5 | Alias moves only if every gate passes |
| 45.2 | Shadow period: keep scoring with the old champion and log both predictions to the ledger | Kushal | M | 45.1, 34.1 | Both model versions have matured outcomes |
| 45.3 | Automatic rollback when the new champion is worse over the shadow period | Kushal | M | 45.2 | Old champion restored (tested) |
| 45.4 | Emit `model_promotions_total`, `model_rollbacks_total` and a log line for each | Kushal | S | 45.1 | Counters visible |
| 45.5 | Tests for 45.1-45.4 | Yogya (rev. Kushal) | S | 45.1-45.4 | CI green |

## Chunk 46: Recorded regime-shift scenario library

Phase: Self-healing | Owner: **Vaibhav**

> This replaces the adversarial variant library. The self-healing demo needs real recorded periods where the market changed character.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 46.1 | Scan the recorded data for regime changes using the chunk 14 tags | Vaibhav | M | 14.2 | List of candidate periods with start, end and regimes |
| 46.2 | Pick three scenarios: calm to volatile, calm to thin, shock then recovery | Vaibhav | S | 46.1 | Each described in `docs/scenarios.md` with a plot |
| 46.3 | Scenario manifest file (name, time range, expected label rate) read by the replay simulator | Vaibhav | S | 46.2, 21.5 | `--scenario NAME` replays it |
| 46.4 | For each scenario, measure how much the champion degrades | Vaibhav | M | 46.3, 26.5 | Before-shift and after-shift metrics in a table |
| 46.5 | Synthetic fallback scenario from chunk 3 in case no clean real shift was recorded | Vaibhav | S | 3.2, 21.4 | Replayable through the same path |
| 46.6 | Package the scenario data as a small downloadable file outside git | Vaibhav | S | 46.3 | A teammate can fetch and replay it with one command |

## Chunk 47: End-to-end incident-timeline demo

Phase: Demo and polish | Owner: **Kushal**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 47.1 | Step 1 script: replay a calm period; show predictions maturing and the champion beating the baseline | Tanuj (rev. Kushal) | S | 46.3, 39.3 | Reliability panel is green |
| 47.2 | Step 2 script: replay the regime shift; show reliability falling | Tanuj (rev. Kushal) | S | 47.1 | `ReliabilityDropped` fires |
| 47.3 | Step 3: the heal flow triggers, trains a challenger and runs the gates | Kushal | S | 42, 43, 44 | Gate report visible in MLflow |
| 47.4 | Step 4: promotion with the "Model Healed" marker and the shadow check | Kushal | S | 45, 41.6 | Marker on the dashboard |
| 47.5 | Step 5 script: continue the replay; show reliability recovered | Kushal | S | 47.4 | Metrics back above the baseline |
| 47.6 | Feed-recovery mini demo: stop a producer, show suspension, restart, show recovery | Tanuj (rev. Kushal) | S | 18.5 | `FeedStale` fires and clears |
| 47.7 | Incident timeline view: anomaly, forecast, verified outcome and heal on one time axis | Kushal | M | 34.6, 41.7 | One screen tells the whole story |
| 47.8 | `make demo` runs all steps; three full dry runs | Kushal | M | 47.1-47.7 | Three clean runs logged |
| 47.9 | Live-mode check: run the whole stack on the live stream for one hour | Kushal | S | 47.8 | No crashes; numbers recorded |

## Chunk 48: Docs: architecture diagram, runbook, ADRs

Phase: Demo and polish | Owner: **Yogya** | Mentor: **Kushal**

> **How to start:** Write in plain Markdown in `docs/`. For the diagram use Mermaid (GitHub renders it) so nobody needs a drawing tool. Ask the owner of each component to review your paragraph about it.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 48.1 | Architecture diagram in Mermaid (feed, stream, store, model, verifier, monitoring, healing) | Yogya | M | - | Renders on GitHub in `docs/architecture.md` |
| 48.2 | Update the README "How to run" and fix broken `docs/` links | Yogya | S | - | Every link in the README opens a real page |
| 48.3 | Runbook: what to do when the feed, stream, API, verifier or heal flow fails | Yogya | M | - | `docs/runbook.md` reviewed by each component owner |
| 48.4 | Five short ADRs (why Redpanda, Feast, MLflow, Prefect, Evidently) | Yogya | M | - | 5 files in `docs/adr/`, half a page each |
| 48.5 | ADR: why depth depletion is the primary label and why we label onset | Yogya (rev. Vaibhav) | S | 9.6 | One file in `docs/adr/` |
| 48.6 | "What this is not" page: no trading, no price prediction, no claims about causes | Yogya | S | - | Linked from the README |

## Chunk 49: Benchmarks and results writeup

Phase: Demo and polish | Owner: **Vaibhav**

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 49.1 | Four-approach comparison table on the held-out period, overall and per regime | Vaibhav | M | 30.2 | Numbers in `docs/results.md` |
| 49.2 | Before/after table: champion vs healed model on each scenario | Vaibhav | M | 47 | Numbers in `docs/results.md` |
| 49.3 | Measure heal time: reliability alert to promotion | Vaibhav | M | 47 | Median of 3 runs |
| 49.4 | Add latency numbers from chunk 36 | Vaibhav | S | 36.4, 36.5 | One table |
| 49.5 | Honest limits section: one symbol, one exchange, short recording period, top-5 depth only | Vaibhav | S | 49.1 | Section in `docs/results.md` |
| 49.6 | Two-page results writeup | Vaibhav | M | 49.1-49.5 | Reviewed by Kushal |

## Chunk 50: Demo video, resume bullets, v1.0 release

Phase: Demo and polish | Owner: **Tanuj** | Mentor: **Kushal**

> **How to start:** Do 50.1 (the script) first and get an OK from Kushal before recording anything.

| ID | Task | Owner | Size | Needs | Done when |
| --- | --- | --- | --- | --- | --- |
| 50.1 | Video script: 5 scenes matching the demo steps | Tanuj | S | 47 | Approved by Kushal |
| 50.2 | Record the screen and voiceover | Tanuj | M | 50.1 | Raw recording saved |
| 50.3 | Edit and export (under 5 minutes) | Tanuj | M | 50.2 | Video file ready |
| 50.4 | Draft resume bullets for each team member (3 versions each) | Tanuj | S | 49 | In `docs/resume-bullets.md` |
| 50.5 | Tag v1.0 and write the release notes | Tanuj | S | all | GitHub release created (Kushal presses the button) |
