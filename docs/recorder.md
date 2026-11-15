# Stream recorder (chunk 7)

Records Binance Spot `aggTrade` and `depth5@100ms` for each symbol to Parquet. Spot order-book depth has no public archive, so this runs continuously from day one. It uses public market data only: no API key, no account endpoints.

## Run it

```bash
python -m sentinalops.data.recorder                      # BTCUSDT into data/raw/binance
python -m sentinalops.data.recorder --symbols BTCUSDT,ETHUSDT --metrics-port 9108
python -m sentinalops.data.recorder --duration 60        # smoke test: stop after 60 s
docker compose up -d recorder                            # long-running, restarts on failure
python -m sentinalops.data.audit_recording --hours 24    # the task 7.9 report
```

Every flag has a `RECORDER_*` environment variable (see `--help`). Stop with Ctrl-C, `SIGTERM`, or Ctrl-Break on Windows; buffered rows are flushed before exit.

## What is on disk

```text
data/raw/binance/
  stream=aggTrade/symbol=BTCUSDT/date=2026-11-15/hour=13/part-<first_ms>-<last_ms>-<id>.parquet
  stream=depth5/symbol=BTCUSDT/date=2026-11-15/hour=13/part-<first_ms>-<last_ms>-<id>.parquet
  _gaps/gap-<start_ms>-<id>.parquet     one row per outage: start_ms, end_ms, duration_ms, reason, scope
  _state/last_seen.json                 last receive time, used to log restart gaps
  _rejected/frames.jsonl                malformed frames, kept for inspection (capped at 5 MB)
```

Partitions use the local receive time in UTC. One file is written per partition per flush (every 60 s by default, or earlier at 50,000 buffered rows), with Zstandard compression.

| Table | Columns |
| --- | --- |
| `aggTrade` | `symbol`, `event_time_ms`, `trade_time_ms`, `agg_trade_id`, `price`, `quantity`, `first_trade_id`, `last_trade_id`, `buyer_is_maker`, `recv_time_ms` |
| `depth5` | `symbol`, `last_update_id`, `recv_time_ms`, then `bid_px_1`, `bid_qty_1` ... `bid_px_5`, `bid_qty_5` and the same for `ask` |

Times are UTC epoch milliseconds (`int64`); prices and quantities are `float64`. The depth stream carries no exchange timestamp, so `recv_time_ms` is its only clock. `buyer_is_maker = true` means the trade was sell-initiated.

## How it avoids losing data

| Risk | What the recorder does |
| --- | --- |
| Connection drops | Reconnects with exponential backoff and jitter (0.5 s doubling to 30 s) and writes the outage to `_gaps/` |
| Connection open but silent | A watchdog reconnects after 10 s with no frames, or 10 s with no depth frame for any symbol |
| Binance's 24-hour connection limit | At 23 hours it opens a replacement connection first, then closes the old one; overlapping rows are de-duplicated by trade ID and update ID |
| Crash during a write | Files are written to `*.tmp`, fsynced and renamed; leftovers are removed at startup |
| Write fails (for example, disk full) | The rows go back into the buffer and are retried on the next flush |
| Shutdown | Buffers are flushed before exit |
| Process was down | On restart, the time since the last recorded frame is written as a `restart` gap |
| Malformed frame | Counted, logged and kept in `_rejected/`; never dropped silently |
| Wrong endpoint | Only `stream.binance.com` and `data-stream.binance.vision` are allowed |

A hard kill or power loss still loses whatever was buffered since the last flush (up to 60 s). That loss shows up as a `restart` gap.

## Health

A `recorder health` JSON log line is written every 30 s with messages per second per stream, buffered rows, last flush latency, reconnects, rotations, gaps, duplicates, malformed frames and the last event timestamp. With `--metrics-port`, the same values are served as Prometheus gauges named `recorder_*`.

## The audit report

`audit_recording` prints three sections and exits 1 on any problem:

1. **Volume:** files, rows, megabytes and rows per second for each symbol and stream, with a projected size per day.
2. **Gaps:** for depth, any receive-time jump above 500 ms; for trades, any hole in the aggregate trade IDs. Each gap is matched against `_gaps/`. A gap the recorder did not log is reported as unexplained.
3. **Integrity:** schema match, null counts, duplicate or out-of-order rows, unreadable files, leftover `.tmp` files.

Trade time gaps are not reported, because a quiet market produces them without any loss. A hole in the trade IDs is the reliable sign.
