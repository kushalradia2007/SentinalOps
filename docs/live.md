# Live system

`src/sentinalops/live/`. Connects to Binance, scores a forecast every second, verifies each forecast 30 s later, and keeps the model healthy. Design: `docs/design/model-slot-and-live-wiring-design.md`.

```
Binance (aggTrade + depth5@100ms)
   │  BinanceFeed: the recorder's reconnect, watchdog, 23 h rotation, de-duplication
   ▼
 raw_trades, raw_depth ──► features worker (StreamProcessor) ──► features, predictions
 raw_depth ─────────────► outcomes worker (OutcomeVerifier)  ──► outcomes
 predictions, outcomes ─► ledger writer ──► data/ledger.sqlite (append-only)
 clock: every 1 s, so ticks keep coming (and are suspended) when the feed goes silent

 heal service (separate process): every 5 min, HealController.tick(now)
   reads the ledger, may retrain the champion's own recipe, promotes through the gates,
   writes models/champion.joblib, and the live process hot-reloads it
```

## Processes

| Command | What runs |
|---|---|
| `python -m sentinalops.live` | Feed, features, outcomes and ledger in one process (in-memory bus) |
| `python -m sentinalops.live --role heal` | The heal service. Caps its BLAS/OpenMP threads at 1 |
| `python -m sentinalops.live --bus kafka --role feed` (or `features`, `outcomes`, `ledger`) | One component per process over Redpanda |

Common options are `--record` (also archive raw rows to Parquet), `--data-dir` (feed state and gap log), and `--health-interval-s` / `--heal-interval-min`. Paths come from the usual environment variables: `MODEL_PATH`, `DATA_DIR`, `DOMAIN_CONFIG`, and `KAFKA_BROKER` for Kafka.

## Behaviour worth knowing

- **Warm-up.** Predictions start about 60 s after connecting (the longest feature window). Outcomes are `warmup` voids for the first 5 minutes, until the label's trailing reference is trusted.
- **Feed silent or disconnected.** After `stale_data_seconds` (3 s) the ticks are suspended (no predictions). The verifier voids any horizon that crosses the gap. The feed reconnects with backoff, and recovery needs a fresh 60 s warm-up.
- **No model file.** Features keep flowing and scoring errors are counted. Predictions start as soon as `champion.joblib` appears.
- **Restarts.** Prediction ids are derived from symbol, tick and model version, so re-scored ticks never duplicate ledger rows.
- **Manual model upgrade.** Train with `python -m sentinalops.training.train` (see `docs/models.md`). The heal service registers the new `champion.joblib` as a version and promotes it ("manual install"). From then on, heal cycles retrain that model's own recipe.
- **Backpressure.** Every queue is bounded and drops its oldest item if a consumer stalls (counted as `bus_drops` / `feed_drops` in the health line). This should never happen at about 20 messages a second.

## Health

`live health` (every 60 s) logs events in, late events (must stay 0), ticks, scored and suspended, the model version and last probability, reconnects, gaps, drops, ledger writes and errors, and the last heal event. `heal health` (every cycle) logs the champion and shadow, the last action and its reason, matured labels, live AP, and the promotion and rollback counts.

## Oracle Cloud (Always Free, Ampere A1)

1. Create an Ubuntu 24.04 **ARM** VM (VM.Standard.A1.Flex, 2 OCPU / 12 GB, boot volume 150 GB or more) in an **India** home region. Binance blocks US locations, and the home region cannot be changed later.
2. SSH in and run:

   ```bash
   git clone https://github.com/kushalradia2007/SentinalOps.git && cd SentinalOps
   BRANCH=main bash deploy/oracle/setup.sh            # add --redpanda and/or --record
   ```

   The script checks Binance access first (`deploy/oracle/preflight.py`) and changes nothing on the system if that fails. It then installs two systemd units, `sentinalops-live` and `sentinalops-heal`, which restart on failure and start at boot.
3. Watch it: `/opt/sentinalops/repo/deploy/oracle/monitor.sh live`.

**Idle reclamation.** Oracle may reclaim an Always Free VM whose CPU, network and memory use all stay under 20% for 7 days. This system's CPU and network use stay far below that. Either upgrade the account to Pay As You Go (Always Free resources stay free), or run with `--redpanda`, which reserves 3 GB of memory. The setup script and the monitor both show which applies.
