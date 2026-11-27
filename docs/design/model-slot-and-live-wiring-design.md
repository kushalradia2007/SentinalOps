# Design: model slot (Part A) and Kafka-ready live wiring

Date: 2026-11-27. Status: awaiting approval.

## Intent

SentinalOps watches Binance's public BTCUSDT order book and trades and, once a second, predicts the chance that liquidity stress starts in the next 30 seconds. The owner wants:

1. **The system frozen, the model swappable.** A model upgrade is "add a model package, change one config line, train". No pipeline, serving, streaming or heal code changes.
2. **The heal loop improves the installed model**, retraining the same model type on fresh data, never swapping types.
3. **The system running live 24/7**, on an Oracle Cloud Free Tier VM.
4. **Kafka-ready:** runs today without a broker, switches to Redpanda by one setting, with no rework.

## Goals, non-goals, anti-goals

- **Goals:** a model contract and registry of recipes; live service from Binance to ledger and heal loop; an event-bus interface with in-memory and Redpanda implementations; deployable on an Oracle Ampere A1 (ARM) VM.
- **Non-goals (later chunks):** new model candidates and the evaluation harness (Part B), Feast, MLflow registry, Grafana dashboards, alert rules, the replay simulator.
- **Anti-goals:** any change to feature, label, pipeline or verifier logic; a live path whose features differ from the offline ones; a heal cycle that silently changes model type; losing recorded data.

## Constraints

- Oracle Always Free (checked 2026-11-27 on docs.oracle.com): Ampere A1 = **2 OCPUs and 12 GB RAM total**, 200 GB block storage, 10 TB/month outbound. All dependencies have ARM64 builds (numpy, pandas, scikit-learn, pyarrow, LightGBM 4.7.0, Redpanda image).
- Oracle reclaims an Always Free VM as idle if, over 7 days, 95th-percentile CPU < 20% **and** network < 20% **and** memory < 20% (A1). Our CPU and network load will be far below 20%, so memory use (Redpanda's reservation) or a Pay As You Go upgrade is what keeps the VM.
- The IITM server keeps recording (no Docker there). Training on months of data can happen there; the Oracle VM runs the live system.

## Approaches considered

| | Approach | Trade-off |
|---|---|---|
| A | One direct process, no bus | Simplest; drops the Kafka layer |
| B | Redpanda from day one | Matches the diagram; can't run where Docker is unavailable |
| **C (chosen)** | Components talk through a small bus interface; in-memory bus now, Redpanda bus by setting | Runs anywhere today, Kafka story kept, no rework |

Chosen by the owner: **C**.

## Part A: the model slot

In short:

- `src/sentinalops/models/`: `base.py` (contract), `__init__.py` (`get_recipe(name)`, `list_recipes()`), `gbm_v1/` (today's HistGradientBoosting, unchanged behaviour).
- Fitted models keep the existing shape (`predict_proba -> (n, 2)`, `feature_names_in_`, writable `sentinal_version_`) and add `sentinal_model_name_` and `sentinal_model_params_`, so serving, the stream processor, the registry and the gates need no changes.
- `model:` block in the YAML and `DomainConfig` (default `gbm_v1`), used for manual training only.
- `heal/retrain.py::train_challenger` takes the champion and retrains with **its** recipe; the config is only a fallback for models without metadata. `heal/controller.py::_retrain` passes the champion in.
- `python -m sentinalops.training.train --model <name>`; `train_baseline` stays as a thin wrapper; `scripts/entrypoint.sh` calls the generic command.
- `docs/models.md`: the contract, the frozen-package rule, the 3-step upgrade.

## Live wiring

### Topology

```
BinanceFeed ──raw_trades / raw_depth──► Bus ─┬─► FeaturesWorker (StreamProcessor) ──features / predictions──► Bus
(recorder code,                              ├─► OutcomesWorker (OutcomeVerifier) ──outcomes──────────────────► Bus
 reconnect + rotation)                       │
                                             └─ LedgerWriter ◄── predictions, outcomes   (SQLite ledger)
HealScheduler: every 5 min -> HealController.tick(now)  (reads the ledger, may promote -> champion.joblib hot-reloads)
Clock: every 1 s -> processor.advance(now), verifier.advance(now)  (ticks keep flowing when the feed is silent)
```

- **Bus interface** (`src/sentinalops/live/bus.py`): `publish(topic, key, value)` and `subscribe(topics) -> async iterator`. Topics: `raw_trades`, `raw_depth`, `features`, `predictions`, `outcomes` (names from chunk 15).
  - `InMemoryBus`: asyncio queues, one per subscriber, bounded (default 10,000 messages, about 8 minutes of feed). On overflow it **drops the oldest** message, counts it per topic and logs a warning. A full queue means a consumer is far behind, and the newest data is what matters for a live forecast. It should never happen at about 20 messages a second.
  - **Clock injection:** the live runner takes a `now_ms()` function. Live it is wall time; replay tests drive the processor and verifier in event time and finish instantly. The processor and verifier already take explicit times, so only the runner needs this.
  - `KafkaBus`: aiokafka producer and consumers, JSON values, key = symbol. Optional dependency (`[stream]` extra). Unit tests use the in-memory bus; one integration test runs only when `KAFKA_BROKER` is set.
- **BinanceFeed:** reuses the recorder's proven connection supervisor (reconnects, stale-feed watchdog, 23-hour rotation, de-duplication). The one change to recorder code: an optional `on_row(kind, row)` callback in `Recorder.handle`, called after de-duplication. With no callback, behaviour is identical. By default the live service does **not** write Parquet; `--record` turns that on so the Oracle VM can also be a backup recorder.
- **Workers** are thin async loops around the existing `StreamProcessor` and `OutcomeVerifier`. No feature or label logic changes, so the existing online/offline consistency and verifier replay tests keep guarding them.
- **Ledger:** the existing SQLite ledger. It already opens each connection with a 30 s busy timeout, indexes `(symbol, ts_ms)`, and writes idempotently (`INSERT OR IGNORE` on `prediction_id` and on the outcome key `(symbol, ts_ms, horizon_s)`). Predictions and outcomes are separate tables joined at read time, so an outcome arriving before its prediction (possible with Kafka) is harmless. Changes:
  - turn on WAL with `synchronous=NORMAL`, so readers never block the writer and two writers wait instead of failing;
  - live predictions get a **deterministic** `prediction_id` (from symbol, tick and model version) instead of a random UUID, so a restarted or replayed features worker can't double-count a tick;
  - **no deletion:** the ledger is append-only by design (database triggers forbid deletes; it is the audit trail of every forecast). Growth is about 90 MB/day, with features stored, and every reader is bounded by time instead. Monthly ledger file rotation is a later option if size ever matters.
- **Heal runs in its own process, always.** Retraining can take seconds to minutes and would freeze the single-process event loop: missed WebSocket pings, a stalled clock, full queues. The heal scheduler is therefore a separate process even in in-memory mode. It needs no bus: it only reads the ledger and writes the file registry, and the live process hot-reloads `champion.joblib` as it does today. Training is limited to one thread (`OMP_NUM_THREADS=1`, `OPENBLAS_NUM_THREADS=1`, `MKL_NUM_THREADS=1`, run under `nice`), leaving the second Oracle CPU to the live feed. The heal process writes its own JSON health line every 60 s (last decision and its reason, last action, champion version, matured labels, live AP), and every action is also an event in `registry.json`.
- **Heal training table from the ledger:** every 5 minutes, `HealController.tick(now)`. Its table comes from the **ledger** (each matured prediction stores its features and outcome), current to the last 30 s; the offline pipeline leaves out the newest hour and would leave the holdout empty. The reader:
  - keeps only matured, non-void rows (`label IS NOT NULL`), so outcomes voided as `already_stressed` (the pipeline's `not_calm`), `gap` or `warmup` never train;
  - keeps one row per tick, deterministically (the earliest-scored prediction; champion and shadow saw identical features);
  - reads only `retrain_window_hours` plus a bounded replay lookback (default 7 days).
  - Warm-up is already guarded: `HealController.decide` does nothing without a champion, with fewer than `min_positives` stress events in the window, or fewer than `min_matured_labels` (2,000) outcomes since promotion; `train_challenger` refuses a window without both classes.
- **Process modes:** always at least two processes.
  - `python -m sentinalops.live` (default `--role pipeline`) runs feed, features, outcomes and ledger writer in one process on the in-memory bus.
  - `python -m sentinalops.live --role heal` runs the heal scheduler.
  - With `--bus kafka`, each pipeline component can also run alone (`--role feed|features|outcomes|ledger`).
- **Health (live process):** a JSON health line every 60 s with events in, ticks, suspended ticks, predictions, outcomes, late events, queue drops and the served model version. For heal status it reads the last heal event from `registry.json`, read-only; the heal process writes its own line too (above). There is also an optional Prometheus port bound to localhost.

### Deployment on Oracle

- One A1 VM, 2 OCPU / 12 GB, Ubuntu 24.04 ARM, 150–200 GB boot volume, home region in India (Mumbai or Hyderabad; Binance is reachable from India, as the IITM server shows, and blocks US locations).
- `deploy/oracle/`: a setup script (Python venv; **two systemd units**, `sentinalops-live` and `sentinalops-heal`, both restart on failure and start at boot; optional Docker plus Redpanda) and a `monitor.sh` like the recorder's that shows both.
- Memory budget: live service about 0.5 GB, recorder 0.1 GB, API 0.2 GB, plus Redpanda. Without Redpanda the VM sits near 7% memory use, below Oracle's 20% idle line (2.4 GB). **Idle-reclamation protection, in order of preference:**
  1. upgrade the account to Pay As You Go (Always Free resources stay $0; the owner decides, because it needs a card);
  2. otherwise run Redpanda with `--memory 3G`, which keeps the VM above 20% and is useful anyway.
  The setup script prints which protection is active.
- **Pre-flight check:** before installing services, the setup script opens a 10 s WebSocket connection to Binance from the VM and must receive both depth and trade messages; otherwise it stops with a clear message. Cloud IP ranges can be treated differently from university networks.

## Testing

- All existing tests pass; `ruff` clean.
- **Part A:** every recipe loads through `ChampionModel`; a config-selection test; heal retrains the champion's type even when the YAML names another.
- **Live wiring:**
  - End-to-end replay through the in-memory bus (synthetic recording -> feed -> features -> predictions -> outcomes -> ledger), with features equal to the offline `compute_ticks` and outcomes equal to `build_labels`.
  - The silent-feed clock produces suspended ticks.
  - Heal fires from ledger data.
  - `KafkaBus` passes the same bus contract tests (integration, broker required).
- A 5-minute run against live Binance from a laptop, and later on the Oracle VM.

## Review notes (external review, 2026-11-27)

Accepted: heal in its own process; WAL plus bounded readers; void filtering and deterministic de-duplication in the ledger reader; deterministic prediction IDs; drop-oldest overflow; clock injection; reclamation protection; Binance pre-flight check.

Declined:
- **Pruning ledger rows:** the ledger is append-only on purpose; readers are bounded instead.
- **Adding `model_role` and `label_status` columns:** the role is already identified by `model_version` (the registry knows which version is champion or shadow), and `void_reason` already carries `label_status`. Adding columns would be a migration on existing ledgers for no new information.

## Left to the builder

Exact module names, bus queue sizes, the heal interval default (5 min, configurable), and the health-line fields.
