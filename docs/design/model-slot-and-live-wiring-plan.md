# Model Slot and Kafka-Ready Live Wiring Implementation Plan


**Goal:** Make the model a plug-in that the heal loop retrains by its own recipe, and run the system live from Binance to the ledger and heal loop over a swappable bus.

**Architecture:** Part A adds `sentinalops.models` (recipe registry and contract) and routes training and healing through it without changing serving, streaming or registry code. Part B adds `sentinalops.live`: a `Bus` interface (in-memory, and Kafka via aiokafka), a feed that reuses the recorder's connection supervisor through one optional callback, thin async workers around the existing `StreamProcessor` / `OutcomeVerifier` / `Ledger`, and a separate heal process that trains from the ledger.

**Tech Stack:** Python 3.11+, asyncio, websockets, pandas, scikit-learn, SQLite, aiokafka (optional), systemd on Ubuntu 24.04 ARM (Oracle A1).

**Spec:** `docs/design/model-slot-and-live-wiring-design.md`.

## Global Constraints

- `requires-python >=3.11`; ruff line length 110; `ruff check src tests` and the full `pytest` must pass after every task.
- No change to feature, label, pipeline or verifier logic (`features/`, `labels.py`, `data/pipeline.py`, `stream/verifier.py`, the body of `stream/processor.py`).
- The only recorder change is an optional hook; with defaults, recorder behaviour and output are byte-identical.
- Fitted models keep `predict_proba -> (n, 2)`, `feature_names_in_ == cfg.feature_names`, writable `sentinal_version_`; new attributes are `sentinal_model_name_`, `sentinal_model_params_`, `sentinal_holdout_ap_`.
- Default `model.name` is `gbm_v1` = `HistGradientBoostingClassifier(max_iter=200, learning_rate=0.08, random_state=seed)` on `cfg.feature_names` (today's behaviour).
- Topics: `raw_trades`, `raw_depth`, `features`, `predictions`, `outcomes`. In-memory queue default 10,000 per subscriber, drop-oldest on overflow.
- Heal: separate process, every 5 min (configurable), `OMP_NUM_THREADS=OPENBLAS_NUM_THREADS=MKL_NUM_THREADS=1`, ledger reader bounded to `retrain_window_hours` + 7 days.
- Ledger stays append-only; WAL + `synchronous=NORMAL`.

## Review Focus

1. Binance feed goes silent or disconnects: ticks are suspended (no predictions), the verifier voids horizons with `gap`, and nothing crashes. Test in Task 7.
2. No model file at startup: the live pipeline keeps producing feature ticks, counts scoring errors, and starts predicting as soon as a champion appears. Test in Task 7.
3. Heal process on an empty or new ledger: logs `no_champion` / `too_few_events` and idles. Test in Task 9.
4. The live process restarts mid-day: re-scored ticks don't duplicate ledger rows (deterministic `prediction_id`). Test in Task 6.
5. Champion trained before this refactor (no `sentinal_model_name_`): heal falls back to `cfg.model.name` instead of failing. Test in Task 3.

---

## Part A: the model slot

### Task 1: Recipe contract, registry, and `gbm_v1`

**Files:**
- Create: `src/sentinalops/models/__init__.py`, `src/sentinalops/models/base.py`, `src/sentinalops/models/gbm_v1/__init__.py`
- Modify: `src/sentinalops/training/train_baseline.py` (`fit` delegates to `gbm_v1`)
- Test: `tests/test_models.py`

**Interfaces:**
- Produces:
  - `base.Recipe` (Protocol): `name: str`; `fit(self, train: pd.DataFrame, cfg: DomainConfig, seed: int = 0, params: dict | None = None) -> object`.
  - `base.stamp(model, name: str, params: dict) -> model` sets `sentinal_model_name_` and `sentinal_model_params_`.
  - `base.check_fitted(model, cfg: DomainConfig) -> None` raises `ValueError` unless the model has `predict_proba`, `feature_names_in_ == cfg.feature_names` and `classes_ == [0, 1]`.
  - `models.get_recipe(name: str) -> Recipe` (`KeyError` listing the known names), `models.list_recipes() -> list[str]`, `models.register_recipe(name: str, import_path: str) -> None`. The registry maps names to `"module:attr"` strings, imported lazily so listing is cheap.
  - `models.model_name(model) -> str | None` reads `sentinal_model_name_`.

- [ ] **Step 1: Write failing tests:** `test_gbm_v1_is_default_and_loads_in_champion_model` (fit on `make_ticks("calm", 4000)`, `check_fitted` passes, dump with joblib, `ChampionModel(path, BASE.feature_names).predict(row)` returns a probability in [0, 1]); `test_gbm_v1_matches_old_baseline` (same seed → `predict_proba` equal to `HistGradientBoostingClassifier(max_iter=200, learning_rate=0.08, random_state=0)` on the same data); `test_unknown_recipe_lists_known` (`KeyError` message contains `gbm_v1`); `test_stamp_survives_pickle`.
- [ ] **Step 2:** Run `pytest tests/test_models.py -v`. Expected: FAIL (module missing).
- [ ] **Step 3:** Implement the files. `gbm_v1.Recipe.fit` uses only `cfg.feature_names` and `label.astype(int)`, accepts `params` overriding `max_iter`, `learning_rate` and the other HistGradientBoosting arguments, and returns the stamped estimator itself, so pickles stay plain sklearn. `train_baseline.fit(df, seed)` calls `get_recipe("gbm_v1").fit(df, get_domain_config(), seed)`.
- [ ] **Step 4:** Run `pytest -q && ruff check src tests`. Expected: all pass.
- [ ] **Step 5:** Commit: `Model slot: recipe contract, registry and gbm_v1`.

### Task 2: `model:` block in the config

**Files:**
- Modify: `src/sentinalops/config.py` (add `ModelConfig`, field `model` on `DomainConfig`), `configs/domain_liquidity.yaml`
- Test: `tests/test_config.py`

**Interfaces:**
- Produces: `ModelConfig(_Strict)`: `name: str = "gbm_v1"`, `params: dict[str, Any] = {}`; `DomainConfig.model: ModelConfig`. A validator rejects names not in `models.list_recipes()`.

- [ ] **Step 1: Write failing tests:** `test_model_block_defaults_to_gbm_v1`; `test_unknown_model_name_rejected` (`ValidationError` mentions the name); `test_yaml_model_block_loads` (YAML has `model: {name: gbm_v1, params: {}}`).
- [ ] **Step 2:** Run `pytest tests/test_config.py -v`. Expected: the new tests FAIL.
- [ ] **Step 3:** Implement. Add to the YAML, after `alert_threshold`, a commented block: `model:` / `name: gbm_v1   # a package in src/sentinalops/models; manual training only, heal follows the champion` / `params: {}`.
- [ ] **Step 4:** Run `pytest -q && ruff check src tests`. Expected: pass.
- [ ] **Step 5:** Commit: `Config: model block selects the recipe for manual training`.

### Task 3: Heal retrains the champion's own recipe

**Files:**
- Modify: `src/sentinalops/heal/retrain.py` (`train_challenger`, new `resolve_recipe`), `src/sentinalops/heal/controller.py` (`_retrain`: load the champion once, pass it; add `model_name` and `params` to the registered meta)
- Test: `tests/test_heal.py`

**Interfaces:**
- Consumes: Task 1 `get_recipe`, `model_name`, `register_recipe`.
- Produces:
  - `resolve_recipe(champion, cfg) -> tuple[str, dict]`: the champion's `sentinal_model_name_` and `sentinal_model_params_` if present, else `(cfg.model.name, cfg.model.params)`.
  - `train_challenger(ts: TrainingSet, cfg: DomainConfig, seed: int = 0, champion=None)`.

- [ ] **Step 1: Write failing tests:**
  - `test_heal_retrains_champion_recipe_not_config`: register a test recipe `logit_test` with `monkeypatch` on the registry dict, so it never leaks into `list_recipes()` (a `LogisticRegression` recipe defined in the test module), stamp the champion as `logit_test`, use a config whose `model.name` is `gbm_v1`, and run `train_challenger(ts, cfg, champion=champ)`. Assert `model_name(result) == "logit_test"`.
  - `test_unstamped_champion_falls_back_to_config`: a plain HistGradientBoosting champion (no attribute) gives `resolve_recipe(...) == ("gbm_v1", {})`.
  - `test_registered_heal_version_records_model_name`: after the main heal story, `registry.meta("v002")["model_name"] == "gbm_v1"`.
- [ ] **Step 2:** Run `pytest tests/test_heal.py -v`. Expected: the new tests FAIL; the existing ones pass.
- [ ] **Step 3:** Implement. Keep the "no positive or no negative labels" `ValueError` exactly as it is.
- [ ] **Step 4:** Run `pytest -q && ruff check src tests`. Expected: pass (the heal story timeline is unchanged).
- [ ] **Step 5:** Commit: `Heal loop retrains the installed champion's recipe`.

### Task 4: Generic training CLI, entrypoint, docs

**Files:**
- Create: `src/sentinalops/training/train.py`, `docs/models.md`
- Modify: `scripts/entrypoint.sh` (call `python -m sentinalops.training.train --out "$MODEL"`), `src/sentinalops/training/train_baseline.py` (`main` keeps its flags and output)
- Test: `tests/test_train.py`

**Interfaces:**
- Produces: `train.main(argv) -> None` with flags `--model` (default `cfg.model.name`), `--params` (JSON string), `--data` (`synthetic` or a training-table dir), `--rows` (synthetic size, default 20000), `--config`, `--out` (default `models/champion.joblib`). Splits:
  - real tables: `label_status == "ok"`, chronological 80/20 with `split_embargo_seconds` between the parts;
  - synthetic: stratified random, as today.
  It stamps `sentinal_holdout_ap_`, runs `check_fitted`, writes atomically (tmp then rename), and prints `model=<name> holdout_ap=<x> rows=<n> out=<path>`.

- [ ] **Step 1: Write failing tests:**
  - `test_train_cli_synthetic_writes_stamped_model` (`--rows 4000`; loaded model has `sentinal_model_name_ == "gbm_v1"` and a float `sentinal_holdout_ap_`);
  - `test_train_cli_on_table_uses_embargoed_time_split` (a small table from `heal_sim.market([("calm", 30)], T0)`; the max train `ts_ms` + embargo < the min holdout `ts_ms`);
  - `test_train_baseline_main_still_works` (keep the existing test).
- [ ] **Step 2:** Run `pytest tests/test_train.py -v`. Expected: the new tests FAIL.
- [ ] **Step 3:** Implement. Write `docs/models.md` covering the contract, the frozen-package rule (never edit a released package; upgrades are a new package), the 3-step upgrade (add the package and register it in `models/__init__.py`; set `model.name`; run the training command and restart or promote), and how heal picks the recipe.
- [ ] **Step 4:** Run `pytest -q && ruff check src tests`, then `sh -n scripts/entrypoint.sh`. Expected: pass.
- [ ] **Step 5:** Commit: `Generic training CLI and model docs`.

## Part B: Kafka-ready live wiring

### Task 5: Recorder hook

**Files:**
- Modify: `src/sentinalops/data/recorder/service.py` (`Recorder.__init__` gains `on_row: Callable[[str, dict], None] | None = None`, `record: bool = True`; `handle` calls `on_row(kind, row)` after de-duplication and skips `sink.add` when `record` is False)
- Test: `tests/test_recorder_unit.py`

**Interfaces:**
- Produces: `Recorder(cfg, connect=..., clock=..., on_row=None, record=True)`.

- [ ] **Step 1: Write failing tests:**
  - `test_on_row_called_once_per_deduplicated_row`: feed the same depth frame twice and a trade frame once via `handle`. Assert `on_row` saw 2 calls, kinds `depth5` and `aggTrade`, and `stats.duplicates == 1`.
  - `test_record_false_writes_no_rows`: with `record=False`, `handle` then `flush`; no parquet files under `data_dir/stream=*`.
- [ ] **Step 2:** Run `pytest tests/test_recorder_unit.py -v`. Expected: the new tests FAIL.
- [ ] **Step 3:** Implement. An `on_row` exception is logged and counted in a new `stats.hook_errors`, never raised, so the recorder can't die from a consumer bug.
- [ ] **Step 4:** Run `pytest -q && ruff check src tests`. Expected: pass, including the recorder integration tests.
- [ ] **Step 5:** Commit: `Recorder: optional on_row hook and record switch`.

### Task 6: Bus, ledger WAL, deterministic IDs, ledger training table

**Files:**
- Create: `src/sentinalops/live/__init__.py`, `src/sentinalops/live/bus.py`, `src/sentinalops/heal/ledger_table.py`
- Modify: `src/sentinalops/serving/ledger.py` (WAL at init; `PRAGMA synchronous=NORMAL` in `_connect`)
- Test: `tests/test_live_bus.py`, `tests/test_verifier_ledger.py`

**Interfaces:**
- Produces:
  - `bus.TOPICS`; `bus.TOPIC_OF_KIND = {AGG_TRADE: "raw_trades", DEPTH5: "raw_depth"}` and its inverse `KIND_OF_TOPIC`.
  - `class Bus(Protocol)`: `async publish(topic: str, key: str, value: dict) -> None`; `subscribe(topics: list[str], group: str) -> AsyncIterator[tuple[str, dict]]`; `async close() -> None`.
  - `InMemoryBus(maxsize: int = 10_000)`: every subscriber gets every message on its topics, in publish order; `drops: dict[str, int]`; `close()` ends all iterators after draining.
  - `bus.stable_prediction_id(symbol: str, ts_ms: int, model_version: str) -> str`: sha1 hex of `f"{symbol}|{ts_ms}|{model_version}"`, first 32 characters.
  - `ledger_table(ledger: Ledger, since_ms: int, until_ms: int, feature_names: list[str]) -> pd.DataFrame`: columns `symbol, ts_ms, <features>, label, label_status="ok", regime="untagged", source="ledger"`. It keeps only matured rows with `label IS NOT NULL`, one row per `(symbol, ts_ms)` (the earliest `scored_at_ms`), sorted by `ts_ms`, and drops rows without features.
- [ ] **Step 1: Write failing tests:**
  - `test_bus_fanout_and_order`;
  - `test_bus_overflow_drops_oldest` (`maxsize=3`, publish 5, the subscriber gets the last 3, `drops["raw_depth"] == 2`);
  - `test_bus_close_ends_subscription`;
  - `test_stable_prediction_id_deterministic`;
  - `test_ledger_wal_enabled` (`PRAGMA journal_mode` returns `wal`);
  - `test_ledger_table_filters_void_and_dedupes_shadow` (two versions per tick, one void outcome; the result has one row per non-void tick with the earlier-scored features).
- [ ] **Step 2:** Run `pytest tests/test_live_bus.py tests/test_verifier_ledger.py -v`. Expected: the new tests FAIL.
- [ ] **Step 3:** Implement. `ledger_table` does a single bounded SQL query on the existing join (`_JOINED`), then `json.loads` the features.
- [ ] **Step 4:** Run `pytest -q && ruff check src tests`. Expected: pass.
- [ ] **Step 5:** Commit: `Live: bus interface, in-memory bus, ledger WAL and ledger training table`.

### Task 7: Pipeline workers with clock injection

**Files:**
- Create: `src/sentinalops/live/pipeline.py`
- Test: `tests/test_live_pipeline.py`

**Interfaces:**
- Consumes: Task 6 bus and `stable_prediction_id`; the existing `StreamProcessor(cfg, model, shadow=...)`, `OutcomeVerifier(cfg)` and `Ledger`.
- Produces:
  - `features_worker(bus, processor) -> Coroutine`: consumes raw topics and publishes `features` and `predictions`. Every prediction is re-issued with `prediction_id=stable_prediction_id(...)` via `model_copy`, and the `predictions` message value is `{**pred.model_dump(mode="json"), "features": <that tick's feature dict>}`. This is self-contained, so there is no cross-topic join (Kafka doesn't order across topics).
  - `outcomes_worker(bus, verifier) -> Coroutine`: consumes `raw_depth`, publishes `outcomes`.
  - `ledger_writer(bus, ledger) -> Coroutine`: consumes only `predictions` (features taken from the message) and `outcomes`. It keeps no cache.
  - `InMemoryBus.drain() -> Coroutine` (added to Task 6): waits until every subscriber queue is empty, for deterministic tests.
  - `clock_loop(bus, processor, verifier, now_ms: Callable[[], int], interval_s: float = 1.0) -> Coroutine`: calls `advance(now_ms())` on both and publishes their outputs.
  - `PipelineStats` (dataclass): events per topic, ticks, suspended, predictions, outcomes, scoring errors, drops.
- [ ] **Step 1: Write failing tests:**
  - `test_replay_matches_offline`: `make_events(H0, 1800, stress=((900, 40),), holes=((1300, 10),))`. Drive time deterministically: for each second, publish that second's events in `recv_time_ms` order and `await bus.drain()`. Only then call the clock step (`clock_step(processor, verifier, t)`, the body of `clock_loop`, exposed for tests). No concurrent `clock_loop` in this test. Assert the ledger features for every tick equal `compute_ticks` to 1e-9, every outcome label equals `build_labels`, and some outcomes are void with `gap`.
  - `test_silent_feed_suspends_ticks`: with no events, advancing the fake clock 10 s produces 0 predictions and counts suspended ticks.
  - `test_no_model_then_model_appears`: start with a missing champion path, and features still flow. Write a model to the path, and predictions start with that model's version.
  - `test_restart_does_not_duplicate_predictions`: replaying the same 120 s twice into one ledger gives the same prediction row count.
- [ ] **Step 2:** Run `pytest tests/test_live_pipeline.py -v`. Expected: FAIL.
- [ ] **Step 3:** Implement. Ledger writes run in `asyncio.to_thread`, so SQLite never blocks the loop.
- [ ] **Step 4:** Run `pytest -q && ruff check src tests`. Expected: pass.
- [ ] **Step 5:** Commit: `Live: feature, outcome, ledger and clock workers`.

### Task 8: Binance feed and `python -m sentinalops.live` (pipeline role)

**Files:**
- Create: `src/sentinalops/live/feed.py`, `src/sentinalops/live/__main__.py`, `src/sentinalops/live/health.py`
- Test: `tests/test_live_feed.py`

**Interfaces:**
- Consumes: Task 5 `Recorder(on_row=..., record=...)`; Tasks 6 and 7.
- Produces:
  - `BinanceFeed(bus, rec_cfg: RecorderConfig, record: bool, connect=websockets.connect, maxsize: int = 10_000)`: `run()` and `request_stop()`. `on_row` puts rows on a bounded internal queue (drop-oldest, counted in `stats.feed_drops`), and a pump task awaits `bus.publish(TOPIC_OF_KIND[kind], symbol, row)`.
  - `live/__main__.py` imports only the standard library at the top level. It parses `--role`, sets the three thread variables for `--role heal`, and only then imports the role's modules.
  - CLI flags: `--role pipeline|heal|feed|features|outcomes|ledger` (default `pipeline`), `--bus memory|kafka` (default `memory`; `memory` only allows `pipeline` and `heal`), `--record`, `--data-dir` (default `data/live`), `--health-interval-s 60`, `--heal-interval-min 5`, `--metrics-port` (bound to 127.0.0.1, off by default).
  - `health.line(stats, registry_state) -> dict`: one JSON log line `live health` with the stats, served model version, and the last heal event read from `registry.json`.
- [ ] **Step 1: Write failing tests:**
  - `test_feed_publishes_raw_topics_via_fake_exchange` (reuse `tests/recorder_helpers.FakeExchange`): 3 depth frames and 2 trades arrive on `raw_depth` and `raw_trades`, with one duplicate dropped.
  - `test_cli_rejects_split_role_on_memory_bus`.
- [ ] **Step 2:** Run `pytest tests/test_live_feed.py -v`. Expected: FAIL.
- [ ] **Step 3:** Implement. SIGTERM and SIGINT trigger a clean stop that closes the bus. The model path comes from `Settings.from_env().model_path`, with the shadow next to it.
- [ ] **Step 4:** Run `pytest -q && ruff check src tests`. Expected: pass.
- [ ] **Step 5: Live smoke test.** Run `python -m sentinalops.live --data-dir <scratch>/live` for 3 minutes on this PC with a champion trained by Task 4. Expected:
  - health lines show depth at about 10/s;
  - predictions start after about 60 s of warm-up;
  - `ledger.sqlite` has rows, and outcomes appear about 30 s behind predictions.
- [ ] **Step 6:** Commit: `Live: Binance feed, CLI and health line`.

### Task 9: Heal process

**Files:**
- Create: `src/sentinalops/live/heal_service.py`
- Modify: `src/sentinalops/live/__main__.py` (`--role heal`)
- Test: `tests/test_live_heal.py`

**Interfaces:**
- Consumes: Task 6 `ledger_table`, `HealController`, `ModelRegistry(root=model_path.parent)`.
- Produces: `HealService(cfg, ledger, registry, interval_min, now_ms)`.
  - `bootstrap_if_needed(now_ms)`: if the registry has no champion and `champion.joblib` exists, register and promote it with `holdout_ap = sentinal_holdout_ap_` (NaN if absent, logged as a warning that heal will not trigger).
  - `tick(now_ms) -> TickResult` calls `controller.tick` with `load_table = lambda now: ledger_table(ledger, now - (window_h * 3600_000 + 7 * 86_400_000), now, names)`.
  - `run()` loops every `interval_min` and logs `heal health`.
  - The CLI sets the three thread variables before importing scikit-learn.
- [ ] **Step 1: Write failing tests:**
  - `test_heal_idles_on_empty_ledger` (the action is `idle`, the reason is `no_champion` or `too_few_events`);
  - `test_bootstrap_registers_existing_champion_with_holdout_ap`;
  - `test_heal_fires_from_ledger_data`: fill the ledger from `heal_sim.market([("calm", 90), ("volatile", 150)])`, scoring the way `heal_sim.run` does, and drive `HealService.tick` every 5 minutes. A `retrained_*` action happens after the shift.
- [ ] **Step 2:** Run `pytest tests/test_live_heal.py -v`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `pytest -q && ruff check src tests`. Expected: pass.
- [ ] **Step 5:** Commit: `Live: heal service trained from the ledger`.

### Task 10: KafkaBus

**Files:**
- Create: `src/sentinalops/live/kafka_bus.py`
- Modify: `pyproject.toml` (`stream` extra: `aiokafka` is already there; nothing new)
- Test: `tests/test_live_bus.py`. Only the shared contract (fan-out, order, close) is parametrized over `[InMemoryBus, KafkaBus]`, with KafkaBus skipped unless `KAFKA_BROKER` is set. `test_bus_overflow_drops_oldest` stays memory-only. `KafkaBus.subscribe` returns only after partition assignment completes, so a test's publish can't race the consumer.

**Interfaces:**
- Produces: `KafkaBus(broker: str, client_id: str = "sentinalops")`.
  - JSON values and UTF-8 keys; `acks="all"`; the topics are created if missing (1 partition, retention by size of 5 GB for raw topics).
  - `subscribe(topics, group)` uses the consumer group `group` and `auto_offset_reset="latest"`.
  - The CLI's `--bus kafka` uses `Settings.kafka_broker`.
- [ ] **Step 1: Write tests:** the existing bus contract tests, parametrized, plus `test_kafka_bus_import_error_message` (a clear error if aiokafka is missing).
- [ ] **Step 2:** Run `pytest tests/test_live_bus.py -v`. Expected: the in-memory tests pass, the Kafka tests are skipped, and the import-error test FAILS.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `pytest -q && ruff check src tests`. Expected: pass (Kafka tests run on Oracle once Redpanda is up).
- [ ] **Step 5:** Commit: `Live: Kafka bus for Redpanda`.

### Task 11: Oracle deployment kit

**Files:**
- Create: `deploy/oracle/setup.sh`, `deploy/oracle/sentinalops-live.service`, `deploy/oracle/sentinalops-heal.service`, `deploy/oracle/preflight.py`, `deploy/oracle/monitor.py`, `deploy/oracle/monitor.sh`, `deploy/oracle/redpanda-compose.yml`, `docs/live.md`
- Test: `tests/test_deploy_files.py`

**Interfaces:**
- `preflight.py`: a 10 s WebSocket connection to Binance. It exits 0 only if at least 1 depth and 1 trade message arrived.
- `setup.sh` (idempotent): clones or updates the repo into `/opt/sentinalops` and creates the venv, then runs `preflight.py` **with the venv Python**, aborting before any system change if it fails. It then trains a champion if none exists, installs and enables both units (`Restart=always`; the heal unit has `Nice=10` and the three thread variables), optionally installs Docker and Redpanda (`--redpanda`, `--memory 3G`), and prints the reclamation protection status.
- `monitor.py`: the same sections as the IITM monitor, plus live (predictions/s, latest probability, alerts in the last hour, outcomes, ledger size) and heal (champion, last action, live AP).
- [ ] **Step 1: Write failing tests:** `test_unit_files_have_restart_and_thread_caps`; `test_setup_script_passes_shellcheck_syntax` (`bash -n`); `test_preflight_parses_args`.
- [ ] **Step 2:** Run `pytest tests/test_deploy_files.py -v`. Expected: FAIL.
- [ ] **Step 3:** Implement. `docs/live.md` covers the topology, the roles, the commands, and switching to Kafka.
- [ ] **Step 4:** Run `pytest -q && ruff check src tests deploy`. Expected: pass.
- [ ] **Step 5:** Commit: `Deploy kit for Oracle Cloud A1`.

### Task 12: Whole-branch verification

- [ ] Run `pytest -q`, `ruff check .` and a 10-minute live run locally (pipeline and heal roles, in-memory bus). Expected: no errors, `late` = 0, outcomes about 30 s behind, and the heal health line shows `too_few_events` or idle.
- [ ] Update `README.md` status and `docs/ROADMAP.md` checkboxes (15–19 in part, 26 not yet, 42 scheduler done).

