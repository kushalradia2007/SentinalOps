# SentinalOps Roadmap: 50 chunks

Every open chunk is split into small tasks in [`TASKS.md`](TASKS.md). Owners are in [`TEAM_PLAN.md`](TEAM_PLAN.md).

**Domain (locked): Liquidity Intelligence.** SentinalOps watches the BTCUSDT order book and trades on Binance's public market-data streams. It detects abnormal liquidity conditions, forecasts liquidity stress 30 seconds ahead, verifies every forecast against the future stream, and heals the model when the market regime changes.

This is an early-warning and monitoring system. It is not a trading bot: it places no orders, needs no API keys and makes no price predictions.

## What changed from the supply-chain plan

- Chunks 1, 4, 5 and 6 stay done. The platform code they built is domain-neutral.
- Chunks 2, 3, 7, 8 and 9 were finished for the old domain and are **reopened** with new content. The old code (`features/extract.py`, `simulator/zero_day.py`, `data/malwarebench.py`, `data/qut_dv25.py`, `data/fetcher.py`) is removed in task 2.6 once its replacements exist.
- Chunk numbers and owners are unchanged, so each person keeps the same lane.

## Design rules that every chunk follows

1. **No future data in features.** A prediction at time `t` uses only events with timestamp at or before `t`. The verifier labels it using only events after `t`.
2. **Depth depletion is the primary label.** BTCUSDT usually trades at a one-tick spread, so spread expansion is a secondary label. Chunk 9 measures both base rates before anything is trained.
3. **Label the onset of stress.** A market that is already stressed is not a forecast. Every model is also compared with a persistence baseline ("stressed now, so stressed in 30 seconds").
4. **Predict once per second.** Horizons overlap, so train and test splits are chronological with an embargo gap of at least the horizon.
5. **Drift alone does not retrain.** Retraining needs a measured drop in matured-label performance; feature drift is supporting evidence.
6. **Record first.** There is no public archive of spot order-book depth, so the recorder (chunk 7) runs from day one.

## Phase A: Foundation
- [x] 1. Repo skeleton, packaging, docker-compose, domain config **[Kushal]**
- [x] 2. Domain switch: config schema, event schemas, default domain, remove legacy code **[Kushal]**
- [x] 3. Regime-shift simulator, synthetic data, champion model, thesis test **[Kushal]**
- [x] 4. Config loader, structured logging, shared pydantic schemas **[Kushal]**
- [x] 5. CI: ruff, pytest, GitHub Actions **[Kushal]**
- [x] 6. Full docker compose stack comes up healthy **[Kushal]**

## Phase B: Data
- [x] 7. Binance stream recorder (`aggTrade` + `depth5@100ms` to Parquet) **[Vaibhav]**
- [ ] 8. Historical trade archive loader **[Yogya, mentor Vaibhav]**
- [ ] 9. Label definition: stress events, onset rule, frozen thresholds, base-rate report **[Vaibhav]**
- [x] 10. Microstructure feature library (spread, depth, imbalance, intensity, volatility) **[Vaibhav]**
- [ ] 11. Feed data quality: gaps, stale data, ordering, clock skew **[Jaswanth]**
- [x] 12. Offline feature and label pipeline to Parquet **[Vaibhav]**
- [ ] 13. Dataset QA: base rates, embargoed temporal split, leakage checks **[Yogya, mentor Vaibhav]**
- [ ] 14. Market regime tagging (calm, volatile, thin, directional, recovery) **[Vaibhav]**

## Phase C: Streaming
- [ ] 15. Redpanda topics and event schemas **[Jaswanth]**
- [ ] 16. Live trade producer to raw topic **[Yogya, mentor Jaswanth]**
- [ ] 17. Live depth producer to raw topic **[Tanuj, mentor Jaswanth]**
- [ ] 18. Feed health: reconnect, stale-data detection, prediction suspension **[Tanuj, mentor Jaswanth]**
- [x] 19. Stream processor: rolling features, publish, score **[Jaswanth]**
- [ ] 20. Dead-letter queue, retries, idempotency, backpressure **[Jaswanth]**
- [ ] 21. Replay simulator with speed control **[Tanuj, mentor Jaswanth]**

## Phase D: Feature store
- [ ] 22. Feast repo: entities, feature views, offline store **[Jaswanth]**
- [ ] 23. Redis online materialization **[Jaswanth]**
- [ ] 24. Point-in-time-correct training sets **[Vaibhav]**
- [ ] 25. Online/offline consistency test **[Vaibhav]**

## Phase E: Models
- [ ] 26. Stress-forecast champion (GBM), persistence baseline, metrics suite **[Vaibhav]**
- [ ] 27. Anomaly detectors: rolling z-score, IsolationForest, PyOD (ECOD/COPOD) **[Vaibhav]**
- [ ] 28. PyTorch autoencoder **[Kushal]**
- [ ] 29. Streaming learners (River Half-Space Trees, ADWIN) **[Jaswanth]**
- [ ] 30. Comparison harness and MLflow experiments **[Kushal]**
- [ ] 31. Calibration and cost-sensitive alert threshold **[Kushal]**
- [ ] 32. MLflow registry: champion/challenger aliases **[Jaswanth]**

## Phase F: Serving
- [ ] 33. `/score` with online feature lookup and alias hot-reload **[Kushal]**
- [x] 34. Prediction ledger and outcome verifier **[Kushal]**
- [ ] 35. Alert policy, severity tiers, SHAP explanations **[Vaibhav]**
- [ ] 36. Load testing and p99 latency targets **[Yogya, mentor Kushal]**

## Phase G: Monitoring
- [ ] 37. Prometheus metrics: scores, latency, throughput, feed lag **[Jaswanth]**
- [ ] 38. Evidently drift jobs (feature and prediction drift) **[Kushal]**
- [ ] 39. Matured-label rolling precision, recall and Brier score **[Kushal]**
- [ ] 40. Alert rules (drift, reliability drop, stale feed) **[Tanuj, mentor Kushal]**
- [ ] 41. Grafana dashboards and "Model Healed" annotations **[Yogya, mentor Kushal]**

## Phase H: Self-healing
- [ ] 42. Prefect flow triggered by performance drop plus label count **[Kushal]**
- [ ] 43. Retrain DAG: recent window plus replay buffer across regimes **[Kushal]**
- [ ] 44. Challenger vs champion gates (gain on current regime, no regression on older ones) **[Kushal]**
- [ ] 45. Auto-promotion and rollback safety **[Kushal]**
- [ ] 46. Recorded regime-shift scenario library **[Vaibhav]**

## Phase I: Demo and polish
- [ ] 47. End-to-end incident-timeline demo on a replayed regime shift **[Kushal]**
- [ ] 48. Docs: architecture diagram, runbook, ADRs **[Yogya, mentor Kushal]**
- [ ] 49. Benchmarks and results writeup (four approaches, heal time, before/after) **[Vaibhav]**
- [ ] 50. Demo video, resume bullets, v1.0 release **[Tanuj, mentor Kushal]**

## Progress notes (2026-11-27)

- **Done beyond the checkboxes:** the model slot (plug-in recipes; the heal loop retrains the champion's own recipe), live wiring (`src/sentinalops/live/`: Binance feed -> bus -> processor, verifier, ledger; heal service as its own process), a Kafka/Redpanda bus behind `--bus kafka`, and an Oracle Cloud deploy kit.
- **Partly done:** 9 (code done; thresholds need recorded data), 15 (topics and bus; Kafka contract tests need a broker), 16-18 (one feed process instead of two producers; reconnect, stale detection and suspension done), 39 and 42-45 (logic and schedule done; MLflow, Prometheus counters and Prefect not used).

## Out of scope for v1.0

Full-depth order-book reconstruction, more than one symbol or exchange, sequence models beyond the autoencoder, price prediction, and any order placement.
