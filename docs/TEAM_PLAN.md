# Team Plan

Chunk numbers match `docs/ROADMAP.md`. The domain is **Liquidity Intelligence** (BTCUSDT on Binance public streams). Chunks 1, 4, 5 and 6 are done; chunks 2, 3, 7, 8 and 9 are reopened for the new domain. Day-to-day work is tracked in [`TASKS.md`](TASKS.md): every open chunk is split into small tasks (about 1–4 hours each), with an owner, size, dependencies and a "done when" check.

Nobody's lane changed with the domain. Each chunk number has the same owner as before.

## Roles

| Person | Level | Lane | Chunks |
| --- | --- | --- | --- |
| **Kushal** | Lead | Domain switch, self-healing core, serving, verifier, final demo | 2, 3, 28, 30, 31, 33, 34, 38, 39, 42, 43, 44, 45, 47 |
| **Vaibhav** | Senior | Data and ML: recorder, labels, features, models, scenario library | 7, 9, 10, 12, 14, 24, 25, 26, 27, 35, 46, 49 |
| **Jaswanth** | Senior | Streaming and MLOps: Redpanda, processor, Feast, MLflow registry, Prometheus | 11, 15, 19, 20, 22, 23, 29, 32, 37 |
| **Yogya** | Junior | Archive loader, dataset QA, trade producer, dashboards, docs (mentored) | 8, 13, 16, 36, 41, 48 |
| **Tanuj** | Junior | Depth producer, feed health, replay, alert rules, demo video (mentored) | 17, 18, 21, 40, 50 |

## Full assignment table

| # | Chunk | Phase | Owner | Mentor | Status |
| --- | --- | --- | --- | --- | --- |
| 1 | Repo skeleton, packaging, docker-compose, domain config | Foundation | **Kushal** | — | done |
| 2 | Domain switch: config schema, event schemas, default domain, remove legacy code | Foundation | **Kushal** | — | todo |
| 3 | Regime-shift simulator, synthetic data, champion model, thesis test | Foundation | **Kushal** | — | todo |
| 4 | Config loader, structured logging, shared pydantic schemas | Foundation | **Kushal** | — | done |
| 5 | CI: ruff, pytest, GitHub Actions | Foundation | **Kushal** | — | done |
| 6 | Full docker compose stack comes up healthy | Foundation | **Kushal** | — | done |
| 7 | Binance stream recorder (`aggTrade` + `depth5@100ms` to Parquet) | Data | **Vaibhav** | — | todo |
| 8 | Historical trade archive loader | Data | **Yogya** | Vaibhav | todo |
| 9 | Label definition: stress events, onset rule, frozen thresholds, base-rate report | Data | **Vaibhav** | — | todo |
| 10 | Microstructure feature library (spread, depth, imbalance, intensity, volatility) | Data | **Vaibhav** | — | todo |
| 11 | Feed data quality: gaps, stale data, ordering, clock skew | Data | **Jaswanth** | — | todo |
| 12 | Offline feature and label pipeline to Parquet | Data | **Vaibhav** | — | todo |
| 13 | Dataset QA: base rates, embargoed temporal split, leakage checks | Data | **Yogya** | Vaibhav | todo |
| 14 | Market regime tagging (calm, volatile, thin, directional, recovery) | Data | **Vaibhav** | — | todo |
| 15 | Redpanda topics and event schemas | Streaming | **Jaswanth** | — | todo |
| 16 | Live trade producer to raw topic | Streaming | **Yogya** | Jaswanth | todo |
| 17 | Live depth producer to raw topic | Streaming | **Tanuj** | Jaswanth | todo |
| 18 | Feed health: reconnect, stale-data detection, prediction suspension | Streaming | **Tanuj** | Jaswanth | todo |
| 19 | Stream processor: rolling features, publish, score | Streaming | **Jaswanth** | — | todo |
| 20 | Dead-letter queue, retries, idempotency, backpressure | Streaming | **Jaswanth** | — | todo |
| 21 | Replay simulator with speed control | Streaming | **Tanuj** | Jaswanth | todo |
| 22 | Feast repo: entities, feature views, offline store | Feature store | **Jaswanth** | — | todo |
| 23 | Redis online materialization | Feature store | **Jaswanth** | — | todo |
| 24 | Point-in-time-correct training sets | Feature store | **Vaibhav** | — | todo |
| 25 | Online/offline consistency test | Feature store | **Vaibhav** | — | todo |
| 26 | Stress-forecast champion (GBM), persistence baseline, metrics suite | Models | **Vaibhav** | — | todo |
| 27 | Anomaly detectors: rolling z-score, IsolationForest, PyOD (ECOD/COPOD) | Models | **Vaibhav** | — | todo |
| 28 | PyTorch autoencoder | Models | **Kushal** | — | todo |
| 29 | Streaming learners (River Half-Space Trees, ADWIN) | Models | **Jaswanth** | — | todo |
| 30 | Comparison harness and MLflow experiments | Models | **Kushal** | — | todo |
| 31 | Calibration and cost-sensitive alert threshold | Models | **Kushal** | — | todo |
| 32 | MLflow registry: champion/challenger aliases | Models | **Jaswanth** | — | todo |
| 33 | `/score` with online feature lookup and alias hot-reload | Serving | **Kushal** | — | todo |
| 34 | Prediction ledger and outcome verifier | Serving | **Kushal** | — | todo |
| 35 | Alert policy, severity tiers, SHAP explanations | Serving | **Vaibhav** | — | todo |
| 36 | Load testing and p99 latency targets | Serving | **Yogya** | Kushal | todo |
| 37 | Prometheus metrics: scores, latency, throughput, feed lag | Monitoring | **Jaswanth** | — | todo |
| 38 | Evidently drift jobs (feature and prediction drift) | Monitoring | **Kushal** | — | todo |
| 39 | Matured-label rolling precision, recall and Brier score | Monitoring | **Kushal** | — | todo |
| 40 | Alert rules (drift, reliability drop, stale feed) | Monitoring | **Tanuj** | Kushal | todo |
| 41 | Grafana dashboards and "Model Healed" annotations | Monitoring | **Yogya** | Kushal | todo |
| 42 | Prefect flow triggered by performance drop plus label count | Self-healing | **Kushal** | — | todo |
| 43 | Retrain DAG: recent window plus replay buffer across regimes | Self-healing | **Kushal** | — | todo |
| 44 | Challenger vs champion gates (gain on current regime, no regression on older ones) | Self-healing | **Kushal** | — | todo |
| 45 | Auto-promotion and rollback safety | Self-healing | **Kushal** | — | todo |
| 46 | Recorded regime-shift scenario library | Self-healing | **Vaibhav** | — | todo |
| 47 | End-to-end incident-timeline demo on a replayed regime shift | Demo and polish | **Kushal** | — | todo |
| 48 | Docs: architecture diagram, runbook, ADRs | Demo and polish | **Yogya** | Kushal | todo |
| 49 | Benchmarks and results writeup (four approaches, heal time, before/after) | Demo and polish | **Vaibhav** | — | todo |
| 50 | Demo video, resume bullets, v1.0 release | Demo and polish | **Tanuj** | Kushal | todo |

## Mentoring (juniors never work alone on a chunk)

| Chunk | Owner | Mentor / reviewer |
| --- | --- | --- |
| 8 Historical trade archive loader | Yogya | Vaibhav |
| 13 Dataset QA | Yogya | Vaibhav |
| 16 Live trade producer | Yogya | Jaswanth |
| 36 Load testing | Yogya | Kushal |
| 41 Grafana dashboards | Yogya | Kushal |
| 48 Docs | Yogya | Kushal |
| 17 Live depth producer | Tanuj | Jaswanth |
| 18 Feed health | Tanuj | Jaswanth |
| 21 Replay simulator | Tanuj | Jaswanth |
| 40 Alert rules | Tanuj | Kushal |
| 50 Demo video and resume bullets | Tanuj | Kushal |

## Junior onboarding (Yogya and Tanuj, before their first chunk)

1. Install Python 3.11+, git, VS Code. Learn basic Python (functions, dicts, files) and git (clone, branch, commit, pull request).
2. Clone the repo, run `pip install -e ".[dev]"`, then `pytest` and `ruff check .`. Read `docs/DEVELOPMENT.md`.
3. Read "Domain primer" below, then open the Binance BTCUSDT order book in a browser and watch the best bid, best ask and depth change for five minutes.
4. Open a first pull request: a small test or doc fix. A senior reviews and merges it.
5. Pair with your mentor for 30 minutes before starting your first chunk.

## Domain primer (ten lines everyone should know)

- **Bid / ask:** the highest price a buyer offers and the lowest price a seller asks.
- **Spread:** ask minus bid. A wide spread means trading is expensive.
- **Depth:** how much quantity is resting near the best prices. We use the top five levels on each side.
- **Imbalance:** whether bids or asks dominate, from -1 (all asks) to +1 (all bids).
- **Liquidity stress:** depth falls well below its recent normal, or the spread widens well above it.
- **Regime:** a stretch of time where the market behaves in one style (calm, volatile, thin).
- **Horizon:** how far ahead we forecast (30 seconds).
- **Outcome verifier:** the job that waits 30 seconds, then checks whether the forecast came true.
- **Leakage:** using information from after the prediction time. It makes results look good and is always a bug.
- **Persistence baseline:** the lazy forecast "whatever is true now stays true". Our model has to beat it.

## Working rules

- One branch per task: `chunk-NN-task-M`. Open a pull request into `main`; never push to `main` directly.
- Every pull request needs CI green (ruff and pytest) and one review. Juniors' pull requests are reviewed by their mentor.
- A chunk is done when it has code, at least one test and a line in `docs/ROADMAP.md` ticked.
- Stuck for more than 30 minutes? Ask. Juniors should not struggle alone.
- Public market data only. No API keys, no account endpoints, no order placement, ever.
- Tests never touch the network. Use saved message fixtures; live checks are opt-in with `RUN_NETWORK_TESTS=1`.
- Every feature function takes a timestamp and must ignore events after it.

## Waves (what can run in parallel)

| Wave | Chunks | Notes |
| --- | --- | --- |
| 0: start recording | 2, 7 | The recorder runs continuously from here on; every later chunk needs its data |
| 1: data and stream foundations | 3, 8, 9, 10, 11, 15, 16, 17, 34 | Juniors onboard first, then 8 and 17. Chunk 9 decides the label before any model work |
| 2: pipelines and first real model | 12, 13, 14, 18, 19, 20, 21, 22, 23, 24, 26, 27 | Needs 12 for 24, 26, 27 |
| 3: models, monitoring, serving | 25, 28, 29, 30, 31, 32, 33, 35, 36, 37, 38, 39, 40, 41 | Needs the champion from 26 and the verifier from 34 |
| 4: self-healing and demo | 42, 43, 44, 45, 46, 47, 48, 49, 50 | Needs matured-label metrics (39) and a recorded regime shift (46) |
