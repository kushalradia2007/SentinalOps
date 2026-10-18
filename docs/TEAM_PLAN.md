# Team Plan

Chunk numbers match `docs/ROADMAP.md`. Chunks 1–9 are done. Day-to-day work is tracked in [`TASKS.md`](TASKS.md): every remaining chunk is split into small tasks (about 1–4 hours each), with an owner, size, dependencies and a "done when" check.

## Roles

| Person | Level | Lane | Chunks |
| --- | --- | --- | --- |
| **Kushal** | Lead | Self-healing core, serving, integration, final demo | 28, 30, 31, 33, 34, 38, 39, 42, 43, 44, 45, 47 |
| **Vaibhav** | Senior | Data and ML: datasets, extractors, models, adversarial library | 7, 9, 10, 12, 14, 24, 25, 26, 27, 35, 46, 49 |
| **Jaswanth** | Senior | Streaming and MLOps: Redpanda, Faust, Feast, MLflow registry, Prometheus | 11, 15, 19, 20, 22, 23, 29, 32, 37 |
| **Yogya** | Junior | Data loading, pollers, dashboards, docs (mentored) | 8, 13, 16, 36, 41, 48 |
| **Tanuj** | Junior | Webhooks, replay, alert rules, demo video (mentored) | 17, 18, 21, 40, 50 |

## Full assignment table

| # | Chunk | Phase | Owner | Mentor | Status |
| --- | --- | --- | --- | --- | --- |
| 1 | Repo skeleton, packaging, docker-compose, domain config | Foundation | **Kushal** | — | done |
| 2 | Feature extractors (AST, entropy, base64, metadata) | Foundation | **Kushal** | — | done |
| 3 | Zero-day simulator, synthetic data, champion model, thesis test | Foundation | **Kushal** | — | done |
| 4 | Config loader, structured logging, shared pydantic schemas | Foundation | **Kushal** | — | done |
| 5 | CI: ruff, pytest, GitHub Actions | Foundation | **Kushal** | — | done |
| 6 | Full docker compose stack comes up healthy | Foundation | **Kushal** | — | done |
| 7 | MalwareBench download and ingest | Data | **Vaibhav** | — | done |
| 8 | QUT-DV25 loader | Data | **Yogya** | Vaibhav | done |
| 9 | Safe package fetcher (PyPI sdist/wheel, static unpack only, never executes) | Data | **Vaibhav** | — | done |
| 10 | npm tarball fetcher plus JS feature extractor (postinstall, eval) | Data | **Vaibhav** | — | todo |
| 11 | Metadata enrichment: maintainer age, email-domain change, dependency depth | Data | **Jaswanth** | — | todo |
| 12 | Real-feature pipeline to Parquet offline store | Data | **Vaibhav** | — | todo |
| 13 | Dataset QA: dedup, leakage checks, temporal split | Data | **Yogya** | Vaibhav | todo |
| 14 | Install-time behavior features (syscalls, network, file writes) | Data | **Vaibhav** | — | todo |
| 15 | Redpanda topics and event schemas | Streaming | **Jaswanth** | — | todo |
| 16 | PyPI live poller to raw topic | Streaming | **Yogya** | Jaswanth | todo |
| 17 | npm registry feed poller | Streaming | **Tanuj** | Jaswanth | todo |
| 18 | GitHub webhook receiver | Streaming | **Tanuj** | Jaswanth | todo |
| 19 | Faust processor: fetch, extract, publish features | Streaming | **Jaswanth** | — | todo |
| 20 | Dead-letter queue, retries, idempotency, backpressure | Streaming | **Jaswanth** | — | todo |
| 21 | Replay simulator with speed control | Streaming | **Tanuj** | Jaswanth | todo |
| 22 | Feast repo: entities, feature views, offline store | Feature store | **Jaswanth** | — | todo |
| 23 | Redis online materialization | Feature store | **Jaswanth** | — | todo |
| 24 | Point-in-time-correct training sets | Feature store | **Vaibhav** | — | todo |
| 25 | Online/offline consistency test | Feature store | **Vaibhav** | — | todo |
| 26 | Real-data champion (GBM) plus metrics suite (PR-AUC, recall@FPR) | Models | **Vaibhav** | — | todo |
| 27 | Classic detectors: IsolationForest, PyOD (ECOD/COPOD) | Models | **Vaibhav** | — | todo |
| 28 | PyTorch autoencoder | Models | **Kushal** | — | todo |
| 29 | Streaming learners (River/PySAD) | Models | **Jaswanth** | — | todo |
| 30 | Comparison harness and MLflow experiments | Models | **Kushal** | — | todo |
| 31 | Calibration and cost-sensitive threshold selection | Models | **Kushal** | — | todo |
| 32 | MLflow registry: champion/challenger aliases | Models | **Jaswanth** | — | todo |
| 33 | /score with feature-store lookup and alias hot-reload | Serving | **Kushal** | — | todo |
| 34 | /labels endpoint, label store, prediction-label join | Serving | **Kushal** | — | todo |
| 35 | Block policy, allowlist, SHAP explanations | Serving | **Vaibhav** | — | todo |
| 36 | Load testing and p99 latency targets | Serving | **Yogya** | Kushal | todo |
| 37 | Prometheus metrics: scores, latency, throughput | Monitoring | **Jaswanth** | — | todo |
| 38 | Evidently drift jobs (feature and prediction drift) | Monitoring | **Kushal** | — | todo |
| 39 | Delayed-label rolling recall/precision | Monitoring | **Kushal** | — | todo |
| 40 | Alert rules (model_drift_high) | Monitoring | **Tanuj** | Kushal | todo |
| 41 | Grafana dashboards and "Model Healed" annotations | Monitoring | **Yogya** | Kushal | todo |
| 42 | Prefect flow with drift plus label trigger | Self-healing | **Kushal** | — | todo |
| 43 | Retrain DAG: 48h window plus replay buffer against forgetting | Self-healing | **Kushal** | — | todo |
| 44 | Challenger vs champion gates (gain on new attacks, no regression on old) | Self-healing | **Kushal** | — | todo |
| 45 | Auto-promotion and rollback safety | Self-healing | **Kushal** | — | todo |
| 46 | Harmless adversarial variant library (dependency confusion, compromised maintainer, staged payload) | Self-healing | **Vaibhav** | — | todo |
| 47 | End-to-end 5-step zero-day demo with GitHub test repo | Demo and polish | **Kushal** | — | todo |
| 48 | Docs: architecture diagram, runbook, ADRs | Demo and polish | **Yogya** | Kushal | todo |
| 49 | Benchmarks and results writeup (heal time, before/after) | Demo and polish | **Vaibhav** | — | todo |
| 50 | Demo video, resume bullets, v1.0 release | Demo and polish | **Tanuj** | Kushal | todo |

## Mentoring (juniors never work alone on a chunk)

| Chunk | Owner | Mentor / reviewer |
| --- | --- | --- |
| 8 QUT-DV25 loader | Yogya | Vaibhav |
| 13 Dataset QA | Yogya | Vaibhav |
| 16 PyPI poller | Yogya | Jaswanth |
| 36 Load testing | Yogya | Kushal |
| 41 Grafana dashboards | Yogya | Kushal |
| 48 Docs | Yogya | Kushal |
| 17 npm poller | Tanuj | Jaswanth |
| 18 GitHub webhook | Tanuj | Jaswanth |
| 21 Replay simulator | Tanuj | Jaswanth |
| 40 Alert rules | Tanuj | Kushal |
| 50 Demo video and resume bullets | Tanuj | Kushal |

## Junior onboarding (Yogya and Tanuj, before their first chunk)

1. Install Python 3.11+, git, VS Code. Learn basic Python (functions, dicts, files) and git (clone, branch, commit, pull request).
2. Clone the repo, run `pip install -e ".[dev]"`, then `pytest` and `ruff check .`. Read `docs/DEVELOPMENT.md`.
3. Read `src/sentinalops/features/extract.py` and `tests/test_thesis.py`. Write one new test of your own.
4. Open a first pull request: a small test or doc fix. A senior reviews and merges it.
5. Pair with your mentor for 30 minutes before starting your first chunk.

## Working rules

- One branch per chunk: `chunk-NN-short-name`. Open a pull request into `main`; never push to `main` directly.
- Every pull request needs CI green (ruff and pytest) and one review. Juniors' pull requests are reviewed by their mentor.
- A chunk is done when it has code, at least one test and a line in `docs/ROADMAP.md` ticked.
- Stuck for more than 30 minutes? Ask. Juniors should not struggle alone.
- Never run downloaded package code. Fetchers only unpack and read files.

## Waves (what can run in parallel)

| Wave | Chunks | Notes |
| --- | --- | --- |
| 1: data and stream foundations | 7, 8, 9, 11, 12, 13, 15, 16, 18, 33, 34 | Juniors onboard first, then 8 and 18 |
| 2: pipelines and first real model | 10, 14, 17, 19, 20, 21, 22, 23, 24, 26, 27 | Needs 12 for 24, 26, 27 |
| 3: models, monitoring, serving | 25, 28, 29, 30, 31, 32, 35, 36, 37, 38, 39, 40, 41 | Needs the champion from 26 |
| 4: self-healing and demo | 42, 43, 44, 45, 46, 47, 48, 49, 50 | Needs drift (38) and labels (34) |
