# Team Plan

Chunk numbers match `docs/ROADMAP.md`. Chunks 1–6 (foundation) are done.

## Roles

| Person | Level | Lane | Chunks |
| --- | --- | --- | --- |
| **Kushal** | Lead | Self-healing core, serving, integration, final demo | 28, 30, 31, 33, 34, 38, 39, 42, 43, 44, 45, 47 |
| **Vaibhav** | Senior | Data and ML: datasets, extractors, models, adversarial library | 7, 9, 10, 12, 14, 24, 25, 26, 27, 35, 46, 49 |
| **Jaswanth** | Senior | Streaming and MLOps: Redpanda, Faust, Feast, MLflow registry, Prometheus | 11, 15, 19, 20, 22, 23, 29, 32, 37 |
| **Yogya** | Junior | Data loading, pollers, dashboards, docs (mentored) | 8, 13, 16, 36, 41, 48 |
| **Tanuj** | Junior | Webhooks, replay, alert rules, demo video (mentored) | 17, 18, 21, 40, 50 |

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
