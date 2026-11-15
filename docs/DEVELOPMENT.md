# Development quickstart

> **Domain switch in progress.** The project domain is now Liquidity Intelligence (see `docs/ROADMAP.md`).
> The commands below still run the earlier supply-chain prototype. They keep working until chunks 2, 3 and 7-10
> replace that code; the "Legacy data loaders" section at the bottom describes the old loaders and goes away with task 2.6.
> The new domain settings are in `configs/domain_liquidity.yaml`; the default stays `domain_supply_chain.yaml` until task 2.3.

```bash
pip install -e ".[dev]"
make test                                   # pytest
ruff check .                                # lint
python -m sentinalops.training.train_baseline   # trains models/champion.joblib (synthetic data for now)
uvicorn sentinalops.serving.app:create_app --factory --reload     # API on :8000
python scripts/smoke.py                          # scores benign + zero-day package, posts a label
```

Full stack: `cp .env.example .env && docker compose up -d --build`
(API :8000, MLflow :5000, Prometheus :9090, Grafana :3000).
The API image trains a baseline champion on first boot if `models/champion.joblib` is missing.

## Layout
- `configs/` per-domain YAML (everything domain-specific)
- `src/sentinalops/features/` pure feature extractors (shared by stream, API, training)
- `src/sentinalops/schemas.py` event + API schemas, `config.py` settings, `logging.py` JSON logs
- `src/sentinalops/serving/` FastAPI app (`/score`, `/labels`, `/health`, `/metrics`)
- `src/sentinalops/simulator/` harmless zero-day demo packages
- `docs/ROADMAP.md` the 50-chunk plan

## Legacy data loaders (old chunks 7-9)

```bash
# MalwareBench (metadata only: names, versions, labels, file lists; full set is request-only)
python -m sentinalops.data.malwarebench download            # public Zenodo sample -> data/raw/malwarebench
python -m sentinalops.data.malwarebench ingest              # -> data/processed/malwarebench/{packages,files}.parquet
# QUT-DV25 install-time traces (Harvard Dataverse doi:10.7910/DVN/LBMXJY)
python -m sentinalops.data.qut_dv25 download --list         # see what the dataset holds
python -m sentinalops.data.qut_dv25 download && python -m sentinalops.data.qut_dv25 ingest
# Safe fetcher: statically read a PyPI release and print its features (never executes anything)
python -m sentinalops.data.fetcher requests 2.31.0
RUN_NETWORK_TESTS=1 pytest -k live                          # optional live test against PyPI
```

MalwareBench contains **no source code**, so malicious packages' code comes from the fetchers or
another corpus; many malicious releases are removed from PyPI.
