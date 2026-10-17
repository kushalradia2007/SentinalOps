# Development quickstart

```bash
pip install -e ".[dev]"
make test                                   # pytest
ruff check .                                # lint
python -m sentinalops.training.train_baseline   # trains models/champion.joblib (synthetic data for now)
uvicorn sentinalops.serving.app:app --reload     # API on :8000
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
