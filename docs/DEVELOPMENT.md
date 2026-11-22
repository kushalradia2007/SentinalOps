# Development quickstart

All settings come from `configs/domain_liquidity.yaml` (typed and validated by `sentinalops.config.DomainConfig`;
unknown keys are errors). Until recorded data flows through the chunk 12 pipeline, training uses synthetic
calm and volatile regimes from `sentinalops.simulator.regimes`.

```bash
pip install -e ".[dev]"
make test                                   # pytest
ruff check .                                # lint
python -m sentinalops.training.train_baseline   # trains models/champion.joblib on the synthetic calm regime
uvicorn sentinalops.serving.app:create_app --factory --reload     # API on :8000
python scripts/smoke.py                          # scores a calm and a pre-stress tick, posts an outcome
```

Full stack: `cp .env.example .env && docker compose up -d --build`
(API :8000, MLflow :5000, Prometheus :9090, Grafana :3000).
The API image trains a baseline champion on first boot if `models/champion.joblib` is missing.

## Layout
- `configs/domain_liquidity.yaml` the domain: symbol, cadence, horizon, windows, label block, embargo, stale limit
- `src/sentinalops/config.py` typed config and env settings; `label_params()` / `feature_params()` feed the modules below
- `src/sentinalops/schemas.py` `TradeEvent`, `DepthSnapshot`, `FeatureTick`, `StressPrediction`, `OutcomeLabel`
- `src/sentinalops/data/recorder/` Binance stream recorder; `data/audit_recording.py` its audit
- `src/sentinalops/data/pipeline.py` recordings -> labelled training table (chunk 12)
- `src/sentinalops/labels.py` the stress label; `features/microstructure.py` the 34 model features
- `src/sentinalops/simulator/regimes.py` synthetic calm and volatile regimes
- `src/sentinalops/training/` baseline champion; `serving/` FastAPI app (`/score`, `/outcomes`, `/health`, `/metrics`)
- `docs/ROADMAP.md` the 50-chunk plan

## Stream recorder (chunk 7)

```bash
python -m sentinalops.data.recorder                    # record BTCUSDT trades + depth to data/raw/binance
python -m sentinalops.data.audit_recording --hours 24  # row counts, disk use, gaps, integrity
```

Details, file layout and the failure cases it handles are in [`recorder.md`](recorder.md).

## Offline feature and label table (chunk 12)

```bash
python -m sentinalops.data.pipeline        # data/raw/binance -> data/processed/features/date=*/hour=*.parquet
python scripts/bench_pipeline.py           # speed check on a synthetic day (target < 10 min)
```

Details in [`pipeline.md`](pipeline.md).
