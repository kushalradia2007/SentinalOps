# SentinalOps: Liquidity Intelligence

### A self-healing streaming ML platform that monitors market liquidity in real time

[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://img.shields.io/badge/License-MIT-yellow.svg)
[![Python](https://img.shields.io/badge/python-3.11-blue.svg)](https://img.shields.io/badge/python-3.11-blue.svg)
[![Docker Compose](https://img.shields.io/badge/docker-compose-blue.svg)](https://img.shields.io/badge/docker-compose-blue.svg)

---

## Overview

SentinalOps watches the BTCUSDT order book and trade flow on Binance's public market-data streams and does four things:

1. **Detects** abnormal liquidity conditions as they happen.
2. **Forecasts** the probability that liquidity stress starts within the next 30 seconds.
3. **Verifies** every forecast against what the market did after it was made.
4. **Heals** itself: when the market regime changes and the forecasts get worse, it retrains, checks the new model against the old one, and promotes it only if it is better.

> **One-line pitch:** a streaming ML platform that detects abnormal market conditions, forecasts liquidity stress, grades its own predictions against the future, and adapts when market behaviour changes.

### What this is not

- It is not a trading bot. It places no orders and needs no API keys.
- It does not predict prices.
- It reports measurable market conditions. It makes no claim about manipulation or about why a liquidity event happened.

> **Status:** data layer, stream processor, prediction ledger, outcome verifier and self-healing loop are built. The model is a plug-in (`src/sentinalops/models/`, see [`docs/models.md`](docs/models.md)), and the heal loop retrains the installed model's own recipe. The live system runs end to end from Binance to the ledger and the heal service (`python -m sentinalops.live`, see [`docs/live.md`](docs/live.md)), over an in-memory bus or Redpanda, with an Oracle Cloud deploy kit in `deploy/oracle/`. Next: champion candidates and evaluation on recorded data (chunks 26, 30, 31); see [`docs/ROADMAP.md`](docs/ROADMAP.md).

---

## Why this domain

| Need | How market liquidity provides it |
| --- | --- |
| A real live stream | Public WebSocket feeds of trades and order-book depth, with updates every 100 ms |
| Ground-truth labels | The future stream itself: 30 seconds after a forecast, we can measure whether stress occurred |
| A reason to self-heal | Markets move between regimes (calm, volatile, thin), so a model trained in one degrades in another |

---

## Architecture

```text
┌──────────────────────┐
│ Binance public WS    │  aggTrade + depth5@100ms (or replay of recorded data)
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Producers + recorder │  feed health, reconnect, Parquet archive
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Redpanda             │  raw_trades, raw_depth, features, predictions, outcomes
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Stream processor     │  one feature tick per second, rolling 1/5/30/60 s windows
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Feature store (Feast)│  Redis online | Parquet offline
└─────┬──────────┬─────┘
      ▼          ▼
┌───────────┐  ┌──────────────────────┐
│ Training  │  │ Online scoring       │  anomaly score + stress probability
│ MLflow    │  │ FastAPI              │
└─────┬─────┘  └──────────┬───────────┘
      │                   ▼
      │        ┌──────────────────────┐
      │        │ Prediction ledger +  │  labels each forecast after its horizon
      │        │ outcome verifier     │
      │        └──────────┬───────────┘
      ▼                   ▼
┌──────────────────────────────────────┐
│ Monitoring                           │  Evidently drift, matured-label
│ Prometheus + Grafana                 │  precision/recall/Brier, feed health
└──────────────────┬───────────────────┘
                   ▼
┌──────────────────────────────────────┐
│ Self-healing (Prefect)               │  retrain → gates → promote → shadow
│ MLflow registry: champion/challenger │  check → rollback if worse
└──────────────────────────────────────┘
```

---

## What the model predicts

**Liquidity stress** at time `t` means either of:

- **Depth depletion (primary):** top-five order-book depth falls below a fixed fraction of its trailing median.
- **Spread expansion (secondary):** relative spread rises above a fixed multiple of its trailing median.

The thresholds are chosen from training-period data and then frozen. The forecast target is the **onset** of stress within the next 30 seconds, made once per second.

Rules that keep the evaluation honest:

- Features at time `t` use only data up to `t`; the verifier uses only data after `t`.
- Train and test periods are chronological, with an embargo gap of at least the horizon.
- Every model is compared with a persistence baseline ("stressed now, so stressed in 30 seconds").
- Feature drift alone never triggers retraining. A measured drop in matured-label performance does.

### Features (about 10–15, each over 1, 5, 30 and 60 second windows)

| Feature | Meaning |
| --- | --- |
| Mid-price | `(best_bid + best_ask) / 2` |
| Relative spread | `(best_ask - best_bid) / mid_price` |
| Bid / ask depth | Quantity across the top five levels on each side |
| Depth imbalance | `(bid_depth - ask_depth) / (bid_depth + ask_depth)` |
| Trade imbalance | Buy-initiated versus sell-initiated volume |
| Trade intensity | Trades per window |
| Realized volatility | Volatility of recent mid-price returns |
| Depth / spread change | Change over each rolling window |

---

## Approaches compared

| Approach | Methods | Library |
| --- | --- | --- |
| Rolling statistical thresholds | z-scores on spread, depth, imbalance | pandas |
| Batch anomaly detection | Isolation Forest, ECOD, COPOD, autoencoder | scikit-learn, PyOD, PyTorch |
| Online anomaly detection | Half-Space Trees, ADWIN drift | River |
| Supervised stress forecaster | Gradient-boosted trees on verified outcomes | scikit-learn |

## Metrics

| Metric | Why it matters |
| --- | --- |
| PR-AUC, precision, recall | Stress events are rare, so accuracy is meaningless |
| Recall at 1% false-positive rate | Alert quality at a tolerable false-alarm level |
| Brier score and calibration | A forecast of 80% should come true about 80% of the time |
| Detection lead time | Seconds of warning before stress begins |
| False alarms per hour | Drives alert fatigue |
| Per-regime breakdown | Shows where a model fails |
| Inference latency (p50/p95/p99) | The forecast has to arrive before the event |
| Heal time | Reliability alert to promoted model |

---

## Tech stack

| Layer | Choice |
| --- | --- |
| Data source | Binance public Spot market data (WebSocket, no API key) |
| Streaming | Redpanda (Kafka-compatible) |
| Stream processing | Faust or a plain Python consumer |
| Feature store | Feast, with Redis online and Parquet offline |
| Models | scikit-learn, PyOD, River, PyTorch |
| Tracking and registry | MLflow |
| Serving | FastAPI |
| Monitoring | Evidently, Prometheus, Grafana |
| Orchestration | Prefect |
| CI/CD | GitHub Actions |
| Infrastructure | Docker Compose |

---

## Quick start

Prerequisites: Docker with Compose v2, Python 3.11+, Git.

```bash
git clone https://github.com/kushalradia2007/SentinalOps.git
cd SentinalOps
pip install -e ".[dev]"
make test
cp .env.example .env
docker compose up -d --build
```

This brings up the API on `:8000`, MLflow on `:5000`, Prometheus on `:9090` and Grafana on `:3000`. See [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md) for what runs today and what is still planned.

---

## The demo

The end-to-end demo replays a recorded period where the market changes regime:

1. **Calm market.** Forecasts mature and the champion beats the persistence baseline.
2. **Regime shift.** Reliability falls and the `ReliabilityDropped` alert fires.
3. **Heal.** The flow retrains a challenger and runs the promotion gates.
4. **Promotion.** The champion alias moves, a "Model Healed" marker appears, and a shadow check guards against a bad promotion.
5. **Recovery.** Reliability returns above the baseline.

A second, shorter demo stops the feed on purpose to show predictions being suspended and resumed.

---

## Scope for v1.0

| In | Out |
| --- | --- |
| One symbol (BTCUSDT), one exchange | Multiple symbols or exchanges |
| Top-five depth snapshots | Full order-book reconstruction |
| 30-second binary stress forecast | Price prediction |
| Monitoring and alerting | Any order placement or trading logic |

---

## Project docs

| Document | Description |
| --- | --- |
| [`docs/ROADMAP.md`](docs/ROADMAP.md) | The 50-chunk plan and design rules |
| [`docs/TASKS.md`](docs/TASKS.md) | Every open chunk split into small tasks |
| [`docs/TEAM_PLAN.md`](docs/TEAM_PLAN.md) | Owners, mentoring, working rules, waves |
| [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md) | Local setup and commands |
| [`configs/domain_liquidity.yaml`](configs/domain_liquidity.yaml) | Domain settings: symbol, horizon, label, thresholds |

## Team

| Name | Role | Focus area |
| --- | --- | --- |
| Kushal | Lead | Domain switch, verifier, self-healing core, serving, demo |
| Vaibhav | Senior | Recorder, labels, features, models, scenarios |
| Jaswanth | Senior | Streaming, feature store, MLOps |
| Yogya | Junior | Archive loader, dataset QA, trade producer, dashboards, docs |
| Tanuj | Junior | Depth producer, feed health, replay, alerts, demo video |

## License

MIT.
