# Real-Time Anomaly Detection Platform

### An end-to-end streaming ML system with feature store, MLOps, and automated retraining

[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://img.shields.io/badge/License-MIT-yellow.svg)
[![Python](https://img.shields.io/badge/python-3.11-blue.svg)](https://img.shields.io/badge/python-3.11-blue.svg)
[![Docker Compose](https://img.shields.io/badge/docker-compose-blue.svg)](https://img.shields.io/badge/docker-compose-blue.svg)

---

## 📌 Overview

Most machine learning projects stop at a Jupyter notebook with a trained model. In the real world, models must handle **streaming data**, serve **low-latency predictions**, maintain **feature consistency** between training and inference, and **self-heal** when data drifts.

This project builds a **production-grade real-time anomaly detection platform** that demonstrates the complete ML lifecycle — from data ingestion to automated retraining. It is designed as a portfolio-quality system that reflects how anomaly detection is actually deployed in industry: fraud detection, infrastructure monitoring, IoT predictive maintenance, and network intrusion detection.

> **One-line pitch:** A streaming ML platform that detects anomalies in real time, maintains online/offline feature consistency, and self-heals through monitoring, drift detection, and automated retraining.

---

## 🎯 Objectives

1. Build a **streaming ingestion pipeline** that simulates real-time data flow.
2. Implement a **feature store** with online/offline consistency and point-in-time correctness.
3. Train and rigorously compare **multiple anomaly detection methods** — statistical, classical ML, deep learning, and streaming.
4. Deploy a **low-latency inference service** behind a REST API.
5. Build **monitoring dashboards** for drift, performance, and data quality.
6. Implement **CI/CD + automated retraining** triggered by drift or schedule.
7. Produce a **reproducible, documented, deployable system** that runs with one command.

---

## 🏗️ Architecture

```text
┌─────────────────┐
│  Data Source    │  (replay / live feed / simulator)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Message Broker │  Redpanda (Kafka-compatible)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Stream Processor│  Faust / Python consumer
│  (feature calc) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Feature Store  │  Feast  (Redis online | Parquet offline)
└────────┬────────┘
         │
         ├──────────────────────────┐
         ▼                          ▼
┌─────────────────┐        ┌─────────────────┐
│  Training       │        │ Online Inference│
│  (offline)      │        │  FastAPI        │
│  MLflow         │        │  (low latency)  │
└────────┬────────┘        └────────┬────────┘
         │                          │
         ▼                          ▼
┌─────────────────┐        ┌─────────────────┐
│ Model Registry  │───────▶│  Alerting       │
│  (MLflow)       │        │  Slack / Webhook│
└────────┬────────┘        └─────────────────┘
         │
         ▼
┌─────────────────────────────────────────────┐
│  Monitoring & Observability                 │
│  Evidently (drift) | Prometheus | Grafana   │
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│  Orchestration & CI/CD                      │
│  Prefect | GitHub Actions | Docker          │
│  → Auto-retrain on drift or schedule        │
└─────────────────────────────────────────────┘
```

> 📐 Full architecture documentation: We will plan properly

---

## 🧰 Tech Stack

| Layer               | Primary Choice                       | Why                                          |
| ------------------- | ------------------------------------ | -------------------------------------------- |
| Streaming           | **Redpanda**                         | Kafka-compatible, lightweight, single binary |
| Stream Processing   | **Faust / Python consumer**          | Simple, Python-native                        |
| Feature Store       | **Feast**                            | Industry-standard, open-source               |
| Online Store        | **Redis**                            | Low-latency feature serving                  |
| Offline Store       | **Parquet**                          | Simple, portable, columnar                   |
| ML Libraries        | **PyOD, scikit-learn, PyTorch**      | Comprehensive anomaly detection              |
| Experiment Tracking | **MLflow**                           | De facto standard                            |
| Model Registry      | **MLflow**                           | Versioning + lineage                         |
| Serving             | **FastAPI + Docker**                 | Fast, async, containerized                   |
| Monitoring          | **Evidently + Prometheus + Grafana** | Drift + metrics + dashboards                 |
| Orchestration       | **Prefect**                          | Python-native, easy DAGs                     |
| CI/CD               | **GitHub Actions**                   | Free for students, integrated                |
| Infrastructure      | **Docker Compose**                   | Reproducible local stack                     |

---

## 📂 Repository Structure

```text
anomaly-detection-platform/
├── docker-compose.yml          # Full local stack
├── README.md
├── LICENSE
├── requirements.txt
├── pyproject.toml
├── .env.example
├── .gitignore
│
├── docs/
│   ├── architecture.md
│   ├── adr/                    # Architecture Decision Records
│   ├── model_card.md
│   └── setup.md
│
├── data/
│   ├── raw/                    # Downloaded datasets (gitignored)
│   └── processed/              # Cleaned, feature-engineered data
│
├── ingestion/
│   ├── producer.py             # Kafka/Redpanda producer
│   └── replay_simulator.py     # Replays historical data as a stream
│
├── streaming/
│   └── feature_pipeline.py     # Consumes stream, computes features
│
├── feature_store/
│   ├── feature_repo/
│   │   ├── features.py         # Feature definitions
│   │   └── feature_store.yaml  # Feast config
│   └── materialize.py          # Offline → online materialization
│
├── training/
│   ├── train_baseline.py       # Isolation Forest, OCSVM, LOF
│   ├── train_deep.py           # Autoencoder, LSTM-AD, VAE
│   ├── train_streaming.py      # River / PySAD models
│   └── evaluate.py             # AUC-PR, F1, detection delay, FPR
│
├── serving/
│   ├── app.py                  # FastAPI inference service
│   ├── model_loader.py
│   └── Dockerfile
│
├── monitoring/
│   ├── evidently_reports/
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       └── dashboards/
│
├── orchestration/
│   └── retrain_dag.py          # Prefect flow: retrain → evaluate → promote
│
├── tests/
│   ├── test_features.py
│   ├── test_serving.py
│   └── test_pipeline.py
│
├── notebooks/
│   └── exploration.ipynb       # EDA only, not production code
│
└── .github/
    ├── workflows/
    │   └── ci.yml
    ├── ISSUE_TEMPLATE/
    └── pull_request_template.md
```

---

## 🚀 Quick Start

### Prerequisites

* Docker & Docker Compose (v2+)
* Python 3.11+
* Git

### 1. Clone the repository

```bash
git clone https://github.com/your-org/anomaly-detection-platform.git
cd anomaly-detection-platform
```

### 2. Set up environment

```bash
cp .env.example .env
# Edit .env with your Slack webhook, credentials if needed
```

### 3. Start the full stack

```bash
docker compose up -d
```

This brings up:

* Redpanda (streaming) → `localhost:9092`
* Redis (online feature store) → `localhost:6379`
* PostgreSQL (MLflow backend) → `localhost:5432`
* MLflow → `http://localhost:5000`
* FastAPI inference → `http://localhost:8000`
* Prometheus → `http://localhost:9090`
* Grafana → `http://localhost:3000` (admin/admin)

### 4. Download data

```bash
python -m data.download --dataset nab
```

### 5. Replay stream + generate features

```bash
python ingestion/replay_simulator.py
```

### 6. Train baseline models

```bash
python training/train_baseline.py --config configs/baseline.yaml
```

### 7. Open the dashboard

* **MLflow:** [http://localhost:5000](http://localhost:5000/)
* **Grafana:** [http://localhost:3000](http://localhost:3000/)
* **API docs:** http://localhost:8000/docs

### 8. Shut down

```bash
docker compose down -v
```

---

## 📊 Evaluation Metrics

We evaluate anomaly detectors on metrics that matter in production — not just accuracy.

| Metric                              | Why It Matters                               |
| ----------------------------------- | -------------------------------------------- |
| **AUC-PR**                          | Better than ROC for highly imbalanced data   |
| **Precision / Recall / F1**         | Standard detection quality                   |
| **Detection Delay**                 | How many events before the anomaly is caught |
| **False Positive Rate**             | Directly drives alert fatigue                |
| **Inference Latency (p50/p95/p99)** | Real-time SLA compliance                     |
| **Drift (PSI, KL, KS)**             | Detects when the model goes stale            |
| **Business Impact**                 | Cost saved / incidents prevented             |

---

## 🔬 Anomaly Detection Methods Compared

| Category      | Methods                               | Library      |
| ------------- | ------------------------------------- | ------------ |
| Statistical   | Z-score, EWMA, Seasonal Decomposition | statsmodels  |
| Classical ML  | Isolation Forest, One-Class SVM, LOF  | PyOD         |
| Deep Learning | Autoencoder, LSTM-AD, VAE             | PyTorch      |
| Streaming     | Half-Space Trees, ADWIN drift         | River, PySAD |

Results and comparison tables are logged to MLflow and summarized in [`docs/model_card.md`](https://docs/model_card.md).

---

## 🔁 MLOps Pipeline

### CI/CD (GitHub Actions)

On every push or PR:

1. **Lint** with Ruff
2. **Test** with pytest
3. **Build** Docker images
4. **Push** to registry (main branch only)
5. **Deploy** to staging (main branch only)

### Automated Retraining (Prefect)

A Prefect flow runs on schedule and on drift trigger:

```text
Ingest latest data
    → Compute features
    → Train candidate model
    → Evaluate against champion
    → If better: promote & deploy
    → Else: keep champion, log result
```

### Monitoring & Drift

* **Evidently** generates data drift + model performance reports.
* **Prometheus** scrapes latency, throughput, and custom metrics.
* **Grafana** visualizes drift, alerts, and SLA compliance.
* **Slack** receives alerts when drift exceeds thresholds or performance degrades.

---

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html

# Lint
ruff check .

# Type check (optional)
mypy .
```

Tests cover:

* Feature computation correctness
* Online/offline feature consistency
* API contract and latency
* Model loading and prediction shape
* Drift detection triggers

---

## 📈 Roadmap

Track live progress on our [GitHub Project Board](https://github.com/your-org/anomaly-detection-platform/projects).

---

## 👥 Team

| Name     | Role                            | Focus Area                            |
| -------- | ------------------------------- | ------------------------------------- |
| Kushal   |                                 |                                       |
| Vaibhav  |                                 |                                       |
| Jaswanth |                                 |                                       |
| Yogya    |                                 |                                       |
| Tanuj    |                                 |                                       |

---

## 📚 Documentation

| Document                                               | Description                         |
| ------------------------------------------------------ | ----------------------------------- |
| [`docs/architecture.md`](https://docs/architecture.md) | System design and data flow         |
| [`docs/setup.md`](https://docs/setup.md)               | Detailed local setup guide          |
| [`docs/model_card.md`](https://docs/model_card.md)     | Model details, metrics, limitations |
| [`docs/adr/`](https://docs/adr/)                       | Architecture Decision Records       |
| [`CONTRIBUTING.md`](https://contributing.md/)          | How to contribute                   |

NOTE - These files will me made soon with time.

---

## 📜 License

This project is licensed under the **MIT License** — see [`LICENSE`](https://license/) for details.

---

<p align="center"><i>Built with ☕ and curiosity at IIT Bhilai</i></p>
