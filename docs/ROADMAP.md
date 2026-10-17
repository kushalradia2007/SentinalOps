# SentinalOps Roadmap: 50 chunks

Domain: software supply chain security. Thesis: detect malicious packages in real time, and self-heal when attackers change tactics.

## Phase A: Foundation
- [x] 1. Repo skeleton, packaging, docker-compose, domain config
- [x] 2. Feature extractors (AST, entropy, base64, metadata)
- [x] 3. Zero-day simulator, synthetic data, champion model, thesis test
- [x] 4. Config loader, structured logging, shared pydantic schemas
- [x] 5. CI: ruff, pytest, GitHub Actions
- [x] 6. Full docker compose stack comes up healthy

## Phase B: Data
- [ ] 7. MalwareBench download and ingest
- [ ] 8. QUT-DV25 loader
- [ ] 9. Safe package fetcher (PyPI sdist/wheel, static unpack only, never executes)
- [ ] 10. npm tarball fetcher plus JS feature extractor (postinstall, eval)
- [ ] 11. Metadata enrichment: maintainer age, email-domain change, dependency depth
- [ ] 12. Real-feature pipeline to Parquet offline store
- [ ] 13. Dataset QA: dedup, leakage checks, temporal split
- [ ] 14. Install-time behavior features (syscalls, network, file writes)

## Phase C: Streaming
- [ ] 15. Redpanda topics and event schemas
- [ ] 16. PyPI live poller to raw topic
- [ ] 17. npm registry feed poller
- [ ] 18. GitHub webhook receiver
- [ ] 19. Faust processor: fetch, extract, publish features
- [ ] 20. Dead-letter queue, retries, idempotency, backpressure
- [ ] 21. Replay simulator with speed control

## Phase D: Feature store
- [ ] 22. Feast repo: entities, feature views, offline store
- [ ] 23. Redis online materialization
- [ ] 24. Point-in-time-correct training sets
- [ ] 25. Online/offline consistency test

## Phase E: Models
- [ ] 26. Real-data champion (GBM) plus metrics suite (PR-AUC, recall@FPR)
- [ ] 27. Classic detectors: IsolationForest, PyOD (ECOD/COPOD)
- [ ] 28. PyTorch autoencoder
- [ ] 29. Streaming learners (River/PySAD)
- [ ] 30. Comparison harness and MLflow experiments
- [ ] 31. Calibration and cost-sensitive threshold selection
- [ ] 32. MLflow registry: champion/challenger aliases

## Phase F: Serving
- [ ] 33. /score with feature-store lookup and alias hot-reload
- [ ] 34. /labels endpoint, label store, prediction-label join
- [ ] 35. Block policy, allowlist, SHAP explanations
- [ ] 36. Load testing and p99 latency targets

## Phase G: Monitoring
- [ ] 37. Prometheus metrics: scores, latency, throughput
- [ ] 38. Evidently drift jobs (feature and prediction drift)
- [ ] 39. Delayed-label rolling recall/precision
- [ ] 40. Alert rules (model_drift_high)
- [ ] 41. Grafana dashboards and "Model Healed" annotations

## Phase H: Self-healing
- [ ] 42. Prefect flow with drift plus label trigger
- [ ] 43. Retrain DAG: 48h window plus replay buffer against forgetting
- [ ] 44. Challenger vs champion gates (gain on new attacks, no regression on old)
- [ ] 45. Auto-promotion and rollback safety
- [ ] 46. Harmless adversarial variant library (dependency confusion, compromised maintainer, staged payload)

## Phase I: Demo and polish
- [ ] 47. End-to-end 5-step zero-day demo with GitHub test repo
- [ ] 48. Docs: architecture diagram, runbook, ADRs
- [ ] 49. Benchmarks and results writeup (heal time, before/after)
- [ ] 50. Demo video, resume bullets, v1.0 release
