# SentinalOps Roadmap: 50 chunks

Domain: software supply chain security. Thesis: detect malicious packages in real time, and self-heal when attackers change tactics.

## Phase A: Foundation
- [x] 1. Repo skeleton, packaging, docker-compose, domain config **[Kushal]**
- [x] 2. Feature extractors (AST, entropy, base64, metadata) **[Kushal]**
- [x] 3. Zero-day simulator, synthetic data, champion model, thesis test **[Kushal]**
- [x] 4. Config loader, structured logging, shared pydantic schemas **[Kushal]**
- [x] 5. CI: ruff, pytest, GitHub Actions **[Kushal]**
- [x] 6. Full docker compose stack comes up healthy **[Kushal]**

## Phase B: Data
- [x] 7. MalwareBench download and ingest **[Vaibhav]**
- [x] 8. QUT-DV25 loader **[Yogya, mentor Vaibhav]**
- [x] 9. Safe package fetcher (PyPI sdist/wheel, static unpack only, never executes) **[Vaibhav]**
- [ ] 10. npm tarball fetcher plus JS feature extractor (postinstall, eval) **[Vaibhav]**
- [ ] 11. Metadata enrichment: maintainer age, email-domain change, dependency depth **[Jaswanth]**
- [ ] 12. Real-feature pipeline to Parquet offline store **[Vaibhav]**
- [ ] 13. Dataset QA: dedup, leakage checks, temporal split **[Yogya, mentor Vaibhav]**
- [ ] 14. Install-time behavior features (syscalls, network, file writes) **[Vaibhav]**

## Phase C: Streaming
- [ ] 15. Redpanda topics and event schemas **[Jaswanth]**
- [ ] 16. PyPI live poller to raw topic **[Yogya, mentor Jaswanth]**
- [ ] 17. npm registry feed poller **[Tanuj, mentor Jaswanth]**
- [ ] 18. GitHub webhook receiver **[Tanuj, mentor Jaswanth]**
- [ ] 19. Faust processor: fetch, extract, publish features **[Jaswanth]**
- [ ] 20. Dead-letter queue, retries, idempotency, backpressure **[Jaswanth]**
- [ ] 21. Replay simulator with speed control **[Tanuj, mentor Jaswanth]**

## Phase D: Feature store
- [ ] 22. Feast repo: entities, feature views, offline store **[Jaswanth]**
- [ ] 23. Redis online materialization **[Jaswanth]**
- [ ] 24. Point-in-time-correct training sets **[Vaibhav]**
- [ ] 25. Online/offline consistency test **[Vaibhav]**

## Phase E: Models
- [ ] 26. Real-data champion (GBM) plus metrics suite (PR-AUC, recall@FPR) **[Vaibhav]**
- [ ] 27. Classic detectors: IsolationForest, PyOD (ECOD/COPOD) **[Vaibhav]**
- [ ] 28. PyTorch autoencoder **[Kushal]**
- [ ] 29. Streaming learners (River/PySAD) **[Jaswanth]**
- [ ] 30. Comparison harness and MLflow experiments **[Kushal]**
- [ ] 31. Calibration and cost-sensitive threshold selection **[Kushal]**
- [ ] 32. MLflow registry: champion/challenger aliases **[Jaswanth]**

## Phase F: Serving
- [ ] 33. /score with feature-store lookup and alias hot-reload **[Kushal]**
- [ ] 34. /labels endpoint, label store, prediction-label join **[Kushal]**
- [ ] 35. Block policy, allowlist, SHAP explanations **[Vaibhav]**
- [ ] 36. Load testing and p99 latency targets **[Yogya, mentor Kushal]**

## Phase G: Monitoring
- [ ] 37. Prometheus metrics: scores, latency, throughput **[Jaswanth]**
- [ ] 38. Evidently drift jobs (feature and prediction drift) **[Kushal]**
- [ ] 39. Delayed-label rolling recall/precision **[Kushal]**
- [ ] 40. Alert rules (model_drift_high) **[Tanuj, mentor Kushal]**
- [ ] 41. Grafana dashboards and "Model Healed" annotations **[Yogya, mentor Kushal]**

## Phase H: Self-healing
- [ ] 42. Prefect flow with drift plus label trigger **[Kushal]**
- [ ] 43. Retrain DAG: 48h window plus replay buffer against forgetting **[Kushal]**
- [ ] 44. Challenger vs champion gates (gain on new attacks, no regression on old) **[Kushal]**
- [ ] 45. Auto-promotion and rollback safety **[Kushal]**
- [ ] 46. Harmless adversarial variant library (dependency confusion, compromised maintainer, staged payload) **[Vaibhav]**

## Phase I: Demo and polish
- [ ] 47. End-to-end 5-step zero-day demo with GitHub test repo **[Kushal]**
- [ ] 48. Docs: architecture diagram, runbook, ADRs **[Yogya, mentor Kushal]**
- [ ] 49. Benchmarks and results writeup (heal time, before/after) **[Vaibhav]**
- [ ] 50. Demo video, resume bullets, v1.0 release **[Tanuj, mentor Kushal]**
