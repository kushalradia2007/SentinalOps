"""FastAPI inference service: /score, /labels, /metrics, /health."""
from __future__ import annotations

import json
import threading
import time

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from prometheus_client import CollectorRegistry, Counter, Histogram, make_asgi_app

from sentinalops.config import Settings, get_domain_config
from sentinalops.features.extract import FEATURE_ORDER, extract_package_features
from sentinalops.logging import configure_logging, get_logger
from sentinalops.schemas import (
    FeatureRecord,
    LabelEvent,
    LabelRequest,
    Prediction,
    ScoreRequest,
)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    configure_logging(settings.log_level)
    log = get_logger("sentinalops.api")
    cfg = get_domain_config(settings.domain_config)
    threshold = settings.effective_threshold(cfg)

    registry = CollectorRegistry()
    scores_h = Histogram("sentinalops_score", "Malicious probability",
                         buckets=[i / 10 for i in range(11)], registry=registry)
    blocked_c = Counter("sentinalops_blocked_total", "Packages blocked", registry=registry)
    labels_c = Counter("sentinalops_labels_total", "Delayed labels received",
                       ["is_malicious"], registry=registry)

    app = FastAPI(title="SentinalOps", version="0.1.0")
    app.mount("/metrics", make_asgi_app(registry=registry))

    state: dict = {"model": None, "mtime": 0.0}
    lock = threading.Lock()

    def get_model():
        """Hot-reload whenever the promoted champion file changes (the 'heal' step)."""
        path = settings.model_path
        if not path.exists():
            raise HTTPException(status_code=503, detail="model not available")
        mtime = path.stat().st_mtime
        with lock:
            if state["model"] is None or mtime != state["mtime"]:
                state["model"], state["mtime"] = joblib.load(path), mtime
                log.info("model loaded", extra={"path": str(path), "mtime": mtime})
            return state["model"], f"champion-{int(state['mtime'])}"

    def append(name: str, row: dict) -> None:
        settings.data_dir.mkdir(parents=True, exist_ok=True)
        with (settings.data_dir / name).open("a") as f:
            f.write(json.dumps(row) + "\n")

    @app.post("/score", response_model=Prediction)
    def score(req: ScoreRequest) -> Prediction:
        model, version = get_model()
        feats = extract_package_features(req.files, req.meta)
        p = float(model.predict_proba(pd.DataFrame([feats])[FEATURE_ORDER])[:, 1][0])
        blocked = p >= threshold
        scores_h.observe(p)
        if blocked:
            blocked_c.inc()
        name = req.meta["name"]
        pred = Prediction(package=name, score=p, blocked=blocked, model_version=version)
        record = FeatureRecord(package=name, features={k: float(v) for k, v in feats.items()})
        append("features.jsonl", {**record.model_dump(), "score": p, "model_version": version})
        log.info("scored", extra={"package": name, "score": round(p, 4), "blocked": blocked})
        return pred

    @app.post("/labels")
    def labels(req: LabelRequest) -> dict:
        event = LabelEvent(**req.model_dump())
        labels_c.labels(str(event.is_malicious).lower()).inc()
        append("labels.jsonl", event.model_dump())
        log.info("label recorded", extra={"package": event.package, "is_malicious": event.is_malicious})
        return {"status": "recorded", "package": event.package}

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "model_loaded": settings.model_path.exists(),
                "block_threshold": threshold, "domain": cfg.domain, "ts": time.time()}

    return app

