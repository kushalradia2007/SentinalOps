"""FastAPI inference service: /score, /outcomes, /metrics, /health.

``POST /score`` takes one feature tick and returns the probability that liquidity stress
starts within the horizon. Each prediction is appended to ``predictions.jsonl`` (the seed of
the prediction ledger, chunk 34). ``POST /outcomes`` takes the verified outcome of an earlier
tick and appends it to ``outcomes.jsonl``.
"""

from __future__ import annotations

import json
import time

from fastapi import FastAPI, HTTPException
from prometheus_client import CollectorRegistry, Counter, Histogram, make_asgi_app

from sentinalops.config import Settings, get_domain_config
from sentinalops.logging import configure_logging, get_logger
from sentinalops.schemas import OutcomeLabel, ScoreRequest, StressPrediction
from sentinalops.serving.model import ChampionModel, ModelUnavailable


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    configure_logging(settings.log_level)
    log = get_logger("sentinalops.api")
    cfg = get_domain_config(settings.domain_config)
    threshold = settings.effective_threshold(cfg)
    feature_order = cfg.feature_names
    expected = set(feature_order)

    registry = CollectorRegistry()
    prob_h = Histogram("sentinalops_stress_probability", "Forecast stress probability",
                       buckets=[i / 10 for i in range(11)], registry=registry)
    alerts_c = Counter("sentinalops_alerts_total", "Ticks scored at or above the alert threshold",
                       registry=registry)
    outcomes_c = Counter("sentinalops_outcomes_total", "Verified outcomes received", ["label"],
                         registry=registry)

    app = FastAPI(title="SentinalOps", version="0.2.0")
    app.mount("/metrics", make_asgi_app(registry=registry))

    champion = ChampionModel(settings.model_path, feature_order)

    def append(name: str, row: dict) -> None:
        settings.data_dir.mkdir(parents=True, exist_ok=True)
        with (settings.data_dir / name).open("a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")

    @app.post("/score", response_model=StressPrediction)
    def score(req: ScoreRequest) -> StressPrediction:
        if req.symbol != cfg.symbol:
            raise HTTPException(status_code=422, detail=f"this service scores {cfg.symbol} only")
        missing, extra = sorted(expected - req.features.keys()), sorted(req.features.keys() - expected)
        if missing or extra:
            raise HTTPException(status_code=422, detail={"missing": missing, "unexpected": extra})
        try:
            p, version = champion.predict(req.features)
        except ModelUnavailable as e:
            raise HTTPException(status_code=503, detail=str(e)) from e
        pred = StressPrediction(symbol=req.symbol, ts_ms=req.ts_ms, horizon_s=cfg.label_horizon_seconds,
                                probability=p, alert=p >= threshold, threshold=threshold,
                                model_version=version)
        prob_h.observe(p)
        if pred.alert:
            alerts_c.inc()
        append("predictions.jsonl", {**pred.model_dump(), "features": req.features})
        log.info("scored", extra={"ts_ms": req.ts_ms, "probability": round(p, 4), "alert": pred.alert})
        return pred

    @app.post("/outcomes")
    def outcomes(outcome: OutcomeLabel) -> dict:
        label = "void" if outcome.label is None else str(outcome.label)
        outcomes_c.labels(label).inc()
        append("outcomes.jsonl", outcome.model_dump())
        log.info("outcome recorded", extra={"ts_ms": outcome.ts_ms, "label": label})
        return {"status": "recorded", "ts_ms": outcome.ts_ms, "label": outcome.label}

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "model_loaded": settings.model_path.exists(), "domain": cfg.domain,
                "symbol": cfg.symbol, "horizon_s": cfg.label_horizon_seconds,
                "alert_threshold": threshold, "ts": time.time()}

    return app
