"""Health lines for the live processes (one JSON log line every ``--health-interval-s``)."""

from __future__ import annotations

import json
from pathlib import Path

HEAL_KINDS = ("heal_attempt", "promote", "rejected", "rollback", "shadow_end")


def last_heal_event(registry_root: str | Path) -> dict | None:
    """The newest heal-related event in ``registry.json`` plus the current champion (read-only)."""
    path = Path(registry_root) / "registry.json"
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    events = [e for e in state.get("events", []) if e.get("kind") in HEAL_KINDS]
    if not events:
        return {"champion": state.get("champion")}
    return {**events[-1], "champion": state.get("champion")}


def pipeline_health(processor, verifier, stats, bus, feed, registry_root) -> dict:
    out: dict = {"published": dict(stats.published), "ledger_predictions": stats.ledger_predictions,
                 "ledger_outcomes": stats.ledger_outcomes, "ledger_errors": stats.ledger_errors,
                 "worker_errors": stats.worker_errors, "bus_drops": dict(getattr(bus, "drops", {}))}
    if processor is not None:
        p = processor.stats
        out.update(events=dict(p.events), late=dict(p.late), ticks=p.ticks, feature_ticks=p.feature_ticks,
                   suspended_ticks=p.suspended_ticks, predictions=p.predictions,
                   shadow_predictions=p.shadow_predictions, scoring_errors=p.scoring_errors,
                   model_version=stats.last_model_version, last_probability=stats.last_probability)
    if verifier is not None:
        out.update(outcomes=dict(verifier.stats.outcomes), verifier_late=verifier.stats.late)
    if feed is not None:
        r = feed.recorder.stats
        out.update(feed_published=feed.published, feed_drops=feed.feed_drops, reconnects=r.reconnects,
                   gaps=r.gaps_written, duplicates=r.duplicates, malformed=r.malformed,
                   hook_errors=r.hook_errors)
    out["heal"] = last_heal_event(registry_root)
    return out
