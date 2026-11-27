"""The heal loop (chunk 42): decide, retrain, gate, promote; then watch the shadow period.

``HealController.tick(now_ms)`` is the whole loop and is safe to call on a schedule (a Prefect
flow, a cron job, or a test with a fake clock):

1. During a shadow period: once it ends, compare the new champion with the old one on the
   same ticks and roll back if the new one is worse by ``rollback_margin`` (chunk 45).
2. Otherwise run the trigger rule:
   - the champion's live AP over the last ``metrics_window_minutes`` is degraded
     (``monitor.degradation``), **and**
   - at least ``min_matured_labels`` outcomes have matured since it was promoted, **and**
   - no attempt was made within ``cooldown_minutes``, **and**
   - this champion has not already been retrained for the same evidence (idempotency).
   Drift alone never triggers a retrain.
3. On a trigger: assemble the data, train a challenger, run the gates, register it, and
   promote it only if every gate passes. The gate report is saved next to the registry.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from sentinalops.config import DomainConfig
from sentinalops.heal.monitor import Degradation, degradation, window_scores
from sentinalops.heal.registry import ModelRegistry
from sentinalops.heal.retrain import GateReport, assemble, evaluate_gates, train_challenger
from sentinalops.logging import get_logger
from sentinalops.serving.ledger import Ledger

log = get_logger("sentinalops.heal")
MINUTE_MS = 60_000


@dataclass
class Decision:
    fire: bool
    reason: str
    degradation: Degradation | None = None
    matured: int = 0


@dataclass
class TickResult:
    action: str  # idle | retrained_promoted | retrained_rejected | shadow_kept | rolled_back | waiting
    decision: Decision | None = None
    version: str | None = None
    report: GateReport | None = None
    details: dict = field(default_factory=dict)


class HealController:
    def __init__(
        self,
        cfg: DomainConfig,
        ledger: Ledger,
        registry: ModelRegistry,
        load_table: Callable[[int], pd.DataFrame],
        seed: int = 0,
    ) -> None:
        self.cfg, self.h = cfg, cfg.heal
        self.ledger, self.registry = ledger, registry
        self.load_table = load_table  # now_ms -> chunk 12 training table up to now
        self.seed = seed

    # ---- setup -------------------------------------------------------------------------------
    def bootstrap(self, model, holdout_ap: float, now_ms: int) -> str:
        """Register and promote the first champion (no shadow)."""
        version = self.registry.register(model, {"holdout_ap": holdout_ap, "source": "bootstrap"}, now_ms)
        self.registry.promote(version, now_ms, "bootstrap", shadow_ms=0)
        return version

    # ---- 42.2-42.5 trigger ---------------------------------------------------------------------
    def decide(self, now_ms: int) -> Decision:
        s = self.registry.state()
        if s.champion is None:
            return Decision(False, "no_champion")
        if s.shadow is not None:
            return Decision(False, "shadow_period")
        champion = s.champion
        since = max(s.promoted_at_ms or 0, now_ms - self.h.metrics_window_minutes * MINUTE_MS)
        current = window_scores(self.ledger, since, now_ms, champion)
        deg = degradation(current, s.versions[champion].get("holdout_ap", math.nan), self.h)
        matured = window_scores(self.ledger, s.promoted_at_ms or 0, now_ms, champion).n
        attempts = [e for e in s.events if e["kind"] == "heal_attempt"]
        last = attempts[-1] if attempts else None

        if not deg.degraded:
            return Decision(False, deg.reason, deg, matured)
        if matured < self.h.min_matured_labels:
            return Decision(False, "too_few_matured_labels", deg, matured)
        if last and now_ms - last["ts_ms"] < self.h.cooldown_minutes * MINUTE_MS:
            return Decision(False, "cooldown", deg, matured)
        same_evidence = matured - (last or {}).get("matured", 0) < self.h.min_matured_labels
        if last and last.get("champion") == champion and same_evidence:
            return Decision(False, "already_attempted", deg, matured)  # 43.6: same evidence, same answer
        return Decision(True, "degraded", deg, matured)

    # ---- one scheduled run ---------------------------------------------------------------------
    def tick(self, now_ms: int) -> TickResult:
        s = self.registry.state()
        if s.shadow is not None:
            return self._review_shadow(now_ms)
        decision = self.decide(now_ms)
        if not decision.fire:
            return TickResult("idle", decision)
        return self._retrain(now_ms, decision)

    def _retrain(self, now_ms: int, decision: Decision) -> TickResult:
        champion = self.registry.state().champion
        self.registry.log_event(now_ms, "heal_attempt", champion=champion, matured=decision.matured,
                                live_ap=decision.degradation.current.ap)
        ts = assemble(self.load_table(now_ms), now_ms, self.cfg, self.seed)
        champion_model = self.registry.load(champion)
        challenger = train_challenger(ts, self.cfg, self.seed, champion=champion_model)
        report = evaluate_gates(champion_model, challenger, ts, self.cfg)
        version = self.registry.register(challenger, {
            "model_name": getattr(challenger, "sentinal_model_name_", None),
            "params": getattr(challenger, "sentinal_model_params_", {}),
            "holdout_ap": report.challenger["current"]["ap"], "source": "heal", "parent": champion,
            "window": list(ts.window), "counts": ts.counts, "gates_passed": report.passed,
        }, now_ms)
        self._save_report(version, report)
        if report.passed:
            self.registry.promote(version, now_ms, "heal: gates passed", self.h.shadow_minutes * MINUTE_MS)
            return TickResult("retrained_promoted", decision, version, report)
        self.registry.log_event(now_ms, "rejected", version=version, champion=champion)
        log.info("challenger rejected", extra={"version": version, "champion": champion})
        return TickResult("retrained_rejected", decision, version, report)

    def _save_report(self, version: str, report: GateReport) -> None:
        out = Path(self.registry.root) / "reports"
        out.mkdir(exist_ok=True)
        (out / f"{version}.json").write_text(report.to_json(), encoding="utf-8")
        (out / f"{version}.md").write_text(report.to_markdown(), encoding="utf-8")

    # ---- 45.2 / 45.3 shadow review ---------------------------------------------------------------
    def _review_shadow(self, now_ms: int) -> TickResult:
        s = self.registry.state()
        if now_ms < s.shadow_until_ms:
            return TickResult("waiting", details={"shadow_until_ms": s.shadow_until_ms})
        new = window_scores(self.ledger, s.promoted_at_ms, now_ms, s.champion)
        old = window_scores(self.ledger, s.promoted_at_ms, now_ms, s.shadow)
        details = {"new": new.to_dict(), "old": old.to_dict()}
        enough = min(new.positives, old.positives) >= self.h.min_positives
        if enough and new.ap < old.ap - self.h.rollback_margin:
            restored = self.registry.rollback(now_ms, f"shadow: AP {new.ap:.3f} < {old.ap:.3f}")
            return TickResult("rolled_back", version=restored, details=details)
        self.registry.end_shadow(now_ms)
        details["verdict"] = "better or equal" if enough else "too few events to judge; keeping"
        return TickResult("shadow_kept", version=s.champion, details=details)
