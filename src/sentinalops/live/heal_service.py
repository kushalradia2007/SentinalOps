"""The heal service: the self-healing loop as its own process (``python -m sentinalops.live --role heal``).

Every ``interval_min`` it runs ``HealController.tick(now)``, with the training table read from the
prediction ledger (``heal/ledger_table.py``). It never touches the bus: it reads the ledger and
writes the file registry, and the live pipeline hot-reloads ``champion.joblib`` on promotion.

**Bootstrap and manual upgrades.** If ``champion.joblib`` is not the registry's champion (no
registry yet, or the file was replaced by hand, e.g. ``python -m sentinalops.training.train``),
it is registered as a new version and promoted, with the holdout AP the training command stamped
on it as the reference for degradation. Without that stamp the reference is NaN and the loop
will never call the model degraded, so a warning is logged.

Retraining runs single-threaded (the CLI caps OpenMP/BLAS threads) so the live pipeline keeps a CPU.
"""

from __future__ import annotations

import asyncio
import math
import time
from pathlib import Path

import joblib

from sentinalops.config import DomainConfig, Settings
from sentinalops.heal.controller import HealController, TickResult
from sentinalops.heal.ledger_table import ledger_table
from sentinalops.heal.registry import ModelRegistry
from sentinalops.live.runner import _install_stop
from sentinalops.logging import get_logger
from sentinalops.serving.ledger import Ledger

log = get_logger("sentinalops.heal.service")
DAY_MS = 86_400_000


class HealService:
    def __init__(self, cfg: DomainConfig, ledger: Ledger, registry: ModelRegistry, replay_days: float = 7,
                 seed: int = 0) -> None:
        self.cfg, self.ledger, self.registry = cfg, ledger, registry
        lookback = int(cfg.retrain_window_hours * 3_600_000 + replay_days * DAY_MS)
        names = cfg.feature_names
        self.controller = HealController(cfg, ledger, registry,
                                         lambda now: ledger_table(ledger, now - lookback, now, names), seed)
        self.last: TickResult | None = None

    def bootstrap_if_needed(self, now_ms: int) -> str | None:
        """Register and promote ``champion.joblib`` if the registry does not know it yet."""
        path = self.registry.champion_path
        if not path.exists():
            return None
        state = self.registry.state()
        model = joblib.load(path)
        if state.champion is not None and getattr(model, "sentinal_version_", None) == state.champion:
            return None
        ap = float(getattr(model, "sentinal_holdout_ap_", math.nan))
        if math.isnan(ap):
            log.warning("champion has no holdout AP: the heal loop cannot detect degradation",
                        extra={"path": str(path)})
        reason = "bootstrap" if state.champion is None else "manual install"
        version = self.registry.register(model, {
            "holdout_ap": ap, "source": reason, "model_name": getattr(model, "sentinal_model_name_", None),
            "params": getattr(model, "sentinal_model_params_", {})}, now_ms)
        self.registry.promote(version, now_ms, reason, shadow_ms=0)
        return version

    def tick(self, now_ms: int) -> TickResult:
        self.bootstrap_if_needed(now_ms)
        self.last = self.controller.tick(now_ms)
        return self.last

    def health(self) -> dict:
        s, r = self.registry.state(), self.last
        d = r.decision if r else None
        return {"champion": s.champion, "shadow": s.shadow, "action": r.action if r else None,
                "reason": d.reason if d else None, "matured": d.matured if d else None,
                "live_ap": d.degradation.current.ap if d and d.degradation else None,
                "promotions": self.registry.count("promote"), "rollbacks": self.registry.count("rollback")}


async def run_heal(
    cfg: DomainConfig, settings: Settings, interval_min: float, health_interval_s: float
) -> None:
    ledger = Ledger(Path(settings.data_dir) / "ledger.sqlite")
    service = HealService(cfg, ledger, ModelRegistry(Path(settings.model_path).parent))
    stop = asyncio.Event()
    _install_stop(stop)
    log.info("heal service started", extra={"interval_min": interval_min,
                                            "registry": str(Path(settings.model_path).parent)})
    while not stop.is_set():
        try:
            await asyncio.to_thread(service.tick, int(time.time() * 1000))
        except Exception as e:  # noqa: BLE001 - a failed cycle is logged; the next one runs anyway
            log.error("heal cycle failed", extra={"error": repr(e)})
        log.info("heal health", extra=service.health())
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_min * 60)
        except TimeoutError:
            pass
    log.info("heal service stopped", extra=service.health())
