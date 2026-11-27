"""Run the live pipeline (or one of its roles) until SIGINT/SIGTERM.

Roles: ``pipeline`` = feed + features + outcomes + ledger in one process. With the Kafka bus each
can also run alone (``feed``, ``features``, ``outcomes``, ``ledger``). The heal loop is always a
separate process (``live/heal_service.py``).
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import time
from pathlib import Path

from sentinalops.config import DomainConfig, Settings
from sentinalops.data.recorder import RecorderConfig
from sentinalops.live.feed import BinanceFeed
from sentinalops.live.health import pipeline_health
from sentinalops.live.pipeline import clock_loop, start_pipeline
from sentinalops.logging import get_logger
from sentinalops.serving.ledger import Ledger
from sentinalops.serving.model import ChampionModel
from sentinalops.stream.processor import StreamProcessor
from sentinalops.stream.verifier import OutcomeVerifier

log = get_logger("sentinalops.live")
ROLES = {"pipeline": {"feed", "features", "outcomes", "ledger"}}


def make_bus(kind: str, settings: Settings):
    if kind == "kafka":
        from sentinalops.live.kafka_bus import KafkaBus

        return KafkaBus(settings.kafka_broker)
    from sentinalops.live.bus import InMemoryBus

    return InMemoryBus()


def now_ms() -> int:
    return int(time.time() * 1000)


def _install_stop(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))


def build_components(roles: set[str], cfg: DomainConfig, settings: Settings):
    """(processor, verifier, ledger) for the roles this process runs; None for the others."""
    names = cfg.feature_names
    registry_root = Path(settings.model_path).parent
    proc = verifier = ledger = None
    if "features" in roles:
        proc = StreamProcessor(cfg, model=ChampionModel(settings.model_path, names),
                               shadow=ChampionModel(registry_root / "shadow.joblib", names),
                               alert_threshold=settings.effective_threshold(cfg))  # honours ALERT_THRESHOLD
    if "outcomes" in roles:
        verifier = OutcomeVerifier(cfg)
    if "ledger" in roles:
        ledger = Ledger(Path(settings.data_dir) / "ledger.sqlite")
    return proc, verifier, ledger


async def run_live(role: str, bus_kind: str, cfg: DomainConfig, settings: Settings, data_dir: Path,
                   record: bool, health_interval_s: float) -> None:
    """Run until SIGINT/SIGTERM. If any component stops on its own, shut down and raise."""
    roles = ROLES.get(role, {role})
    registry_root = Path(settings.model_path).parent
    bus = make_bus(bus_kind, settings)
    feed = None
    proc, verifier, ledger = build_components(roles, cfg, settings)
    tasks, stats = await start_pipeline(bus, proc, verifier, ledger)
    if "feed" in roles:
        feed = BinanceFeed(bus, RecorderConfig(symbols=(cfg.symbol,), data_dir=data_dir), record=record)

    stop = asyncio.Event()
    _install_stop(stop)
    side = []
    if proc is not None or verifier is not None:
        side.append(asyncio.create_task(clock_loop(bus, proc, verifier, now_ms, stats), name="clock"))

    async def health() -> None:
        while True:
            await asyncio.sleep(health_interval_s)
            log.info("live health", extra=pipeline_health(proc, verifier, stats, bus, feed, registry_root))

    side.append(asyncio.create_task(health(), name="health"))
    feed_task = asyncio.create_task(feed.run(), name="feed") if feed is not None else None
    log.info("live started", extra={"role": role, "bus": bus_kind, "roles": sorted(roles),
                                    "model_path": str(settings.model_path), "record": record})
    stop_task = asyncio.create_task(stop.wait())
    watched = [stop_task, *tasks, *side] + ([feed_task] if feed_task else [])
    done, _ = await asyncio.wait(watched, return_when=asyncio.FIRST_COMPLETED)
    failed = [t for t in done if t is not stop_task]
    if failed:  # nothing should end on its own: shut down and exit non-zero so systemd restarts us
        t = failed[0]
        err = t.exception() if not t.cancelled() else None
        log.error("live component stopped", extra={"component": t.get_name(), "error": repr(err)})

    stop_task.cancel()
    if feed is not None:
        feed.request_stop()
        await asyncio.gather(feed_task, return_exceptions=True)
    for t in side:
        t.cancel()
    await asyncio.gather(*side, return_exceptions=True)
    await bus.close()
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 30)
    log.info("live stopped", extra=pipeline_health(proc, verifier, stats, bus, feed, registry_root))
    if failed:
        t = failed[0]
        err = t.exception() if not t.cancelled() else None
        raise RuntimeError(f"live component {t.get_name()!r} stopped: {err!r}") from err
