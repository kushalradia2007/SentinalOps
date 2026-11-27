"""Live pipeline workers: thin async loops around the existing processor, verifier and ledger.

```
raw_trades, raw_depth ──► features_worker (StreamProcessor) ──► features, predictions
raw_depth            ──► outcomes_worker (OutcomeVerifier)  ──► outcomes
predictions, outcomes ──► ledger_writer (Ledger, SQLite)
clock: advance(now) on processor and verifier, so ticks keep coming while the feed is silent
```

No feature or label logic lives here; it is all in ``StreamProcessor`` / ``OutcomeVerifier``,
which the online/offline consistency tests already guard. A ``predictions`` message carries the
features it was scored on, so the ledger writer never joins topics (Kafka does not order across
topics). Prediction ids are deterministic per (symbol, tick, model version).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field

from sentinalops.live.bus import KIND_OF_TOPIC, Bus, stable_prediction_id
from sentinalops.logging import get_logger
from sentinalops.schemas import FeatureTick, OutcomeLabel, StressPrediction
from sentinalops.serving.ledger import Ledger
from sentinalops.stream.processor import FEATURES_TOPIC, PREDICTIONS_TOPIC, StreamProcessor
from sentinalops.stream.verifier import OUTCOMES_TOPIC, OutcomeVerifier

log = get_logger("sentinalops.live")


@dataclass
class PipelineStats:
    published: dict[str, int] = field(default_factory=dict)
    ledger_predictions: int = 0
    ledger_outcomes: int = 0
    ledger_errors: int = 0
    worker_errors: int = 0
    last_probability: float | None = None
    last_model_version: str | None = None

    def count(self, topic: str) -> None:
        self.published[topic] = self.published.get(topic, 0) + 1


async def publish_processor_outputs(bus: Bus, outputs: list, stats: PipelineStats) -> None:
    """Publish a processor's outputs; predictions carry their tick's features and a stable id."""
    features: dict[int, dict[str, float]] = {}
    for topic, msg in outputs:
        if topic == FEATURES_TOPIC:
            assert isinstance(msg, FeatureTick)
            features[msg.ts_ms] = msg.features
            value = msg.model_dump(mode="json")
        else:
            assert topic == PREDICTIONS_TOPIC and isinstance(msg, StressPrediction)
            msg = msg.model_copy(update={
                "prediction_id": stable_prediction_id(msg.symbol, msg.ts_ms, msg.model_version)})
            value = {**msg.model_dump(mode="json"), "features": features.get(msg.ts_ms)}
            stats.last_probability, stats.last_model_version = msg.probability, msg.model_version
        await bus.publish(topic, msg.symbol, value)
        stats.count(topic)


async def publish_outcomes(bus: Bus, outcomes: list[OutcomeLabel], stats: PipelineStats) -> None:
    for o in outcomes:
        await bus.publish(OUTCOMES_TOPIC, o.symbol, o.model_dump(mode="json"))
        stats.count(OUTCOMES_TOPIC)


async def features_worker(bus: Bus, sub, processor: StreamProcessor, stats: PipelineStats) -> None:
    async for topic, row in sub:
        try:
            outputs = processor.on_event(KIND_OF_TOPIC[topic], row)
        except Exception as e:  # noqa: BLE001 - one bad row must not stop the pipeline
            stats.worker_errors += 1
            log.error("features worker failed on a row", extra={"error": repr(e)})
            continue
        await publish_processor_outputs(bus, outputs, stats)


async def outcomes_worker(bus: Bus, sub, verifier: OutcomeVerifier, stats: PipelineStats) -> None:
    async for topic, row in sub:
        try:
            outcomes = verifier.on_event(KIND_OF_TOPIC[topic], row)
        except Exception as e:  # noqa: BLE001
            stats.worker_errors += 1
            log.error("outcomes worker failed on a row", extra={"error": repr(e)})
            continue
        await publish_outcomes(bus, outcomes, stats)


def _write_batch(ledger: Ledger, batch: list[tuple[str, dict]]) -> tuple[int, int]:
    preds, outs = [], []
    for topic, value in batch:
        if topic == PREDICTIONS_TOPIC:
            value = dict(value)
            features = value.pop("features", None)
            preds.append((StressPrediction(**value), features))
        else:
            outs.append(OutcomeLabel(**value))
    return ledger.add_many(preds, outs)


async def ledger_writer(sub, ledger: Ledger, stats: PipelineStats, max_batch: int = 1000) -> None:
    """Write what has arrived in one SQLite transaction, off the event loop.

    A pump moves messages into a bounded local queue; the writer takes everything waiting there
    (up to ``max_batch``) per transaction, so a burst costs one commit instead of one per message.
    """
    pending: asyncio.Queue = asyncio.Queue(maxsize=10_000)
    end = object()

    async def pump() -> None:
        try:
            async for msg in sub:
                await pending.put(msg)
        finally:
            await pending.put(end)

    pump_task = asyncio.create_task(pump())
    done = False
    while not done:
        batch = [await pending.get()]
        while not pending.empty() and len(batch) < max_batch:
            batch.append(pending.get_nowait())
        if batch[-1] is end:
            batch.pop()
            done = True
        if not batch:
            continue
        try:
            added_p, added_o = await asyncio.to_thread(_write_batch, ledger, batch)
        except Exception as e:  # noqa: BLE001 - keep writing; the error is counted and logged
            stats.ledger_errors += 1
            log.error("ledger write failed", extra={"messages": len(batch), "error": repr(e)})
            continue
        stats.ledger_predictions += added_p
        stats.ledger_outcomes += added_o
    await pump_task


async def clock_step(bus: Bus, processor: StreamProcessor | None, verifier: OutcomeVerifier | None,
                     now_ms: int, stats: PipelineStats) -> None:
    """Move the watermarks to ``now_ms`` and publish whatever ticks became due (None = not here)."""
    if processor is not None:
        await publish_processor_outputs(bus, processor.advance(now_ms), stats)
    if verifier is not None:
        await publish_outcomes(bus, verifier.advance(now_ms), stats)


async def clock_loop(bus: Bus, processor: StreamProcessor | None, verifier: OutcomeVerifier | None,
                     now_ms: Callable[[], int], stats: PipelineStats, interval_s: float = 1.0) -> None:
    while True:
        await asyncio.sleep(interval_s)
        await clock_step(bus, processor, verifier, now_ms(), stats)


async def start_pipeline(
    bus: Bus,
    processor: StreamProcessor | None,
    verifier: OutcomeVerifier | None,
    ledger: Ledger | None,
    stats: PipelineStats | None = None,
) -> tuple[list[asyncio.Task], PipelineStats]:
    """Subscribe every worker first (so nothing published afterwards is missed), then start them.

    Pass ``None`` for a component another process runs (Kafka mode). The tasks end when the bus
    is closed.
    """
    stats = stats or PipelineStats()
    plan = []
    if processor is not None:
        plan.append((features_worker, ["raw_trades", "raw_depth"], "features", (bus,), (processor, stats)))
    if verifier is not None:
        plan.append((outcomes_worker, ["raw_depth"], "outcomes", (bus,), (verifier, stats)))
    if ledger is not None:
        plan.append((ledger_writer, [PREDICTIONS_TOPIC, OUTCOMES_TOPIC], "ledger", (), (ledger, stats)))
    subs = [await bus.subscribe(topics, group=group) for _, topics, group, _, _ in plan]
    tasks = [asyncio.create_task(fn(*pre, sub, *post), name=group)
             for (fn, _, group, pre, post), sub in zip(plan, subs, strict=True)]
    return tasks, stats
