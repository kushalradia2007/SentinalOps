"""The message bus between live components: one interface, swappable transports.

Topics (chunk 15): ``raw_trades`` and ``raw_depth`` (recorder-schema rows), ``features``
(``FeatureTick``), ``predictions`` (``StressPrediction`` plus the features it was scored on) and
``outcomes`` (``OutcomeLabel``). Messages are JSON-compatible dicts keyed by symbol.

- ``InMemoryBus``: asyncio queues inside one process (the default; no broker needed).
- ``KafkaBus`` (``live/kafka_bus.py``): Redpanda/Kafka, so components can run as separate processes.

Every subscriber sees every message on its topics, in publish order per topic.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator
from typing import Protocol

from sentinalops.data.recorder.messages import AGG_TRADE, DEPTH5
from sentinalops.logging import get_logger

log = get_logger("sentinalops.live.bus")

TOPICS = ("raw_trades", "raw_depth", "features", "predictions", "outcomes")
TOPIC_OF_KIND = {AGG_TRADE: "raw_trades", DEPTH5: "raw_depth"}
KIND_OF_TOPIC = {v: k for k, v in TOPIC_OF_KIND.items()}

Message = tuple[str, dict]  # (topic, value)


class Bus(Protocol):
    async def publish(self, topic: str, key: str, value: dict) -> None: ...

    async def subscribe(self, topics: list[str], group: str) -> AsyncIterator[Message]: ...

    async def close(self) -> None: ...


def check_topics(topics) -> None:
    unknown = sorted(set(topics) - set(TOPICS))
    if unknown:
        raise ValueError(f"unknown topic(s) {unknown}; topics are {list(TOPICS)}")


def stable_prediction_id(symbol: str, ts_ms: int, model_version: str) -> str:
    """Same tick, same model -> same id, so a replayed or restarted scorer can't double-count."""
    return hashlib.sha1(f"{symbol}|{ts_ms}|{model_version}".encode()).hexdigest()[:32]


_CLOSED = object()


class _Subscription:
    def __init__(self, topics: frozenset[str], maxsize: int, bus: InMemoryBus) -> None:
        self.topics = topics
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize + 1)  # +1 slot for the close marker
        self.maxsize = maxsize
        self.bus = bus
        self._holding = False  # a message was handed out and the consumer has not asked for the next

    def put(self, topic: str, value: dict) -> None:
        if self.queue.qsize() >= self.maxsize:
            self.queue.get_nowait()  # drop the oldest: for a live forecast the newest data matters
            self.queue.task_done()
            self.bus.drops[topic] = self.bus.drops.get(topic, 0) + 1
            if self.bus.drops[topic] in (1, 10, 100) or self.bus.drops[topic] % 1000 == 0:
                log.warning("bus queue full, dropped oldest", extra={"topic": topic,
                                                                    "dropped": self.bus.drops[topic]})
        self.queue.put_nowait((topic, value))

    def __aiter__(self) -> _Subscription:
        return self

    async def __anext__(self) -> Message:
        # The previous message counts as done only now that its consumer is back for more, so
        # ``drain()`` waits for messages to be handled, not just taken off the queue.
        if self._holding:
            self._holding = False
            self.queue.task_done()
        item = await self.queue.get()
        if item is _CLOSED:
            self.queue.task_done()
            raise StopAsyncIteration
        self._holding = True
        return item


class InMemoryBus:
    """Bounded per-subscriber queues; on overflow the oldest message is dropped and counted."""

    def __init__(self, maxsize: int = 10_000) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be >= 1")
        self.maxsize = maxsize
        self.drops: dict[str, int] = {}
        self._subs: list[_Subscription] = []
        self._closed = False

    async def publish(self, topic: str, key: str, value: dict) -> None:
        self.publish_nowait(topic, key, value)

    def publish_nowait(self, topic: str, key: str, value: dict) -> None:
        check_topics([topic])
        if self._closed:
            raise RuntimeError("bus is closed")
        for sub in self._subs:
            if topic in sub.topics:
                sub.put(topic, value)

    async def subscribe(self, topics: list[str], group: str) -> _Subscription:
        check_topics(topics)
        sub = _Subscription(frozenset(topics), self.maxsize, self)
        self._subs.append(sub)
        return sub

    async def drain(self) -> None:
        """Wait until every subscriber has taken every message published so far."""
        await asyncio.gather(*(s.queue.join() for s in self._subs))

    async def close(self) -> None:
        """Subscribers get what was already published, then their iteration ends."""
        self._closed = True
        for sub in self._subs:
            sub.queue.put_nowait(_CLOSED)
