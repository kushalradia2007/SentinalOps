"""Kafka / Redpanda transport for the live bus (``--bus kafka``; needs ``pip install -e '.[stream]'``).

Same contract as ``InMemoryBus``: JSON values keyed by symbol, every subscriber group sees every
message on its topics, in order per topic (one partition per topic). Differences:

- Components can run as separate processes (``--role feed|features|outcomes|ledger``).
- Consumers use a group per role and commit offsets, so a restarted role resumes where it left off.
  Delivery is at least once; the ledger and the deterministic prediction ids make replays harmless.
- A new group starts at the newest message (``auto_offset_reset='latest'``): a live forecaster has
  no use for a backlog from before it existed.
- ``subscribe`` returns only after partitions are assigned, so nothing published afterwards is missed.
- ``close`` lets each subscriber read up to the end offsets it saw at close time, then ends it.

Topics are created on first use with one partition; raw topics keep at most ``raw_retention_bytes``.
"""

from __future__ import annotations

import asyncio
import json

from sentinalops.live.bus import TOPICS, Message, check_topics
from sentinalops.logging import get_logger

log = get_logger("sentinalops.live.kafka")
RAW_TOPICS = ("raw_trades", "raw_depth")


def encode(value: dict) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


def decode(raw: bytes) -> dict:
    return json.loads(raw)


def _aiokafka():
    try:
        import aiokafka
        import aiokafka.admin
    except ImportError as e:
        raise RuntimeError("the Kafka bus needs aiokafka: pip install -e '.[stream]'") from e
    return aiokafka


class _KafkaSubscription:
    def __init__(self, bus: KafkaBus, consumer) -> None:
        self.bus, self.consumer = bus, consumer
        self._buffer: list[Message] = []
        self._stop_at: dict | None = None  # end offsets captured when the bus closed
        self._positions: dict = {}

    def __aiter__(self) -> _KafkaSubscription:
        return self

    async def __anext__(self) -> Message:
        while not self._buffer:
            if self.bus.closing and self._stop_at is None:
                self._stop_at = await self.consumer.end_offsets(list(self.consumer.assignment()))
            if self._stop_at is not None:
                self._positions = {tp: await self.consumer.position(tp) for tp in self._stop_at}
                if all(self._positions[tp] >= end for tp, end in self._stop_at.items()):
                    await self.consumer.stop()
                    raise StopAsyncIteration
            batches = await self.consumer.getmany(timeout_ms=200)
            for tp, records in batches.items():
                self._buffer += [(tp.topic, decode(r.value)) for r in records]
        return self._buffer.pop(0)


class KafkaBus:
    def __init__(self, broker: str, client_id: str = "sentinalops",
                 raw_retention_bytes: int = 5 * 2**30) -> None:
        self._ak = _aiokafka()
        self.broker, self.client_id = broker, client_id
        self.raw_retention_bytes = raw_retention_bytes
        self.closing = False
        self.drops: dict[str, int] = {}  # the in-memory overflow counter has no Kafka equivalent
        self._producer = None
        self._consumers: list = []
        self._topics_ready = False
        self._lock = asyncio.Lock()

    async def _ensure_topics(self) -> None:
        if self._topics_ready:
            return
        admin = self._ak.admin.AIOKafkaAdminClient(bootstrap_servers=self.broker, client_id=self.client_id)
        await admin.start()
        try:
            existing = set(await admin.list_topics())
            raw_cfg = {"retention.bytes": str(self.raw_retention_bytes)}
            new = [self._ak.admin.NewTopic(t, num_partitions=1, replication_factor=1,
                                           topic_configs=raw_cfg if t in RAW_TOPICS else {})
                   for t in TOPICS if t not in existing]
            if new:
                await admin.create_topics(new)
                log.info("kafka topics created", extra={"topics": [t.name for t in new]})
        finally:
            await admin.close()
        self._topics_ready = True

    async def publish(self, topic: str, key: str, value: dict) -> None:
        check_topics([topic])
        async with self._lock:
            if self._producer is None:
                await self._ensure_topics()
                self._producer = self._ak.AIOKafkaProducer(bootstrap_servers=self.broker, acks="all",
                                                           client_id=self.client_id)
                await self._producer.start()
        await self._producer.send_and_wait(topic, encode(value), key=key.encode())

    async def subscribe(self, topics: list[str], group: str) -> _KafkaSubscription:
        check_topics(topics)
        await self._ensure_topics()
        assigned = asyncio.Event()

        class _Listener(self._ak.ConsumerRebalanceListener):
            async def on_partitions_revoked(self, revoked):
                pass

            async def on_partitions_assigned(self, partitions):
                if partitions:
                    assigned.set()

        consumer = self._ak.AIOKafkaConsumer(
            bootstrap_servers=self.broker, group_id=f"{self.client_id}-{group}", client_id=self.client_id,
            auto_offset_reset="latest", enable_auto_commit=True)
        await consumer.start()
        consumer.subscribe(topics=list(topics), listener=_Listener())
        sub = _KafkaSubscription(self, consumer)
        while not assigned.is_set():  # polling drives the group join; keep whatever arrives
            batches = await consumer.getmany(timeout_ms=100)
            for tp, records in batches.items():
                sub._buffer += [(tp.topic, decode(r.value)) for r in records]
        self._consumers.append(consumer)
        return sub

    async def close(self) -> None:
        self.closing = True
        if self._producer is not None:
            await self._producer.stop()
