"""Kafka / Redpanda transport for the live bus (``--bus kafka``; needs ``pip install -e '.[stream]'``).

Same contract as ``InMemoryBus``: JSON values keyed by symbol, every subscriber group sees every
message on its topics, in order per topic (one partition per topic). Differences:

- Components can run as separate processes (``--role feed|features|outcomes|ledger``).
- Consumers use a group per role and commit offsets, so a restarted role resumes where it left off.
  Delivery is at least once; the ledger and the deterministic prediction ids make replays harmless.
- A new group starts at the newest message (``auto_offset_reset='latest'``): a live forecaster has
  no use for a backlog from before it existed.
- ``subscribe`` returns only after partitions are assigned, so nothing published afterwards is missed.
- ``close`` lets each subscriber read up to the end offsets it saw at close time (for at most
  ``close_timeout_s``), then stops every consumer, including ones nobody is iterating.
- Records from one fetch are merged across topics by ``recv_time_ms``, so after consumer lag the
  depth and trade streams reach the processor interleaved rather than one topic far ahead.

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


def _merge(batches: dict) -> list[Message]:
    """One fetch's records, interleaved across topics by receive time (stable within a topic)."""
    msgs = [(tp.topic, decode(r.value)) for tp, records in batches.items() for r in records]
    if len(batches) > 1:
        msgs.sort(key=lambda m: m[1].get("recv_time_ms", 0))
    return msgs


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
        self.done = asyncio.Event()

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
                    self.done.set()
                    raise StopAsyncIteration
            self._buffer += _merge(await self.consumer.getmany(timeout_ms=200))
        return self._buffer.pop(0)


class KafkaBus:
    def __init__(self, broker: str, client_id: str = "sentinalops",
                 raw_retention_bytes: int = 5 * 2**30, assign_timeout_s: float = 60.0,
                 close_timeout_s: float = 10.0) -> None:
        self._ak = _aiokafka()
        self.broker, self.client_id = broker, client_id
        self.raw_retention_bytes = raw_retention_bytes
        self.assign_timeout_s, self.close_timeout_s = assign_timeout_s, close_timeout_s
        self.closing = False
        self._subs: list[_KafkaSubscription] = []
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
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.assign_timeout_s
        while not assigned.is_set():  # polling drives the group join; keep whatever arrives
            if loop.time() > deadline:
                await consumer.stop()
                raise RuntimeError(f"no partitions assigned to group {self.client_id}-{group} within "
                                   f"{self.assign_timeout_s:.0f} s; is the broker at {self.broker} up?")
            sub._buffer += _merge(await consumer.getmany(timeout_ms=100))
        self._consumers.append(consumer)
        self._subs.append(sub)
        return sub

    async def close(self) -> None:
        self.closing = True
        if self._producer is not None:
            await self._producer.stop()
        pending = [s.done.wait() for s in self._subs if not s.done.is_set()]
        if pending:  # give active readers time to reach the end offsets
            try:
                await asyncio.wait_for(asyncio.gather(*pending), self.close_timeout_s)
            except TimeoutError:
                pass
        for consumer in self._consumers:  # then stop every consumer, iterated or not
            try:
                await consumer.stop()
            except Exception as e:  # noqa: BLE001 - already stopped or broker gone
                log.debug("consumer stop failed", extra={"error": repr(e)})
