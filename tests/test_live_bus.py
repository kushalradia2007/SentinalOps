"""The live bus: shared contract (memory and Kafka) and the in-memory overflow policy."""

import asyncio
import os

import pytest

from sentinalops.live.bus import InMemoryBus, stable_prediction_id

BUSES = ["memory"] + (["kafka"] if os.environ.get("KAFKA_BROKER") else [])


def make_bus(kind: str, **kw):
    if kind == "memory":
        return InMemoryBus(**kw)
    from sentinalops.live.kafka_bus import KafkaBus

    return KafkaBus(os.environ["KAFKA_BROKER"], client_id=f"test-{os.getpid()}")


async def take(sub, n, timeout=10.0):
    out = []
    async with asyncio.timeout(timeout):
        async for msg in sub:
            out.append(msg)
            if len(out) == n:
                break
    return out


@pytest.mark.parametrize("kind", BUSES)
def test_bus_fanout_and_order(kind):
    async def main():
        bus = make_bus(kind)
        a = await bus.subscribe(["raw_depth", "raw_trades"], group="a")
        b = await bus.subscribe(["raw_depth"], group="b")
        for i in range(5):
            await bus.publish("raw_depth", "BTCUSDT", {"i": i})
        await bus.publish("raw_trades", "BTCUSDT", {"i": 99})
        got_a, got_b = await take(a, 6), await take(b, 5)
        await bus.close()
        return got_a, got_b

    got_a, got_b = asyncio.run(main())
    assert [v["i"] for t, v in got_a if t == "raw_depth"] == [0, 1, 2, 3, 4]
    assert ("raw_trades", {"i": 99}) in got_a
    assert [v["i"] for _, v in got_b] == [0, 1, 2, 3, 4]


@pytest.mark.parametrize("kind", BUSES)
def test_bus_close_ends_subscription(kind):
    async def main():
        bus = make_bus(kind)
        sub = await bus.subscribe(["outcomes"], group="c")
        await bus.publish("outcomes", "BTCUSDT", {"x": 1})
        await bus.close()
        async with asyncio.timeout(10):
            return [m async for m in sub]

    assert asyncio.run(main()) == [("outcomes", {"x": 1})]


def test_bus_overflow_drops_oldest():
    async def main():
        bus = InMemoryBus(maxsize=3)
        sub = await bus.subscribe(["raw_depth"], group="slow")
        for i in range(5):
            await bus.publish("raw_depth", "BTCUSDT", {"i": i})
        got = await take(sub, 3)
        return bus, got

    bus, got = asyncio.run(main())
    assert [v["i"] for _, v in got] == [2, 3, 4]
    assert bus.drops["raw_depth"] == 2


def test_bus_rejects_unknown_topic():
    async def main():
        bus = InMemoryBus()
        with pytest.raises(ValueError, match="unknown topic"):
            await bus.publish("nope", "k", {})
        with pytest.raises(ValueError, match="unknown topic"):
            await bus.subscribe(["nope"], group="g")

    asyncio.run(main())


def test_drain_waits_for_consumers():
    async def main():
        bus = InMemoryBus()
        sub = await bus.subscribe(["features"], group="d")
        seen = []

        async def consume():
            async for _, v in sub:
                await asyncio.sleep(0)
                seen.append(v["i"])

        task = asyncio.create_task(consume())
        for i in range(20):
            await bus.publish("features", "BTCUSDT", {"i": i})
        await bus.drain()
        n = len(seen)
        await bus.close()
        await task
        return n

    assert asyncio.run(main()) == 20


def test_stable_prediction_id_deterministic():
    a = stable_prediction_id("BTCUSDT", 1_763_200_800_000, "v001")
    assert a == stable_prediction_id("BTCUSDT", 1_763_200_800_000, "v001")
    assert a != stable_prediction_id("BTCUSDT", 1_763_200_801_000, "v001")
    assert a != stable_prediction_id("BTCUSDT", 1_763_200_800_000, "v002")
    assert len(a) == 32 and int(a, 16) >= 0


def test_kafka_bus_import_error_message(monkeypatch):
    import sys

    from sentinalops.live import kafka_bus

    monkeypatch.setitem(sys.modules, "aiokafka", None)  # as if aiokafka were not installed
    with pytest.raises(RuntimeError, match=r"pip install .*\[stream\]"):
        kafka_bus.KafkaBus("localhost:9092")


def test_kafka_bus_serialises_json_and_rejects_unknown_topics():
    from sentinalops.live.kafka_bus import decode, encode

    assert decode(encode({"a": 1.5, "b": None})) == {"a": 1.5, "b": None}

    async def main():
        from sentinalops.live.kafka_bus import KafkaBus

        bus = KafkaBus("localhost:1")  # never connects: the topic check comes first
        with pytest.raises(ValueError, match="unknown topic"):
            await bus.publish("nope", "k", {})

    asyncio.run(main())
