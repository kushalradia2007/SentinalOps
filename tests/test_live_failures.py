"""Failure handling in the live path: crashes must surface (so systemd restarts), never hang."""

import asyncio
import time
import types

import pytest

from recorder_helpers import LOCAL, FakeExchange, depth_frame
from sentinalops.config import Settings, get_domain_config
from sentinalops.data.recorder import RecorderConfig
from sentinalops.live import kafka_bus as K
from sentinalops.live import runner as R
from sentinalops.live.bus import InMemoryBus
from sentinalops.live.feed import BinanceFeed

CFG = get_domain_config()


class BrokenBus(InMemoryBus):
    async def publish(self, topic, key, value):
        raise ConnectionError("broker went away")


def test_feed_exits_when_publishing_fails(tmp_path):
    async def script(ws, n):
        for i in range(1, 50):
            await ws.send(depth_frame(i))
        await asyncio.sleep(30)

    async def main():
        async with FakeExchange(script) as ex:
            cfg = RecorderConfig(symbols=("BTCUSDT",), data_dir=tmp_path, base_url=ex.base_url,
                                 allowed_hosts=LOCAL, flush_interval_s=3600, stale_after_s=5,
                                 stats_interval_s=3600, open_timeout_s=5)
            feed = BinanceFeed(BrokenBus(), cfg)
            async with asyncio.timeout(15):
                await feed.run()

    t0 = time.monotonic()
    with pytest.raises(ConnectionError, match="broker went away"):
        asyncio.run(main())
    assert time.monotonic() - t0 < 5, "the feed must exit as soon as publishing fails, not hang"


def test_runner_passes_alert_threshold_override(tmp_path):
    settings = Settings(model_path=tmp_path / "m" / "champion.joblib", data_dir=tmp_path,
                        alert_threshold=0.83)
    proc, verifier, ledger = R.build_components({"features", "outcomes", "ledger"}, CFG, settings)
    assert proc.threshold == 0.83 and verifier is not None and ledger is not None


def test_runner_exits_when_a_worker_crashes(tmp_path, monkeypatch):
    class IdleFeed:
        def __init__(self, *a, **k):
            self.stop = asyncio.Event()
            self.recorder = types.SimpleNamespace(stats=types.SimpleNamespace(
                reconnects=0, gaps_written=0, duplicates=0, malformed=0, hook_errors=0))
            self.published = self.feed_drops = 0

        def request_stop(self):
            self.stop.set()

        async def run(self):
            await self.stop.wait()

    real_start = R.start_pipeline

    async def start_with_a_crashing_worker(*a, **k):
        tasks, stats = await real_start(*a, **k)

        async def crash():
            await asyncio.sleep(0.2)
            raise RuntimeError("worker bug")

        return [*tasks, asyncio.create_task(crash(), name="ledger")], stats

    monkeypatch.setattr(R, "BinanceFeed", IdleFeed)
    monkeypatch.setattr(R, "start_pipeline", start_with_a_crashing_worker)
    settings = Settings(model_path=tmp_path / "m" / "champion.joblib", data_dir=tmp_path)

    async def main():
        async with asyncio.timeout(15):
            await R.run_live("pipeline", "memory", CFG, settings, tmp_path / "live", False, 3600)

    with pytest.raises(RuntimeError, match="ledger"):
        asyncio.run(main())


# ---- KafkaBus against a fake aiokafka ------------------------------------------------------------
class _TP:
    def __init__(self, topic):
        self.topic = topic

    def __hash__(self):
        return hash(self.topic)

    def __eq__(self, other):
        return self.topic == other.topic


def fake_aiokafka(assign: bool, batches=()):
    consumers = []

    class Consumer:
        def __init__(self, *a, **k):
            self.stopped, self.listener, self.batches = False, None, list(batches)
            consumers.append(self)

        async def start(self):
            pass

        def subscribe(self, topics, listener):
            self.topics, self.listener = topics, listener

        async def getmany(self, timeout_ms=0):
            if assign and self.listener is not None:
                listener, self.listener = self.listener, None
                await listener.on_partitions_assigned({_TP(t) for t in self.topics})
            if self.batches:
                return self.batches.pop(0)
            await asyncio.sleep(timeout_ms / 1000)
            return {}

        def assignment(self):
            return {_TP(t) for t in self.topics}

        async def stop(self):
            self.stopped = True

    class Admin:
        def __init__(self, *a, **k):
            pass

        async def start(self):
            pass

        async def close(self):
            pass

        async def list_topics(self):
            return list(K.TOPICS)

    module = types.SimpleNamespace(AIOKafkaConsumer=Consumer, AIOKafkaProducer=None,
                                   ConsumerRebalanceListener=object,
                                   admin=types.SimpleNamespace(AIOKafkaAdminClient=Admin, NewTopic=None))
    return module, consumers


def test_kafka_subscribe_times_out_without_assignment(monkeypatch):
    module, _ = fake_aiokafka(assign=False)
    monkeypatch.setattr(K, "_aiokafka", lambda: module)

    async def main():
        bus = K.KafkaBus("broker:9092", assign_timeout_s=0.3)
        await bus.subscribe(["raw_depth"], group="g")

    with pytest.raises(RuntimeError, match="no partitions assigned"):
        asyncio.run(main())


def test_kafka_close_stops_every_consumer(monkeypatch):
    module, consumers = fake_aiokafka(assign=True)
    monkeypatch.setattr(K, "_aiokafka", lambda: module)

    async def main():
        bus = K.KafkaBus("broker:9092", close_timeout_s=0.2)
        await bus.subscribe(["raw_depth"], group="a")
        await bus.subscribe(["outcomes"], group="b")
        await bus.close()  # nobody iterated: consumers must not leak

    asyncio.run(main())
    assert len(consumers) == 2 and all(c.stopped for c in consumers)


def test_kafka_batches_are_merged_in_receive_order(monkeypatch):
    def rec(t):
        return types.SimpleNamespace(value=K.encode({"recv_time_ms": t}))

    batch = {_TP("raw_depth"): [rec(3), rec(5)], _TP("raw_trades"): [rec(4), rec(6)]}
    module, _ = fake_aiokafka(assign=True, batches=[{}, batch])
    monkeypatch.setattr(K, "_aiokafka", lambda: module)

    async def main():
        bus = K.KafkaBus("broker:9092")
        sub = await bus.subscribe(["raw_depth", "raw_trades"], group="f")
        return [(await sub.__anext__())[1]["recv_time_ms"] for _ in range(4)]

    assert asyncio.run(main()) == [3, 4, 5, 6]
