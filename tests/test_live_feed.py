"""Binance feed -> bus, the live CLI's argument rules, and the health line."""

import asyncio
import json

import pytest
from sentinalops.live.feed import BinanceFeed

from recorder_helpers import LOCAL, FakeExchange, depth_frame, trade_frame
from sentinalops.data.recorder import RecorderConfig
from sentinalops.live.bus import InMemoryBus


def test_feed_publishes_raw_topics_via_fake_exchange(tmp_path):
    async def script(ws, n):
        for i in (1, 2, 3, 3):  # the last one is a duplicate update id
            await ws.send(depth_frame(i))
        for i in (1, 2):
            await ws.send(trade_frame(i))
        await asyncio.sleep(30)

    async def main():
        async with FakeExchange(script) as ex:
            cfg = RecorderConfig(symbols=("BTCUSDT",), data_dir=tmp_path, base_url=ex.base_url,
                                 allowed_hosts=LOCAL, flush_interval_s=3600, stale_after_s=5,
                                 stats_interval_s=3600, open_timeout_s=5)
            bus = InMemoryBus()
            sub = await bus.subscribe(["raw_depth", "raw_trades"], group="t")
            feed = BinanceFeed(bus, cfg, record=False)
            task = asyncio.create_task(feed.run())
            got = []
            async with asyncio.timeout(15):
                async for msg in sub:
                    got.append(msg)
                    if len(got) == 5:
                        break
            feed.request_stop()
            await asyncio.wait_for(task, 10)
            return got, feed

    got, feed = asyncio.run(main())
    assert [t for t, _ in got].count("raw_depth") == 3 and [t for t, _ in got].count("raw_trades") == 2
    assert feed.recorder.stats.duplicates == 1 and feed.feed_drops == 0
    assert not list(tmp_path.glob("stream=*/**/*.parquet"))  # record=False: nothing archived


def test_feed_queue_drops_oldest_when_full(tmp_path):
    cfg = RecorderConfig(symbols=("BTCUSDT",), data_dir=tmp_path, base_url="ws://127.0.0.1:1",
                         allowed_hosts=LOCAL)

    async def main():
        feed = BinanceFeed(InMemoryBus(), cfg, record=False, maxsize=2)
        for i in range(5):
            feed._on_row("depth5", {"symbol": "BTCUSDT", "i": i})
        return feed, [feed._queue.get_nowait()[1]["i"] for _ in range(2)]

    feed, kept = asyncio.run(main())
    assert kept == [3, 4] and feed.feed_drops == 3


def test_cli_rejects_split_role_on_memory_bus(capsys):
    from sentinalops.live.__main__ import main

    with pytest.raises(SystemExit):
        main(["--role", "features", "--bus", "memory"])
    assert "only with --bus kafka" in capsys.readouterr().err


def test_health_line_reads_last_heal_event(tmp_path):
    from sentinalops.live.health import last_heal_event

    (tmp_path / "registry.json").write_text(json.dumps({
        "champion": "v002", "events": [{"ts_ms": 1, "kind": "register", "version": "v002"},
                                       {"ts_ms": 2, "kind": "heal_attempt", "champion": "v001"},
                                       {"ts_ms": 3, "kind": "promote", "version": "v002"}]}))
    ev = last_heal_event(tmp_path)
    assert ev == {"ts_ms": 3, "kind": "promote", "version": "v002", "champion": "v002"}
    assert last_heal_event(tmp_path / "missing") is None
