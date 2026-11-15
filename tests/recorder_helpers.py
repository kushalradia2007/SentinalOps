"""Fake Binance combined-stream server and message builders for recorder tests."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable

import websockets

from sentinalops.data.recorder import Recorder, RecorderConfig

LOCAL = frozenset({"127.0.0.1"})


def trade_frame(i: int, symbol: str = "BTCUSDT") -> str:
    now = int(time.time() * 1000)
    data = {
        "e": "aggTrade",
        "E": now,
        "s": symbol,
        "a": i,
        "p": "100.50",
        "q": "0.25",
        "f": i * 2,
        "l": i * 2 + 1,
        "T": now - 1,
        "m": i % 2 == 0,
        "M": True,
    }
    return json.dumps({"stream": f"{symbol.lower()}@aggTrade", "data": data})


def depth_frame(i: int, symbol: str = "BTCUSDT") -> str:
    data = {
        "lastUpdateId": i,
        "bids": [[f"{100 - k * 0.01:.2f}", "1.5"] for k in range(5)],
        "asks": [[f"{100.01 + k * 0.01:.2f}", "2.5"] for k in range(5)],
    }
    return json.dumps({"stream": f"{symbol.lower()}@depth5@100ms", "data": data})


class FakeExchange:
    """Local WebSocket server. ``script(ws, connection_number)`` drives each connection."""

    def __init__(self, script: Callable[[object, int], Awaitable[None]]) -> None:
        self.script = script
        self.connections = 0
        self.port = 0
        self._server = None

    async def _handler(self, ws, *_):
        self.connections += 1
        try:
            await self.script(ws, self.connections)
        except websockets.ConnectionClosed:
            pass

    async def __aenter__(self) -> FakeExchange:
        self._server = await websockets.serve(self._handler, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc) -> None:
        self._server.close()
        await self._server.wait_closed()

    @property
    def base_url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"


def make_recorder(tmp_path, exchange: FakeExchange, **overrides) -> Recorder:
    cfg = dict(
        symbols=("BTCUSDT",),
        data_dir=tmp_path,
        base_url=exchange.base_url,
        allowed_hosts=LOCAL,
        flush_interval_s=3600,
        stale_after_s=5,
        backoff_base_s=0.02,
        backoff_max_s=0.05,
        stats_interval_s=3600,
        open_timeout_s=5,
    )
    cfg.update(overrides)
    return Recorder(RecorderConfig(**cfg))


async def run_until(recorder: Recorder, condition: Callable[[], bool], timeout: float = 15.0) -> None:
    """Run the recorder until ``condition()`` is true, then stop it gracefully."""
    task = asyncio.create_task(recorder.run())
    deadline = time.monotonic() + timeout
    try:
        while not condition():
            if task.done():
                task.result()
                raise AssertionError("recorder exited before the condition was met")
            if time.monotonic() > deadline:
                raise AssertionError(f"timed out; stats={recorder.stats}")
            await asyncio.sleep(0.01)
    finally:
        recorder.request_stop()
        await asyncio.wait_for(task, 10)
