"""Binance feed for the live pipeline: the recorder's connection handling, publishing to the bus.

Reuses ``Recorder`` whole: reconnects with backoff, the stale-feed watchdog, the 23-hour
connection rotation, de-duplication by sequence id, and the gap log. Every de-duplicated row is
handed to ``on_row``, queued (bounded; the oldest is dropped if the bus stalls) and published to
``raw_trades`` / ``raw_depth``. With ``record=True`` the rows are also archived to Parquet, so a
live machine can double as a backup recorder.
"""

from __future__ import annotations

import asyncio
import contextlib

import websockets

from sentinalops.data.recorder import Recorder, RecorderConfig
from sentinalops.live.bus import TOPIC_OF_KIND, Bus
from sentinalops.logging import get_logger

log = get_logger("sentinalops.live.feed")


class BinanceFeed:
    def __init__(self, bus: Bus, rec_cfg: RecorderConfig, record: bool = False,
                 connect=websockets.connect, maxsize: int = 10_000) -> None:
        self.bus = bus
        self.recorder = Recorder(rec_cfg, connect=connect, on_row=self._on_row, record=record)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self.feed_drops = 0
        self.published = 0

    def _on_row(self, kind: str, row: dict) -> None:
        if self._queue.full():
            self._queue.get_nowait()  # a stalled bus must not grow memory: drop the oldest row
            self.feed_drops += 1
            if self.feed_drops in (1, 10, 100) or self.feed_drops % 1000 == 0:
                log.warning("feed queue full, dropped oldest row", extra={"dropped": self.feed_drops})
        self._queue.put_nowait((kind, row))

    async def _pump(self) -> None:
        while True:
            kind, row = await self._queue.get()
            await self.bus.publish(TOPIC_OF_KIND[kind], row["symbol"], row)
            self.published += 1

    def request_stop(self) -> None:
        self.recorder.request_stop()

    async def run(self) -> None:
        pump = asyncio.create_task(self._pump())
        try:
            await self.recorder.run()
            while not self._queue.empty() and not pump.done():  # deliver what was already received
                await asyncio.sleep(0.01)
        finally:
            pump.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pump
