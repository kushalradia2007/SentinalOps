"""The recorder service: connections, reconnects, watchdog, flushing, shutdown."""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import websockets

from sentinalops.data.recorder.messages import (
    AGG_TRADE,
    ALLOWED_HOSTS,
    DEFAULT_BASE_URL,
    DEPTH5,
    SEQUENCE_COLUMN,
    STREAM_KINDS,
    build_url,
    normalize_symbols,
    parse_message,
)
from sentinalops.data.recorder.sink import ParquetSink
from sentinalops.logging import get_logger

log = get_logger("sentinalops.recorder")

MAX_REJECT_BYTES = 5 * 1024 * 1024


@dataclass
class RecorderConfig:
    symbols: tuple[str, ...] = ("BTCUSDT",)
    data_dir: Path = Path("data/raw/binance")
    base_url: str = DEFAULT_BASE_URL
    allowed_hosts: frozenset[str] = ALLOWED_HOSTS
    compression: str = "zstd"
    flush_interval_s: float = 60.0
    flush_rows: int = 50_000  # flush early when this many rows are buffered
    stale_after_s: float = 10.0  # no frame at all, or no depth frame for a symbol
    max_connection_s: float = 23 * 3600.0  # Binance closes connections at 24 hours
    open_timeout_s: float = 10.0
    backoff_base_s: float = 0.5
    backoff_max_s: float = 30.0
    stats_interval_s: float = 30.0
    rotation_drain_s: float = 10.0  # how long the old connection gets to deliver its backlog
    metrics_port: int | None = None

    def __post_init__(self) -> None:
        self.symbols = normalize_symbols(self.symbols)
        self.data_dir = Path(self.data_dir)


@dataclass
class RecorderStats:
    messages: dict[str, int] = field(default_factory=lambda: dict.fromkeys(STREAM_KINDS, 0))
    duplicates: int = 0
    malformed: int = 0
    reconnects: int = 0
    rotations: int = 0
    rotation_drain_timeouts: int = 0
    connect_failures: int = 0
    gaps_written: int = 0
    flushes: int = 0
    flush_errors: int = 0
    rows_written: int = 0
    bytes_written: int = 0
    last_flush_seconds: float = 0.0
    last_recv_ms: int = 0
    last_event_ms: int = 0
    hook_errors: int = 0  # on_row raised; counted, never propagated


class _Tap:
    """Holds a new connection's frames back until ``release()``, then passes them straight on."""

    def __init__(self, forward: Callable[[str | bytes, int], None]) -> None:
        self.held: list[tuple[str | bytes, int]] | None = []
        self.first_seq: dict[tuple[str, str], int] = {}
        self.forward = forward

    def on_frame(self, raw: str | bytes, recv_ms: int) -> None:
        if self.held is None:
            self.forward(raw, recv_ms)
            return
        self.held.append((raw, recv_ms))
        try:
            parsed = parse_message(raw, recv_ms)
        except ValueError:
            return  # counted as malformed when replayed
        if parsed is not None:
            kind, row = parsed
            self.first_seq.setdefault((kind, row["symbol"]), row[SEQUENCE_COLUMN[kind]])

    def release(self) -> list[tuple[str | bytes, int]]:
        held, self.held = self.held or [], None
        return held


def backoff_delay(attempt: int, base: float, cap: float, rng: Callable[[], float] = random.random) -> float:
    """Exponential backoff with equal jitter: between half and all of min(cap, base * 2**attempt)."""
    d = min(cap, base * (2 ** min(attempt, 30)))
    return d / 2 + rng() * d / 2


class Recorder:
    def __init__(
        self,
        cfg: RecorderConfig,
        connect: Callable = websockets.connect,
        clock: Callable[[], float] = time.time,
        on_row: Callable[[str, dict], None] | None = None,
        record: bool = True,
    ) -> None:
        """``on_row(kind, row)`` sees every de-duplicated row (the live pipeline's feed);
        ``record=False`` keeps rows out of the Parquet archive (gaps and state are still kept)."""
        self.cfg = cfg
        self._on_row = on_row
        self._record = record
        self.url = build_url(cfg.symbols, cfg.base_url, cfg.allowed_hosts)
        self.sink = ParquetSink(cfg.data_dir, cfg.compression)
        self.stats = RecorderStats()
        self._connect = connect
        self._clock = clock
        self._stop = asyncio.Event()
        self._flush_lock = asyncio.Lock()
        self._last_seq: dict[tuple[str, str], int] = {}
        self._last_depth_ms: dict[str, int] = {}
        self._open_gap: tuple[int, str, str] | None = None  # (start_ms, reason, scope)
        self._reject_bytes = 0
        self._prom: dict | None = None

    # ---- public ----------------------------------------------------------------
    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        """Record until ``request_stop()``; always flushes buffered rows before returning."""
        removed = self.sink.cleanup_tmp()
        previous = self.sink.read_state()
        if previous is not None:
            # The process was down since `previous`: that is a gap too.
            self._open_gap = (previous, "restart", "all")
        log.info(
            "recorder starting",
            extra={
                "url": self.url,
                "data_dir": str(self.cfg.data_dir),
                "tmp_removed": removed,
                "previous_last_seen_ms": previous,
            },
        )
        self._start_metrics_server()
        side = [asyncio.create_task(self._flush_loop()), asyncio.create_task(self._stats_loop())]
        try:
            await self._supervise()
        finally:
            for t in side:
                t.cancel()
            await asyncio.gather(*side, return_exceptions=True)
            await self.flush()
            log.info("recorder stopped", extra=self._snapshot())

    async def flush(self) -> None:
        async with self._flush_lock:
            batches = self.sink.take()
            if batches:
                res = await asyncio.to_thread(self.sink.write, batches)
                if res.failed:
                    self.sink.restore(res.failed)
                    self.stats.flush_errors += len(res.errors)
                    log.error("flush failed, rows kept in memory", extra={"errors": res.errors})
                self.stats.flushes += 1
                self.stats.rows_written += res.rows
                self.stats.bytes_written += res.bytes
                self.stats.last_flush_seconds = res.seconds
            if self.stats.last_recv_ms:
                with contextlib.suppress(OSError):
                    self.sink.write_state(self.stats.last_recv_ms)

    # ---- message handling ------------------------------------------------------
    def handle(self, raw: str | bytes, recv_ms: int) -> None:
        try:
            parsed = parse_message(raw, recv_ms)
        except ValueError as e:
            self.stats.malformed += 1
            self._keep_reject(raw, recv_ms, str(e))
            return
        if parsed is None:
            return
        kind, row = parsed
        symbol = row["symbol"]
        if self._open_gap is not None:
            start_ms, reason, scope = self._open_gap
            self._open_gap = None
            self.sink.write_gap(start_ms, recv_ms, reason, scope)
            self.stats.gaps_written += 1
            log.warning(
                "gap recorded",
                extra={
                    "start_ms": start_ms,
                    "end_ms": recv_ms,
                    "duration_ms": recv_ms - start_ms,
                    "reason": reason,
                },
            )
        self.stats.last_recv_ms = recv_ms
        if kind == DEPTH5:
            self._last_depth_ms[symbol] = recv_ms
        else:
            self.stats.last_event_ms = row["event_time_ms"]
        seq = row[SEQUENCE_COLUMN[kind]]
        key = (kind, symbol)
        if seq <= self._last_seq.get(key, -1):
            self.stats.duplicates += 1  # overlap during a connection rotation, or a repeat
            return
        self._last_seq[key] = seq
        self.stats.messages[kind] += 1
        if self._record:
            self.sink.add(kind, row)
        if self._on_row is not None:
            try:
                self._on_row(kind, row)
            except Exception as e:  # noqa: BLE001 - a consumer bug must never stop the feed
                self.stats.hook_errors += 1
                if self.stats.hook_errors <= 5 or self.stats.hook_errors % 1000 == 0:
                    log.error("on_row failed", extra={"error": f"{type(e).__name__}: {e}",
                                                      "count": self.stats.hook_errors})

    def _keep_reject(self, raw: str | bytes, recv_ms: int, error: str) -> None:
        """Malformed frames are never dropped silently: counted, logged, and kept on disk (capped)."""
        text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else str(raw)
        if self.stats.malformed <= 5:
            log.warning("malformed frame", extra={"error": error, "frame": text[:300]})
        if self._reject_bytes > MAX_REJECT_BYTES:
            return
        path = self.cfg.data_dir / "_rejected" / "frames.jsonl"
        with contextlib.suppress(OSError):
            path.parent.mkdir(parents=True, exist_ok=True)
            line = json.dumps({"recv_time_ms": recv_ms, "error": error, "frame": text}) + "\n"
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
            self._reject_bytes += len(line)

    # ---- connections -----------------------------------------------------------
    def _stale_depth_symbol(self, now_ms: int, opened_ms: int) -> str | None:
        limit = self.cfg.stale_after_s * 1000
        for s in self.cfg.symbols:
            if now_ms - max(self._last_depth_ms.get(s, 0), opened_ms) > limit:
                return s
        return None

    async def _connection(self, ready: asyncio.Event, on_frame: Callable | None = None) -> str:
        """Run one WebSocket connection until it ends. Returns the reason it ended."""
        try:
            async with self._connect(
                self.url,
                open_timeout=self.cfg.open_timeout_s,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=2,
                max_size=2**20,
                max_queue=4096,
            ) as ws:
                opened_ms = int(self._clock() * 1000)
                log.info("connected", extra={"url": self.url})
                while True:
                    try:
                        # asyncio.timeout, not wait_for: on Python 3.11 wait_for can swallow a
                        # cancellation that arrives as a frame does, leaving this task running.
                        async with asyncio.timeout(self.cfg.stale_after_s):
                            raw = await ws.recv()
                    except TimeoutError:
                        return "stale:all"
                    recv_ms = int(self._clock() * 1000)
                    (on_frame or self.handle)(raw, recv_ms)
                    ready.set()
                    stale = self._stale_depth_symbol(recv_ms, opened_ms)
                    if stale:
                        return f"stale:depth5:{stale}"
        except asyncio.CancelledError:
            raise
        except websockets.ConnectionClosed as e:
            return f"closed:{getattr(e.rcvd, 'code', None) or 'abnormal'}"
        except Exception as e:  # noqa: BLE001 - DNS, TLS, refused, handshake timeout...
            if not ready.is_set():
                self.stats.connect_failures += 1
            return f"error:{type(e).__name__}:{e}"

    async def _supervise(self) -> None:
        stop = asyncio.create_task(self._stop.wait())
        attempt = 0
        current: asyncio.Task | None = None
        try:
            while not self._stop.is_set():
                ready = asyncio.Event()
                current = asyncio.create_task(self._connection(ready))
                while True:
                    done, _ = await asyncio.wait(
                        {current, stop},
                        timeout=self.cfg.max_connection_s,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if stop in done or current in done:
                        break
                    # Connection is old: open its replacement before closing it, so nothing is missed.
                    rotated = await self._rotate(current, stop)
                    if rotated is not None:
                        current, ready = rotated
                    elif current.done() or self._stop.is_set():
                        break
                if self._stop.is_set():
                    break
                reason = current.result()
                attempt = 0 if ready.is_set() else attempt + 1
                self.stats.reconnects += 1
                scope = reason.split(":", 1)[1] if reason.startswith("stale:depth5") else "all"
                if self._open_gap is None:
                    start = self.stats.last_recv_ms or int(self._clock() * 1000)
                    self._open_gap = (start, reason, scope)
                delay = backoff_delay(attempt, self.cfg.backoff_base_s, self.cfg.backoff_max_s)
                log.warning(
                    "disconnected",
                    extra={"reason": reason, "attempt": attempt, "retry_in_s": round(delay, 3)},
                )
                with contextlib.suppress(TimeoutError):
                    async with asyncio.timeout(delay):
                        await self._stop.wait()
        finally:
            await self._cancel(current)
            stop.cancel()

    async def _rotate(
        self, current: asyncio.Task, stop: asyncio.Task
    ) -> tuple[asyncio.Task, asyncio.Event] | None:
        """Replace ``current`` with a fresh connection without losing or reordering frames.

        The new connection's frames are held back while the old connection keeps delivering
        its backlog. Once the old one has caught up with the first frame the new one saw, for
        every stream, it is closed and the held frames are replayed (duplicates are dropped by
        sequence number). If the old one cannot catch up in time, the switch still happens and
        the possible hole is written to the gap log. Returns ``None`` if the new connection failed.
        """
        tap = _Tap(self.handle)
        new_ready = asyncio.Event()
        successor = asyncio.create_task(self._connection(new_ready, tap.on_frame))
        waiter = asyncio.create_task(new_ready.wait())
        await asyncio.wait(
            {waiter, successor, stop, current}, timeout=30, return_when=asyncio.FIRST_COMPLETED
        )
        waiter.cancel()
        if not new_ready.is_set() or successor.done() or self._stop.is_set():
            await self._cancel(successor)
            log.warning("rotation failed, keeping the old connection for now")
            return None

        deadline = time.monotonic() + self.cfg.rotation_drain_s
        while not self._caught_up(tap.first_seq):
            if current.done() or self._stop.is_set() or time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.01)
        caught_up = self._caught_up(tap.first_seq)
        await self._cancel(current)
        if not caught_up and not self._stop.is_set():
            self.stats.rotation_drain_timeouts += 1
            if self._open_gap is None:
                self._open_gap = (
                    self.stats.last_recv_ms or int(self._clock() * 1000),
                    "rotation_drain",
                    "all",
                )
            log.warning("old connection did not catch up before the switch", extra={"behind": tap.first_seq})
        for raw, recv_ms in tap.release():  # synchronous: nothing can interleave with the replay
            self.handle(raw, recv_ms)
        self.stats.rotations += 1
        log.info("connection rotated", extra={"rotations": self.stats.rotations, "caught_up": caught_up})
        return successor, new_ready

    def _caught_up(self, first_seq: dict[tuple[str, str], int]) -> bool:
        """Has the old connection delivered everything before the new connection's first frames?"""
        for (kind, symbol), first in first_seq.items():
            # trade ids are contiguous; depth update ids are not, so depth needs a snapshot as new
            need = first - 1 if kind == AGG_TRADE else first
            if self._last_seq.get((kind, symbol), -1) < need:
                return False
        return bool(first_seq)

    @staticmethod
    async def _cancel(task: asyncio.Task | None) -> None:
        # Re-cancel until the task has really ended, so a swallowed cancellation cannot hang shutdown.
        while task is not None and not task.done():
            task.cancel()
            await asyncio.wait({task}, timeout=1)

    # ---- background loops ------------------------------------------------------
    async def _flush_loop(self) -> None:
        last = time.monotonic()
        while True:
            await asyncio.sleep(min(1.0, self.cfg.flush_interval_s))
            due = time.monotonic() - last >= self.cfg.flush_interval_s
            if due or self.sink.buffered_rows >= self.cfg.flush_rows:
                await self.flush()
                last = time.monotonic()

    def _snapshot(self) -> dict:
        s = self.stats
        now_ms = int(self._clock() * 1000)
        return {
            "messages": dict(s.messages),
            "buffered_rows": self.sink.buffered_rows,
            "rows_written": s.rows_written,
            "bytes_written": s.bytes_written,
            "flushes": s.flushes,
            "flush_errors": s.flush_errors,
            "last_flush_ms": round(s.last_flush_seconds * 1000, 1),
            "reconnects": s.reconnects,
            "rotations": s.rotations,
            "connect_failures": s.connect_failures,
            "gaps_written": s.gaps_written,
            "duplicates": s.duplicates,
            "malformed": s.malformed,
            **({"hook_errors": s.hook_errors} if self._on_row is not None else {}),
            "last_event_ms": s.last_event_ms,
            "last_recv_ms": s.last_recv_ms,
            "seconds_since_last_message": round((now_ms - s.last_recv_ms) / 1000, 3)
            if s.last_recv_ms
            else None,
        }

    async def _stats_loop(self) -> None:
        prev, prev_t = dict(self.stats.messages), time.monotonic()
        while True:
            await asyncio.sleep(self.cfg.stats_interval_s)
            now = time.monotonic()
            dt = max(now - prev_t, 1e-9)
            rates = {k: round((self.stats.messages[k] - prev[k]) / dt, 2) for k in STREAM_KINDS}
            prev, prev_t = dict(self.stats.messages), now
            snap = self._snapshot()
            log.info("recorder health", extra={"messages_per_s": rates, **snap})
            self._update_metrics(rates, snap)

    # ---- optional Prometheus endpoint -------------------------------------------
    def _start_metrics_server(self) -> None:
        if self.cfg.metrics_port is None:
            return
        from prometheus_client import Gauge, start_http_server

        names = [
            "buffered_rows",
            "rows_written",
            "bytes_written",
            "flush_errors",
            "last_flush_ms",
            "reconnects",
            "rotations",
            "gaps_written",
            "duplicates",
            "malformed",
            "last_event_ms",
        ]
        self._prom = {n: Gauge(f"recorder_{n}", f"Stream recorder: {n.replace('_', ' ')}") for n in names}
        self._prom["rate"] = Gauge("recorder_messages_per_second", "Messages per second", ["stream"])
        self._prom["age"] = Gauge("recorder_seconds_since_last_message", "Seconds since the last frame")
        start_http_server(self.cfg.metrics_port)

    def _update_metrics(self, rates: dict, snap: dict) -> None:
        if self._prom is None:
            return
        for name, gauge in self._prom.items():
            if name == "rate":
                for kind, v in rates.items():
                    gauge.labels(stream=kind).set(v)
            elif name == "age":
                gauge.set(snap["seconds_since_last_message"] or 0)
            else:
                gauge.set(snap[name])
