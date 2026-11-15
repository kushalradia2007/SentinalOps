"""Buffered, crash-safe Parquet output for the stream recorder.

Layout under the data root::

    stream=aggTrade/symbol=BTCUSDT/date=2026-11-15/hour=13/part-<first_ms>-<last_ms>-<id>.parquet
    stream=depth5/symbol=BTCUSDT/date=.../hour=.../part-....parquet
    _gaps/gap-<start_ms>-<id>.parquet        one row per outage
    _state/last_seen.json                    last receive time, to detect restart gaps

Every file is written to ``*.tmp``, fsynced, then renamed, so a crash never leaves a
partial Parquet file. Rows that fail to write go back into the buffer.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sentinalops.data.recorder.messages import GAP_SCHEMA, SCHEMAS
from sentinalops.logging import get_logger

log = get_logger("sentinalops.recorder.sink")

PartitionKey = tuple[str, str, str, str]  # (kind, symbol, date, hour)
Batches = dict[PartitionKey, list[dict]]


@dataclass
class FlushResult:
    files: int = 0
    rows: int = 0
    bytes: int = 0
    seconds: float = 0.0
    errors: list[str] = field(default_factory=list)
    failed: Batches = field(default_factory=dict)


def partition_of(kind: str, symbol: str, recv_time_ms: int) -> PartitionKey:
    dt = datetime.fromtimestamp(recv_time_ms / 1000, tz=UTC)
    return kind, symbol, dt.strftime("%Y-%m-%d"), dt.strftime("%H")


def _atomic_write_table(table: pa.Table, final: Path, compression: str) -> int:
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(final.name + ".tmp")
    with open(tmp, "wb") as f:
        pq.write_table(table, f, compression=compression)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, final)
    return final.stat().st_size


class ParquetSink:
    def __init__(self, root: str | Path, compression: str = "zstd") -> None:
        self.root = Path(root)
        self.compression = compression
        self._buffers: Batches = {}
        self._rows = 0

    # ---- buffering -------------------------------------------------------------
    def add(self, kind: str, row: dict) -> None:
        key = partition_of(kind, row["symbol"], row["recv_time_ms"])
        self._buffers.setdefault(key, []).append(row)
        self._rows += 1

    @property
    def buffered_rows(self) -> int:
        return self._rows

    def take(self) -> Batches:
        """Detach the current buffers so they can be written off the event loop."""
        batches, self._buffers, self._rows = self._buffers, {}, 0
        return batches

    def restore(self, batches: Batches) -> None:
        """Put unwritten rows back in front of anything buffered since."""
        for key, rows in batches.items():
            self._buffers[key] = rows + self._buffers.get(key, [])
            self._rows += len(rows)

    # ---- writing ---------------------------------------------------------------
    def write(self, batches: Batches) -> FlushResult:
        """Write each partition's rows to one new Parquet file. Safe to run in a thread."""
        res = FlushResult()
        t0 = time.perf_counter()
        for key, rows in batches.items():
            if not rows:
                continue
            kind, symbol, date, hour = key
            name = f"part-{rows[0]['recv_time_ms']}-{rows[-1]['recv_time_ms']}-{uuid.uuid4().hex[:8]}.parquet"
            final = self.root / f"stream={kind}" / f"symbol={symbol}" / f"date={date}" / f"hour={hour}" / name
            try:
                table = pa.Table.from_pylist(rows, schema=SCHEMAS[kind])
                res.bytes += _atomic_write_table(table, final, self.compression)
                res.files += 1
                res.rows += len(rows)
            except Exception as e:  # noqa: BLE001 - keep the rows whatever went wrong
                res.errors.append(f"{final}: {e!r}")
                res.failed[key] = rows
        res.seconds = time.perf_counter() - t0
        return res

    def flush(self) -> FlushResult:
        """Synchronous take + write + restore-on-failure."""
        res = self.write(self.take())
        if res.failed:
            self.restore(res.failed)
        return res

    # ---- gaps and state --------------------------------------------------------
    def write_gap(self, start_ms: int, end_ms: int, reason: str, scope: str = "all") -> Path:
        row = {
            "start_ms": int(start_ms),
            "end_ms": int(end_ms),
            "duration_ms": int(end_ms - start_ms),
            "reason": reason,
            "scope": scope,
        }
        final = self.root / "_gaps" / f"gap-{int(start_ms)}-{uuid.uuid4().hex[:8]}.parquet"
        _atomic_write_table(pa.Table.from_pylist([row], schema=GAP_SCHEMA), final, self.compression)
        return final

    def write_state(self, last_seen_ms: int) -> None:
        path = self.root / "_state" / "last_seen.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps({"last_seen_ms": int(last_seen_ms)}), encoding="utf-8")
        os.replace(tmp, path)

    def read_state(self) -> int | None:
        try:
            raw = (self.root / "_state" / "last_seen.json").read_text(encoding="utf-8")
            return int(json.loads(raw)["last_seen_ms"])
        except (OSError, ValueError, KeyError, TypeError):
            return None

    def cleanup_tmp(self) -> int:
        """Remove ``*.tmp`` leftovers from a crash mid-write. Returns how many were removed."""
        n = 0
        if self.root.exists():
            for tmp in self.root.rglob("*.tmp"):
                try:
                    tmp.unlink()
                    n += 1
                except OSError as e:
                    log.warning("could not remove temp file", extra={"path": str(tmp), "error": repr(e)})
        return n
