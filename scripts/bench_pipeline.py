"""Speed check for the offline pipeline (task 12.6): build one synthetic recorded day and time it.

    python scripts/bench_pipeline.py [--hours 24] [--trades-per-s 20] [--dir /tmp/bench]

Depth arrives every 100 ms like ``depth5@100ms``; trades at a busy-day BTCUSDT rate.
Target: one day in under 10 minutes.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests")]

from pipeline_helpers import H0, make_events  # noqa: E402
from sentinalops.config import get_domain_config  # noqa: E402
from sentinalops.data import pipeline as P  # noqa: E402
from sentinalops.data.recorder.messages import AGG_TRADE, DEPTH5, SCHEMAS  # noqa: E402
from sentinalops.data.recorder.sink import partition_of  # noqa: E402


def write_hourly(root: Path, kind: str, df) -> None:
    hours = df["recv_time_ms"] // P.HOUR_MS
    for h, part in df.groupby(hours, sort=True):
        _, symbol, date, hour = partition_of(kind, "BTCUSDT", int(h) * P.HOUR_MS)
        path = root / f"stream={kind}" / f"symbol={symbol}" / f"date={date}" / f"hour={hour}"
        path = path / "part-bench.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(part, schema=SCHEMAS[kind], preserve_index=False), path)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--trades-per-s", type=float, default=20.0)
    ap.add_argument("--dir", default=None)
    args = ap.parse_args()
    base = Path(args.dir or tempfile.mkdtemp(prefix="bench-"))
    raw, out = base / "raw", base / "out"

    t0 = time.perf_counter()
    stress = tuple((s, 20) for s in range(1800, args.hours * 3600, 1800))
    depth, trades = make_events(H0, args.hours * 3600, snapshot_ms=100, trades_per_s=args.trades_per_s,
                                stress=stress)
    write_hourly(raw, DEPTH5, depth)
    write_hourly(raw, AGG_TRADE, trades)
    print(f"generated {len(depth):,} snapshots and {len(trades):,} trades in {time.perf_counter() - t0:.1f}s")

    s = P.run(raw, out, get_domain_config(ROOT / "configs" / "domain_liquidity.yaml"), include_last=True)
    print(P.format_summary(s))
    per_day = s.seconds * 24 / args.hours
    print(f"pipeline: {s.seconds:.1f}s for {args.hours} h -> {per_day:.1f}s per day "
          f"({'PASS' if per_day < 600 else 'FAIL'}: target < 600 s)")


if __name__ == "__main__":
    main()
