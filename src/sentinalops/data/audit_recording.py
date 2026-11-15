"""Audit recorded Binance stream data (task 7.9).

    python -m sentinalops.data.audit_recording --data-dir data/raw/binance --hours 24

Reports, per symbol and stream: row counts and disk footprint, schema and null checks,
duplicate or out-of-order rows, and gaps. A depth gap is a jump in receive time above
``--depth-gap-ms``. A trade gap is a hole in the aggregate trade IDs, which is the only
reliable sign of lost trades (a quiet market also produces time gaps). Every gap is
cross-referenced with the recorder's own gap log; gaps it did not log are "unexplained".

Exits 1 when it finds integrity problems or unexplained gaps (disable with ``--no-fail``).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from sentinalops.data.recorder.messages import AGG_TRADE, DEPTH5, GAP_SCHEMA, SCHEMAS, SEQUENCE_COLUMN

EXPLAIN_TOLERANCE_MS = 1500


def _iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"


def _file_span(path: Path) -> tuple[int, int] | None:
    """(first_ms, last_ms) parsed from ``part-<first>-<last>-<id>.parquet``."""
    parts = path.stem.split("-")
    try:
        return int(parts[1]), int(parts[2])
    except (IndexError, ValueError):
        return None


def load_gap_log(root: Path, start_ms: int | None, end_ms: int | None) -> list[dict]:
    gaps: list[dict] = []
    for f in sorted((root / "_gaps").glob("gap-*.parquet")):
        try:
            gaps += pq.read_table(f, schema=GAP_SCHEMA).to_pylist()
        except Exception:  # noqa: BLE001 - reported as unreadable by the caller's file scan
            continue
    return [
        g
        for g in gaps
        if (end_ms is None or g["start_ms"] <= end_ms) and (start_ms is None or g["end_ms"] >= start_ms)
    ]


def _explained(start_ms: int, end_ms: int, gap_log: list[dict], scope_symbol: str) -> str | None:
    for g in gap_log:
        if g["scope"] not in ("all", f"depth5:{scope_symbol}"):
            continue
        if g["start_ms"] - EXPLAIN_TOLERANCE_MS <= end_ms and g["end_ms"] + EXPLAIN_TOLERANCE_MS >= start_ms:
            return g["reason"]
    return None


def _audit_stream(root: Path, kind: str, symbol_dir: Path, start_ms, end_ms, depth_gap_ms, gap_log) -> dict:
    symbol = symbol_dir.name.split("=", 1)[1]
    schema = SCHEMAS[kind]
    seq_col = SEQUENCE_COLUMN[kind]
    out: dict = {
        "symbol": symbol,
        "stream": kind,
        "files": 0,
        "rows": 0,
        "bytes": 0,
        "first_recv_ms": None,
        "last_recv_ms": None,
        "schema_errors": [],
        "unreadable": [],
        "null_counts": {},
        "duplicate_or_out_of_order": 0,
        "gaps": [],
    }
    files = sorted(symbol_dir.rglob("part-*.parquet"), key=lambda p: (_file_span(p) or (0, 0), p.name))
    times, seqs = [], []
    for f in files:
        span = _file_span(f)
        if span and (
            (end_ms is not None and span[0] > end_ms) or (start_ms is not None and span[1] < start_ms)
        ):
            continue
        try:
            table = pq.read_table(f)
        except Exception as e:  # noqa: BLE001
            out["unreadable"].append(f"{f.relative_to(root)}: {e!r}")
            continue
        out["files"] += 1
        out["bytes"] += f.stat().st_size
        if not table.schema.equals(schema, check_metadata=False):
            out["schema_errors"].append(
                f"{f.relative_to(root)}: {table.schema.names} / types differ from expected"
            )
            if "recv_time_ms" not in table.schema.names or seq_col not in table.schema.names:
                continue
        for name in table.schema.names:
            nulls = table.column(name).null_count
            if nulls:
                out["null_counts"][name] = out["null_counts"].get(name, 0) + nulls
        times.append(table.column("recv_time_ms").to_numpy(zero_copy_only=False))
        seqs.append(table.column(seq_col).to_numpy(zero_copy_only=False))

    if not times:
        return out
    t, s = np.concatenate(times), np.concatenate(seqs)
    keep = np.ones(len(t), dtype=bool)
    if start_ms is not None:
        keep &= t >= start_ms
    if end_ms is not None:
        keep &= t <= end_ms
    t, s = t[keep], s[keep]
    out["rows"] = int(len(t))
    if len(t) == 0:
        return out
    out["first_recv_ms"], out["last_recv_ms"] = int(t[0]), int(t[-1])
    out["duplicate_or_out_of_order"] = int(np.sum(np.diff(s) <= 0))

    if kind == DEPTH5:
        idx = np.nonzero(np.diff(t) > depth_gap_ms)[0]
        for i in idx:
            a, b = int(t[i]), int(t[i + 1])
            out["gaps"].append(
                {
                    "start_ms": a,
                    "end_ms": b,
                    "duration_ms": b - a,
                    "explained_by": _explained(a, b, gap_log, symbol),
                }
            )
    else:
        idx = np.nonzero(np.diff(s) > 1)[0]
        for i in idx:
            a, b = int(t[i]), int(t[i + 1])
            out["gaps"].append(
                {
                    "start_ms": a,
                    "end_ms": b,
                    "duration_ms": b - a,
                    "missing_trades": int(s[i + 1] - s[i] - 1),
                    "explained_by": _explained(a, b, gap_log, symbol),
                }
            )
    return out


def audit(
    data_dir: str | Path, start_ms: int | None = None, end_ms: int | None = None, depth_gap_ms: int = 500
) -> dict:
    root = Path(data_dir)
    gap_log = load_gap_log(root, start_ms, end_ms)
    streams = []
    for kind in (AGG_TRADE, DEPTH5):
        for symbol_dir in sorted((root / f"stream={kind}").glob("symbol=*")):
            streams.append(_audit_stream(root, kind, symbol_dir, start_ms, end_ms, depth_gap_ms, gap_log))
    tmp_files = [str(p.relative_to(root)) for p in root.rglob("*.tmp")] if root.exists() else []
    rejected = root / "_rejected" / "frames.jsonl"
    rejected_frames = sum(1 for _ in rejected.open(encoding="utf-8")) if rejected.exists() else 0

    firsts = [s["first_recv_ms"] for s in streams if s["first_recv_ms"] is not None]
    lasts = [s["last_recv_ms"] for s in streams if s["last_recv_ms"] is not None]
    span_s = (max(lasts) - min(firsts)) / 1000 if firsts else 0.0
    total_bytes = sum(s["bytes"] for s in streams)
    unexplained = sum(1 for s in streams for g in s["gaps"] if g["explained_by"] is None)
    problems = []
    if not streams or not any(s["rows"] for s in streams):
        problems.append("no recorded rows found")
    for s in streams:
        tag = f"{s['symbol']}/{s['stream']}"
        if s["schema_errors"]:
            problems.append(f"{tag}: {len(s['schema_errors'])} file(s) with a wrong schema")
        if s["unreadable"]:
            problems.append(f"{tag}: {len(s['unreadable'])} unreadable file(s)")
        if s["null_counts"]:
            problems.append(f"{tag}: null values in {sorted(s['null_counts'])}")
        if s["duplicate_or_out_of_order"]:
            problems.append(f"{tag}: {s['duplicate_or_out_of_order']} duplicate or out-of-order row(s)")
    if unexplained:
        problems.append(f"{unexplained} gap(s) not covered by the recorder's gap log")
    if tmp_files:
        problems.append(f"{len(tmp_files)} leftover .tmp file(s)")

    return {
        "data_dir": str(root),
        "window": {"start_ms": start_ms, "end_ms": end_ms},
        "depth_gap_threshold_ms": depth_gap_ms,
        "span_seconds": round(span_s, 3),
        "total_rows": sum(s["rows"] for s in streams),
        "total_bytes": total_bytes,
        "projected_mb_per_day": round(total_bytes / 1e6 * 86400 / span_s, 1) if span_s > 0 else None,
        "streams": streams,
        "gap_log": gap_log,
        "unexplained_gaps": unexplained,
        "rejected_frames": rejected_frames,
        "tmp_files": tmp_files,
        "problems": problems,
        "ok": not problems,
    }


def format_report(r: dict, max_gaps: int = 20) -> str:
    lines = [
        f"Recording audit: {r['data_dir']}",
        f"Window: {_iso(r['window']['start_ms']) or 'beginning'} to {_iso(r['window']['end_ms']) or 'end'}",
        f"Covered span: {r['span_seconds']:.1f} s | total rows: {r['total_rows']:,} | "
        f"disk: {r['total_bytes'] / 1e6:.2f} MB | projected: {r['projected_mb_per_day']} MB/day",
        "",
        "1. Volume",
        f"  {'symbol':<10} {'stream':<9} {'files':>6} {'rows':>12} {'MB':>9} {'rows/s':>8}"
        "  first .. last (UTC)",
    ]
    for s in r["streams"]:
        dur = ((s["last_recv_ms"] or 0) - (s["first_recv_ms"] or 0)) / 1000
        rate = s["rows"] / dur if dur > 0 else 0.0
        lines.append(
            f"  {s['symbol']:<10} {s['stream']:<9} {s['files']:>6} {s['rows']:>12,} {s['bytes'] / 1e6:>9.3f} "
            f"{rate:>8.2f}  {_iso(s['first_recv_ms'])} .. {_iso(s['last_recv_ms'])}"
        )
    lines += [
        "",
        f"2. Gaps (depth5: receive-time jump > {r['depth_gap_threshold_ms']} ms;"
        " aggTrade: missing trade IDs)",
    ]
    for s in r["streams"]:
        gaps = s["gaps"]
        unexplained = [g for g in gaps if g["explained_by"] is None]
        longest = max((g["duration_ms"] for g in gaps), default=0)
        extra = (
            f", {sum(g.get('missing_trades', 0) for g in gaps)} trades missing"
            if s["stream"] == AGG_TRADE
            else ""
        )
        lines.append(
            f"  {s['symbol']}/{s['stream']}: {len(gaps)} gap(s), {len(unexplained)} unexplained, "
            f"longest {longest} ms{extra}"
        )
        for g in gaps[:max_gaps]:
            why = g["explained_by"] or "UNEXPLAINED"
            lines.append(f"    {_iso(g['start_ms'])} -> {_iso(g['end_ms'])}  {g['duration_ms']:>7} ms  {why}")
        if len(gaps) > max_gaps:
            lines.append(f"    ... {len(gaps) - max_gaps} more")
    lines.append(f"  Recorder gap log: {len(r['gap_log'])} outage(s)")
    for g in r["gap_log"][:max_gaps]:
        lines.append(
            f"    {_iso(g['start_ms'])} -> {_iso(g['end_ms'])}  {g['duration_ms']:>7} ms  "
            f"{g['reason']} [{g['scope']}]"
        )
    lines += ["", "3. Integrity"]
    for s in r["streams"]:
        ok = not (s["schema_errors"] or s["unreadable"] or s["null_counts"] or s["duplicate_or_out_of_order"])
        lines.append(
            f"  {s['symbol']}/{s['stream']}: schema {'ok' if not s['schema_errors'] else 'MISMATCH'}, "
            f"nulls {sum(s['null_counts'].values())}, duplicates/out-of-order "
            f"{s['duplicate_or_out_of_order']}, unreadable {len(s['unreadable'])}"
            f"{'' if ok else '  <-- problem'}"
        )
        for msg in (s["schema_errors"] + s["unreadable"])[:5]:
            lines.append(f"    {msg}")
    lines.append(
        f"  Rejected (malformed) frames kept: {r['rejected_frames']}"
        f" | leftover .tmp files: {len(r['tmp_files'])}"
    )
    lines += ["", "RESULT: " + ("PASS" if r["ok"] else "FAIL")]
    lines += [f"  - {p}" for p in r["problems"]]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Audit recorded Binance stream Parquet files")
    ap.add_argument("--data-dir", default="data/raw/binance")
    ap.add_argument("--hours", type=float, default=None, help="audit the last N hours (default: everything)")
    ap.add_argument("--start-ms", type=int, default=None)
    ap.add_argument("--end-ms", type=int, default=None)
    ap.add_argument("--depth-gap-ms", type=int, default=500)
    ap.add_argument("--json", action="store_true", help="print the full report as JSON")
    ap.add_argument("--no-fail", action="store_true", help="always exit 0")
    args = ap.parse_args(argv)

    start_ms, end_ms = args.start_ms, args.end_ms
    if args.hours is not None:
        end_ms = end_ms or int(time.time() * 1000)
        start_ms = end_ms - int(args.hours * 3600 * 1000)
    report = audit(args.data_dir, start_ms, end_ms, args.depth_gap_ms)
    print(json.dumps(report, indent=2) if args.json else format_report(report))
    if not report["ok"] and not args.no_fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
