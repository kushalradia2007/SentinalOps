"""Offline feature and label pipeline (chunk 12): recorded Parquet -> one labelled tick per second.

Input is the recorder's layout (``docs/recorder.md``); output is the training table::

    <out>/date=2026-11-15/hour=13.parquet      one file per recorded hour, written atomically
    <out>/_runs/run-<started>.json              run summary

The unit of work is one UTC hour. An hour is computed from the events of the previous, the
same and the next hour, which covers the 600 s label reference, the 60 s feature windows and
the 30 s label horizon, so every hour comes out exactly as if the whole recording had been
processed at once (tested). Finished hours are skipped on the next run, so a crash or a new
day of data only costs the hours that are missing.

Rules on top of chunks 9 and 10:

- A tick has features only if ``compute_ticks`` says it is valid **and** no recorded outage
  overlaps its feature window ``(t - 60 s, t]`` (an outage may lose trades while the book
  still looks fresh).
- A label is kept only if no recorded outage overlaps its horizon ``(t, t + 30 s]``.
  ``label_status`` says why a label is missing.
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from sentinalops.config import DomainConfig, get_domain_config
from sentinalops.data.recorder.messages import AGG_TRADE, DEPTH5, SEQUENCE_COLUMN
from sentinalops.features.microstructure import FeatureParams, compute_ticks
from sentinalops.labels import LabelParams, add_labels, add_stress, book_state, to_ticks
from sentinalops.logging import get_logger

log = get_logger("sentinalops.pipeline")

HOUR_MS = 3_600_000
TARGET_COLUMNS = ["time_to_onset_s", "fwd_max_rel_spread", "fwd_min_depth_ratio"]
LABEL_STATUSES = ("ok", "not_calm", "void_unknown", "void_gap")
SOURCE_RECORDED = "recorded_binance_stream"
REGIME_UNTAGGED = "untagged"  # chunk 14 fills this in


# ---- 12.1 output schema -------------------------------------------------------------------------
def table_schema(feature_names: list[str]) -> pa.Schema:
    """The training table. Every feature is non-null; ``label`` is null when ``label_status != 'ok'``."""
    return pa.schema(
        [
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("ts_ms", pa.int64(), nullable=False),
            *[pa.field(name, pa.float64(), nullable=False) for name in feature_names],
            pa.field("label", pa.int8()),
            pa.field("label_status", pa.string(), nullable=False),
            *[pa.field(name, pa.float64()) for name in TARGET_COLUMNS],
            pa.field("regime", pa.string(), nullable=False),
            pa.field("source", pa.string(), nullable=False),
        ]
    )


def validate_table(df: pd.DataFrame, feature_names: list[str]) -> list[str]:
    """Problems with a training table, empty if it is valid."""
    problems: list[str] = []
    expected = table_schema(feature_names).names
    if list(df.columns) != expected:
        missing = sorted(set(expected) - set(df.columns))
        extra = sorted(set(df.columns) - set(expected))
        return [f"columns differ from the schema: missing={missing} unexpected={extra}"]
    if len(df) == 0:
        return problems
    for symbol, g in df.groupby("symbol", sort=False):
        ts = g["ts_ms"].to_numpy()
        if (np.diff(ts) <= 0).any():
            problems.append(f"{symbol}: ts_ms is not strictly increasing")
    feats = df[feature_names].to_numpy(float)
    if not np.isfinite(feats).all():
        problems.append(f"{int((~np.isfinite(feats)).any(axis=1).sum())} rows have non-finite features")
    status = df["label_status"]
    if not status.isin(LABEL_STATUSES).all():
        problems.append(f"unknown label_status values: {sorted(set(status) - set(LABEL_STATUSES))}")
    ok = (status == "ok").to_numpy()
    label = df["label"]
    if label[ok].isna().any() or label[~ok].notna().any():
        problems.append("label must be set exactly when label_status is 'ok'")
    if not label[ok].isin([0, 1]).all():
        problems.append("label must be 0 or 1")
    return problems


# ---- 12.2 reading and merging events --------------------------------------------------------------
def hour_start(ms: int) -> int:
    return ms - ms % HOUR_MS


def _hour_dir(root: Path, kind: str, symbol: str, h_ms: int) -> Path:
    dt = datetime.fromtimestamp(h_ms / 1000, tz=UTC)
    return root / f"stream={kind}" / f"symbol={symbol}" / f"date={dt:%Y-%m-%d}" / f"hour={dt:%H}"


def recorded_hours(root: str | Path, symbol: str) -> list[int]:
    """Start times (ms) of every hour that has depth data for ``symbol``."""
    base = Path(root) / f"stream={DEPTH5}" / f"symbol={symbol}"
    hours = []
    for d in base.glob("date=*/hour=*"):
        if any(d.glob("part-*.parquet")):
            dt = datetime.strptime(f"{d.parent.name[5:]} {d.name[5:]}", "%Y-%m-%d %H").replace(tzinfo=UTC)
            hours.append(int(dt.timestamp() * 1000))
    return sorted(hours)


def read_events(root: str | Path, kind: str, symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    """Recorded rows of one stream with ``start_ms <= recv_time_ms < end_ms``, deduplicated and ordered.

    Duplicates (the same exchange sequence number twice, e.g. across a restart) keep the first
    receive. Ties on receive time are broken by the sequence number, so the order is deterministic.
    """
    root = Path(root)
    files = [
        f
        for h in range(hour_start(start_ms), end_ms, HOUR_MS)
        for f in sorted(_hour_dir(root, kind, symbol, h).glob("part-*.parquet"))
    ]
    if not files:
        return pd.DataFrame()
    df = pd.concat([pq.read_table(f).to_pandas() for f in files], ignore_index=True)
    df = df[(df["recv_time_ms"] >= start_ms) & (df["recv_time_ms"] < end_ms)]
    seq = SEQUENCE_COLUMN[kind]
    df = df.sort_values(["recv_time_ms", seq], kind="stable").drop_duplicates(seq, keep="first")
    return df.reset_index(drop=True)


_KIND_RANK = {DEPTH5: 0, AGG_TRADE: 1}


def merge_events(trades: pd.DataFrame, depth: pd.DataFrame) -> pd.DataFrame:
    """One time-ordered stream of both kinds: ``recv_time_ms, kind, seq, row`` (``row`` indexes the input).

    Order: receive time, then depth before trades, then sequence number. This is the replay
    order for the stream processor; ties never depend on file or dict order.
    """
    parts = []
    for kind, df in ((DEPTH5, depth), (AGG_TRADE, trades)):
        if df is None or len(df) == 0:
            continue
        parts.append(
            pd.DataFrame(
                {
                    "recv_time_ms": df["recv_time_ms"].to_numpy(np.int64),
                    "kind": kind,
                    "seq": df[SEQUENCE_COLUMN[kind]].to_numpy(np.int64),
                    "row": np.arange(len(df)),
                }
            )
        )
    if not parts:
        return pd.DataFrame(columns=["recv_time_ms", "kind", "seq", "row"])
    out = pd.concat(parts, ignore_index=True)
    out["_rank"] = out["kind"].map(_KIND_RANK)
    out = out.sort_values(["recv_time_ms", "_rank", "seq"], kind="stable").drop(columns="_rank")
    return out.reset_index(drop=True)


# ---- recorded outages -----------------------------------------------------------------------------
def load_gaps(root: str | Path, symbol: str) -> np.ndarray:
    """Recorded outages that affect ``symbol`` as merged, sorted ``[start_ms, end_ms]`` rows."""
    from sentinalops.data.audit_recording import load_gap_log  # raises on unreadable files

    rows = [
        (g["start_ms"], g["end_ms"])
        for g in load_gap_log(Path(root), None, None)
        if g["scope"] in ("all", f"{DEPTH5}:{symbol}")
    ]
    if not rows:
        return np.empty((0, 2), np.int64)
    merged: list[list[int]] = []
    for start, end in sorted(rows):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return np.asarray(merged, np.int64)


def overlaps_gap(lo: np.ndarray, hi: np.ndarray, gaps: np.ndarray) -> np.ndarray:
    """For each interval ``(lo, hi]``: does a recorded outage ``(start, end)`` overlap it?"""
    if len(gaps) == 0:
        return np.zeros(len(lo), dtype=bool)
    i = np.searchsorted(gaps[:, 0], hi, side="left") - 1  # last outage starting before hi
    return (i >= 0) & (gaps[np.clip(i, 0, None), 1] > lo)


# ---- 12.3 / 12.4 one hour ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Padding:
    """Extra history and future loaded around an hour so its ticks match a whole-recording run."""

    before_ms: int
    after_ms: int

    @classmethod
    def for_config(cls, fp: FeatureParams, lp: LabelParams, margin_ms: int = 60_000) -> Padding:
        before = lp.reference_window_s * 1000 + lp.cadence_s * 1000 + fp.max_window_ms + fp.stale_ms
        after = (lp.horizon_s + lp.confirm_ticks * lp.cadence_s) * 1000 + lp.stale_ms
        return cls(before + margin_ms, after + margin_ms)


def label_status(lab: pd.DataFrame, gap_in_horizon: np.ndarray) -> np.ndarray:
    stressed = lab["stressed"].to_numpy(float)
    eligible = lab["eligible"].to_numpy(bool)
    return np.select(
        [stressed == 1, np.isnan(stressed), gap_in_horizon, ~eligible],
        ["not_calm", "void_unknown", "void_gap", "void_unknown"],
        default="ok",
    )


def build_window(
    depth: pd.DataFrame,
    trades: pd.DataFrame,
    gaps: np.ndarray,
    symbol: str,
    cfg: DomainConfig,
    start_ms: int,
    end_ms: int,
) -> pd.DataFrame:
    """Training rows for ticks in ``[start_ms, end_ms)`` from events that cover the padding around them."""
    fp, lp = cfg.feature_params(), cfg.label_params()
    names = fp.names
    empty = pd.DataFrame({c: pd.Series(dtype=t.to_pandas_dtype()) for c, t in
                          zip(table_schema(names).names, table_schema(names).types, strict=True)})
    if depth is None or len(depth) == 0:
        return empty
    feats = compute_ticks(depth, trades, fp)
    lab = add_labels(add_stress(to_ticks(book_state(depth), lp), lp), lp)
    feats = feats[(feats.index >= start_ms) & (feats.index < end_ms)]
    ts = feats.index.to_numpy(np.int64)
    gap_in_window = overlaps_gap(ts - fp.max_window_ms, ts, gaps)
    keep = feats["valid"].to_numpy(bool) & ~gap_in_window
    feats, ts = feats[keep], ts[keep]
    if len(ts) == 0:
        return empty
    lab = lab.reindex(ts)
    gap_in_horizon = overlaps_gap(ts, ts + lp.horizon_s * 1000, gaps)
    status = label_status(lab, gap_in_horizon)
    ok = status == "ok"
    targets_known = ~gap_in_horizon

    out = pd.DataFrame({"symbol": symbol, "ts_ms": ts})
    for name in names:
        out[name] = feats[name].to_numpy(float)
    out["label"] = pd.array(np.where(ok, lab["label"].to_numpy(float), np.nan), dtype="Int8")
    out["label_status"] = status
    out["time_to_onset_s"] = np.where(ok, lab["time_to_onset_s"].to_numpy(float), np.nan)
    for name in TARGET_COLUMNS[1:]:
        out[name] = np.where(targets_known, lab[name].to_numpy(float), np.nan)
    out["regime"] = REGIME_UNTAGGED
    out["source"] = SOURCE_RECORDED
    return out


def hour_path(out: str | Path, h_ms: int) -> Path:
    dt = datetime.fromtimestamp(h_ms / 1000, tz=UTC)
    return Path(out) / f"date={dt:%Y-%m-%d}" / f"hour={dt:%H}.parquet"


def build_hour(root: str | Path, symbol: str, h_ms: int, cfg: DomainConfig, gaps: np.ndarray) -> pd.DataFrame:
    pad = Padding.for_config(cfg.feature_params(), cfg.label_params())
    lo, hi = h_ms - pad.before_ms, h_ms + HOUR_MS + pad.after_ms
    depth = read_events(root, DEPTH5, symbol, lo, hi)
    trades = read_events(root, AGG_TRADE, symbol, lo, hi)
    return build_window(depth, trades, gaps, symbol, cfg, h_ms, h_ms + HOUR_MS)


def write_hour(df: pd.DataFrame, path: Path, feature_names: list[str]) -> None:
    """Validate, then write atomically (``.tmp`` + rename) so a crash never leaves a half file."""
    problems = validate_table(df, feature_names)
    if problems:
        raise ValueError(f"{path.name}: " + "; ".join(problems))
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, schema=table_schema(feature_names), preserve_index=False)
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(path)


@dataclass
class RunSummary:
    symbol: str
    started: str
    hours_recorded: int = 0
    hours_written: list[str] = field(default_factory=list)
    hours_skipped: int = 0  # already done
    hours_pending: list[str] = field(default_factory=list)  # newest hour, may still be recording
    rows: int = 0
    label_status: dict[str, int] = field(default_factory=lambda: dict.fromkeys(LABEL_STATUSES, 0))
    positives: int = 0
    gaps: int = 0
    seconds: float = 0.0

    @property
    def label_rate(self) -> float | None:
        n = self.label_status["ok"]
        return self.positives / n if n else None

    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items()}
        d["label_rate"] = self.label_rate
        return d


def _iso_hour(h_ms: int) -> str:
    return f"{datetime.fromtimestamp(h_ms / 1000, tz=UTC):%Y-%m-%dT%H}"


def run(
    root: str | Path,
    out: str | Path,
    cfg: DomainConfig,
    symbol: str | None = None,
    force: bool = False,
    include_last: bool = False,
    hours: Iterable[int] | None = None,
) -> RunSummary:
    """Build every recorded hour that is not built yet. Returns what was done."""
    t0 = time.monotonic()
    symbol = symbol or cfg.symbol
    s = RunSummary(symbol=symbol, started=datetime.now(UTC).isoformat(timespec="seconds"))
    recorded = recorded_hours(root, symbol)
    s.hours_recorded = len(recorded)
    todo = sorted(hours) if hours is not None else recorded
    if recorded and not include_last and todo and todo[-1] == recorded[-1]:
        s.hours_pending.append(_iso_hour(todo[-1]))  # its labels need the next hour
        todo = todo[:-1]
    gaps = load_gaps(root, symbol)
    s.gaps = len(gaps)
    names = cfg.feature_names
    for h in todo:
        path = hour_path(out, h)
        if path.exists() and not force:
            s.hours_skipped += 1
            continue
        df = build_hour(root, symbol, h, cfg, gaps)
        write_hour(df, path, names)
        s.hours_written.append(_iso_hour(h))
        s.rows += len(df)
        for k, v in df["label_status"].value_counts().items():
            s.label_status[k] += int(v)
        s.positives += int((df["label"] == 1).sum())
        log.info("hour built", extra={"hour": _iso_hour(h), "rows": len(df)})
    s.seconds = round(time.monotonic() - t0, 2)
    runs = Path(out) / "_runs"
    runs.mkdir(parents=True, exist_ok=True)
    (runs / f"run-{s.started.replace(':', '')}.json").write_text(json.dumps(s.to_dict(), indent=2))
    return s


def read_table(out: str | Path) -> pd.DataFrame:
    """The whole training table, ordered by symbol and time."""
    files = sorted(Path(out).glob("date=*/hour=*.parquet"))
    if not files:
        return pd.DataFrame()
    df = pd.concat([pq.read_table(f).to_pandas() for f in files], ignore_index=True)
    return df.sort_values(["symbol", "ts_ms"], kind="stable").reset_index(drop=True)


def format_summary(s: RunSummary) -> str:
    rate = "n/a" if s.label_rate is None else f"{s.label_rate:.4f}"
    lines = [
        f"symbol {s.symbol}: {s.hours_recorded} recorded hours, {len(s.hours_written)} built, "
        f"{s.hours_skipped} already done, {len(s.hours_pending)} pending",
        f"rows {s.rows}  labelled {s.label_status['ok']}  positives {s.positives}  label rate {rate}",
        "label status: " + ", ".join(f"{k}={v}" for k, v in s.label_status.items()),
        f"recorded outages {s.gaps}  time {s.seconds}s",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Build the offline feature and label table from recordings")
    ap.add_argument("--data-dir", default="data/raw/binance")
    ap.add_argument("--out", default="data/processed/features")
    ap.add_argument("--config", default="configs/domain_liquidity.yaml")
    ap.add_argument("--symbol", default=None)
    ap.add_argument("--force", action="store_true", help="rebuild hours that are already built")
    ap.add_argument("--include-last", action="store_true",
                    help="also build the newest hour (only once the recording has stopped)")
    args = ap.parse_args(argv)
    s = run(args.data_dir, args.out, get_domain_config(args.config), args.symbol, args.force,
            args.include_last)
    print(format_summary(s))


if __name__ == "__main__":
    main()
