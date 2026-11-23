"""Offline feature and label pipeline (chunk 12)."""

import numpy as np
import pandas as pd
import pytest

from pipeline_helpers import H0, make_events, write_recording
from sentinalops.config import get_domain_config
from sentinalops.data import pipeline as P
from sentinalops.data.recorder.messages import AGG_TRADE, DEPTH5
from sentinalops.data.recorder.sink import ParquetSink
from sentinalops.features.microstructure import compute_tick

CFG = get_domain_config()
NAMES = CFG.feature_names
H = CFG.label_horizon_seconds * 1000
START = H0 - 10 * 60_000  # recording 09:50 -> 12:10, so hours 09, 10, 11, 12
STRESS = ((1500, 20), (4000, 15), (6500, 25))


@pytest.fixture(scope="module")
def events():
    return make_events(START, 140 * 60, stress=STRESS)


@pytest.fixture(scope="module")
def recording(tmp_path_factory, events):
    root = tmp_path_factory.mktemp("rec")
    write_recording(root / "raw", *events)
    s = P.run(root / "raw", root / "out", CFG, include_last=True)
    return root, s, P.read_table(root / "out")


# ---- 12.1 schema ----------------------------------------------------------------------------------
def test_output_matches_the_schema_and_validates(recording):
    _, _, df = recording
    assert list(df.columns) == P.table_schema(NAMES).names
    assert P.validate_table(df, NAMES) == []
    assert (df["symbol"] == "BTCUSDT").all() and (df["source"] == P.SOURCE_RECORDED).all()
    assert (df["ts_ms"] % 1000 == 0).all()


@pytest.mark.parametrize(
    "breakit, message",
    [
        (lambda d: d.drop(columns="rel_spread"), "columns differ"),
        (lambda d: d.assign(rel_spread=np.where(d.index == 3, np.nan, d.rel_spread)), "non-finite"),
        (lambda d: pd.concat([d.iloc[:5], d.iloc[4:6]]), "strictly increasing"),
        (lambda d: d.assign(label_status="maybe"), "unknown label_status"),
        (lambda d: d.assign(label=pd.array([pd.NA] * len(d), dtype="Int8")), "exactly when"),
        (lambda d: d.assign(label=pd.array([2] * len(d), dtype="Int8"), label_status="ok"), "0 or 1"),
    ],
)
def test_validator_catches_broken_tables(recording, breakit, message):
    _, _, df = recording
    sample = df[df.label_status == "ok"].iloc[:10].reset_index(drop=True)
    problems = P.validate_table(breakit(sample), NAMES)
    assert any(message in p for p in problems), problems


# ---- 12.2 reading and merging -----------------------------------------------------------------------
def test_merge_orders_by_time_then_kind_then_sequence():
    depth = pd.DataFrame({"recv_time_ms": [10, 10, 5], "last_update_id": [8, 7, 1]})
    trades = pd.DataFrame({"recv_time_ms": [10, 3], "agg_trade_id": [2, 1]})
    m = P.merge_events(trades, depth)
    assert list(zip(m.recv_time_ms, m.kind, m.seq, strict=True)) == [
        (3, AGG_TRADE, 1), (5, DEPTH5, 1), (10, DEPTH5, 7), (10, DEPTH5, 8), (10, AGG_TRADE, 2)
    ]
    shuffled = P.merge_events(trades.iloc[::-1], depth.iloc[[2, 0, 1]])
    assert shuffled[["recv_time_ms", "kind", "seq"]].equals(m[["recv_time_ms", "kind", "seq"]])


def test_read_events_dedupes_and_spans_hours(tmp_path):
    depth, trades = make_events(H0 - 60_000, 120, seed=3)  # crosses an hour boundary
    write_recording(tmp_path, depth, trades)
    write_recording(tmp_path, depth.iloc[100:110], trades.iloc[:5])  # a restart re-wrote some rows
    got = P.read_events(tmp_path, DEPTH5, "BTCUSDT", H0 - 60_000, H0 + 60_000)
    assert len(got) == len(depth) and got["last_update_id"].is_unique
    assert got["recv_time_ms"].is_monotonic_increasing
    t = P.read_events(tmp_path, AGG_TRADE, "BTCUSDT", H0, H0 + 30_000)
    assert t["recv_time_ms"].between(H0, H0 + 29_999).all()
    assert len(t) == ((trades.recv_time_ms >= H0) & (trades.recv_time_ms < H0 + 30_000)).sum()


# ---- 12.3 features ----------------------------------------------------------------------------------
def test_hourly_build_equals_processing_the_whole_recording(recording, events):
    """Hour files plus padding must give exactly the rows of one pass over everything."""
    _, _, df = recording
    whole = P.build_window(*events, np.empty((0, 2), np.int64), "BTCUSDT", CFG, START, START + 10**9)
    pd.testing.assert_frame_equal(df, whole.reset_index(drop=True), check_dtype=False)
    assert df["ts_ms"].min() == START + 60_000 + 1_000  # warm-up: first full 60 s window


def test_rows_agree_with_the_reference_compute_tick(recording, events):
    _, _, df = recording
    depth, trades = events
    rng = np.random.default_rng(0)
    for i in rng.choice(len(df), 15, replace=False):
        row = df.iloc[i]
        ref = compute_tick(depth, trades, int(row.ts_ms), CFG.feature_params())
        assert ref is not None
        np.testing.assert_allclose([row[n] for n in NAMES], [ref[n] for n in NAMES], rtol=1e-9, atol=1e-12)


# ---- 12.4 labels and gaps ---------------------------------------------------------------------------
def test_every_stress_episode_is_labelled_ahead_of_time(recording):
    _, s, df = recording
    ok = df[df.label_status == "ok"]
    for off, _ in STRESS:
        onset = START + off * 1000
        before = ok[(ok.ts_ms >= onset - H + 2000) & (ok.ts_ms < onset - 2000)]
        assert len(before) > 20 and (before.label == 1).all()
    assert s.positives == int((df.label == 1).sum()) > 0
    assert (df.loc[df.label_status != "ok", "label"].isna()).all()


def test_recorded_gaps_drop_features_and_void_labels(tmp_path):
    # A 2 s hole is shorter than the stale limit, so only the gap log reveals it.
    hole_at = 1800
    depth, trades = make_events(START, 50 * 60, stress=((1200, 20),), holes=((hole_at, 2),), seed=5)
    write_recording(tmp_path / "raw", depth, trades)
    g0, g1 = START + hole_at * 1000 - 100, START + (hole_at + 2) * 1000 + 100
    ParquetSink(tmp_path / "raw").write_gap(g0, g1, "socket_closed", "all")
    ParquetSink(tmp_path / "raw").write_gap(g0 - 10**6, g1 - 10**6, "other", "depth5:ETHUSDT")  # ignored
    s = P.run(tmp_path / "raw", tmp_path / "out", CFG, include_last=True)
    df = P.read_table(tmp_path / "out")
    assert s.gaps == 1 and P.validate_table(df, NAMES) == []
    ts = df["ts_ms"].to_numpy()
    # no kept row has a feature window touching the outage
    assert not ((ts - 60_000 < g1) & (ts > g0)).any()
    # no label looks across it
    ok = df[df.label_status == "ok"].ts_ms.to_numpy()
    assert not ((ok < g1) & (ok + H > g0)).any()
    voided = df[df.label_status == "void_gap"]
    assert len(voided) > 0 and (voided.ts_ms + H > g0).all() and (voided.ts_ms < g0).all()
    assert voided["fwd_min_depth_ratio"].isna().all()


def test_future_data_never_changes_past_rows(tmp_path, events):
    """Leakage: wiping everything after T leaves every row whose horizon ends before T unchanged."""
    depth, trades = events
    cut = START + 5000 * 1000
    early = P.build_window(depth[depth.recv_time_ms < cut], trades[trades.recv_time_ms < cut],
                           np.empty((0, 2), np.int64), "BTCUSDT", CFG, START, START + 10**9)
    full = P.build_window(depth, trades, np.empty((0, 2), np.int64), "BTCUSDT", CFG, START, START + 10**9)
    safe = full.ts_ms + H + CFG.stale_ms < cut
    a = full[safe].reset_index(drop=True)
    b = early[early.ts_ms.isin(a.ts_ms)].reset_index(drop=True)
    pd.testing.assert_frame_equal(a, b)
    assert len(a) > 4000


def test_overlaps_gap_on_hand_made_intervals():
    gaps = np.array([[100, 200], [500, 600]])
    lo = np.array([0, 150, 199, 200, 300, 460, 600])
    hi = lo + 50
    assert P.overlaps_gap(lo, hi, gaps).tolist() == [False, True, True, False, False, True, False]
    assert not P.overlaps_gap(lo, hi, np.empty((0, 2), np.int64)).any()


# ---- 12.5 output, resume ----------------------------------------------------------------------------
def test_files_are_hourly_and_a_second_run_does_nothing(recording):
    root, s, _ = recording
    files = sorted((root / "out").glob("date=*/hour=*.parquet"))
    assert [f"{f.parent.name}/{f.name}" for f in files] == [
        "date=2025-11-15/hour=09.parquet", "date=2025-11-15/hour=10.parquet",
        "date=2025-11-15/hour=11.parquet", "date=2025-11-15/hour=12.parquet",
    ]
    assert len(s.hours_written) == 4 and not list((root / "out").rglob("*.tmp"))
    again = P.run(root / "raw", root / "out", CFG, include_last=True)
    assert again.hours_written == [] and again.hours_skipped == 4
    assert len(list((root / "out" / "_runs").glob("run-*.json"))) >= 1


def test_resume_rebuilds_only_missing_hours_and_skips_the_newest(tmp_path, recording):
    root, _, df = recording
    out = tmp_path / "out"
    first = P.run(root / "raw", out, CFG)  # newest hour may still be recording
    assert first.hours_pending == ["2025-11-15T12"] and len(first.hours_written) == 3
    (out / "date=2025-11-15" / "hour=10.parquet").unlink()
    second = P.run(root / "raw", out, CFG)
    assert second.hours_written == ["2025-11-15T10"] and second.hours_skipped == 2
    rebuilt = P.read_table(out)
    pd.testing.assert_frame_equal(rebuilt, df[df.ts_ms < H0 + 2 * P.HOUR_MS].reset_index(drop=True))


def test_cli_prints_a_summary(tmp_path, recording, capsys):
    root, _, _ = recording
    P.main(["--data-dir", str(root / "raw"), "--out", str(tmp_path / "o"), "--include-last"])
    text = capsys.readouterr().out
    assert "4 built" in text and "label rate" in text


def test_read_events_with_an_unbounded_range_is_instant(tmp_path):
    """Regression: a range starting at epoch 0 used to loop over every hour since 1970."""
    import time

    depth, trades = make_events(H0 - 60_000, 120, seed=4)  # two hour folders
    write_recording(tmp_path, depth, trades)
    stray = tmp_path / f"stream={DEPTH5}" / "symbol=BTCUSDT" / "date=notes" / "hour=xx"
    stray.mkdir(parents=True)
    (stray / "part-junk.parquet").write_text("not parquet")

    t0 = time.perf_counter()
    got = P.read_events(tmp_path, DEPTH5, "BTCUSDT", 0, 10**15)
    assert time.perf_counter() - t0 < 5
    assert len(got) == len(depth)
    assert len(P.read_events(tmp_path, AGG_TRADE, "BTCUSDT", 0, 10**15)) == len(trades)
    assert P.read_events(tmp_path, DEPTH5, "BTCUSDT", 0, H0 - 3_600_000).empty  # before any data
    assert P.recorded_hours(tmp_path, "BTCUSDT") == [H0 - P.HOUR_MS, H0]
