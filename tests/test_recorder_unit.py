"""Unit tests for recorder parsing, backoff, the Parquet sink and message handling."""

import json

import pyarrow.parquet as pq
import pytest

from recorder_helpers import LOCAL, depth_frame, trade_frame
from sentinalops.data.recorder import Recorder, RecorderConfig, backoff_delay
from sentinalops.data.recorder import sink as sink_mod
from sentinalops.data.recorder.messages import (
    AGG_TRADE,
    DEPTH5,
    DEPTH_SCHEMA,
    TRADE_SCHEMA,
    build_url,
    parse_message,
    stream_names,
)
from sentinalops.data.recorder.sink import ParquetSink

T0 = 1_763_210_000_000  # 2025-11-15 12:33:20 UTC


def test_parse_agg_trade():
    kind, row = parse_message(trade_frame(7), T0)
    assert kind == AGG_TRADE
    assert row["symbol"] == "BTCUSDT" and row["agg_trade_id"] == 7
    assert row["price"] == 100.5 and row["quantity"] == 0.25
    assert row["buyer_is_maker"] is False and row["recv_time_ms"] == T0
    assert set(row) == set(TRADE_SCHEMA.names)


def test_parse_depth5():
    kind, row = parse_message(depth_frame(42), T0)
    assert kind == DEPTH5
    assert row["last_update_id"] == 42 and row["bid_px_1"] == 100.0 and row["ask_px_1"] == 100.01
    assert row["bid_px_5"] < row["bid_px_1"] < row["ask_px_1"] < row["ask_px_5"]
    assert set(row) == set(DEPTH_SCHEMA.names)


def test_parse_ack_is_ignored_and_bad_frames_raise():
    assert parse_message('{"result": null, "id": 1}', T0) is None
    bad_depth = json.loads(depth_frame(1))
    bad_depth["data"]["bids"] = bad_depth["data"]["bids"][:3]
    for bad in [
        "not json",
        "[]",
        '{"stream": "btcusdt@kline_1m", "data": {}}',
        '{"stream": "btcusdt@aggTrade", "data": {"a": 1}}',
        json.dumps(bad_depth),
    ]:
        with pytest.raises(ValueError):
            parse_message(bad, T0)


def test_url_and_host_allowlist():
    assert stream_names(["btcusdt", "ETHUSDT"]) == [
        "btcusdt@aggTrade",
        "btcusdt@depth5@100ms",
        "ethusdt@aggTrade",
        "ethusdt@depth5@100ms",
    ]
    url = build_url(["BTCUSDT"])
    assert url == "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@depth5@100ms"
    for bad in ["wss://evil.example.com", "wss://stream.binance.com.evil.io", "https://stream.binance.com"]:
        with pytest.raises(ValueError):
            build_url(["BTCUSDT"], bad)
    with pytest.raises(ValueError):
        build_url(["BTC/USDT"])


def test_backoff_grows_is_capped_and_jittered():
    lo = [backoff_delay(a, 0.5, 30, rng=lambda: 0.0) for a in range(10)]
    hi = [backoff_delay(a, 0.5, 30, rng=lambda: 1.0) for a in range(10)]
    assert lo[:3] == [0.25, 0.5, 1.0] and hi[:3] == [0.5, 1.0, 2.0]
    assert max(hi) == 30 and lo[-1] == 15
    assert backoff_delay(10_000, 0.5, 30) <= 30


def _rows(n, start=0, t=T0):
    trades = [parse_message(trade_frame(start + i), t + i)[1] for i in range(n)]
    depths = [parse_message(depth_frame(start + i), t + i)[1] for i in range(n)]
    return trades, depths


def test_sink_writes_typed_partitioned_files_atomically(tmp_path):
    sink = ParquetSink(tmp_path)
    trades, depths = _rows(10)
    for r in trades:
        sink.add(AGG_TRADE, r)
    for r in depths:
        sink.add(DEPTH5, r)
    next_hour = parse_message(trade_frame(99), T0 + 3_600_000)[1]
    sink.add(AGG_TRADE, next_hour)
    assert sink.buffered_rows == 21

    res = sink.flush()
    assert (res.files, res.rows, res.errors) == (3, 21, []) and sink.buffered_rows == 0
    files = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.parquet"))
    assert files[0].startswith("stream=aggTrade/symbol=BTCUSDT/date=2025-11-15/hour=12/part-")
    assert files[1].startswith("stream=aggTrade/symbol=BTCUSDT/date=2025-11-15/hour=13/part-")
    assert files[2].startswith("stream=depth5/symbol=BTCUSDT/date=2025-11-15/hour=12/part-")
    assert not list(tmp_path.rglob("*.tmp"))

    trade_table = pq.read_table(tmp_path / files[0])
    assert trade_table.schema.equals(TRADE_SCHEMA, check_metadata=False)
    assert trade_table.column("agg_trade_id").to_pylist() == list(range(10))
    assert pq.read_table(tmp_path / files[2]).schema.equals(DEPTH_SCHEMA, check_metadata=False)
    assert pq.ParquetFile(tmp_path / files[0]).metadata.row_group(0).column(0).compression == "ZSTD"


def test_sink_keeps_rows_when_a_write_fails(tmp_path, monkeypatch):
    sink = ParquetSink(tmp_path)
    trades, _ = _rows(5)
    for r in trades:
        sink.add(AGG_TRADE, r)

    def boom(*_a, **_k):
        raise OSError("disk full")

    monkeypatch.setattr(sink_mod, "_atomic_write_table", boom)
    res = sink.flush()
    assert res.rows == 0 and len(res.errors) == 1 and sink.buffered_rows == 5
    assert not list(tmp_path.rglob("*.parquet"))

    monkeypatch.undo()
    sink.add(AGG_TRADE, parse_message(trade_frame(5), T0 + 5)[1])
    assert sink.flush().rows == 6
    (f,) = tmp_path.rglob("*.parquet")
    assert pq.read_table(f).column("agg_trade_id").to_pylist() == [0, 1, 2, 3, 4, 5]  # order preserved


def test_sink_cleanup_gap_and_state(tmp_path):
    sink = ParquetSink(tmp_path)
    leftover = tmp_path / "stream=depth5" / "part-1-2-x.parquet.tmp"
    leftover.parent.mkdir(parents=True)
    leftover.write_bytes(b"partial")
    assert sink.cleanup_tmp() == 1 and not leftover.exists()

    assert sink.read_state() is None
    sink.write_state(T0)
    assert sink.read_state() == T0
    gap = sink.write_gap(T0, T0 + 1200, "closed:1006")
    assert pq.read_table(gap).to_pylist() == [
        {"start_ms": T0, "end_ms": T0 + 1200, "duration_ms": 1200, "reason": "closed:1006", "scope": "all"}
    ]


def _recorder(tmp_path) -> Recorder:
    return Recorder(RecorderConfig(data_dir=tmp_path, base_url="ws://127.0.0.1:1", allowed_hosts=LOCAL))


def test_handle_drops_duplicates_and_keeps_malformed_frames(tmp_path):
    rec = _recorder(tmp_path)
    for i in (1, 2, 2, 1, 3):  # overlap, as during a connection rotation
        rec.handle(trade_frame(i), T0 + i)
        rec.handle(depth_frame(i), T0 + i)
    rec.handle("garbage", T0 + 9)
    assert rec.stats.messages == {AGG_TRADE: 3, DEPTH5: 3}
    assert rec.stats.duplicates == 4 and rec.stats.malformed == 1 and rec.sink.buffered_rows == 6
    kept = (tmp_path / "_rejected" / "frames.jsonl").read_text(encoding="utf-8").splitlines()
    assert json.loads(kept[0])["frame"] == "garbage"


def test_restart_is_recorded_as_a_gap(tmp_path):
    first = _recorder(tmp_path)
    first.sink.write_state(T0)
    second = _recorder(tmp_path)
    second._open_gap = (second.sink.read_state(), "restart", "all")
    second.handle(depth_frame(1), T0 + 60_000)
    (gap,) = (tmp_path / "_gaps").glob("gap-*.parquet")
    row = pq.read_table(gap).to_pylist()[0]
    assert (row["start_ms"], row["duration_ms"], row["reason"]) == (T0, 60_000, "restart")


def test_recorder_refuses_hosts_outside_the_allowlist(tmp_path):
    with pytest.raises(ValueError):
        Recorder(RecorderConfig(data_dir=tmp_path, base_url="wss://example.com"))


def _rec(tmp_path, **kw) -> Recorder:
    cfg = RecorderConfig(symbols=("BTCUSDT",), data_dir=tmp_path, base_url="ws://127.0.0.1:1",
                         allowed_hosts=LOCAL)
    return Recorder(cfg, **kw)


def test_on_row_called_once_per_deduplicated_row(tmp_path):
    seen = []
    rec = _rec(tmp_path, on_row=lambda kind, row: seen.append((kind, row["symbol"])))
    rec.handle(depth_frame(5), T0)
    rec.handle(depth_frame(5), T0 + 100)  # duplicate update id
    rec.handle(trade_frame(1), T0 + 150)
    assert seen == [(DEPTH5, "BTCUSDT"), (AGG_TRADE, "BTCUSDT")]
    assert rec.stats.duplicates == 1


def test_record_false_writes_no_rows(tmp_path):
    rec = _rec(tmp_path, on_row=lambda kind, row: None, record=False)
    rec.handle(depth_frame(5), T0)
    rec.handle(trade_frame(1), T0 + 150)
    rec.sink.flush()
    assert not list(tmp_path.glob("stream=*/**/*.parquet"))
    assert rec.stats.messages[DEPTH5] == 1


def test_on_row_errors_never_stop_the_recorder(tmp_path):
    def boom(kind, row):
        raise RuntimeError("consumer bug")

    rec = _rec(tmp_path, on_row=boom)
    rec.handle(depth_frame(5), T0)
    rec.handle(depth_frame(6), T0 + 100)
    assert rec.stats.hook_errors == 2 and rec.sink.buffered_rows == 2
