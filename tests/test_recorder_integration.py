"""Integration tests: the recorder against a local fake exchange, plus the audit CLI."""

import asyncio
import itertools
import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from recorder_helpers import FakeExchange, depth_frame, make_recorder, run_until, trade_frame
from sentinalops.data import audit_recording
from sentinalops.data.recorder.messages import AGG_TRADE, DEPTH5


def _column(root: Path, kind: str, column: str) -> list:
    out = []
    for f in sorted((root / f"stream={kind}").rglob("part-*.parquet")):
        out += pq.read_table(f).column(column).to_pylist()
    return sorted(out)


def test_disconnect_reconnect_records_everything_and_logs_the_gap(tmp_path):
    async def script(ws, n):
        if n == 1:
            for i in range(1, 51):
                await ws.send(trade_frame(i))
                await ws.send(depth_frame(i))
            await ws.close(code=1012)  # exchange restarts
        else:
            await asyncio.sleep(0.7)  # outage long enough for the audit to see a depth gap
            for i in range(51, 101):
                await ws.send(trade_frame(i))
                await ws.send(depth_frame(i))
            await asyncio.sleep(30)

    async def go():
        async with FakeExchange(script) as ex:
            rec = make_recorder(tmp_path, ex)
            await run_until(rec, lambda: rec.stats.messages[DEPTH5] >= 100)
            return rec, ex

    rec, ex = asyncio.run(go())
    assert ex.connections == 2 and rec.stats.reconnects == 1 and rec.stats.gaps_written == 1
    assert _column(tmp_path, AGG_TRADE, "agg_trade_id") == list(range(1, 101))
    assert _column(tmp_path, DEPTH5, "last_update_id") == list(range(1, 101))
    assert not list(tmp_path.rglob("*.tmp"))

    report = audit_recording.audit(tmp_path)
    assert report["ok"], report["problems"]
    assert report["total_rows"] == 200 and report["unexplained_gaps"] == 0
    (gap,) = report["gap_log"]
    assert gap["reason"].startswith("closed") and gap["duration_ms"] >= 600
    depth = next(s for s in report["streams"] if s["stream"] == DEPTH5)
    assert len(depth["gaps"]) == 1 and depth["gaps"][0]["explained_by"] == gap["reason"]
    assert "RESULT: PASS" in audit_recording.format_report(report)


def test_stale_stream_watchdog_forces_a_reconnect(tmp_path):
    async def script(ws, n):
        await ws.send(depth_frame(n))
        await ws.send(trade_frame(n))
        await asyncio.sleep(30)  # connection stays open but silent

    async def go():
        async with FakeExchange(script) as ex:
            rec = make_recorder(tmp_path, ex, stale_after_s=0.3)
            await run_until(rec, lambda: rec.stats.messages[DEPTH5] >= 3)
            return rec

    rec = asyncio.run(go())
    assert rec.stats.reconnects >= 2 and rec.stats.gaps_written >= 2
    reasons = {g["reason"] for g in audit_recording.load_gap_log(tmp_path, None, None)}
    assert reasons == {"stale:all"}


def test_silent_depth_stream_is_detected_while_trades_still_flow(tmp_path):
    async def script(ws, n):
        await ws.send(depth_frame(n))
        for i in itertools.count(n * 1000):
            await ws.send(trade_frame(i))
            await asyncio.sleep(0.05)

    async def go():
        async with FakeExchange(script) as ex:
            rec = make_recorder(tmp_path, ex, stale_after_s=0.3)
            await run_until(rec, lambda: rec.stats.reconnects >= 1 and rec.stats.messages[DEPTH5] >= 2)

    asyncio.run(go())
    gaps = audit_recording.load_gap_log(tmp_path, None, None)
    assert gaps[0]["reason"] == "stale:depth5:BTCUSDT" and gaps[0]["scope"] == "depth5:BTCUSDT"


def test_connection_rotation_loses_nothing(tmp_path):
    t0 = time.monotonic()

    async def script(ws, n):
        # Every connection broadcasts the same global sequence, like a real exchange.
        sent = int((time.monotonic() - t0) / 0.01)
        while True:
            latest = int((time.monotonic() - t0) / 0.01)
            for i in range(sent + 1, latest + 1):
                await ws.send(trade_frame(i))
                await ws.send(depth_frame(i))
            sent = latest
            await asyncio.sleep(0.01)

    async def go():
        async with FakeExchange(script) as ex:
            rec = make_recorder(tmp_path, ex, max_connection_s=0.4)
            await run_until(rec, lambda: rec.stats.rotations >= 2)
            return rec

    rec = asyncio.run(go())
    assert rec.stats.reconnects == 0 and rec.stats.gaps_written == 0
    assert rec.stats.duplicates > 0  # the overlap between old and new connection was de-duplicated
    for kind, column in ((AGG_TRADE, "agg_trade_id"), (DEPTH5, "last_update_id")):
        ids = _column(tmp_path, kind, column)
        assert ids == list(range(ids[0], ids[-1] + 1)) and len(ids) > 50  # contiguous: nothing lost


def test_exchange_down_at_start_backs_off_then_connects(tmp_path):
    async def script(ws, n):
        for i in range(1, 6):
            await ws.send(depth_frame(i))
        await asyncio.sleep(30)

    async def go():
        async with FakeExchange(script) as ex:
            url = ex.base_url
        # server is closed now: connections are refused
        ex2 = FakeExchange(script)
        ex2.port = int(url.rsplit(":", 1)[1])
        rec = make_recorder(tmp_path, ex2)
        task = asyncio.create_task(rec.run())
        while rec.stats.connect_failures < 3:
            await asyncio.sleep(0.01)
        rec.request_stop()
        await asyncio.wait_for(task, 10)
        return rec

    rec = asyncio.run(go())
    assert rec.stats.connect_failures >= 3 and rec.stats.rows_written == 0


def test_periodic_flush_and_stop_flush(tmp_path):
    async def script(ws, n):
        for i in itertools.count(1):
            await ws.send(depth_frame(i))
            await asyncio.sleep(0.005)

    async def go():
        async with FakeExchange(script) as ex:
            rec = make_recorder(tmp_path, ex, flush_interval_s=0.2)
            await run_until(rec, lambda: rec.stats.flushes >= 2)
            return rec

    rec = asyncio.run(go())
    assert rec.sink.buffered_rows == 0
    assert len(_column(tmp_path, DEPTH5, "last_update_id")) == rec.stats.messages[DEPTH5]
    assert rec.sink.read_state() == rec.stats.last_recv_ms


CHILD = """
import asyncio, itertools, sys
sys.path.insert(0, {tests!r})
from recorder_helpers import FakeExchange, depth_frame, make_recorder, trade_frame
from sentinalops.data.recorder.__main__ import install_signal_handlers

async def script(ws, n):
    for i in itertools.count(1):
        await ws.send(trade_frame(i)); await ws.send(depth_frame(i))
        await asyncio.sleep(0.005)

async def main():
    async with FakeExchange(script) as ex:
        rec = make_recorder({out!r}, ex)          # flush interval is one hour: only shutdown flushes
        install_signal_handlers(rec, asyncio.get_running_loop())
        task = asyncio.create_task(rec.run())
        while rec.stats.messages["depth5"] < 50:
            await asyncio.sleep(0.01)
        print("READY", flush=True)
        await task
        print("STOPPED", rec.stats.messages["depth5"], flush=True)

asyncio.run(main())
"""


def test_termination_signal_flushes_buffered_rows(tmp_path):
    """SIGTERM on POSIX; Ctrl-Break on Windows, where SIGTERM cannot be caught by another process."""
    out = tmp_path / "data"
    code = textwrap.dedent(CHILD).format(tests=str(Path(__file__).parent), out=str(out))
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).parent.parent / "src")}
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
    proc = subprocess.Popen(
        [sys.executable, "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        creationflags=flags,
    )
    try:
        assert proc.stdout.readline().strip() == "READY", proc.stderr.read()
        assert not list(out.rglob("part-*.parquet"))  # nothing on disk yet
        time.sleep(0.1)
        proc.send_signal(signal.CTRL_BREAK_EVENT if sys.platform == "win32" else signal.SIGTERM)
        stdout, stderr = proc.communicate(timeout=20)
    finally:
        if proc.poll() is None:
            proc.kill()
    assert proc.returncode == 0, stderr
    received = int(stdout.split("STOPPED")[1])
    ids = _column(out, DEPTH5, "last_update_id")
    assert len(ids) == received >= 50 and ids == list(range(1, received + 1))
    assert not list(out.rglob("*.tmp"))


def test_audit_flags_missing_trades_bad_schema_and_leftovers(tmp_path, capsys):
    async def script(ws, n):
        for i in [1, 2, 3, 7, 8]:  # trades 4-6 never arrive and nothing logged a gap
            await ws.send(trade_frame(i))
        await ws.send(depth_frame(1))
        await asyncio.sleep(30)

    async def go():
        async with FakeExchange(script) as ex:
            rec = make_recorder(tmp_path, ex)
            await run_until(rec, lambda: rec.stats.messages[DEPTH5] >= 1)

    asyncio.run(go())
    (trade_file,) = (tmp_path / f"stream={AGG_TRADE}").rglob("part-*.parquet")
    bad = pq.read_table(trade_file).drop(["price"])
    pq.write_table(bad, trade_file.with_name("part-1-2-bad.parquet"))
    trade_file.with_name("part-3-4-x.parquet.tmp").write_bytes(b"partial")

    report = audit_recording.audit(tmp_path)
    trades = next(s for s in report["streams"] if s["stream"] == AGG_TRADE)
    assert not report["ok"]
    assert trades["gaps"][0]["missing_trades"] == 3 and trades["gaps"][0]["explained_by"] is None
    assert len(trades["schema_errors"]) == 1 and report["tmp_files"]

    with pytest.raises(SystemExit) as e:
        audit_recording.main(["--data-dir", str(tmp_path)])
    assert e.value.code == 1 and "RESULT: FAIL" in capsys.readouterr().out
    audit_recording.main(["--data-dir", str(tmp_path), "--no-fail", "--json"])
