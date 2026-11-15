"""CLI: ``python -m sentinalops.data.recorder``.

Settings come from flags, falling back to ``RECORDER_*`` environment variables.
Stops cleanly on SIGINT/SIGTERM (Ctrl-C or Ctrl-Break on Windows) after flushing.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys

from sentinalops.data.recorder.messages import ALLOWED_HOSTS, DEFAULT_BASE_URL
from sentinalops.data.recorder.service import Recorder, RecorderConfig
from sentinalops.logging import configure_logging, get_logger

log = get_logger("sentinalops.recorder")


def _env(name: str, default: str) -> str:
    return os.environ.get(name) or default


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Record Binance Spot aggTrade and depth5@100ms streams to Parquet"
    )
    ap.add_argument(
        "--symbols", default=_env("RECORDER_SYMBOLS", "BTCUSDT"), help="comma-separated, e.g. BTCUSDT"
    )
    ap.add_argument("--data-dir", default=_env("RECORDER_DATA_DIR", "data/raw/binance"))
    ap.add_argument(
        "--base-url",
        default=_env("RECORDER_BASE_URL", DEFAULT_BASE_URL),
        help=f"must be one of the allowed hosts: {sorted(ALLOWED_HOSTS)}",
    )
    ap.add_argument("--compression", default=_env("RECORDER_COMPRESSION", "zstd"), choices=["zstd", "snappy"])
    ap.add_argument("--flush-interval", type=float, default=float(_env("RECORDER_FLUSH_INTERVAL_S", "60")))
    ap.add_argument("--flush-rows", type=int, default=int(_env("RECORDER_FLUSH_ROWS", "50000")))
    ap.add_argument("--stale-after", type=float, default=float(_env("RECORDER_STALE_AFTER_S", "10")))
    ap.add_argument(
        "--max-connection-hours", type=float, default=float(_env("RECORDER_MAX_CONNECTION_HOURS", "23"))
    )
    ap.add_argument("--stats-interval", type=float, default=float(_env("RECORDER_STATS_INTERVAL_S", "30")))
    ap.add_argument(
        "--metrics-port",
        type=int,
        default=int(_env("RECORDER_METRICS_PORT", "0")) or None,
        help="serve Prometheus metrics on this port (off by default)",
    )
    ap.add_argument(
        "--duration", type=float, default=None, help="stop after this many seconds (for smoke tests)"
    )
    ap.add_argument("--log-level", default=_env("LOG_LEVEL", "INFO"))
    return ap


def config_from_args(args: argparse.Namespace) -> RecorderConfig:
    return RecorderConfig(
        symbols=tuple(args.symbols.split(",")),
        data_dir=args.data_dir,
        base_url=args.base_url,
        compression=args.compression,
        flush_interval_s=args.flush_interval,
        flush_rows=args.flush_rows,
        stale_after_s=args.stale_after,
        max_connection_s=args.max_connection_hours * 3600,
        stats_interval_s=args.stats_interval,
        metrics_port=args.metrics_port,
    )


def install_signal_handlers(recorder: Recorder, loop: asyncio.AbstractEventLoop) -> None:
    def stop(signame: str) -> None:
        log.info("stop requested", extra={"signal": signame})
        recorder.request_stop()

    names = ["SIGINT", "SIGTERM"] + (["SIGBREAK"] if sys.platform == "win32" else [])
    for name in names:
        sig = getattr(signal, name)
        try:
            loop.add_signal_handler(sig, stop, name)
        except NotImplementedError:  # Windows event loops
            signal.signal(sig, lambda *_, n=name: loop.call_soon_threadsafe(stop, n))


async def _amain(args: argparse.Namespace) -> None:
    recorder = Recorder(config_from_args(args))
    loop = asyncio.get_running_loop()
    install_signal_handlers(recorder, loop)
    if args.duration:
        loop.call_later(args.duration, recorder.request_stop)
    await recorder.run()


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()
