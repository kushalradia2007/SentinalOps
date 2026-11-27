"""Pre-flight: can this machine receive Binance market data? Run with the project's venv Python.

Opens the same combined stream the recorder uses for ``--seconds`` and exits 0 only if at least
one order-book snapshot and one trade arrived. Cloud IP ranges can be treated differently from
home or university networks, so ``setup.sh`` runs this before installing any service.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time


def parse(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seconds", type=float, default=10.0)
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--base-url", default="wss://stream.binance.com:9443")
    return ap.parse_args(argv)


def verdict(counts: dict[str, int]) -> bool:
    return counts.get("depth5", 0) > 0 and counts.get("aggTrade", 0) > 0


async def listen(args: argparse.Namespace) -> dict[str, int]:
    import websockets

    from sentinalops.data.recorder.messages import build_url, parse_message

    counts = {"depth5": 0, "aggTrade": 0}
    url = build_url([args.symbol], args.base_url)
    deadline = time.monotonic() + args.seconds
    async with websockets.connect(url, open_timeout=10) as ws:
        while time.monotonic() < deadline:
            try:
                async with asyncio.timeout(max(0.1, deadline - time.monotonic())):
                    raw = await ws.recv()
            except TimeoutError:
                break
            parsed = parse_message(raw, int(time.time() * 1000))
            if parsed:
                counts[parsed[0]] += 1
    return counts


def main(argv: list[str] | None = None) -> int:
    args = parse(argv)
    try:
        counts = asyncio.run(listen(args))
    except Exception as e:  # noqa: BLE001 - report any network/TLS/HTTP failure plainly
        print(f"PREFLIGHT FAILED: cannot open the Binance stream: {type(e).__name__}: {e}")
        print("Common causes: no outbound internet, a blocked region (HTTP 451), or a firewall.")
        return 1
    ok = verdict(counts)
    print(f"PREFLIGHT {'OK' if ok else 'FAILED'}: {counts['depth5']} order-book snapshots, "
          f"{counts['aggTrade']} trades in {args.seconds:.0f} s")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
