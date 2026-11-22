"""End-to-end smoke test against a running stack: python scripts/smoke.py [base_url]."""
from __future__ import annotations

import json
import sys
import time
import urllib.request

from sentinalops.simulator.regimes import example_ticks


def call(base: str, path: str, body: dict | None = None):
    req = urllib.request.Request(
        base + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def wait_healthy(base: str, tries: int = 30) -> None:
    for _ in range(tries):
        try:
            if call(base, "/health")["status"] == "ok":
                return
        except OSError:
            time.sleep(2)
    sys.exit("API never became healthy")


def main() -> None:
    base = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    wait_healthy(base)
    ts_ms = int(time.time() * 1000)
    calm, stressed = example_ticks()
    for i, (label, features) in enumerate((("calm", calm), ("stressed", stressed))):
        body = {"symbol": "BTCUSDT", "ts_ms": ts_ms + i * 1000, "features": features}
        r = call(base, "/score", body)
        print(f"{label:9s} probability={r['probability']:.3f} alert={r['alert']}")
    outcome = {"symbol": "BTCUSDT", "ts_ms": ts_ms + 1000, "horizon_s": 30, "label": 1}
    print("outcome", call(base, "/outcomes", outcome))
    print("smoke OK")


if __name__ == "__main__":
    main()
