"""End-to-end smoke test against a running stack: python scripts/smoke.py [base_url]."""
from __future__ import annotations

import json
import sys
import time
import urllib.request

from sentinalops.simulator.zero_day import benign_package, evil_package


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
    for label, pkg in (("benign", benign_package()), ("evil", evil_package())):
        files, meta = pkg
        print(label, call(base, "/score", {"files": files, "meta": meta}))
    print("label", call(base, "/labels", {"package": "trusted-core-lib", "is_malicious": True}))
    print("smoke OK")


if __name__ == "__main__":
    main()
