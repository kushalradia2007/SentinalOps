"""``python -m sentinalops.live``: the live pipeline, or the heal service with ``--role heal``.

Only the standard library is imported at module level: the heal role must set its thread limits
before NumPy / scikit-learn create their thread pools.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

THREAD_VARS = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")
SPLIT_ROLES = ("feed", "features", "outcomes", "ledger")


def parse(argv: list[str] | None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="python -m sentinalops.live", description=__doc__)
    ap.add_argument("--role", default="pipeline", choices=["pipeline", "heal", *SPLIT_ROLES])
    ap.add_argument("--bus", default="memory", choices=["memory", "kafka"])
    ap.add_argument("--record", action="store_true", help="also archive raw rows to Parquet")
    ap.add_argument("--data-dir", default="data/live", type=Path,
                    help="feed state, gap log (and Parquet with --record)")
    ap.add_argument("--health-interval-s", type=float, default=60.0)
    ap.add_argument("--heal-interval-min", type=float, default=5.0)
    args = ap.parse_args(argv)
    if args.bus == "memory" and args.role in SPLIT_ROLES:
        ap.error(f"--role {args.role} runs only with --bus kafka; on the memory bus use --role pipeline")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse(argv)
    if args.role == "heal":
        for var in THREAD_VARS:  # before any NumPy / scikit-learn import
            os.environ.setdefault(var, "1")

    from sentinalops.config import Settings, get_domain_config
    from sentinalops.logging import configure_logging

    settings = Settings.from_env()
    configure_logging(settings.log_level)
    cfg = get_domain_config(settings.domain_config)
    if args.role == "heal":
        from sentinalops.live.heal_service import run_heal

        asyncio.run(run_heal(cfg, settings, args.heal_interval_min, args.health_interval_s))
    else:
        from sentinalops.live.runner import run_live

        asyncio.run(run_live(args.role, args.bus, cfg, settings, args.data_dir, args.record,
                             args.health_interval_s))


if __name__ == "__main__":
    main()
