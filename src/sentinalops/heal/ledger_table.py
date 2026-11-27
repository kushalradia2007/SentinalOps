"""The heal loop's training table, read from the prediction ledger (live wiring).

Every matured prediction in the ledger already carries the features it was scored on and its
verified outcome, up to 30 s ago. The offline pipeline leaves out the newest hour, which would
leave the heal loop's holdout (the newest slice of the window) empty.

Rows kept: matured and not void (``label IS NOT NULL``; outcomes voided as ``already_stressed``,
``gap`` or ``warmup`` never train), with stored features. Champion and shadow score the same tick
on identical features, so one row per tick is kept: the earliest scored. The read is bounded to
``[since_ms, until_ms)``.
"""

from __future__ import annotations

import json
from contextlib import closing

import pandas as pd

from sentinalops.serving.ledger import Ledger

_QUERY = """
SELECT p.symbol, p.ts_ms, p.features, o.label
FROM predictions p
JOIN outcomes o ON o.symbol = p.symbol AND o.ts_ms = p.ts_ms AND o.horizon_s = p.horizon_s
WHERE p.ts_ms >= ? AND p.ts_ms < ? AND o.label IS NOT NULL AND p.features IS NOT NULL
ORDER BY p.symbol, p.ts_ms, p.scored_at_ms, p.prediction_id
"""


def ledger_table(ledger: Ledger, since_ms: int, until_ms: int, feature_names: list[str]) -> pd.DataFrame:
    """Training-table-shaped rows (``symbol, ts_ms, <features>, label, label_status, regime, source``)."""
    with closing(ledger._connect()) as db:
        rows = db.execute(_QUERY, (since_ms, until_ms)).fetchall()
    records, seen = [], set()
    for r in rows:
        key = (r["symbol"], r["ts_ms"])
        if key in seen:
            continue
        seen.add(key)
        feats = json.loads(r["features"])
        if any(name not in feats for name in feature_names):
            continue  # scored on a different feature set (e.g. before a config change)
        records.append({"symbol": r["symbol"], "ts_ms": r["ts_ms"],
                        **{n: float(feats[n]) for n in feature_names}, "label": int(r["label"])})
    df = pd.DataFrame(records, columns=["symbol", "ts_ms", *feature_names, "label"])
    df["label_status"], df["regime"], df["source"] = "ok", "untagged", "ledger"
    return df.reset_index(drop=True)
