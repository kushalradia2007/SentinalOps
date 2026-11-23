"""Prediction ledger (chunk 34): every prediction, its outcome once matured, and the scores.

SQLite, append-only: two insert-only tables, nothing is ever updated or deleted.

- ``predictions``: one row per ``StressPrediction`` plus the feature snapshot it was scored on.
- ``outcomes``: one row per verified tick (``OutcomeLabel``), whoever produced it.

A prediction's outcome is the outcome row with the same ``symbol``, ``ts_ms`` and
``horizon_s``. Writes are idempotent (a replayed message is ignored), so the ledger can be fed
from a topic that delivers at least once.

Each matured prediction is one of: ``hit`` (alert, stress came), ``miss`` (no alert, stress
came), ``false_alarm``, ``correct_negative``, or ``void`` (outcome unknowable; excluded from
every metric). Predictions whose outcome has not arrived yet are ``pending``.
"""

from __future__ import annotations

import json
import math
import sqlite3
from contextlib import closing
from dataclasses import asdict, dataclass
from pathlib import Path

from sentinalops.schemas import OutcomeLabel, StressPrediction

_SCHEMA = """
CREATE TABLE IF NOT EXISTS predictions (
    prediction_id TEXT PRIMARY KEY,
    symbol        TEXT    NOT NULL,
    ts_ms         INTEGER NOT NULL,
    horizon_s     INTEGER NOT NULL,
    probability   REAL    NOT NULL,
    alert         INTEGER NOT NULL,
    threshold     REAL    NOT NULL,
    model_version TEXT    NOT NULL,
    scored_at_ms  INTEGER NOT NULL,
    features      TEXT
);
CREATE INDEX IF NOT EXISTS predictions_time ON predictions (symbol, ts_ms);
CREATE TABLE IF NOT EXISTS outcomes (
    symbol         TEXT    NOT NULL,
    ts_ms          INTEGER NOT NULL,
    horizon_s      INTEGER NOT NULL,
    label          INTEGER,
    void_reason    TEXT,
    source         TEXT    NOT NULL,
    verified_at_ms INTEGER NOT NULL,
    PRIMARY KEY (symbol, ts_ms, horizon_s)
);
CREATE TRIGGER IF NOT EXISTS predictions_append_only BEFORE UPDATE ON predictions
BEGIN SELECT RAISE(ABORT, 'ledger is append-only'); END;
CREATE TRIGGER IF NOT EXISTS predictions_no_delete BEFORE DELETE ON predictions
BEGIN SELECT RAISE(ABORT, 'ledger is append-only'); END;
CREATE TRIGGER IF NOT EXISTS outcomes_append_only BEFORE UPDATE ON outcomes
BEGIN SELECT RAISE(ABORT, 'ledger is append-only'); END;
CREATE TRIGGER IF NOT EXISTS outcomes_no_delete BEFORE DELETE ON outcomes
BEGIN SELECT RAISE(ABORT, 'ledger is append-only'); END;
"""

_JOINED = """
SELECT p.prediction_id, p.symbol, p.ts_ms, p.horizon_s, p.probability, p.alert, p.threshold,
       p.model_version, p.scored_at_ms, p.features, o.label, o.void_reason, o.verified_at_ms,
       o.symbol IS NOT NULL AS matured
FROM predictions p
LEFT JOIN outcomes o ON o.symbol = p.symbol AND o.ts_ms = p.ts_ms AND o.horizon_s = p.horizon_s
"""


def classify(alert: bool, label: int | None, matured: bool) -> str:
    if not matured:
        return "pending"
    if label is None:
        return "void"
    if alert:
        return "hit" if label == 1 else "false_alarm"
    return "miss" if label == 1 else "correct_negative"


@dataclass
class Scores:
    """Confusion table and probability scores over matured, non-void predictions."""

    hit: int = 0
    miss: int = 0
    false_alarm: int = 0
    correct_negative: int = 0
    void: int = 0
    pending: int = 0
    brier: float = math.nan
    base_rate: float = math.nan

    @property
    def scored(self) -> int:
        return self.hit + self.miss + self.false_alarm + self.correct_negative

    @property
    def precision(self) -> float:
        n = self.hit + self.false_alarm
        return self.hit / n if n else math.nan

    @property
    def recall(self) -> float:
        n = self.hit + self.miss
        return self.hit / n if n else math.nan

    def to_dict(self) -> dict:
        return {**asdict(self), "scored": self.scored, "precision": self.precision, "recall": self.recall}


class Ledger:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db:
            db.executescript(_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        # One short-lived connection per call: safe from FastAPI's thread pool and async consumers.
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    # ---- writes (34.1, 34.3) -------------------------------------------------------------------
    def add_prediction(self, pred: StressPrediction, features: dict[str, float] | None = None) -> bool:
        """Store a prediction. Returns False if this ``prediction_id`` was already stored."""
        with closing(self._connect()) as db, db:
            cur = db.execute(
                "INSERT OR IGNORE INTO predictions VALUES (?,?,?,?,?,?,?,?,?,?)",
                (pred.prediction_id, pred.symbol, pred.ts_ms, pred.horizon_s, pred.probability,
                 int(pred.alert), pred.threshold, pred.model_version, pred.scored_at_ms,
                 json.dumps(features) if features is not None else None),
            )
            return cur.rowcount == 1

    def add_outcome(self, outcome: OutcomeLabel) -> bool:
        """Store a verified outcome. The first outcome for a tick wins; replays are ignored."""
        with closing(self._connect()) as db, db:
            cur = db.execute(
                "INSERT OR IGNORE INTO outcomes VALUES (?,?,?,?,?,?,?)",
                (outcome.symbol, outcome.ts_ms, outcome.horizon_s, outcome.label, outcome.void_reason,
                 outcome.source, outcome.verified_at_ms),
            )
            return cur.rowcount == 1

    def add_outcomes(self, outcomes: list[OutcomeLabel]) -> int:
        with closing(self._connect()) as db, db:
            cur = db.executemany(
                "INSERT OR IGNORE INTO outcomes VALUES (?,?,?,?,?,?,?)",
                [(o.symbol, o.ts_ms, o.horizon_s, o.label, o.void_reason, o.source, o.verified_at_ms)
                 for o in outcomes],
            )
            return cur.rowcount

    # ---- reads (34.5, 34.6) --------------------------------------------------------------------
    def _rows(self, where: str = "", params: tuple = ()) -> list[dict]:
        with closing(self._connect()) as db:
            rows = db.execute(f"{_JOINED} {where} ORDER BY p.ts_ms, p.prediction_id", params).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["alert"], d["matured"] = bool(d["alert"]), bool(d["matured"])
            d["features"] = json.loads(d["features"]) if d["features"] else None
            d["result"] = classify(d["alert"], d["label"], d["matured"])
            out.append(d)
        return out

    def get(self, prediction_id: str) -> dict | None:
        rows = self._rows("WHERE p.prediction_id = ?", (prediction_id,))
        return rows[0] if rows else None

    def predictions(self, since_ms: int = 0, until_ms: int | None = None,
                    model_version: str | None = None) -> list[dict]:
        clauses, params = ["p.ts_ms >= ?"], [since_ms]
        if until_ms is not None:
            clauses.append("p.ts_ms < ?")
            params.append(until_ms)
        if model_version is not None:
            clauses.append("p.model_version = ?")
            params.append(model_version)
        return self._rows("WHERE " + " AND ".join(clauses), tuple(params))

    def scores(self, since_ms: int = 0, until_ms: int | None = None,
               model_version: str | None = None) -> Scores:
        """Confusion table and Brier score for predictions made in ``[since_ms, until_ms)``."""
        s = Scores()
        sq_err, positives = 0.0, 0
        for r in self.predictions(since_ms, until_ms, model_version):
            setattr(s, r["result"], getattr(s, r["result"]) + 1)
            if r["result"] in ("pending", "void"):
                continue
            sq_err += (r["probability"] - r["label"]) ** 2
            positives += r["label"]
        if s.scored:
            s.brier = sq_err / s.scored
            s.base_rate = positives / s.scored
        return s
