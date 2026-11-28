"""Evaluation metrics for a rare-event forecast (chunk 26.2, 26.3).

Accuracy is meaningless when stress is rare, so the scorecard is:

- **Ranking:** average precision (PR-AUC, tie-aware) and ROC-AUC.
- **Calibration:** Brier score and a reliability table.
- **Operating points:** at the configured alert threshold (realistic) and at the threshold that
  gives a 1% false-positive rate on the scored data (an *oracle* point: it compares how well models
  rank, but could not be chosen live). Each reports precision, recall, false alarms per hour and
  episode-level lead time.
- **Latency:** single-row scoring through ``ChampionModel``.

Episodes: every positive row points at a stress onset (``ts_ms + time_to_onset_s``). An episode is
caught if any of its rows alerted; its lead time is the largest ``time_to_onset_s`` that alerted.

Undefined values (no positives, no alerts) are ``None``, so reports are strict JSON.
"""

from __future__ import annotations

import math
import time

import numpy as np
from sklearn.metrics import roc_auc_score

from sentinalops.labels import average_precision as _ap


def _num(x) -> float | None:
    if x is None:
        return None
    x = float(x)
    return None if math.isnan(x) or math.isinf(x) else x


def average_precision(y, p) -> float | None:
    y = np.asarray(y, float)
    return _num(_ap(y, np.asarray(p, float))) if y.sum() > 0 else None


def roc_auc(y, p) -> float | None:
    y = np.asarray(y, float)
    if len(np.unique(y)) < 2:
        return None
    return _num(roc_auc_score(y, np.asarray(p, float)))


def brier(y, p) -> float | None:
    y, p = np.asarray(y, float), np.asarray(p, float)
    return _num(np.mean((p - y) ** 2)) if len(y) else None


def reliability(y, p, bins: int = 10) -> list[dict]:
    y, p = np.asarray(y, float), np.asarray(p, float)
    edges = np.linspace(0, 1, bins + 1)
    idx = np.clip(np.digitize(p, edges[1:-1]), 0, bins - 1)
    out = []
    for b in range(bins):
        m = idx == b
        out.append({"lo": float(edges[b]), "hi": float(edges[b + 1]), "n": int(m.sum()),
                    "mean_p": _num(p[m].mean()) if m.any() else None,
                    "frac_pos": _num(y[m].mean()) if m.any() else None})
    return out


def threshold_at_fpr(y, p, fpr: float = 0.01) -> float | None:
    """Lowest score threshold whose false-positive rate is at most ``fpr`` (None if none exists)."""
    y, p = np.asarray(y, float), np.asarray(p, float)
    neg = np.sort(p[y == 0])
    if len(neg) == 0:
        return None
    allowed = math.floor(fpr * len(neg))
    candidates = np.unique(p)[::-1]  # high to low
    best = None
    for t in candidates:
        if len(neg) - np.searchsorted(neg, t, side="left") > allowed:  # negatives with score >= t
            break
        best = float(t)
    return best


def operating_point(y, p, threshold: float | None, ts_ms, time_to_onset_s) -> dict:
    y, p = np.asarray(y, float), np.asarray(p, float)
    ts = np.asarray(ts_ms, np.int64)
    tto = np.asarray(time_to_onset_s, float)
    out = {"threshold": _num(threshold), "alerts": None, "precision": None, "recall": None,
           "false_alarms_per_hour": None, "episodes": None, "episodes_caught_share": None,
           "median_lead_s": None}
    if threshold is None or len(y) == 0:
        return out
    alert = p >= threshold
    tp, fp, pos = int((alert & (y == 1)).sum()), int((alert & (y == 0)).sum()), int(y.sum())
    hours = (int(ts.max()) - int(ts.min()) + 1000) / 3_600_000
    out.update(alerts=int(alert.sum()),
               precision=tp / alert.sum() if alert.any() else None,
               recall=tp / pos if pos else None,
               false_alarms_per_hour=fp / hours if hours > 0 else None)
    is_pos = (y == 1) & ~np.isnan(tto)
    if is_pos.any():
        onset = ts[is_pos] + (tto[is_pos] * 1000).astype(np.int64)
        leads: dict[int, float] = {}
        episodes = set(onset.tolist())
        for o, lead, a in zip(onset.tolist(), tto[is_pos].tolist(), alert[is_pos].tolist(), strict=True):
            if a:
                leads[o] = max(leads.get(o, 0.0), lead)
        out.update(episodes=len(episodes), episodes_caught_share=len(leads) / len(episodes),
                   median_lead_s=float(np.median(list(leads.values()))) if leads else None)
    return out


def summarize(y, p, ts_ms, time_to_onset_s, alert_threshold: float) -> dict:
    y, p = np.asarray(y, float), np.asarray(p, float)
    ap = average_precision(y, p)
    base = _num(y.mean()) if len(y) else None
    return {
        "n": int(len(y)), "positives": int(y.sum()), "base_rate": base,
        "ap": ap, "roc_auc": roc_auc(y, p), "brier": brier(y, p),
        "lift": ap / base if ap is not None and base else None,
        "at_alert": operating_point(y, p, alert_threshold, ts_ms, time_to_onset_s),
        "at_1pct_fpr": {**operating_point(y, p, threshold_at_fpr(y, p, 0.01), ts_ms, time_to_onset_s),
                        "oracle": True},
        "reliability": reliability(y, p),
    }


def latency_ms(model, rows: list[dict], n: int = 200, warmup: int = 20) -> dict:
    """Single-row scoring latency through ``model.predict(features) -> (p, version)``."""
    if not rows:
        return {"p50": None, "p95": None, "p99": None}
    for i in range(warmup):
        model.predict(rows[i % len(rows)])
    times = []
    for i in range(n):
        t0 = time.perf_counter()
        model.predict(rows[i % len(rows)])
        times.append((time.perf_counter() - t0) * 1000)
    q = np.percentile(times, [50, 95, 99])
    return {"p50": float(q[0]), "p95": float(q[1]), "p99": float(q[2])}
