"""Evaluation metrics: rare-event ranking, calibration, operating points and episode lead time."""

import json

import numpy as np
import pytest

from sentinalops.training import metrics as M


def test_ap_constant_scores_equals_prevalence():
    assert M.average_precision(np.array([1, 0, 0, 0]), np.full(4, 0.3)) == pytest.approx(0.25)


def test_metrics_none_without_positives():
    n = 100
    s = M.summarize(np.zeros(n), np.linspace(0, 1, n), np.arange(n) * 1000, np.full(n, np.nan), 0.5)
    assert s["ap"] is None and s["lift"] is None and s["roc_auc"] is None
    json.dumps(s, allow_nan=False)


def test_threshold_at_fpr_with_ties():
    y = np.r_[np.zeros(100), np.ones(10)]
    p = np.r_[np.arange(100) / 100, np.full(10, 0.995)]
    t = M.threshold_at_fpr(y, p, 0.01)
    alerts = p >= t
    assert alerts[y == 0].mean() <= 0.01 and alerts[y == 1].mean() == 1.0


def test_threshold_at_fpr_constant_scores_never_exceeds_target():
    y = np.r_[np.zeros(50), np.ones(5)]
    t = M.threshold_at_fpr(y, np.full(55, 0.2), 0.01)
    assert t is None or ((np.full(55, 0.2) >= t)[y == 0].mean() <= 0.01)


def test_episode_lead_time():
    # Episode A: onset at 100 s; positive ticks 70..99 (lead 30..1); alerts at leads 20 and 10.
    # Episode B: onset at 300 s; positives 280..299; no alerts.
    ts = np.arange(400) * 1000
    tto = np.full(400, np.nan)
    tto[70:100] = 100 - np.arange(70, 100)
    tto[280:300] = 300 - np.arange(280, 300)
    y = (~np.isnan(tto)).astype(int)
    p = np.zeros(400)
    p[80], p[90] = 0.9, 0.9  # leads 20 and 10
    op = M.operating_point(y, p, 0.5, ts, tto)
    assert op["episodes"] == 2 and op["episodes_caught_share"] == 0.5
    assert op["median_lead_s"] == 20


def test_false_alarms_per_hour():
    y = np.zeros(3600)
    p = np.zeros(3600)
    p[[10, 20, 30]] = 0.9
    op = M.operating_point(y, p, 0.5, np.arange(3600) * 1000, np.full(3600, np.nan))
    assert op["false_alarms_per_hour"] == pytest.approx(3.0)
    assert op["precision"] == 0.0 and op["recall"] is None


def test_brier_and_reliability_bins_sum_to_n():
    rng = np.random.default_rng(0)
    p = rng.random(500)
    y = (rng.random(500) < p).astype(int)
    assert M.brier(y, p) == pytest.approx(np.mean((p - y) ** 2))
    bins = M.reliability(y, p, bins=10)
    assert len(bins) == 10 and sum(b["n"] for b in bins) == 500


def test_summarize_has_both_operating_points():
    rng = np.random.default_rng(1)
    y = rng.binomial(1, 0.05, 2000)
    p = np.clip(y * 0.6 + rng.random(2000) * 0.5, 0, 1)
    tto = np.where(y == 1, 5.0, np.nan)
    s = M.summarize(y, p, np.arange(2000) * 1000, tto, 0.5)
    assert set(s) >= {"ap", "roc_auc", "brier", "base_rate", "lift", "at_alert", "at_1pct_fpr", "reliability"}
    assert s["at_1pct_fpr"]["oracle"] is True and s["lift"] > 1
