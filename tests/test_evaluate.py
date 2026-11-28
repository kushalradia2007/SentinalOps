"""The evaluation harness end to end on synthetic recordings (plumbing, not model quality)."""

import json

import pytest

from pipeline_helpers import H0, make_events, write_recording
from sentinalops.config import get_domain_config
from sentinalops.training import evaluate as E
from sentinalops.training.relabel import load_recording, stress_reference

CFG = get_domain_config()
FAST = ["persistence_v1", "logit_v1", "lgbm_v1"]


def recording(tmp_path, hours, stress):
    depth, trades = make_events(H0, hours * 3600, stress=stress, seed=8)
    write_recording(tmp_path / "raw", depth, trades)
    table, depth_read, gaps = load_recording(tmp_path / "raw", tmp_path / "features", CFG)
    return table, stress_reference(depth_read, CFG.label_params()), gaps


@pytest.fixture(scope="module")
def ten_hours(tmp_path_factory):
    episodes = tuple((s, 45) for s in range(1500, 10 * 3600 - 600, 1500))
    return recording(tmp_path_factory.mktemp("ten"), 10, episodes)


def test_evaluate_end_to_end_writes_reports(ten_hours, tmp_path):
    table, ref, gaps = ten_hours
    report = E.evaluate(table, ref, gaps, CFG, FAST, n_folds=3, stress_share=0.01, min_train_s=2 * 3600)
    scored = [f for f in report["folds"] if "skipped" not in f]
    assert report["preliminary"] is True and len(scored) == 3
    for f in scored:
        assert {"depth_fraction", "spread_multiple", "use_spread"} <= set(f["label_params"])
        assert f["base_rate"]["eligible_ticks"] > 0 and "verdict" in f
        assert {"base_rate", "persistence_offline", *FAST} <= set(f["results"])
        assert f["results"]["lgbm_v1"]["fit_info"]["calibrated"] in (True, False)
    assert report["latency"]["lgbm_v1"]["p99"] is not None
    json.dumps(report, allow_nan=False)
    md = E.render_markdown(report)
    assert "PRELIMINARY" in md and "persistence_offline" in md and "oracle" in md.lower()
    for name in FAST:
        assert name in md


def test_main_writes_markdown_and_json(tmp_path):
    depth, trades = make_events(H0, 4 * 3600, stress=((2000, 60), (6000, 60), (9000, 60)), seed=9)
    write_recording(tmp_path / "raw", depth, trades)
    md, js = tmp_path / "results.md", tmp_path / "reports"
    E.main(["--data-dir", str(tmp_path / "raw"), "--table-dir", str(tmp_path / "features"),
            "--models", "persistence_v1,logit_v1", "--folds", "1", "--min-train-hours", "1",
            "--report", str(md), "--json-dir", str(js)])
    assert "PRELIMINARY" in md.read_text(encoding="utf-8")
    [out] = list(js.glob("eval-*.json"))
    assert json.loads(out.read_text(encoding="utf-8"))["folds"]


def test_fold_skipped_when_reference_too_short(ten_hours):
    table, ref, gaps = ten_hours
    report = E.evaluate(table, ref, gaps, CFG, ["persistence_v1"], n_folds=3, min_train_s=600)
    assert "at least 1000" in report["folds"][0]["skipped"]
    assert any("skipped" not in f for f in report["folds"][1:])


def test_quiet_fold_has_none_metrics(tmp_path):
    # stress only in the first two hours: thresholds fitted there are never crossed later
    table, ref, gaps = recording(tmp_path, 4, ((900, 60), (3000, 60), (5400, 60)))
    report = E.evaluate(table, ref, gaps, CFG, ["persistence_v1"], n_folds=2, min_train_s=2 * 3600)
    quiet = [f for f in report["folds"] if "skipped" not in f and f["test_positives"] == 0]
    assert quiet, report["folds"]
    assert quiet[0]["results"]["persistence_v1"]["ap"] is None
    json.dumps(report, allow_nan=False)
