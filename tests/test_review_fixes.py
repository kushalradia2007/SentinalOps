"""Regression tests for the external code review (ledger, hot reload, training, small edge cases)."""

import asyncio
import json
import math
import os
import sqlite3
import tomllib
from pathlib import Path

import joblib
import pytest

from sentinalops.config import get_domain_config
from sentinalops.schemas import OutcomeLabel, StressPrediction
from sentinalops.serving.ledger import Ledger
from sentinalops.serving.model import ChampionModel
from sentinalops.simulator.regimes import make_ticks

CFG = get_domain_config()
ROOT = Path(__file__).resolve().parents[1]
T0 = 1_763_200_800_000


def plan(db_path, sql, params=()):
    with sqlite3.connect(db_path) as db:
        return " | ".join(r[-1] for r in db.execute("EXPLAIN QUERY PLAN " + sql, params))


def test_time_range_queries_use_an_index(tmp_path):
    Ledger(tmp_path / "l.sqlite")
    p = plan(tmp_path / "l.sqlite", "SELECT * FROM predictions WHERE ts_ms >= ? AND ts_ms < ?", (0, 1))
    o = plan(tmp_path / "l.sqlite", "SELECT count(*) FROM outcomes WHERE ts_ms >= ?", (0,))
    assert "USING INDEX predictions_ts" in p, p
    assert "USING" in o and "INDEX outcomes_ts" in o, o


def test_scores_do_not_parse_features(tmp_path, monkeypatch):
    led = Ledger(tmp_path / "l.sqlite")
    led.add_many([(StressPrediction(symbol="BTCUSDT", ts_ms=T0, horizon_s=30, probability=0.7, alert=True,
                                    threshold=0.5, model_version="v1"), {"a": 1.0})],
                 [OutcomeLabel(symbol="BTCUSDT", ts_ms=T0, horizon_s=30, label=1)])

    def no_parsing(*a, **k):
        raise AssertionError("features were parsed")

    monkeypatch.setattr(json, "loads", no_parsing)
    assert led.scores(0).hit == 1


@pytest.fixture(scope="module")
def two_models():
    from sentinalops.training.train_baseline import fit

    a = fit(make_ticks("calm", 3000, seed=1))
    b = fit(make_ticks("volatile", 3000, seed=2))
    a.sentinal_version_, b.sentinal_version_ = "vA", "vB"
    return a, b


def test_hot_reload_sees_a_swap_within_the_same_mtime(tmp_path, two_models):
    path = tmp_path / "champion.joblib"
    joblib.dump(two_models[0], path)
    served = ChampionModel(path, CFG.feature_names)
    assert served.current()[1] == "vA"
    st = path.stat()
    tmp = tmp_path / "new.joblib"
    joblib.dump(two_models[1], tmp)
    os.replace(tmp, path)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns))  # same timestamp as the old file
    assert served.current()[1] == "vB"


def test_train_baseline_main_stamps_holdout_ap_and_writes_atomically(tmp_path):
    from sentinalops.training import train_baseline

    out = tmp_path / "champion.joblib"
    train_baseline.main(["--out", str(out), "--rows", "3000"])
    model = joblib.load(out)
    assert 0 < model.sentinal_holdout_ap_ <= 1 and model.sentinal_model_name_ == "gbm_v1"
    assert not list(tmp_path.glob("*.tmp"))


def test_makefile_train_uses_the_generic_command():
    make = (ROOT / "Makefile").read_text(encoding="utf-8")
    assert "sentinalops.training.train\n" in make.replace("\r\n", "\n")
    assert "train_baseline" not in make


def test_gbm_v1_accepts_random_state_in_params():
    from sentinalops.models import get_recipe

    model = get_recipe("gbm_v1").fit(make_ticks("calm", 2000), CFG, seed=0,
                                     params={"random_state": 5, "max_iter": 10})
    assert model.random_state == 5


def test_train_cli_rejects_non_object_params(tmp_path):
    from sentinalops.training import train

    with pytest.raises(SystemExit):
        train.main(["--params", "[1, 2]", "--rows", "2000", "--out", str(tmp_path / "m.joblib")])


def test_in_memory_bus_close_twice_is_safe():
    from sentinalops.live.bus import InMemoryBus

    async def main():
        bus = InMemoryBus(maxsize=1)
        await bus.subscribe(["raw_depth"], group="g")
        await bus.publish("raw_depth", "BTCUSDT", {"i": 1})
        await bus.close()
        await bus.close()

    asyncio.run(main())


def test_heal_health_is_strict_json(tmp_path, two_models):
    from sentinalops.heal.registry import ModelRegistry
    from sentinalops.live.heal_service import HealService

    reg = ModelRegistry(tmp_path / "m")
    joblib.dump(two_models[0], reg.champion_path)  # a champion with no evidence yet: live AP is NaN
    svc = HealService(CFG, Ledger(tmp_path / "l.sqlite"), reg)
    assert svc.tick(T0).decision.reason == "too_few_events"
    h = svc.health()
    json.dumps(h, allow_nan=False)
    assert h["live_ap"] is None or not math.isnan(h["live_ap"])


def test_stream_extra_has_no_unused_framework():
    extras = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"][
        "optional-dependencies"]
    assert "aiokafka" in extras["stream"] and not any("faust" in d for d in extras["stream"])


def test_setup_rerun_keeps_redpanda_ownership_and_waits_for_the_broker():
    text = (ROOT / "deploy" / "oracle" / "setup.sh").read_text(encoding="utf-8")
    assert 'chown -R "$ME":"$ME" "$BASE" "$STATE"\n' not in text  # would reset $STATE/redpanda
    assert '"$STATE"/{models,data,live}' in text
    assert "9092" in text and "for _ in" in text  # waits for Redpanda before starting the services
