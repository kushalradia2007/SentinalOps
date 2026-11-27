from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from sentinalops.config import DomainConfig, Settings, get_domain_config, load_domain_config
from sentinalops.features.microstructure import FEATURE_ORDER
from sentinalops.labels import LabelParams

REPO_CONFIG = "configs/domain_liquidity.yaml"


def _raw() -> dict:
    return yaml.safe_load(Path(REPO_CONFIG).read_text(encoding="utf-8"))


def test_repo_config_loads_every_key_typed():
    cfg = load_domain_config(REPO_CONFIG)
    assert cfg.domain == "market_liquidity" and cfg.symbol == "BTCUSDT"
    assert cfg.features_module == "sentinalops.features.microstructure"
    assert cfg.feature_windows_seconds == (1, 5, 30, 60)
    assert cfg.label_horizon_seconds == 30 and cfg.stale_ms == 3000
    assert cfg.label.depth_fraction == 0.5 and cfg.label.secondary == "spread_expansion"
    assert cfg.sources.live == ["binance_ws_aggTrade", "binance_ws_depth5_100ms"]
    assert cfg.feature_names == FEATURE_ORDER


def test_default_domain_is_liquidity():
    assert get_domain_config().domain == "market_liquidity"
    assert get_domain_config() is get_domain_config()


def test_label_and_feature_params_come_from_the_config():
    cfg = load_domain_config(REPO_CONFIG)
    lp = cfg.label_params()
    assert lp == LabelParams.from_domain_yaml(REPO_CONFIG)
    assert (lp.horizon_s, lp.reference_window_s, lp.min_reference_ticks, lp.stale_ms) == (30, 600, 300, 3000)
    assert lp.use_spread is True
    fp = cfg.feature_params()
    assert fp.windows_s == (1, 5, 30, 60) and fp.stale_ms == 3000 and fp.names == FEATURE_ORDER


@pytest.mark.parametrize(
    "patch",
    [
        {"unknown_key": 1},  # typo protection
        {"alert_threshold": 1.5},
        {"symbol": "btc-usdt"},
        {"label_horizon_seconds": 0},
        {"feature_windows_seconds": [60, 30]},
        {"feature_windows_seconds": [5, 5]},
        {"split_embargo_seconds": 10},  # shorter than the horizon leaks labels across the split
        {"drift_watch": ["no_such_feature"]},
        {"drift_watch": []},
        {"stale_data_seconds": 0},
        {"label": {"depth_fraction": 1.2}},
        {"label": {"spread_multiple": 0.9}},
        {"label": {"onset_only": False}},
        {"label": {"primary": "something_else"}},
        {"label": {"reference_window_seconds": 100, "min_reference_seconds": 300}},
        {"label": {"typo": 1}},
        {"prediction_cadence_seconds": 7},  # 30 s horizon is not a multiple of 7 s
    ],
)
def test_bad_values_raise(patch):
    raw = _raw()
    for k, v in patch.items():
        raw[k] = {**raw[k], **v} if isinstance(v, dict) and isinstance(raw.get(k), dict) else v
    with pytest.raises(ValidationError):
        DomainConfig(**raw)


def test_spread_label_can_be_switched_off():
    raw = _raw()
    raw["label"]["secondary"] = None
    assert DomainConfig(**raw).label_params().use_spread is False


def test_non_mapping_yaml_is_rejected(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("- just\n- a list\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_domain_config(p)


def test_settings_defaults_and_env_override():
    s = Settings.from_env({})
    assert s.model_path == Path("models/champion.joblib") and s.alert_threshold is None
    assert s.domain_config == Path("configs/domain_liquidity.yaml")
    s = Settings.from_env({"ALERT_THRESHOLD": "0.8", "LOG_LEVEL": "DEBUG", "KAFKA_BROKER": ""})
    assert s.alert_threshold == 0.8 and s.log_level == "DEBUG"
    assert s.kafka_broker == "localhost:9092"  # empty string ignored


def test_effective_threshold_prefers_env_over_domain():
    cfg = load_domain_config(REPO_CONFIG)
    assert Settings().effective_threshold(cfg) == cfg.alert_threshold
    assert Settings(alert_threshold=0.9).effective_threshold(cfg) == 0.9


def test_model_block_defaults_to_gbm_v1():
    raw = _raw()
    raw.pop("model", None)
    cfg = DomainConfig(**raw)
    assert cfg.model.name == "gbm_v1" and cfg.model.params == {}


def test_unknown_model_name_rejected():
    with pytest.raises(ValidationError, match="unknown model 'no_such_model'"):
        DomainConfig(**{**_raw(), "model": {"name": "no_such_model"}})


def test_yaml_model_block_loads():
    assert _raw()["model"] == {"name": "gbm_v1", "params": {}}
    assert load_domain_config(REPO_CONFIG).model.name == "gbm_v1"
