import pytest
from pydantic import ValidationError

from sentinalops.config import DomainConfig, Settings, get_domain_config, load_domain_config


def test_domain_config_loads_repo_file():
    cfg = load_domain_config("configs/domain_supply_chain.yaml")
    assert cfg.domain == "software_supply_chain"
    assert "base64_entropy" in cfg.drift_watch
    assert cfg.retrain_window_hours == 48


def test_domain_config_validates_ranges():
    with pytest.raises(ValidationError):
        DomainConfig(domain="x", features_module="m", drift_watch=[], block_threshold=1.5)


def test_get_domain_config_is_cached():
    assert get_domain_config() is get_domain_config()


def test_settings_defaults_and_env_override():
    s = Settings.from_env({})
    assert str(s.model_path) == "models/champion.joblib" and s.block_threshold is None
    s = Settings.from_env({"BLOCK_THRESHOLD": "0.8", "LOG_LEVEL": "DEBUG", "KAFKA_BROKER": ""})
    assert s.block_threshold == 0.8 and s.log_level == "DEBUG"
    assert s.kafka_broker == "localhost:9092"  # empty string ignored


def test_effective_threshold_prefers_env_over_domain():
    cfg = load_domain_config("configs/domain_supply_chain.yaml")
    assert Settings().effective_threshold(cfg) == cfg.block_threshold
    assert Settings(block_threshold=0.9).effective_threshold(cfg) == 0.9
