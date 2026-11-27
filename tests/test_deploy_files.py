"""The Oracle deployment kit: unit files, setup script syntax, preflight and monitor."""

import importlib.util
import shutil
import subprocess
from pathlib import Path

import pytest

KIT = Path(__file__).resolve().parents[1] / "deploy" / "oracle"


def load(name):
    spec = importlib.util.spec_from_file_location(name, KIT / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("unit", ["sentinalops-live.service", "sentinalops-heal.service"])
def test_unit_files_restart_and_start_at_boot(unit):
    text = (KIT / unit).read_text(encoding="utf-8")
    assert "Restart=always" in text and "WantedBy=multi-user.target" in text
    assert "EnvironmentFile=/etc/sentinalops.env" in text and "WorkingDirectory=" in text


def test_heal_unit_caps_threads_and_runs_niced():
    text = (KIT / "sentinalops-heal.service").read_text(encoding="utf-8")
    for var in ("OMP_NUM_THREADS=1", "OPENBLAS_NUM_THREADS=1", "MKL_NUM_THREADS=1"):
        assert var in text
    assert "Nice=10" in text and "--role heal" in text


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")
@pytest.mark.parametrize("script", ["setup.sh", "monitor.sh"])
def test_shell_scripts_parse(script):
    r = subprocess.run(["bash", "-n", script], cwd=KIT, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_setup_runs_preflight_with_venv_python_before_installing_units():
    text = (KIT / "setup.sh").read_text(encoding="utf-8")
    pre = text.index('"$VENV/bin/python" "$REPO/deploy/oracle/preflight.py"')
    assert text.index("python3 -m venv") < pre < text.index("systemctl enable")


def test_preflight_parses_args_and_counts():
    pf = load("preflight")
    args = pf.parse(["--seconds", "3"])
    assert args.seconds == 3 and "binance" in args.base_url
    assert pf.verdict({"depth5": 2, "aggTrade": 0}) is False
    assert pf.verdict({"depth5": 2, "aggTrade": 1}) is True


def test_monitor_renders_without_services(tmp_path):
    mon = load("monitor")
    text = mon.render(data_dir=tmp_path, models_dir=tmp_path, journal=lambda unit: "")
    for section in ("SERVICES", "LIVE PIPELINE", "HEAL", "LEDGER", "DISK AND MEMORY"):
        assert section in text


def test_setup_installs_only_aiokafka_not_the_stream_extra():
    text = (KIT / "setup.sh").read_text(encoding="utf-8")
    assert "[stream]" not in text  # faust-streaming is unused and a needless install risk on ARM
    assert 'pip" install -q -e "$REPO" aiokafka' in text


@pytest.mark.parametrize("name", ["setup.sh", "monitor.sh", "sentinalops-live.service",
                                  "sentinalops-heal.service", "redpanda-compose.yml"])
def test_linux_files_have_unix_line_endings(name):
    assert b"\r" not in (KIT / name).read_bytes(), f"{name} has CRLF line endings"
