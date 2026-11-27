"""One screen for the live system on the Oracle VM: services, live pipeline, heal loop, ledger, host.

    deploy/oracle/monitor.sh          one snapshot
    deploy/oracle/monitor.sh live     refresh every 10 s (Ctrl-C to quit)
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

STATE = Path(os.environ.get("SENTINALOPS_STATE", "/var/lib/sentinalops"))
UNITS = ("sentinalops-live", "sentinalops-heal")


def journal(unit: str) -> str:
    try:
        r = subprocess.run(["journalctl", "-u", unit, "-n", "400", "-o", "cat", "--no-pager"],
                           capture_output=True, text=True, timeout=10)
        return r.stdout
    except (OSError, subprocess.TimeoutExpired):
        return ""


def last_json(text: str, msg: str) -> dict | None:
    for line in reversed(text.splitlines()):
        if f'"msg": "{msg}"' in line:
            try:
                return json.loads(line)
            except ValueError:
                continue
    return None


def unit_state(unit: str) -> str:
    try:
        r = subprocess.run(["systemctl", "is-active", unit], capture_output=True, text=True, timeout=5)
        return r.stdout.strip() or "unknown"
    except (OSError, subprocess.TimeoutExpired):
        return "unknown"


def age(ts: str | None) -> str:
    if not ts:
        return "never"
    s = time.time() - datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    return f"{s:.0f}s ago" if s < 120 else f"{s / 60:.0f}m ago"


def ledger_section(data_dir: Path) -> list[str]:
    path = data_dir / "ledger.sqlite"
    if not path.exists():
        return ["no ledger yet"]
    now = int(time.time() * 1000)
    hour = now - 3_600_000
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        n, alerts, avg_p = db.execute("SELECT count(*), sum(alert), avg(probability) FROM predictions "
                                      "WHERE ts_ms >= ?", (hour,)).fetchone()
        last = db.execute("SELECT ts_ms, probability, model_version FROM predictions "
                          "ORDER BY ts_ms DESC LIMIT 1").fetchone()
        o = db.execute("SELECT count(*), sum(label = 1), sum(label = 0), sum(label IS NULL) FROM outcomes "
                       "WHERE ts_ms >= ?", (hour,)).fetchone()
        total = db.execute("SELECT count(*) FROM predictions").fetchone()[0]
    finally:
        db.close()
    lines = [f"last hour   : {n or 0:,} predictions, {alerts or 0} alerts, mean probability {avg_p or 0:.4f}",
             f"outcomes 1h : {o[0] or 0:,} (stress {o[1] or 0}, calm {o[2] or 0}, void {o[3] or 0})",
             f"all time    : {total:,} predictions; ledger {path.stat().st_size / 2**20:,.1f} MB"]
    if last:
        lines.append(f"latest      : p={last[1]:.4f} by {last[2]} ({(now - last[0]) / 1000:.0f}s ago)")
    return lines


def render(data_dir: Path = STATE / "data", models_dir: Path = STATE / "models", journal=journal) -> str:
    out = [f"SentinalOps live monitor | {datetime.now(UTC):%Y-%m-%d %H:%M:%S} UTC", "", "== SERVICES"]
    out += [f"{u:<18}: {unit_state(u)}" for u in UNITS]

    out += ["", "== LIVE PIPELINE"]
    h = last_json(journal("sentinalops-live"), "live health")
    if h:
        ev = h.get("events", {})
        out += [f"as of       : {h.get('ts')} ({age(h.get('ts'))})",
                f"events in   : depth {ev.get('depth5', 0):,}  trades {ev.get('aggTrade', 0):,}  "
                f"late {sum(h.get('late', {}).values())}",
                f"ticks       : {h.get('ticks', 0):,}  scored {h.get('predictions', 0):,}  "
                f"suspended {h.get('suspended_ticks', 0):,}  scoring errors {h.get('scoring_errors', 0)}",
                f"model       : {h.get('model_version')}  last p={h.get('last_probability')}",
                f"feed        : reconnects {h.get('reconnects', 0)}  gaps {h.get('gaps', 0)}  "
                f"drops {h.get('feed_drops', 0)}  bus drops {sum(h.get('bus_drops', {}).values())}",
                f"ledger      : written {h.get('ledger_predictions', 0):,} predictions, "
                f"{h.get('ledger_outcomes', 0):,} outcomes, errors {h.get('ledger_errors', 0)}"]
    else:
        out.append("no health line yet (first one ~60 s after start)")

    out += ["", "== HEAL"]
    hh = last_json(journal("sentinalops-heal"), "heal health")
    if hh:
        out += [f"as of       : {hh.get('ts')} ({age(hh.get('ts'))})",
                f"champion    : {hh.get('champion')}  shadow {hh.get('shadow')}",
                f"last cycle  : {hh.get('action')} ({hh.get('reason')}), matured labels {hh.get('matured')}, "
                f"live AP {hh.get('live_ap')}",
                f"history     : {hh.get('promotions', 0)} promotions, {hh.get('rollbacks', 0)} rollbacks"]
    else:
        out.append("no heal cycle yet (every 5 minutes)")

    out += ["", "== LEDGER", *ledger_section(data_dir)]

    out += ["", "== DISK AND MEMORY"]
    du = shutil.disk_usage(data_dir if data_dir.exists() else "/")
    out.append(f"disk        : {du.free / 2**30:,.1f} GB free of {du.total / 2**30:,.1f} GB")
    try:
        mem = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
        total = int(mem["MemTotal"].split()[0])
        avail = int(mem["MemAvailable"].split()[0])
        used = 100 * (total - avail) / total
        flag = "" if used >= 20 else "  (under 20%: idle-reclamation risk on Always Free)"
        out.append(f"memory      : {used:.0f}% in use of {total / 2**20:.1f} GB{flag}")
    except (OSError, KeyError, ValueError):
        out.append("memory      : n/a")
    champion = models_dir / "champion.joblib"
    out.append(f"champion    : {'present' if champion.exists() else 'MISSING'} ({champion})")
    return "\n".join(out)


if __name__ == "__main__":
    print(render())
