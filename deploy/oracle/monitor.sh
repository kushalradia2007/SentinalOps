#!/usr/bin/env bash
# deploy/oracle/monitor.sh        one snapshot
# deploy/oracle/monitor.sh live   refresh every 10 s (Ctrl-C to quit)
PY=/opt/sentinalops/venv/bin/python
HERE="$(cd "$(dirname "$0")" && pwd)"
if [ "${1:-}" = "live" ]; then
  exec watch -n 10 -t "$PY $HERE/monitor.py"
fi
exec "$PY" "$HERE/monitor.py"
