#!/usr/bin/env bash
# Install or update SentinalOps live on an Oracle Cloud A1 VM (Ubuntu 24.04, ARM). Idempotent.
#
#   bash setup.sh                 # in-memory bus, no Parquet archive
#   bash setup.sh --record        # also archive raw rows (a backup recorder)
#   bash setup.sh --redpanda      # Docker + Redpanda (3 GB reserved) and --bus kafka
#
# Nothing on the system changes until the Binance pre-flight check has passed.
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/kushalradia2007/SentinalOps.git}"
BRANCH="${BRANCH:-main}"
BASE=/opt/sentinalops
REPO="$BASE/repo"
VENV="$BASE/venv"
STATE=/var/lib/sentinalops
RECORD=0
REDPANDA=0
for arg in "$@"; do
  case "$arg" in
    --record) RECORD=1 ;;
    --redpanda) REDPANDA=1 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done
ME="$(id -un)"

echo "== 1/7 packages"
sudo apt-get update -qq
sudo apt-get install -y -qq git python3-venv python3-pip sqlite3 >/dev/null

echo "== 2/7 code in $REPO (branch $BRANCH)"
sudo mkdir -p "$BASE" "$STATE"/{models,data,live}
# not all of $STATE: $STATE/redpanda belongs to the Redpanda container (uid 101)
sudo chown "$ME":"$ME" "$STATE"
sudo chown -R "$ME":"$ME" "$BASE" "$STATE"/{models,data,live}
if [ -d "$REPO/.git" ]; then
  git -C "$REPO" fetch -q origin "$BRANCH" && git -C "$REPO" checkout -q "$BRANCH" && git -C "$REPO" pull -q --ff-only
else
  git clone -q --branch "$BRANCH" "$REPO_URL" "$REPO"
fi

echo "== 3/7 python environment"
[ -x "$VENV/bin/python" ] || python3 -m venv "$VENV"
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q -e "$REPO" aiokafka   # aiokafka for --bus kafka; nothing else needed

echo "== 4/7 pre-flight: can this VM receive Binance data?"
if ! "$VENV/bin/python" "$REPO/deploy/oracle/preflight.py" --seconds 10; then
  echo "Stopping before any system change. Fix network access to Binance, then run setup.sh again." >&2
  exit 1
fi

echo "== 5/7 champion model"
export MODEL_PATH="$STATE/models/champion.joblib"
if [ ! -f "$MODEL_PATH" ]; then
  (cd "$REPO" && "$VENV/bin/python" -m sentinalops.training.train --out "$MODEL_PATH")
else
  echo "keeping existing $MODEL_PATH"
fi

BUS=memory
if [ "$REDPANDA" = 1 ]; then
  echo "== 5b redpanda"
  if ! command -v docker >/dev/null; then
    sudo apt-get install -y -qq docker.io docker-compose-v2 >/dev/null
    sudo systemctl enable --now docker
  fi
  sudo mkdir -p "$STATE/redpanda" && sudo chown 101:101 "$STATE/redpanda"
  sudo docker compose -f "$REPO/deploy/oracle/redpanda-compose.yml" up -d
  echo "waiting for Redpanda on 127.0.0.1:9092"
  for _ in $(seq 1 60); do
    if (exec 3<>/dev/tcp/127.0.0.1/9092) 2>/dev/null; then break; fi
    sleep 2
  done
  BUS=kafka
fi

echo "== 6/7 configuration and services"
FLAGS=""
[ "$RECORD" = 1 ] && FLAGS="--record"
sudo tee /etc/sentinalops.env >/dev/null <<EOF
MODEL_PATH=$STATE/models/champion.joblib
DATA_DIR=$STATE/data
DOMAIN_CONFIG=$REPO/configs/domain_liquidity.yaml
LOG_LEVEL=INFO
KAFKA_BROKER=127.0.0.1:9092
SENTINALOPS_BUS=$BUS
SENTINALOPS_LIVE_FLAGS=$FLAGS
EOF
for unit in sentinalops-live sentinalops-heal; do
  sed "s/__USER__/$ME/" "$REPO/deploy/oracle/$unit.service" | sudo tee "/etc/systemd/system/$unit.service" >/dev/null
done
sudo systemctl daemon-reload
sudo systemctl enable sentinalops-live sentinalops-heal
sudo systemctl restart sentinalops-live sentinalops-heal
chmod +x "$REPO/deploy/oracle/monitor.sh"

echo "== 7/7 status"
sleep 5
systemctl --no-pager --lines=0 status sentinalops-live sentinalops-heal | grep -E "●|Active:" || true
USED_PCT=$(free | awk '/^Mem:/ {printf "%d", ($3 / $2) * 100}')
echo "memory in use: ${USED_PCT}%"
if [ "$USED_PCT" -lt 20 ] && [ "$REDPANDA" = 0 ]; then
  echo "WARNING: memory use is under 20%. On an Always Free account Oracle may reclaim an idle VM."
  echo "         Upgrade the account to Pay As You Go, or run: bash setup.sh --redpanda"
elif [ "$REDPANDA" = 1 ]; then
  echo "idle-reclamation protection: Redpanda reserves 3 GB, keeping memory use above 20%"
else
  echo "idle-reclamation protection: memory use is above 20%"
fi
echo
echo "Watch it live:   $REPO/deploy/oracle/monitor.sh live"
echo "Logs:            journalctl -u sentinalops-live -f"
