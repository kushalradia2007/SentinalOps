#!/bin/sh
# Train a baseline champion on first boot so the API is always servable.
set -e
MODEL="${MODEL_PATH:-models/champion.joblib}"
if [ ! -f "$MODEL" ]; then
  echo "no champion at $MODEL, training baseline"
  python -m sentinalops.training.train_baseline --out "$MODEL"
fi
exec "$@"
