#!/bin/sh
# Train a baseline champion on first boot so the API is always servable. Also retrain when the
# saved model expects different inputs than the configured features (e.g. a pre-chunk-2 model).
set -e
MODEL="${MODEL_PATH:-models/champion.joblib}"
if ! python - "$MODEL" <<'EOF'
import sys
from pathlib import Path

import joblib

from sentinalops.config import Settings, get_domain_config

path = Path(sys.argv[1])
if not path.exists():
    sys.exit(f"no champion at {path}")
names = list(getattr(joblib.load(path), "feature_names_in_", []))
if names != get_domain_config(Settings.from_env().domain_config).feature_names:
    sys.exit(f"champion at {path} does not match the configured features")
EOF
then
  echo "training baseline champion"
  python -m sentinalops.training.train_baseline --out "$MODEL"
fi
exec "$@"
