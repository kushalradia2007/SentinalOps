"""Train the baseline stress-forecast champion on calm-regime ticks and save it.

Until recorded data flows through chunk 12, training uses the synthetic regimes in
``sentinalops.simulator.regimes``. The saved model is a fitted scikit-learn classifier whose
``feature_names_in_`` is ``FEATURE_ORDER``; the API refuses a model with different inputs.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score
from sklearn.model_selection import train_test_split

from sentinalops.features.microstructure import FEATURE_ORDER
from sentinalops.simulator.regimes import make_ticks

MODEL_PATH = Path("models/champion.joblib")


def fit(df: pd.DataFrame, seed: int = 0) -> HistGradientBoostingClassifier:
    model = HistGradientBoostingClassifier(max_iter=200, learning_rate=0.08, random_state=seed)
    return model.fit(df[FEATURE_ORDER], df["label"])


def evaluate(model, df: pd.DataFrame) -> float:
    """Average precision (PR-AUC) of ``model`` on ``df``."""
    return float(average_precision_score(df["label"], model.predict_proba(df[FEATURE_ORDER])[:, 1]))


def train(df: pd.DataFrame, seed: int = 0) -> tuple[HistGradientBoostingClassifier, float]:
    """Fit on 80% of ``df`` and return the model with its average precision on the other 20%."""
    tr, te = train_test_split(df, test_size=0.2, stratify=df["label"], random_state=seed)
    model = fit(tr, seed)
    return model, evaluate(model, te)


def main(argv: list[str] | None = None) -> None:
    ap_ = argparse.ArgumentParser(description=__doc__)
    ap_.add_argument("--out", default=str(MODEL_PATH))
    ap_.add_argument("--rows", type=int, default=20_000)
    args = ap_.parse_args(argv)
    model, ap = train(make_ticks("calm", args.rows))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, out)
    print(f"champion saved to {out}  (calm-regime AP={ap:.3f})")
    try:
        import mlflow

        with mlflow.start_run(run_name="champion-baseline"):
            mlflow.log_metric("avg_precision", ap)
            mlflow.sklearn.log_model(model, "model")
    except Exception as e:  # MLflow is optional locally
        print(f"(mlflow skipped: {e.__class__.__name__})")


if __name__ == "__main__":
    main()
