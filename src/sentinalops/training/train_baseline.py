"""Train the Champion on classic attacks and save it (and log to MLflow if present)."""
from __future__ import annotations

import argparse
from pathlib import Path

import joblib
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import average_precision_score
from sklearn.model_selection import train_test_split

from sentinalops.features.extract import FEATURE_ORDER
from sentinalops.training.synthetic import make_dataset

MODEL_PATH = Path("models/champion.joblib")


def train(df, seed: int = 0):
    X_tr, X_te, y_tr, y_te = train_test_split(
        df[FEATURE_ORDER], df["label"], test_size=0.2, stratify=df["label"], random_state=seed)
    model = GradientBoostingClassifier(random_state=seed).fit(X_tr, y_tr)
    ap = average_precision_score(y_te, model.predict_proba(X_te)[:, 1])
    return model, ap


def main():
    ap_ = argparse.ArgumentParser()
    ap_.add_argument("--out", default=str(MODEL_PATH))
    args = ap_.parse_args()
    model, ap = train(make_dataset("classic"))
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, args.out)
    print(f"champion saved to {args.out}  (classic-regime AP={ap:.3f})")
    try:
        import mlflow
        with mlflow.start_run(run_name="champion-baseline"):
            mlflow.log_metric("avg_precision", ap)
            mlflow.sklearn.log_model(model, "model")
    except Exception as e:  # MLflow optional locally
        print(f"(mlflow skipped: {e.__class__.__name__})")


if __name__ == "__main__":
    main()
