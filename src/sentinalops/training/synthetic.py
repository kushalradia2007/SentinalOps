"""Synthetic stand-in for MalwareBench / QUT-DV25 until real data is wired in.

Two regimes:
  * classic: malware is mostly typosquats from young accounts (champion trains here)
  * zero_day: malware hides in trusted packages with obfuscated install hooks
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from sentinalops.features.extract import FEATURE_ORDER


def _benign(n: int, rng: np.random.Generator) -> pd.DataFrame:
    return pd.DataFrame({
        "n_exec_eval": rng.poisson(0.3, n), "n_os_system": rng.poisson(0.2, n),
        "n_subprocess": rng.poisson(0.4, n), "n_network": rng.poisson(0.5, n),
        "has_install_hook": rng.binomial(1, 0.1, n),
        "max_file_entropy": rng.normal(4.6, 0.25, n),
        "base64_entropy": rng.choice([0.0, 4.5], n, p=[0.9, 0.1]) + rng.normal(0, 0.1, n).clip(0),
        "base64_blob_ratio": rng.beta(1, 80, n),
        "maintainer_age_days": rng.gamma(3, 300, n),
        "email_domain_changed": rng.binomial(1, 0.02, n),
        "dep_depth": rng.poisson(2, n),
        "repo_url_mismatch": rng.binomial(1, 0.03, n),
        "typosquat_score": np.zeros(n),
    })


def _classic_malware(n: int, rng: np.random.Generator) -> pd.DataFrame:
    df = _benign(n, rng)
    df["typosquat_score"] = rng.uniform(0.82, 0.98, n)
    df["maintainer_age_days"] = rng.gamma(1.2, 20, n)
    df["n_exec_eval"] = rng.poisson(0.8, n)
    df["has_install_hook"] = rng.binomial(1, 0.5, n)
    return df


def _zero_day_malware(n: int, rng: np.random.Generator) -> pd.DataFrame:
    df = _benign(n, rng)
    df["base64_entropy"] = rng.normal(5.6, 0.2, n)
    df["base64_blob_ratio"] = rng.uniform(0.15, 0.6, n)
    df["max_file_entropy"] = rng.normal(5.4, 0.3, n)
    df["maintainer_age_days"] = rng.gamma(8, 300, n)
    df["email_domain_changed"] = rng.binomial(1, 0.8, n)
    df["repo_url_mismatch"] = rng.binomial(1, 0.7, n)
    df["has_install_hook"] = 1
    df["n_exec_eval"] = rng.poisson(1.5, n)
    return df


def make_dataset(
    regime: str = "classic", n_benign: int = 4000, n_mal: int = 400, seed: int = 0
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    mal = {"classic": _classic_malware, "zero_day": _zero_day_malware}[regime](n_mal, rng)
    df = pd.concat([_benign(n_benign, rng).assign(label=0), mal.assign(label=1)], ignore_index=True)
    return df[[*FEATURE_ORDER, "label"]].sample(frac=1, random_state=seed).reset_index(drop=True)
