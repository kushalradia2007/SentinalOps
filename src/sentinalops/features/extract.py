"""Domain feature extraction for software supply chain security.

Turns a package (file contents + registry metadata) into a flat numeric
feature dict. Pure functions, no I/O, so the same code runs in the stream
processor, the API, and offline training (no train/serve skew).
"""
from __future__ import annotations

import ast
import difflib
import math
import re
from collections import Counter

POPULAR_PACKAGES = [
    "requests", "numpy", "pandas", "urllib3", "setuptools", "boto3", "django",
    "flask", "pytest", "scipy", "pillow", "cryptography", "pyyaml", "colorama",
]

DANGEROUS_BUILTINS = {"exec", "eval", "compile", "__import__"}
DANGEROUS_ATTRS = {
    ("os", "system"), ("os", "popen"),
    ("subprocess", "run"), ("subprocess", "Popen"),
    ("subprocess", "call"), ("subprocess", "check_output"),
}
NETWORK_MODULES = {"socket", "urllib", "urllib.request", "requests", "http.client"}
BLOB_RE = re.compile(r"[A-Za-z0-9+/=]{40,}")

FEATURE_ORDER = [
    "n_exec_eval", "n_os_system", "n_subprocess", "n_network", "has_install_hook",
    "max_file_entropy", "base64_entropy", "base64_blob_ratio",
    "maintainer_age_days", "email_domain_changed", "dep_depth",
    "repo_url_mismatch", "typosquat_score",
]


def shannon_entropy(data: str) -> float:
    if not data:
        return 0.0
    counts = Counter(data)
    n = len(data)
    return -sum(c / n * math.log2(c / n) for c in counts.values())


def ast_features(source: str) -> dict:
    out = {"n_exec_eval": 0, "n_os_system": 0, "n_subprocess": 0, "n_network": 0}
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return out
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            f = node.func
            if isinstance(f, ast.Name) and f.id in DANGEROUS_BUILTINS:
                out["n_exec_eval"] += 1
            elif isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name):
                pair = (f.value.id, f.attr)
                if pair in DANGEROUS_ATTRS:
                    key = "n_os_system" if pair[0] == "os" else "n_subprocess"
                    out[key] += 1
        elif isinstance(node, ast.Import):
            out["n_network"] += sum(a.name in NETWORK_MODULES for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module in NETWORK_MODULES:
            out["n_network"] += 1
    return out


def blob_features(source: str) -> dict:
    blobs = BLOB_RE.findall(source)
    if not blobs:
        return {"base64_entropy": 0.0, "base64_blob_ratio": 0.0}
    joined = "".join(blobs)
    return {
        "base64_entropy": shannon_entropy(joined),
        "base64_blob_ratio": len(joined) / max(len(source), 1),
    }


def typosquat_score(name: str) -> float:
    """Highest similarity to a popular package, 0 if exact match or unrelated."""
    name = name.lower()
    if name in POPULAR_PACKAGES:
        return 0.0
    best = max(difflib.SequenceMatcher(None, name, p).ratio() for p in POPULAR_PACKAGES)
    return best if best >= 0.8 else 0.0


def metadata_features(meta: dict) -> dict:
    return {
        "maintainer_age_days": float(meta.get("maintainer_age_days", 0)),
        "email_domain_changed": int(bool(meta.get("email_domain_changed", False))),
        "dep_depth": int(meta.get("dep_depth", 0)),
        "repo_url_mismatch": int(bool(meta.get("repo_url_mismatch", False))),
        "typosquat_score": typosquat_score(meta.get("name", "")),
    }


def extract_package_features(files: dict[str, str], meta: dict) -> dict:
    """files: {relative_path: text}. meta: registry metadata incl. 'name'."""
    feats = {"n_exec_eval": 0, "n_os_system": 0, "n_subprocess": 0, "n_network": 0}
    max_ent, b64_ent, b64_ratio = 0.0, 0.0, 0.0
    for path, text in files.items():
        if path.endswith(".py"):
            for k, v in ast_features(text).items():
                feats[k] += v
        max_ent = max(max_ent, shannon_entropy(text))
        bf = blob_features(text)
        b64_ent, b64_ratio = max(b64_ent, bf["base64_entropy"]), max(b64_ratio, bf["base64_blob_ratio"])
    setup = files.get("setup.py", "")
    feats.update(
        has_install_hook=int("cmdclass" in setup or "postinstall" in setup.lower()),
        max_file_entropy=max_ent,
        base64_entropy=b64_ent,
        base64_blob_ratio=b64_ratio,
    )
    feats.update(metadata_features(meta))
    return {k: feats[k] for k in FEATURE_ORDER}
