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

DANGEROUS_CALLS = {
    "exec": "n_exec_eval", "eval": "n_exec_eval", "compile": "n_exec_eval", "__import__": "n_exec_eval",
    "os.system": "n_os_system", "os.popen": "n_os_system",
    "subprocess.run": "n_subprocess", "subprocess.Popen": "n_subprocess",
    "subprocess.call": "n_subprocess", "subprocess.check_output": "n_subprocess",
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


# Used when a file does not parse (Python 2 syntax, deliberate syntax errors, absurd nesting).
# Failing to parse must never zero the danger features, or breaking the syntax becomes an evasion.
_FALLBACK = {
    "n_exec_eval": re.compile(r"\b(?:exec|eval|__import__)\b|\bcompile\s*\("),
    "n_os_system": re.compile(r"\bos\s*\.\s*(?:system|popen)\b"),
    "n_subprocess": re.compile(r"\bsubprocess\s*\.\s*(?:run|Popen|call|check_output)\b"),
    "n_network": re.compile(r"\b(?:import|from)\s+(?:socket|urllib|requests|http\.client)\b"),
}


def _regex_features(source: str) -> dict:
    return {k: len(rx.findall(source)) for k, rx in _FALLBACK.items()}


def _imports(tree: ast.AST) -> tuple[dict[str, str], int]:
    """Local name -> dotted origin (so `import os as o` and `from os import system` resolve),
    plus the number of network-module imports."""
    aliases: dict[str, str] = {}
    n_network = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                aliases[a.asname or a.name.split(".")[0]] = a.name if a.asname else a.name.split(".")[0]
                n_network += a.name in NETWORK_MODULES
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            for a in node.names:
                aliases[a.asname or a.name] = f"{node.module}.{a.name}"
            n_network += node.module in NETWORK_MODULES
    return aliases, n_network


def _call_target(func: ast.expr, aliases: dict[str, str]) -> str | None:
    if isinstance(func, ast.Name):
        target = aliases.get(func.id, func.id)
    elif isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
        target = f"{aliases.get(func.value.id, func.value.id)}.{func.attr}"
    else:
        return None
    return target.removeprefix("builtins.")  # `import builtins as b; b.exec(...)` is still exec


def ast_features(source: str) -> dict:
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError, RecursionError, MemoryError):
        return _regex_features(source)
    aliases, n_network = _imports(tree)
    out = {"n_exec_eval": 0, "n_os_system": 0, "n_subprocess": 0, "n_network": n_network}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and (key := DANGEROUS_CALLS.get(_call_target(node.func, aliases))):
            out[key] += 1
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


_HOOK_COMMANDS = r"install|develop|egg_info|build_py|build_ext|bdist_wheel|sdist"
_CMDCLASS_RE = re.compile(rf"cmdclass[^{{]*\{{[^}}]*['\"](?:{_HOOK_COMMANDS})['\"]", re.S)
_HOOK_CLASS_RE = re.compile(rf"class\s+\w+\(\s*(?:\w+\.)*(?:{_HOOK_COMMANDS})\s*\)")


def _has_install_hook(setup_py: str) -> bool:
    """True when setup.py overrides a build/install command (code runs at pip install time).

    A custom *test* command (e.g. requests) is not an install hook, so a bare `cmdclass` is not enough.
    """
    return bool(
        _CMDCLASS_RE.search(setup_py)
        or _HOOK_CLASS_RE.search(setup_py)
        or "postinstall" in setup_py.lower()
    )


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
        has_install_hook=int(_has_install_hook(setup)),
        max_file_entropy=max_ent,
        base64_entropy=b64_ent,
        base64_blob_ratio=b64_ratio,
    )
    feats.update(metadata_features(meta))
    return {k: feats[k] for k in FEATURE_ORDER}
