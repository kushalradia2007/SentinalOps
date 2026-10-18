"""Shared helpers for dataset loaders: label parsing, downloads, table output."""
from __future__ import annotations

import hashlib
import re
import urllib.request
from pathlib import Path

import pandas as pd

from sentinalops.logging import get_logger

log = get_logger("sentinalops.data")

MALICIOUS_LABELS = {"malware", "malicious", "mal", "1", "true", "yes", "1.0"}
BENIGN_LABELS = {"neutral", "benign", "clean", "0", "false", "no", "0.0"}


def normalize_label(value) -> int:
    """Map the many spellings of a label (malware/neutral/1/0/...) to 1 (malicious) or 0."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        raise ValueError("missing label")
    v = str(value).strip().lower()
    if v in MALICIOUS_LABELS:
        return 1
    if v in BENIGN_LABELS:
        return 0
    raise ValueError(f"unrecognised label: {value!r}")


def snake(name: str) -> str:
    """'Read_Processes' / 'fileName' / 'File Size' -> lowercase alphanumerics joined by '_'."""
    return re.sub(r"[^0-9a-zA-Z]+", "_", str(name).strip()).strip("_").lower()


def download_file(
    url: str,
    dest: str | Path,
    *,
    max_bytes: int = 500 * 1024 * 1024,
    md5: str | None = None,
    strict: bool = True,
    timeout: int = 60,
) -> Path:
    """Stream a URL to disk with a size cap and optional MD5 check."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "sentinalops-data/0.1"})
    h, total = hashlib.md5(), 0  # noqa: S324 - integrity check against published checksums only
    with urllib.request.urlopen(req, timeout=timeout) as r, open(dest, "wb") as f:
        while chunk := r.read(1 << 16):
            total += len(chunk)
            if total > max_bytes:
                dest.unlink(missing_ok=True)
                raise ValueError(f"download exceeds {max_bytes} bytes: {url}")
            h.update(chunk)
            f.write(chunk)
    if md5 and h.hexdigest() != md5:
        msg = f"md5 mismatch for {dest.name}: got {h.hexdigest()}, expected {md5}"
        if strict:
            dest.unlink(missing_ok=True)
            raise ValueError(msg)
        log.warning(msg)
    log.info("downloaded", extra={"url": url, "bytes": total, "dest": str(dest)})
    return dest


def write_table(df: pd.DataFrame, path: str | Path) -> Path:
    """Write Parquet (preferred); fall back to CSV if pyarrow is unavailable."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False)
        return path
    except ImportError:
        csv = path.with_suffix(".csv")
        log.warning("pyarrow missing, writing CSV", extra={"path": str(csv)})
        df.to_csv(csv, index=False)
        return csv
