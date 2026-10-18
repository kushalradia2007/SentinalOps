"""Chunk 8: QUT-DV25 loader (install-time behavior features).

QUT-DV25 (NeurIPS 2025 D&B): 14,271 PyPI packages (7,127 malicious / 7,144 benign) with
dynamic traces collected during install and a 120 s post-install window. Hosted on
Harvard Dataverse (doi:10.7910/DVN/LBMXJY). The paper names 36 features in 6 groups but
does not publish the exact CSV layout, so this loader is deliberately tolerant: it matches
columns case/punctuation-insensitively, finds the label column by name, and reports which
of the expected features it did and did not find instead of failing silently.
"""
from __future__ import annotations

import argparse
import json
import urllib.request
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pandas as pd

from sentinalops.data.common import download_file, normalize_label, snake, write_table
from sentinalops.logging import get_logger

log = get_logger("sentinalops.data.qut_dv25")

DOI = "doi:10.7910/DVN/LBMXJY"
DATAVERSE = "https://dataverse.harvard.edu"

FEATURE_GROUPS: dict[str, list[str]] = {
    "filetop": ["Read_Processes", "Write_Processes", "Read_Data_Transfer", "Write_Data_Transfer",
                "File_Access_Processes"],
    "install": ["Total_Dependencies", "Direct_Dependencies", "Indirect_Dependencies"],
    "opensnoop": ["Root_DIR_Access", "Temp_DIR_Access", "Home_DIR_Access", "User_DIR_Access",
                  "Sys_DIR_Access", "Etc_DIR_Access", "Other_DIR_Access"],
    "tcp": ["State_Transition", "Local_IPs_Access", "Remote_IPs_Access", "Local_Port_Access",
            "Remote_Port_Access"],
    "syscall": ["IO_Operations", "File_Operations", "Network_Operations", "Time_Operations",
                "Security_Operations", "Process_Operations"],
    "pattern": [f"Pattern_{i}" for i in range(1, 11)],
}
EXPECTED_FEATURES = [snake(f) for fs in FEATURE_GROUPS.values() for f in fs]
LABEL_CANDIDATES = ["label", "class", "malicious", "is_malicious", "target", "y"]
KEY_CANDIDATES = ["package", "package_name", "pkg", "name", "package_id"]


@dataclass
class LoadReport:
    files: list[str] = field(default_factory=list)
    rows: int = 0
    label_column: str = ""
    key_column: str | None = None
    found: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    extra: list[str] = field(default_factory=list)
    malicious: int = 0
    benign: int = 0

    @property
    def coverage(self) -> float:
        return len(self.found) / len(EXPECTED_FEATURES)

    def to_dict(self) -> dict:
        return {**asdict(self), "coverage": round(self.coverage, 3)}


def _first(cols: list[str], candidates: list[str]) -> str | None:
    return next((c for c in candidates if c in cols), None)


def _read_all(path: Path) -> tuple[list[pd.DataFrame], list[str]]:
    paths = [path] if path.is_file() else sorted(path.rglob("*.csv"))
    if not paths:
        raise FileNotFoundError(f"no CSV files under {path}")
    frames = []
    for p in paths:
        df = pd.read_csv(p)
        df.columns = [snake(c) for c in df.columns]
        frames.append(df)
    return frames, [p.name for p in paths]


def _combine(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """One file -> as is. Several -> merge on a shared key column (trace-per-file layout),
    otherwise stack rows (shard-per-file layout)."""
    if len(frames) == 1:
        return frames[0]
    key = next((k for k in KEY_CANDIDATES if all(k in f.columns for f in frames)), None)
    if key and not all(set(f.columns) == set(frames[0].columns) for f in frames):
        out = frames[0]
        for f in frames[1:]:
            out = out.merge(f, on=key, how="outer", suffixes=("", "_dup"))
            for dup in [c for c in out.columns if c.endswith("_dup")]:
                out[dup[:-4]] = out[dup[:-4]].combine_first(out[dup])
                out = out.drop(columns=dup)
        return out
    return pd.concat(frames, ignore_index=True)


def load_qut_dv25(path: str | Path, min_coverage: float = 0.5) -> tuple[pd.DataFrame, LoadReport]:
    """Load QUT-DV25 CSV(s). Returns (df, report); df has expected features + 'label' (+ 'package')."""
    path = Path(path)
    frames, names = _read_all(path)
    df = _combine(frames)
    label_col = _first(list(df.columns), LABEL_CANDIDATES)
    if label_col is None:
        raise ValueError(
            f"no label column found (looked for {LABEL_CANDIDATES}); got {list(df.columns)[:12]}"
        )
    key_col = _first(list(df.columns), KEY_CANDIDATES)
    found = [c for c in EXPECTED_FEATURES if c in df.columns]
    report = LoadReport(files=names, rows=len(df), label_column=label_col, key_column=key_col,
                        found=found, missing=[c for c in EXPECTED_FEATURES if c not in df.columns])
    if report.coverage < min_coverage:
        raise ValueError(f"only {len(found)}/{len(EXPECTED_FEATURES)} expected features found; "
                         f"missing e.g. {report.missing[:5]} - is this the right file?")
    df = df.dropna(subset=[label_col]).copy()
    out = df[found].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    out["label"] = df[label_col].map(normalize_label).astype(int)
    if key_col:
        out.insert(0, "package", df[key_col].astype(str))
    report.extra = [c for c in df.columns if c not in found and c not in (label_col, key_col)]
    report.rows = len(out)
    report.malicious, report.benign = int(out["label"].sum()), int((out["label"] == 0).sum())
    if report.missing:
        log.warning("expected features missing", extra={"missing": report.missing})
    return out.reset_index(drop=True), report


def list_dataverse_files(doi: str = DOI, base: str = DATAVERSE, timeout: int = 60) -> list[dict]:
    """Ask the Dataverse API which files the dataset holds (needs network access)."""
    url = f"{base}/api/datasets/:persistentId/?persistentId={doi}"
    req = urllib.request.Request(url, headers={"User-Agent": "sentinalops-data/0.1"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = json.load(r)
    return [{"id": f["dataFile"]["id"], "name": f["dataFile"]["filename"],
             "size": f["dataFile"].get("filesize", 0)} for f in data["data"]["latestVersion"]["files"]]


def download_dataset(out_dir: str | Path, doi: str = DOI, base: str = DATAVERSE,
                     max_total_bytes: int = 5 * 1024**3) -> list[Path]:
    """Download every file in the dataset, as the original upload format (not Dataverse's .tab)."""
    files = list_dataverse_files(doi, base)
    if sum(f["size"] for f in files) > max_total_bytes:
        raise ValueError("dataset larger than max_total_bytes; raise the cap deliberately")
    out = Path(out_dir)
    return [download_file(f"{base}/api/access/datafile/{f['id']}?format=original", out / f["name"],
                          max_bytes=max_total_bytes) for f in files]


def main() -> None:
    ap = argparse.ArgumentParser(description="QUT-DV25 download/ingest")
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("download", help="fetch from Harvard Dataverse")
    d.add_argument("--out", default="data/raw/qut_dv25")
    d.add_argument("--list", action="store_true", help="only list the files")
    i = sub.add_parser("ingest", help="validate + write parquet")
    i.add_argument("--src", default="data/raw/qut_dv25")
    i.add_argument("--out", default="data/processed/qut_dv25")
    args = ap.parse_args()
    if args.cmd == "download":
        if args.list:
            print(json.dumps(list_dataverse_files(), indent=2))
        else:
            print([str(p) for p in download_dataset(args.out)])
    else:
        df, report = load_qut_dv25(args.src)
        out = Path(args.out)
        write_table(df, out / "features.parquet")
        (out / "report.json").write_text(json.dumps(report.to_dict(), indent=2))
        print(json.dumps(report.to_dict(), indent=2))


if __name__ == "__main__":
    main()
