import numpy as np
import pandas as pd
import pytest

from sentinalops.data.qut_dv25 import (
    EXPECTED_FEATURES,
    FEATURE_GROUPS,
    load_qut_dv25,
)


def _frame(n=6, cols=None, label_name="Label", labels=None, key="Package_Name"):
    rng = np.random.default_rng(0)
    cols = cols or [f for fs in FEATURE_GROUPS.values() for f in fs]  # original mixed-case names
    df = pd.DataFrame({c: rng.integers(0, 50, n) for c in cols})
    df.insert(0, key, [f"pkg{i}" for i in range(n)])
    df[label_name] = labels or (["malicious", "benign"] * n)[:n]
    return df


def test_expected_feature_count_matches_paper():
    assert len(EXPECTED_FEATURES) == 36 and len(set(EXPECTED_FEATURES)) == 36


def test_load_single_csv_full_coverage(tmp_path):
    _frame().to_csv(tmp_path / "dv25.csv", index=False)
    df, rep = load_qut_dv25(tmp_path / "dv25.csv")
    assert rep.coverage == 1.0 and rep.missing == [] and rep.label_column == "label"
    assert rep.key_column == "package_name" and list(df.columns[:2]) == ["package", "read_processes"]
    assert set(df["label"]) == {0, 1} and rep.malicious == 3 and rep.benign == 3


def test_numeric_labels_and_alternate_label_name(tmp_path):
    _frame(label_name="Class", labels=[1, 0, 1, 0, 1, 0]).to_csv(tmp_path / "a.csv", index=False)
    df, rep = load_qut_dv25(tmp_path)
    assert rep.label_column == "class" and df["label"].sum() == 3


def test_missing_features_reported_not_hidden(tmp_path):
    cols = [f for fs in FEATURE_GROUPS.values() for f in fs if not f.startswith("Pattern_")]
    _frame(cols=cols).to_csv(tmp_path / "a.csv", index=False)
    _, rep = load_qut_dv25(tmp_path)
    assert len(rep.missing) == 10 and rep.coverage == pytest.approx(26 / 36)


def test_wrong_file_rejected(tmp_path):
    pd.DataFrame({"foo": [1, 2], "Label": ["malicious", "benign"]}).to_csv(tmp_path / "x.csv", index=False)
    with pytest.raises(ValueError, match="expected features"):
        load_qut_dv25(tmp_path)


def test_no_label_column_rejected(tmp_path):
    _frame().drop(columns="Label").to_csv(tmp_path / "x.csv", index=False)
    with pytest.raises(ValueError, match="no label column"):
        load_qut_dv25(tmp_path)


def test_trace_per_file_layout_merges_on_key(tmp_path):
    full = _frame()
    fs = list(full.columns)
    a = full[["Package_Name", "Label", *FEATURE_GROUPS["filetop"], *FEATURE_GROUPS["syscall"]]]
    rest = [c for c in fs if c not in a.columns or c == "Package_Name"]
    b = full[rest]
    a.to_csv(tmp_path / "a.csv", index=False)
    b.to_csv(tmp_path / "b.csv", index=False)
    df, rep = load_qut_dv25(tmp_path)
    assert rep.coverage == 1.0 and len(df) == 6


def test_shards_are_stacked(tmp_path):
    _frame(4).to_csv(tmp_path / "s1.csv", index=False)
    _frame(4).to_csv(tmp_path / "s2.csv", index=False)
    df, rep = load_qut_dv25(tmp_path)
    assert len(df) == 8 and rep.files == ["s1.csv", "s2.csv"]


def test_missing_values_become_zero_and_bad_labels_dropped(tmp_path):
    f = _frame()
    f.loc[0, "Read_Processes"] = np.nan
    f.loc[1, "Label"] = np.nan
    f.to_csv(tmp_path / "a.csv", index=False)
    df, _ = load_qut_dv25(tmp_path)
    assert len(df) == 5 and df["read_processes"].isna().sum() == 0


def test_filled_missing_is_reported(tmp_path):
    f = _frame()
    f.loc[0, "Read_Processes"] = np.nan
    f["IO_Operations"] = f["IO_Operations"].astype(object)
    f.loc[2, "IO_Operations"] = "n/a"
    f.to_csv(tmp_path / "a.csv", index=False)
    _, rep = load_qut_dv25(tmp_path)
    assert rep.filled_missing == 2


def test_dataverse_listing_and_original_format_only_for_tabular(tmp_path, monkeypatch):
    import io
    import json

    from sentinalops.data import qut_dv25

    payload = {"data": {"latestVersion": {"files": [
        {"dataFile": {"id": 1, "filename": "features.csv", "filesize": 10, "tabularData": True}},
        {"dataFile": {"id": 2, "filename": "traces.zip", "filesize": 20}},
    ]}}}
    monkeypatch.setattr(qut_dv25.urllib.request, "urlopen", lambda *a, **k: io.StringIO(json.dumps(payload)))
    files = qut_dv25.list_dataverse_files()
    assert [(f["name"], f["tabular"]) for f in files] == [("features.csv", True), ("traces.zip", False)]

    urls = []
    monkeypatch.setattr(qut_dv25, "download_file", lambda url, dest, **k: urls.append(url) or dest)
    qut_dv25.download_dataset(tmp_path)
    assert urls[0].endswith("/datafile/1?format=original") and urls[1].endswith("/datafile/2")
    with pytest.raises(ValueError, match="larger than"):
        qut_dv25.download_dataset(tmp_path, max_total_bytes=5)
