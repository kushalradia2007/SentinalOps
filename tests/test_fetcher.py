import hashlib
import io
import json
import os
import tarfile
import zipfile

import pytest

from sentinalops.data.fetcher import (
    FetchError,
    Limits,
    UnsafeArchive,
    build_meta,
    fetch_pypi_package,
    safe_member_path,
    unpack_archive,
)
from sentinalops.features.extract import extract_package_features
from sentinalops.simulator.zero_day import evil_setup


def make_tar(members: dict[str, bytes | tuple], mode="w:gz") -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode=mode) as tf:
        for name, content in members.items():
            if isinstance(content, tuple):  # ("symlink"|"hardlink", target)
                kind, target = content
                ti = tarfile.TarInfo(name)
                ti.type = tarfile.SYMTYPE if kind == "symlink" else tarfile.LNKTYPE
                ti.linkname = target
                tf.addfile(ti)
            else:
                ti = tarfile.TarInfo(name)
                ti.size = len(content)
                tf.addfile(ti, io.BytesIO(content))
    return buf.getvalue()


def make_zip(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in members.items():
            zf.writestr(name, content)
    return buf.getvalue()


# ---------------------------------------------------------------- path safety


@pytest.mark.parametrize("bad", ["../etc/passwd", "a/../../b", "/etc/passwd", "C:\\win\\x", "a\\..\\..\\b",
                                 "a\x00b"])
def test_safe_member_path_rejects(bad):
    with pytest.raises(UnsafeArchive):
        safe_member_path(bad)


def test_safe_member_path_normalises():
    assert safe_member_path("./pkg-1.0//setup.py") == "pkg-1.0/setup.py"


# ---------------------------------------------------------------- unpacking


def test_sdist_tar_strips_root_and_reads_text():
    data = make_tar({"pkg-1.0/setup.py": b"print('hi')\n", "pkg-1.0/pkg/__init__.py": b""})
    res = unpack_archive(data, "pkg-1.0.tar.gz")
    assert set(res.files) == {"setup.py", "pkg/__init__.py"}


def test_wheel_zip_unpacks():
    wheel = make_zip({"pkg/__init__.py": b"x = 1\n", "pkg-1.0.dist-info/METADATA": b"Name: pkg\n"})
    res = unpack_archive(wheel, "pkg-1.0-py3-none-any.whl")
    assert "pkg/__init__.py" in res.files


@pytest.mark.parametrize("maker,fname", [
    (lambda: make_zip({"../../evil.py": b"x"}), "a.zip"),
    (lambda: make_tar({"../../evil.py": b"x"}), "a.tar.gz"),
    (lambda: make_zip({"/abs/evil.py": b"x"}), "a.whl"),
    (lambda: make_tar({"ok/setup.py": b"x", "ok/../../evil": b"y"}), "a.tgz"),
])
def test_traversal_rejected(maker, fname):
    with pytest.raises(UnsafeArchive):
        unpack_archive(maker(), fname)


def test_links_are_skipped_not_followed():
    data = make_tar({"p/setup.py": b"x=1\n", "p/shadow": ("symlink", "/etc/shadow"),
                     "p/hard": ("hardlink", "p/setup.py")})
    res = unpack_archive(data, "p.tar.gz")
    assert set(res.files) == {"setup.py"}


def test_binary_files_counted_and_skipped():
    res = unpack_archive(make_zip({"p/a.py": b"x=1\n", "p/lib.so": b"\x7fELF\x00\x00binary"}), "p.zip")
    assert res.n_binary == 1 and ("p/lib.so", "binary") in res.skipped and "lib.so" not in res.files


def test_large_text_truncated():
    data = make_zip({"a/big.txt": b"A" * 5000, "a/s.py": b"x"})
    res = unpack_archive(data, "a.zip", Limits(max_file_bytes=1000))
    assert len(res.files["big.txt"]) == 1000 and ("a/big.txt", "truncated") in res.skipped


def test_zip_bomb_rejected_by_ratio():
    bomb = make_zip({"zeros.bin": b"\x00" * (30 * 1024 * 1024)})
    assert len(bomb) < 100_000
    with pytest.raises(UnsafeArchive, match="ratio"):
        unpack_archive(bomb, "bomb.zip")


def test_declared_size_limit_and_file_flood():
    with pytest.raises(UnsafeArchive, match="declared size"):
        unpack_archive(make_zip({"a": b"1" * 5000}), "a.zip", Limits(max_total_bytes=1000))
    flood = make_tar({f"p/f{i}.py": b"x" for i in range(30)})
    with pytest.raises(UnsafeArchive, match="members"):
        unpack_archive(flood, "p.tar.gz", Limits(max_files=10))


def test_actual_bytes_limit_even_if_headers_lie():
    # three files each below the per-file cap but over the total once actually read
    data = make_zip({f"p/{i}.txt": b"B" * 400 for i in range(3)})
    with pytest.raises(UnsafeArchive):
        unpack_archive(data, "p.zip", Limits(max_total_bytes=1000, max_file_bytes=500))


def test_bad_and_unsupported_archives():
    with pytest.raises(UnsafeArchive):
        unpack_archive(b"not a zip", "a.zip")
    with pytest.raises(UnsafeArchive):
        unpack_archive(b"x", "a.exe")


# ---------------------------------------------------------------- fetch flow


def fake_pypi(name, version, archive: bytes, filename: str, sha=None, url=None, **info):
    sha = sha or hashlib.sha256(archive).hexdigest()
    url = url or f"https://files.pythonhosted.org/packages/{filename}"
    project = {
        "info": {"version": version, "requires_dist": ["requests>=2", "pytest; extra == 'test'"],
                 "home_page": "https://github.com/someone/other-thing", **info},
        "releases": {version: [{"packagetype": "sdist", "filename": filename, "url": url,
                                "digests": {"sha256": sha}, "upload_time_iso_8601": "2020-01-01T00:00:00Z"}],
                     "0.1": [{"packagetype": "sdist", "filename": "x", "url": url,
                              "upload_time_iso_8601": "2019-01-01T00:00:00Z"}]},
    }

    def http_get(u, max_bytes):
        if u.startswith("https://pypi.org/pypi/"):
            return json.dumps(project).encode()
        assert u == url
        return archive
    return http_get


def test_full_fetch_extracts_features(tmp_path):
    marker = tmp_path / "EXECUTED"
    setup = evil_setup() + f"\nopen({str(marker)!r}, 'w').write('pwned')\n"  # would fire if run
    archive = make_tar({"evil-1.0/setup.py": setup.encode(), "evil-1.0/evil/__init__.py": b""})
    get = fake_pypi("evil", "1.0", archive, "evil-1.0.tar.gz")
    pkg = fetch_pypi_package("evil", http_get=get, now=1_700_000_000)
    feats = extract_package_features(pkg.files, pkg.meta)
    assert feats["has_install_hook"] == 1 and feats["n_exec_eval"] >= 1 and feats["base64_entropy"] > 4.5
    assert pkg.meta["dep_depth"] == 1  # extras excluded
    assert pkg.meta["repo_url_mismatch"] is True
    assert pkg.meta["maintainer_age_days"] > 365 * 3  # earliest upload is 2019
    assert not marker.exists(), "fetched code must never be executed"


def test_sha256_mismatch_rejected():
    archive = make_tar({"p-1/setup.py": b"x=1"})
    get = fake_pypi("p", "1", archive, "p-1.tar.gz", sha="0" * 64)
    with pytest.raises(UnsafeArchive, match="sha256"):
        fetch_pypi_package("p", http_get=get)


def test_foreign_host_rejected():
    archive = make_tar({"p-1/setup.py": b"x=1"})
    get = fake_pypi("p", "1", archive, "p-1.tar.gz", url="https://evil.example.com/p-1.tar.gz")
    with pytest.raises(FetchError, match="refusing"):
        fetch_pypi_package("p", http_get=get)


def test_invalid_names_and_missing_versions():
    for bad in ["../x", "a b", "", "x;rm"]:
        with pytest.raises(FetchError):
            fetch_pypi_package(bad, http_get=lambda *a: b"")
    get = fake_pypi("p", "1", make_tar({"p-1/a.py": b"x"}), "p-1.tar.gz")
    with pytest.raises(FetchError, match="not found"):
        fetch_pypi_package("p", "9.9", http_get=get)


def test_wheel_fallback_and_yanked_skipped():
    project = {"info": {"version": "1"}, "releases": {"1": [
        {"packagetype": "sdist", "yanked": True, "filename": "a.tar.gz",
         "url": "https://files.pythonhosted.org/a"},
        {"packagetype": "bdist_wheel", "filename": "a-1-py3-none-any.whl",
         "url": "https://files.pythonhosted.org/a.whl"}]}}
    wheel = make_zip({"a/__init__.py": b"x=1\n"})

    def get(u, n):
        return json.dumps(project).encode() if "pypi.org" in u else wheel
    assert fetch_pypi_package("a", http_get=get).filename == "a-1-py3-none-any.whl"


def test_build_meta_without_uploads():
    m = build_meta("x", "1", {"info": {}, "releases": {}})
    assert m["maintainer_age_days"] == 0.0 and m["repo_url_mismatch"] is False


@pytest.mark.skipif(not os.getenv("RUN_NETWORK_TESTS"), reason="set RUN_NETWORK_TESTS=1 to hit PyPI")
def test_live_pypi_requests():
    pkg = fetch_pypi_package("requests", "2.31.0")
    feats = extract_package_features(pkg.files, pkg.meta)
    assert "setup.py" in pkg.files and feats["typosquat_score"] == 0.0
