"""Chunk 9: safe PyPI package fetcher.

Downloads a release and reads its files STATICALLY. Hard rules:
  * archives are read in memory, never extracted to disk, never imported or executed
  * only https://pypi.org and https://files.pythonhosted.org are contacted
  * download is size-capped and verified against the sha256 PyPI publishes
  * zip-slip / absolute / drive-letter paths, symlinks, hardlinks, devices, zip bombs and
    file-count floods are rejected or skipped
The result plugs straight into `extract_package_features(files, meta)`.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import tarfile
import time
import urllib.request
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import quote, urlparse

from sentinalops.features.extract import extract_package_features
from sentinalops.logging import get_logger

log = get_logger("sentinalops.data.fetcher")

ALLOWED_HOSTS = {"pypi.org", "files.pythonhosted.org"}
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+!-]*$")
REPO_HOSTS = ("github.com", "gitlab.com", "bitbucket.org")


class UnsafeArchive(Exception):
    """The archive is malicious or malformed in a way that makes it untrustworthy to read."""


class FetchError(Exception):
    """Network / registry level failure (not an attack)."""


@dataclass(frozen=True)
class Limits:
    max_download_bytes: int = 50 * 1024 * 1024
    max_files: int = 25000  # numpy's sdist alone has ~8.5k members; floods are far larger
    max_total_bytes: int = 200 * 1024 * 1024  # sum of declared sizes AND bytes actually read
    max_file_bytes: int = 2 * 1024 * 1024  # kept text per file; longer is truncated
    max_ratio: int = 200  # declared/compressed, only enforced above `ratio_floor_bytes`
    ratio_floor_bytes: int = 10 * 1024 * 1024


DEFAULT_LIMITS = Limits()


@dataclass
class UnpackResult:
    files: dict[str, str] = field(default_factory=dict)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    n_binary: int = 0
    total_bytes: int = 0


@dataclass
class FetchedPackage:
    name: str
    version: str
    filename: str
    sha256: str
    files: dict[str, str]
    meta: dict
    skipped: list[tuple[str, str]]
    n_binary: int


def safe_member_path(name: str) -> str:
    """Normalise an archive member path or raise UnsafeArchive for traversal tricks."""
    p = name.replace("\\", "/")
    if "\x00" in p or p.startswith("/") or re.match(r"^[A-Za-z]:", p):
        raise UnsafeArchive(f"absolute or malformed path: {name!r}")
    parts = [x for x in p.split("/") if x not in ("", ".")]
    if ".." in parts:
        raise UnsafeArchive(f"path traversal: {name!r}")
    return "/".join(parts)


def _decode(data: bytes) -> str | None:
    """Text for source-like content, None for binary."""
    if b"\x00" in data[:8192]:
        return None
    return data.decode("utf-8", errors="replace")


def _strip_common_root(files: dict[str, str]) -> dict[str, str]:
    roots = {p.split("/", 1)[0] for p in files if "/" in p}
    if len(roots) == 1 and all("/" in p for p in files):
        root = next(iter(roots))
        return {p[len(root) + 1:]: t for p, t in files.items()}
    return files


def unpack_archive(data: bytes, filename: str, limits: Limits = DEFAULT_LIMITS) -> UnpackResult:
    """Read a zip/whl/tar(.gz/.bz2/.xz) fully in memory. Never touches the filesystem."""
    low = filename.lower()
    if low.endswith((".zip", ".whl", ".egg")):
        members = _zip_members(data, limits)
    elif low.endswith((".tar.gz", ".tgz", ".tar.bz2", ".tar.xz", ".tar")):
        members = _tar_members(data, limits)
    else:
        raise UnsafeArchive(f"unsupported archive type: {filename}")
    res = UnpackResult()
    raw: dict[str, str] = {}
    for path, _size, reader in members:
        chunk = reader(limits.max_file_bytes + 1)
        res.total_bytes += len(chunk)
        if res.total_bytes > limits.max_total_bytes:
            raise UnsafeArchive("decompressed bytes exceed limit (zip bomb?)")
        text = _decode(chunk[: limits.max_file_bytes])
        if text is None:
            res.n_binary += 1
            res.skipped.append((path, "binary"))
            continue
        if len(chunk) > limits.max_file_bytes:
            res.skipped.append((path, "truncated"))
        raw[path] = text
    res.files = _strip_common_root(raw)
    return res


def _check_totals(declared: int, compressed: int, n: int, limits: Limits) -> None:
    if n > limits.max_files:
        raise UnsafeArchive(f"{n} members exceeds limit {limits.max_files}")
    if declared > limits.max_total_bytes:
        raise UnsafeArchive(f"declared size {declared} exceeds limit")
    if declared > limits.ratio_floor_bytes and declared / max(compressed, 1) > limits.max_ratio:
        raise UnsafeArchive(f"compression ratio {declared // max(compressed, 1)}:1 (zip bomb?)")


def _read_zip(zf: zipfile.ZipFile, info: zipfile.ZipInfo, n: int) -> bytes:
    with zf.open(info) as f:
        return f.read(n)


def _zip_members(data: bytes, limits: Limits):
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as e:
        raise UnsafeArchive(f"bad zip: {e}") from e
    infos = [i for i in zf.infolist() if not i.is_dir()]
    _check_totals(sum(i.file_size for i in infos), len(data), len(infos), limits)
    out = []
    for i in infos:
        path = safe_member_path(i.filename)
        if (i.external_attr >> 16) & 0o170000 == 0o120000:  # symlink
            continue
        out.append((path, i.file_size, lambda n, i=i: _read_zip(zf, i, n)))
    return out


def _tar_members(data: bytes, limits: Limits):
    try:
        tf = tarfile.open(fileobj=io.BytesIO(data), mode="r:*")
    except tarfile.TarError as e:
        raise UnsafeArchive(f"bad tar: {e}") from e
    members = tf.getmembers()
    regular = [m for m in members if m.isreg()]
    _check_totals(sum(m.size for m in regular), len(data), len(members), limits)
    out = []
    for m in members:
        path = safe_member_path(m.name)  # validate every member, even ones we skip
        if m.isreg():
            out.append((path, m.size, lambda n, m=m: tf.extractfile(m).read(n)))
    return out


# ---------------------------------------------------------------- network


HttpGet = Callable[[str, int], bytes]


def _check_url(url: str) -> None:
    u = urlparse(url)
    if u.scheme != "https" or u.hostname not in ALLOWED_HOSTS:
        raise FetchError(f"refusing to fetch {url!r}: only https to {sorted(ALLOWED_HOSTS)}")


class _AllowedRedirects(urllib.request.HTTPRedirectHandler):
    """Re-apply the host allowlist to every redirect hop, not just the first URL."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        _check_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def default_http_get(url: str, max_bytes: int, timeout: int = 30) -> bytes:
    _check_url(url)
    req = urllib.request.Request(url, headers={"User-Agent": "sentinalops-fetcher/0.1"})
    buf = bytearray()
    with urllib.request.build_opener(_AllowedRedirects).open(req, timeout=timeout) as r:
        while chunk := r.read(1 << 16):
            buf += chunk
            if len(buf) > max_bytes:
                raise FetchError(f"response larger than {max_bytes} bytes: {url}")
    return bytes(buf)


def _select_artifact(files: list[dict]) -> dict:
    """Prefer an sdist (has setup.py, where install hooks live), else a wheel."""
    usable = [f for f in files if not f.get("yanked")]
    for kind in ("sdist", "bdist_wheel"):
        for f in usable:
            if f.get("packagetype") == kind:
                return f
    raise FetchError("release has no downloadable sdist or wheel")


def _ts(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def _repo_mismatch(info: dict, name: str) -> bool:
    urls = [info.get("home_page") or "", *(info.get("project_urls") or {}).values()]
    repos = [u for u in urls if u and any(h in u for h in REPO_HOSTS)]
    if not repos:
        return False
    key = re.sub(r"[-_.]+", "", name.lower())
    return not any(key in re.sub(r"[-_.]+", "", urlparse(u).path.lower()) for u in repos)


def build_meta(name: str, version: str, project: dict, now: float | None = None) -> dict:
    """Registry metadata -> the `meta` dict the feature extractor expects.

    maintainer_age_days is a PROXY (days since the project's first upload): PyPI's public API
    exposes no account age. email_domain_changed needs history across releases (chunk 11).
    """
    info = project.get("info", {})
    uploads = [_ts(f["upload_time_iso_8601"]) for fs in project.get("releases", {}).values()
               for f in fs if f.get("upload_time_iso_8601")]
    now = time.time() if now is None else now
    reqs = [r for r in (info.get("requires_dist") or []) if "extra ==" not in r]
    return {
        "name": name,
        "version": version,
        "maintainer_age_days": max(0.0, (now - min(uploads)) / 86400) if uploads else 0.0,
        "email_domain_changed": False,
        "dep_depth": len(reqs),
        "repo_url_mismatch": _repo_mismatch(info, name),
    }


def fetch_pypi_package(name: str, version: str | None = None, *, http_get: HttpGet = default_http_get,
                       limits: Limits = DEFAULT_LIMITS, now: float | None = None) -> FetchedPackage:
    if not NAME_RE.match(name) or (version and not VERSION_RE.match(version)):
        raise FetchError(f"invalid package name/version: {name!r} {version!r}")
    project = json.loads(http_get(f"https://pypi.org/pypi/{quote(name)}/json", 20 * 1024 * 1024))
    version = version or project["info"]["version"]
    release = project.get("releases", {}).get(version)
    if not release:
        raise FetchError(f"{name}=={version} not found on PyPI")
    art = _select_artifact(release)
    _check_url(art["url"])
    data = http_get(art["url"], limits.max_download_bytes)
    digest = hashlib.sha256(data).hexdigest()
    expected = (art.get("digests") or {}).get("sha256")
    if expected and digest != expected:
        raise UnsafeArchive(f"sha256 mismatch for {art['filename']}: got {digest}, expected {expected}")
    res = unpack_archive(data, art["filename"], limits)
    log.info("fetched", extra={"package": name, "version": version, "files": len(res.files),
                               "binary": res.n_binary, "artifact": art["filename"]})
    return FetchedPackage(name, version, art["filename"], digest, res.files,
                          build_meta(name, version, project, now), res.skipped, res.n_binary)


def main() -> None:
    ap = argparse.ArgumentParser(description="Statically fetch a PyPI release and print its features")
    ap.add_argument("name")
    ap.add_argument("version", nargs="?")
    args = ap.parse_args()
    pkg = fetch_pypi_package(args.name, args.version)
    print(json.dumps({"artifact": pkg.filename, "sha256": pkg.sha256, "n_files": len(pkg.files),
                      "n_binary": pkg.n_binary, "meta": pkg.meta,
                      "features": extract_package_features(pkg.files, pkg.meta)}, indent=2))


if __name__ == "__main__":
    main()
