"""File-based model registry with champion and shadow aliases (chunk 45).

A stand-in for the MLflow registry (chunk 32) with the same idea: versions are immutable,
aliases move. Layout under ``root``::

    versions/<version>.joblib   every registered model, never changed
    champion.joblib             the served model (what the API and processor load)
    shadow.joblib               the previous champion, during a shadow period only
    registry.json               aliases, version metadata and an event log

``champion.joblib`` and ``shadow.joblib`` are replaced atomically (write, then rename), so a
reader sees the old model or the new one, never half a file; the API and the stream processor
pick the new champion up through their existing hot reload. Each model carries its version
in ``sentinal_version_``, so a prediction's ``model_version`` always names the file that made it.
"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import joblib

from sentinalops.logging import get_logger

log = get_logger("sentinalops.registry")


@dataclass
class RegistryState:
    champion: str | None = None
    shadow: str | None = None
    shadow_until_ms: int | None = None
    promoted_at_ms: int | None = None
    versions: dict[str, dict] = field(default_factory=dict)
    events: list[dict] = field(default_factory=list)


class ModelRegistry:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        (self.root / "versions").mkdir(parents=True, exist_ok=True)

    # ---- paths and state ---------------------------------------------------------------------
    @property
    def champion_path(self) -> Path:
        return self.root / "champion.joblib"

    @property
    def shadow_path(self) -> Path:
        return self.root / "shadow.joblib"

    def version_path(self, version: str) -> Path:
        return self.root / "versions" / f"{version}.joblib"

    def state(self) -> RegistryState:
        path = self.root / "registry.json"
        if not path.exists():
            return RegistryState()
        return RegistryState(**json.loads(path.read_text(encoding="utf-8")))

    def _save(self, s: RegistryState) -> None:
        path = self.root / "registry.json"
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(s.__dict__, indent=2), encoding="utf-8")
        os.replace(tmp, path)

    def _install(self, version: str, dest: Path) -> None:
        tmp = dest.with_suffix(".joblib.tmp")
        shutil.copyfile(self.version_path(version), tmp)
        os.replace(tmp, dest)

    def meta(self, version: str) -> dict:
        return self.state().versions[version]

    def load(self, version: str):
        return joblib.load(self.version_path(version))

    def count(self, kind: str) -> int:
        """Number of ``promote`` or ``rollback`` events so far (``model_promotions_total`` etc.)."""
        return sum(1 for e in self.state().events if e["kind"] == kind)

    # ---- writes ------------------------------------------------------------------------------
    def register(self, model, meta: dict, now_ms: int) -> str:
        """Store a new immutable version and return its name (``v001``, ``v002``, ...)."""
        s = self.state()
        version = f"v{len(s.versions) + 1:03d}"
        model.sentinal_version_ = version
        tmp = self.version_path(version).with_suffix(".joblib.tmp")
        joblib.dump(model, tmp)
        os.replace(tmp, self.version_path(version))
        s.versions[version] = {**meta, "registered_at_ms": now_ms}
        s.events.append({"ts_ms": now_ms, "kind": "register", "version": version})
        self._save(s)
        return version

    def promote(self, version: str, now_ms: int, reason: str, shadow_ms: int = 0) -> None:
        """Make ``version`` the champion. The old champion scores in shadow for ``shadow_ms``."""
        s = self.state()
        if version not in s.versions:
            raise KeyError(version)
        previous = s.champion
        if previous is not None and shadow_ms > 0:
            self._install(previous, self.shadow_path)
            s.shadow, s.shadow_until_ms = previous, now_ms + shadow_ms
        else:
            self.shadow_path.unlink(missing_ok=True)
            s.shadow, s.shadow_until_ms = None, None
        self._install(version, self.champion_path)
        s.champion, s.promoted_at_ms = version, now_ms
        s.events.append({"ts_ms": now_ms, "kind": "promote", "version": version, "from": previous,
                         "reason": reason})
        self._save(s)
        log.info("model promoted", extra={"version": version, "from": previous, "reason": reason})

    def end_shadow(self, now_ms: int) -> None:
        s = self.state()
        if s.shadow is None:
            return
        self.shadow_path.unlink(missing_ok=True)
        s.events.append({"ts_ms": now_ms, "kind": "shadow_end", "version": s.champion, "shadow": s.shadow})
        s.shadow, s.shadow_until_ms = None, None
        self._save(s)

    def rollback(self, now_ms: int, reason: str) -> str:
        """Restore the shadow (previous) champion. Returns the restored version."""
        s = self.state()
        if s.shadow is None:
            raise RuntimeError("nothing to roll back to: no shadow champion")
        bad, good = s.champion, s.shadow
        self._install(good, self.champion_path)
        self.shadow_path.unlink(missing_ok=True)
        s.champion, s.shadow, s.shadow_until_ms, s.promoted_at_ms = good, None, None, now_ms
        s.events.append({"ts_ms": now_ms, "kind": "rollback", "version": good, "from": bad, "reason": reason})
        self._save(s)
        log.warning("model rolled back", extra={"version": good, "from": bad, "reason": reason})
        return good

    def log_event(self, now_ms: int, kind: str, **details) -> None:
        s = self.state()
        s.events.append({"ts_ms": now_ms, "kind": kind, **details})
        self._save(s)
