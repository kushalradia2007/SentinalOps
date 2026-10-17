"""Zero-day simulation packages for the demo.

Everything here is HARMLESS: the "malicious" payload only prints PWNED.
It mimics the *shape* of a supply-chain attack (base64-obfuscated setup.py,
install hook, trusted old maintainer, no typosquat) so the AST/entropy
features fire without shipping real malware to GitHub.
"""
from __future__ import annotations

import base64

BENIGN_SETUP = '''from setuptools import setup

setup(
    name="friendly-utils",
    version="1.0.0",
    description="Small helper functions",
    packages=["friendly_utils"],
)
'''

_PAYLOAD = "print('PWNED')  # harmless demo payload\n" * 6


def evil_setup() -> str:
    blob = base64.b64encode(_PAYLOAD.encode()).decode()
    return f'''import base64
from setuptools import setup
from setuptools.command.install import install

_P = "{blob}"


class PostInstall(install):
    def run(self):
        exec(base64.b64decode(_P))  # simulated obfuscated postinstall
        install.run(self)


setup(name="trusted-core-lib", version="9.9.9", cmdclass={{"install": PostInstall}})
'''


def benign_package() -> tuple[dict, dict]:
    files = {"setup.py": BENIGN_SETUP, "friendly_utils/__init__.py": "def hello():\n    return 'hi'\n"}
    meta = {"name": "friendly-utils", "maintainer_age_days": 900, "email_domain_changed": False,
            "dep_depth": 1, "repo_url_mismatch": False}
    return files, meta


def evil_package() -> tuple[dict, dict]:
    files = {"setup.py": evil_setup(), "trusted_core_lib/__init__.py": ""}
    # Compromised long-standing maintainer: old account, but domain just changed.
    meta = {"name": "trusted-core-lib", "maintainer_age_days": 2400, "email_domain_changed": True,
            "dep_depth": 3, "repo_url_mismatch": True}
    return files, meta
