import pytest

from sentinalops.features.extract import _has_install_hook


@pytest.mark.parametrize("setup,expected", [
    ("setup(name='x')", False),
    ("setup(name='x', cmdclass={'test': PyTest})", False),  # requests-style test command
    ("setup(cmdclass={'install': PostInstall})", True),
    ('setup(cmdclass={"develop": Dev, "test": T})', True),
    ("setup(\n  cmdclass = {\n    'egg_info': Egg,\n  },\n)", True),
    ("from setuptools.command.install import install\nclass P(install):\n    pass", True),
    ("class P(setuptools.command.develop.develop): pass", True),
    ("# runs a postinstall step", True),
    ("", False),
])
def test_install_hook_detection(setup, expected):
    assert _has_install_hook(setup) is expected
