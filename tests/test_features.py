import pytest

from sentinalops.features.extract import _has_install_hook, ast_features


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


# ---- evasion hardening: breaking the syntax or aliasing imports must not hide dangerous calls
@pytest.mark.parametrize("src,key", [
    ("import os as o\no.system('id')", "n_os_system"),
    ("from os import system\nsystem('id')", "n_os_system"),
    ("from subprocess import run\nrun(['id'])", "n_subprocess"),
    ("import subprocess as sp\nsp.Popen(['id'])", "n_subprocess"),
    ("import subprocess\nsubprocess.call('id')", "n_subprocess"),
    ("from builtins import exec as e\ne('x')", "n_exec_eval"),
    ("import builtins as b\nb.eval('1')", "n_exec_eval"),
])
def test_aliased_dangerous_calls_detected(src, key):
    assert ast_features(src)[key] == 1


@pytest.mark.parametrize("src,key", [
    ("print 'hi'\nexec 'x'", "n_exec_eval"),  # Python 2 syntax
    ("x = " + "(" * 3000 + "1" + ")" * 3000 + "\nexec('x')", "n_exec_eval"),  # absurd nesting
    ("exec('x')\x00", "n_exec_eval"),  # NUL byte
    ("def broken(:\n    os.system('id')", "n_os_system"),  # deliberate syntax error
    ("this is not python\nsubprocess.Popen(['id'])", "n_subprocess"),
    ("import socket\n!!!", "n_network"),
])
def test_unparseable_source_still_flags_danger(src, key):
    assert ast_features(src)[key] >= 1


def test_local_function_named_run_is_not_flagged():
    out = ast_features("def run():\n    pass\nrun()\nimport os\nos.path.join('a')")
    assert sum(out.values()) == 0
