"""Tests for installing the SkyPilot wheel without the network first."""
import os
import pathlib
import stat
import subprocess

import pytest

from sky.skylet import constants

_HASH = 'abc123'


def _run(tmp_path: pathlib.Path, offline_rc: int, online_rc: int):
    """Runs the install commands against a fake uv; returns (rc, its calls)."""
    uv = tmp_path / '.local' / 'bin' / 'uv'
    uv.parent.mkdir(parents=True)
    calls = tmp_path / 'calls'
    uv.write_text(f'''#!/bin/bash
echo "$*" >> {calls}
case "$*" in
  *"install --offline"*) exit {offline_rc} ;;
  *"pip install"*) exit {online_rc} ;;
esac
exit 0
''')
    uv.chmod(uv.stat().st_mode | stat.S_IEXEC)
    wheels = tmp_path / '.sky' / 'wheels' / _HASH
    wheels.mkdir(parents=True)
    (wheels / f'skypilot-{constants._sky_version}-py3-none-any.whl').touch()  # pylint: disable=protected-access
    command = constants.SKYPILOT_WHEEL_INSTALLATION_COMMANDS.replace(
        '{sky_wheel_hash}', _HASH).replace('{cloud}', 'kubernetes')
    env = dict(os.environ, HOME=str(tmp_path))
    env.pop(constants.SKY_RUNTIME_DIR_ENV_VAR_KEY, None)
    proc = subprocess.run(['bash', '-c', command],
                          env=env,
                          cwd=tmp_path,
                          capture_output=True,
                          text=True,
                          check=False)
    installs = [
        line for line in calls.read_text().splitlines()
        if line.startswith('pip install')
    ]
    return proc.returncode, installs


def _wheel_hash_written(tmp_path: pathlib.Path) -> bool:
    return (tmp_path / '.sky' / 'wheels' / 'current_sky_wheel_hash').exists()


def test_offline_success_never_uses_the_network(tmp_path):
    rc, installs = _run(tmp_path, offline_rc=0, online_rc=1)
    assert rc == 0
    assert len(installs) == 1
    assert '--offline' in installs[0]
    assert '[kubernetes, remote]' in installs[0]
    assert _wheel_hash_written(tmp_path)


def test_offline_failure_falls_back_to_the_network(tmp_path):
    rc, installs = _run(tmp_path, offline_rc=1, online_rc=0)
    assert rc == 0
    assert len(installs) == 2
    assert '--offline' in installs[0]
    assert '--offline' not in installs[1]
    assert _wheel_hash_written(tmp_path)


def test_both_failing_fails_the_setup(tmp_path):
    rc, installs = _run(tmp_path, offline_rc=1, online_rc=1)
    assert rc != 0
    assert len(installs) == 2
    assert not _wheel_hash_written(tmp_path)


@pytest.mark.parametrize('line', [
    'if [ -z "$(ls -A ~/.cache/uv/archive-v0 2>/dev/null)" ]; then',
])
def test_template_skips_the_dry_run_with_a_populated_cache(line):
    template = (pathlib.Path(constants.__file__).parent.parent / 'templates' /
                'kubernetes-ray.yml.j2').read_text()
    guard = template.index(line)
    dry_run = template.index('uv pip install --dry-run', guard)
    # The guard wraps the dry run, with nothing else between them.
    assert 'fi\n' not in template[guard:dry_run]
