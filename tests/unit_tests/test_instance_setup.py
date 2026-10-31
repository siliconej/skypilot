"""Tests for sky.provision.instance_setup."""
import os
import resource
import subprocess
import sys
from typing import Tuple

import pytest

from sky.provision import instance_setup
from sky.skylet import constants

# pylint: disable=protected-access,redefined-outer-name
_TARGET = instance_setup._TARGET_NOFILE


def _process_hard_nofile_limit() -> int:
    """This process' hard nofile limit, or -1 if it is unlimited."""
    return resource.getrlimit(resource.RLIMIT_NOFILE)[1]


def _skip_if_hard_limit_below(hard: int) -> None:
    process_hard = _process_hard_nofile_limit()
    if 0 <= process_hard < hard:
        pytest.skip(f'hard nofile limit of this process is {process_hard}, '
                    f'below the {hard} this test needs')


def _raise_nofile_limit(soft: int, hard: int) -> Tuple[int, int]:
    """Runs RAISE_NOFILE_LIMIT_CMD under the given limits.

    The limits are lowered by the shell itself (a process may always lower its
    own limits), so no privileges are needed. Returns the resulting (soft,
    hard) limits.
    """
    # Soft first: lowering the hard limit below the current soft limit is
    # rejected with EINVAL.
    script = (f'ulimit -Sn {soft} && ulimit -Hn {hard} && '
              f'{instance_setup.RAISE_NOFILE_LIMIT_CMD}; '
              'echo "$(ulimit -Sn) $(ulimit -Hn)"')
    out = subprocess.run(['bash', '-c', script],
                         capture_output=True,
                         text=True,
                         check=True).stdout.split()
    return int(out[0]), int(out[1])


def test_raise_nofile_limit_clamps_to_hard_limit():
    """Below the target, the soft limit is raised as far as it can go."""
    _skip_if_hard_limit_below(2048)
    assert _raise_nofile_limit(soft=1024, hard=2048) == (2048, 2048)


def test_raise_nofile_limit_reaches_target():
    """The real target is reached when the host's hard limit allows it."""
    _skip_if_hard_limit_below(_TARGET)
    process_hard = _process_hard_nofile_limit()
    hard = _TARGET * 2 if process_hard < 0 else process_hard
    assert _raise_nofile_limit(soft=1024, hard=hard) == (_TARGET, hard)


_FALLBACK = 'FALLBACK_TO_ATTEMPT_SKYLET'


@pytest.fixture
def skylet_runtime_dir(tmp_path):
    """A runtime dir holding a live process that looks like skylet."""
    if not os.path.isdir('/proc/self'):
        pytest.skip('needs /proc')
    (tmp_path / '.sky').mkdir()
    # The check matches on the module name in the process' command line.
    proc = subprocess.Popen([
        sys.executable, '-c', 'import time; time.sleep(60)', 'sky.skylet.skylet'
    ])
    try:
        yield tmp_path, proc
    finally:
        proc.kill()
        proc.wait()


def _run_skylet_check(runtime_dir, version: str, pid: str) -> str:
    """Runs the shell fast path of MAYBE_SKYLET_RESTART_CMD."""
    (runtime_dir / constants.SKYLET_VERSION_FILE).write_text(version + '\n')
    (runtime_dir / constants.SKYLET_PID_FILE).write_text(pid + '\n')
    script = (f'{instance_setup._SKYLET_RUNNING_LATEST_CHECK} || '
              f'echo {_FALLBACK}')
    return subprocess.run(
        ['bash', '-c', script],
        env={
            **os.environ, constants.SKY_RUNTIME_DIR_ENV_VAR_KEY:
                str(runtime_dir)
        },
        capture_output=True,
        text=True,
        check=True).stdout


def test_skylet_check_skips_attempt_skylet_when_up_to_date(skylet_runtime_dir):
    runtime_dir, proc = skylet_runtime_dir
    out = _run_skylet_check(runtime_dir, constants.SKYLET_VERSION,
                            str(proc.pid))
    assert 'Skylet is running with the latest version' in out
    assert _FALLBACK not in out


def test_skylet_check_falls_through_on_stale_version(skylet_runtime_dir):
    runtime_dir, proc = skylet_runtime_dir
    out = _run_skylet_check(runtime_dir, 'stale', str(proc.pid))
    assert out.strip() == _FALLBACK


def test_skylet_check_falls_through_on_dead_process(skylet_runtime_dir):
    runtime_dir, proc = skylet_runtime_dir
    proc.kill()
    proc.wait()
    out = _run_skylet_check(runtime_dir, constants.SKYLET_VERSION,
                            str(proc.pid))
    assert out.strip() == _FALLBACK


def test_skylet_check_falls_through_on_non_skylet_process(tmp_path):
    (tmp_path / '.sky').mkdir()
    out = _run_skylet_check(tmp_path, constants.SKYLET_VERSION,
                            str(os.getpid()))
    assert out.strip() == _FALLBACK


def test_skylet_check_falls_through_without_pid_file(tmp_path):
    (tmp_path / '.sky').mkdir()
    out = _run_skylet_check(tmp_path, constants.SKYLET_VERSION, '')
    assert out.strip() == _FALLBACK
