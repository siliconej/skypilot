"""`ray stop` runs before `ray start` only when Ray is running.

A freshly booted node has no Ray to stop, yet `ray stop` costs a Python start
(1.5-4 s on a busy node) before `ray start` can begin, and on the head that
delays every worker.
"""
import os
import shutil
import stat
import subprocess

import pytest

from sky.provision import instance_setup
from sky.skylet import constants

_STOP = f'{constants.SKY_RAY_CMD} stop'


def _run_guard(tmp_path, pgrep_exit_code=None):
    """Runs the guard with `ray stop` swapped for a marker.

    pgrep_exit_code: None leaves no pgrep on PATH; otherwise a stub pgrep
    exits with it (0: a Ray process was found, 1: none).
    """
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    if pgrep_exit_code is not None:
        stub = bin_dir / 'pgrep'
        stub.write_text(f'#!/bin/sh\nexit {pgrep_exit_code}\n')
        stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    guard = instance_setup.RAY_STOP_IF_RUNNING.replace(_STOP, 'echo STOPPED')
    proc = subprocess.run([shutil.which('bash'), '-c', guard + ' echo END'],
                          env={'PATH': str(bin_dir)},
                          capture_output=True,
                          text=True,
                          check=True)
    return proc.stdout.split()


@pytest.mark.parametrize('pgrep_exit_code,stopped', [(0, True), (1, False),
                                                     (None, True)])
def test_ray_is_stopped_only_when_running(tmp_path, pgrep_exit_code, stopped):
    out = _run_guard(tmp_path, pgrep_exit_code)
    assert out == (['STOPPED', 'END'] if stopped else ['END'])


@pytest.mark.skipif(shutil.which('pgrep') is None, reason='needs pgrep')
def test_the_guard_does_not_match_its_own_command_line():
    """The pattern sits in the command line of the shell that runs it (the
    pod's bash -c, a kubectl exec); it must not find itself there."""
    guard = instance_setup.RAY_STOP_IF_RUNNING.replace(_STOP, 'echo STOPPED')
    probe = subprocess.run(['pgrep', '-f', 'ray/core/src/ray/gcs/gcs_server'],
                           capture_output=True,
                           check=False)
    if probe.returncode == 0:
        pytest.skip('a Ray GCS is running on this machine')
    out = subprocess.run(['bash', '-c', guard + ' echo END'],
                         env={
                             'PATH': os.environ['PATH']
                         },
                         capture_output=True,
                         text=True,
                         check=True).stdout.split()
    assert out == ['END']


@pytest.mark.parametrize('cmd', [
    instance_setup.ray_head_start_command(None, None),
    instance_setup.ray_worker_start_command(None, None, no_restart=False),
])
def test_start_commands_stop_ray_only_through_the_guard(cmd):
    assert instance_setup.RAY_STOP_IF_RUNNING in cmd
    # No unconditional `ray stop` left besides the guarded one.
    assert cmd.count(_STOP) == 1
    assert cmd.index(instance_setup.RAY_STOP_IF_RUNNING) < cmd.index(
        f'{constants.SKY_RAY_CMD} start')
