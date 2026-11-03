"""Tests for creating an upload's target path in the same call as rsync.

These run the real rsync binary and the real rsync_helper.sh, with stand-ins for
ssh and kubectl that run the remote command locally.
"""
# pylint: disable=protected-access
import os
import pathlib
import stat
from unittest import mock

import pytest

from sky import exceptions
from sky.utils import command_runner


def _executable(path: pathlib.Path, text: str) -> str:
    path.write_text(text)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return str(path)


@pytest.fixture(name='fake_ssh')
def _fake_ssh(tmp_path):
    """An rsh that, like ssh, joins its arguments for a remote shell."""
    calls = tmp_path / 'ssh_calls'
    return _executable(
        tmp_path / 'fake_ssh', '#!/bin/bash\n'
        f'echo call >> {calls}\n'
        '[ "$1" = "-l" ] && shift 3 || shift\n'
        'exec bash -c "$*"\n')


@pytest.fixture(name='fake_kubectl')
def _fake_kubectl(tmp_path, monkeypatch):
    """A kubectl on PATH that, like exec, runs what comes after `--` as argv."""
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    calls = tmp_path / 'kubectl_calls'
    _executable(
        bin_dir / 'kubectl', '#!/bin/bash\n'
        f'echo call >> {calls}\n'
        'while [ "$1" != "--" ]; do shift; done\n'
        'shift\n'
        'exec "$@"\n')
    monkeypatch.setenv('PATH', f'{bin_dir}{os.pathsep}{os.environ["PATH"]}')
    return calls


def _ssh_upload(fake_ssh, source, target):
    runner = mock.MagicMock(spec=command_runner.CommandRunner)
    command_runner.CommandRunner._rsync(runner,
                                        str(source),
                                        str(target),
                                        node_destination='user@host',
                                        up=True,
                                        rsh_option=fake_ssh,
                                        max_retry=1,
                                        stream_logs=False,
                                        create_target_dir=True)


def _k8s_runner():
    return command_runner.KubernetesCommandRunner((('default', 'ctx'), 'pod-1'))


@pytest.fixture(name='sources')
def _sources(tmp_path):
    src_file = tmp_path / 'src.txt'
    src_file.write_text('file')
    src_dir = tmp_path / 'src_dir'
    src_dir.mkdir()
    (src_dir / 'inner.txt').write_text('dir')
    return src_file, src_dir


def test_ssh_creates_the_parent_of_a_file(tmp_path, sources, fake_ssh):
    target = tmp_path / 'a' / 'b' / 'dst.txt'
    _ssh_upload(fake_ssh, sources[0], target)
    assert target.read_text() == 'file'


def test_ssh_creates_a_directory_target(tmp_path, sources, fake_ssh):
    target = tmp_path / 'a' / 'b' / 'dst_dir'
    _ssh_upload(fake_ssh, sources[1], target)
    assert (target / 'inner.txt').read_text() == 'dir'


def test_ssh_handles_a_space_in_the_target_dir(tmp_path, sources, fake_ssh):
    target = tmp_path / 'with space' / 'b' / 'dst.txt'
    runner = mock.MagicMock(spec=command_runner.CommandRunner)
    with mock.patch.object(command_runner.log_lib,
                           'run_with_log',
                           return_value=(0, '', '')) as run:
        command_runner.CommandRunner._rsync(runner,
                                            str(sources[0]),
                                            str(target),
                                            node_destination='user@host',
                                            up=True,
                                            rsh_option=fake_ssh,
                                            stream_logs=False,
                                            create_target_dir=True)
    assert '--rsync-path=' in run.call_args.args[0]
    assert 'with space' in run.call_args.args[0]


def test_ssh_failed_mkdir_fails_the_rsync(tmp_path, sources, fake_ssh):
    blocker = tmp_path / 'a_file'
    blocker.write_text('')
    with mock.patch.object(command_runner.time, 'sleep'):
        with pytest.raises(exceptions.CommandError):
            _ssh_upload(fake_ssh, sources[0], blocker / 'sub' / 'dst.txt')


def test_ssh_rsync_path_keeps_the_home_expandable():
    assert command_runner._quote_path('~/a b/c') == "~/'a b/c'"
    assert command_runner._quote_path('~') == '~'
    assert command_runner._quote_path('/a b') == "'/a b'"


@pytest.mark.parametrize('is_file', [True, False])
def test_kubernetes_uses_one_exec_through_the_real_helper(
        tmp_path, sources, fake_kubectl, is_file):
    source = sources[0] if is_file else sources[1]
    target = tmp_path / 'a' / 'b' / ('dst.txt' if is_file else 'dst')
    runner = _k8s_runner()
    with mock.patch.object(runner, 'run') as run:
        runner.rsync_setup_to_new_path(str(source),
                                       str(target),
                                       stream_logs=False)
    run.assert_not_called()
    assert fake_kubectl.read_text().count('call') == 1
    if is_file:
        assert target.read_text() == 'file'
    else:
        assert (target / 'inner.txt').read_text() == 'dir'


def test_kubernetes_plain_rsync_creates_nothing(tmp_path, sources,
                                                fake_kubectl):
    target = tmp_path / 'missing' / 'b' / 'dst.txt'
    runner = _k8s_runner()
    with mock.patch.object(command_runner.time, 'sleep'):
        with pytest.raises(exceptions.CommandError):
            runner.rsync(str(sources[0]),
                         str(target),
                         up=True,
                         stream_logs=False)
    assert not target.parent.exists()
    assert fake_kubectl.exists()


def test_kubernetes_failed_mkdir_fails_the_rsync(tmp_path, sources,
                                                 fake_kubectl):
    blocker = tmp_path / 'a_file'
    blocker.write_text('')
    runner = _k8s_runner()
    with mock.patch.object(command_runner.time, 'sleep'):
        with pytest.raises(exceptions.CommandError):
            runner.rsync_setup_to_new_path(str(sources[0]),
                                           str(blocker / 'sub' / 'dst.txt'),
                                           stream_logs=False)
    assert fake_kubectl.exists()


@pytest.mark.parametrize('is_file,expected_dir', [(True, '/a/b'),
                                                  (False, '/a/b/dst')])
def test_default_runs_mkdir_then_rsync_setup(sources, is_file, expected_dir):
    source = sources[0] if is_file else sources[1]
    target = '/a/b/dst'
    runner = mock.MagicMock(spec=command_runner.CommandRunner)
    runner.run_setup.return_value = (0, '', '')
    command_runner.CommandRunner.rsync_setup_to_new_path(
        runner, str(source), target)
    assert runner.mock_calls[0] == mock.call.run_setup(
        f'mkdir -p {expected_dir}',
        log_path=os.devnull,
        stream_logs=True,
        require_outputs=True,
        timeout=None)
    assert runner.mock_calls[1] == mock.call.rsync_setup(str(source),
                                                         target,
                                                         up=True,
                                                         log_path=os.devnull,
                                                         stream_logs=True,
                                                         timeout=None)


def test_default_failed_mkdir_skips_rsync(sources):
    runner = mock.MagicMock(spec=command_runner.CommandRunner)
    runner.run_setup.return_value = (1, '', 'denied')
    with pytest.raises(exceptions.CommandError):
        command_runner.CommandRunner.rsync_setup_to_new_path(
            runner, str(sources[0]), '/a/b/dst')
    runner.rsync_setup.assert_not_called()


def test_slurm_keeps_the_two_calls():
    """mkdir has to run where rsync does, through srun or a container."""
    assert (command_runner.SlurmLoginNodeCommandRunner.rsync_setup_to_new_path
            is not command_runner.SSHCommandRunner.rsync_setup_to_new_path)
    runner = mock.MagicMock(spec=command_runner.SlurmCommandRunner)
    runner.run_setup.return_value = (0, '', '')
    command_runner.SlurmLoginNodeCommandRunner.rsync_setup_to_new_path(
        runner, '/no/such/source', '/a/b/dst')
    runner.run_setup.assert_called_once()
    runner.rsync_setup.assert_called_once()
    runner.rsync.assert_not_called()


def test_local_runner_keeps_the_two_calls():
    """--rsync-path is ignored without a remote shell, so it can't mkdir."""
    assert (command_runner.LocalProcessCommandRunner.rsync_setup_to_new_path is
            command_runner.CommandRunner.rsync_setup_to_new_path)
