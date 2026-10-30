"""How a freshly booted K8s cluster waits for its pods' Ray to be ready.

The head blocks in one remote call until every node has joined, and the
skylet starts while that wait runs.
"""
# pylint: disable=protected-access
import base64
import contextlib
import json
import re
import shlex
import socket
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from unittest import mock

import pytest

from sky import clouds
from sky.provision import common
from sky.provision import instance_setup
from sky.provision import logging as provision_logging
from sky.provision import provisioner
from sky.skylet import constants
from sky.utils import message_utils
from sky.utils import resources_utils

_PORT = 20601

# --- The wait script, run for real against a fake `ray._raylet`. ---


def _fake_raylet(states_per_call):
    """A GcsClient whose n-th node query answers states_per_call[n].

    The last answer repeats. Node state 0 is ALIVE.
    """
    return ('import types\n'
            f'_answers = {states_per_call!r}\n'
            '_calls = [0]\n'
            'class GcsClient:\n'
            '    def __init__(self, address):\n'
            '        pass\n'
            '    def get_all_node_info(self, timeout):\n'
            '        states = _answers[min(_calls[0], len(_answers) - 1)]\n'
            '        _calls[0] += 1\n'
            '        return {i: types.SimpleNamespace(state=s)\n'
            '                for i, s in enumerate(states)}\n')


def _run_wait(tmp_path,
              expected,
              listening=True,
              raylet='',
              timeout=5.0,
              write_port_file=True):
    (tmp_path / 'ray').mkdir(exist_ok=True)
    (tmp_path / 'ray' / '__init__.py').write_text('')
    (tmp_path / 'ray' / '_raylet.py').write_text(raylet)
    with socket.socket() as server:
        server.bind(('127.0.0.1', 0))
        if listening:
            server.listen()
        port = server.getsockname()[1]
        runtime_dir = tmp_path / 'runtime'
        port_file = runtime_dir / constants.SKY_REMOTE_RAY_PORT_FILE
        port_file.parent.mkdir(parents=True, exist_ok=True)
        if write_port_file:
            port_file.write_text(json.dumps({'ray_port': port}))
        start = time.monotonic()
        proc = subprocess.run([
            sys.executable, '-c', instance_setup._WAIT_ALIVE_NODES_PY,
            str(expected),
            str(timeout), '0.01', 'TEST_RUNTIME_DIR',
            constants.SKY_REMOTE_RAY_PORT_FILE,
            str(port)
        ],
                              env={
                                  'TEST_RUNTIME_DIR': str(runtime_dir),
                                  'PYTHONPATH': str(tmp_path)
                              },
                              capture_output=True,
                              text=True,
                              timeout=60,
                              check=False)
    return proc, port, time.monotonic() - start


def test_the_wait_returns_once_every_node_has_joined(tmp_path):
    """Workers join one by one; the wait ends on the poll that sees all."""
    raylet = _fake_raylet([[0], [0, 0], [0, 0, 0], [0, 0, 0, 0]])
    proc, port, elapsed = _run_wait(tmp_path, expected=4, raylet=raylet)
    assert proc.returncode == 0, proc.stderr
    assert 'SKY_ALIVE_NODES 4 4' in proc.stdout
    assert message_utils.decode_payload(proc.stdout) == {'ray_port': port}
    assert proc.stdout.rstrip().endswith(instance_setup.RAY_WAIT_DONE_MARKER)
    # Did not sit out the 5s deadline.
    assert elapsed < 4


def test_the_wait_retries_while_the_gcs_is_coming_up(tmp_path):
    """The GCS port is open but queries fail at first, as right after
    `ray start --head`; the wait must keep going, not report a dead head."""
    raylet = ('import types\n'
              '_calls = [0]\n'
              'class GcsClient:\n'
              '    def __init__(self, address):\n'
              '        pass\n'
              '    def get_all_node_info(self, timeout):\n'
              '        _calls[0] += 1\n'
              '        if _calls[0] < 3:\n'
              '            raise RuntimeError("GCS not ready")\n'
              '        return {0: types.SimpleNamespace(state=0),\n'
              '                1: types.SimpleNamespace(state=0)}\n')
    proc, _, _ = _run_wait(tmp_path, expected=2, raylet=raylet)
    assert proc.returncode == 0, proc.stderr
    assert 'SKY_ALIVE_NODES 2 2' in proc.stdout


def test_the_wait_reports_the_partial_count_at_the_deadline(tmp_path):
    """The head is up but a worker never joins: exit 0 (the head needs no
    restart) with the count, so only the workers are started."""
    raylet = _fake_raylet([[0, 0, 1]])
    proc, _, elapsed = _run_wait(tmp_path,
                                 expected=3,
                                 raylet=raylet,
                                 timeout=0.5)
    assert proc.returncode == 0, proc.stderr
    assert 'SKY_ALIVE_NODES 2 3' in proc.stdout
    assert proc.stdout.rstrip().endswith(instance_setup.RAY_WAIT_DONE_MARKER)
    assert elapsed < 10


def test_the_wait_reports_no_cluster_when_nothing_listens(tmp_path):
    proc, port, _ = _run_wait(tmp_path,
                              expected=2,
                              listening=False,
                              raylet='raise SystemExit(7)\n',
                              timeout=0.3)
    assert proc.returncode == 1, proc.stderr
    assert 'SKY_ALIVE_NODES' not in proc.stdout
    assert f'Ray cluster is not found at 127.0.0.1:{port}' in proc.stdout
    # Still names the port and finishes cleanly: Ray is down, not the call.
    assert message_utils.decode_payload(proc.stdout) == {'ray_port': port}
    assert proc.stdout.rstrip().endswith(instance_setup.RAY_WAIT_DONE_MARKER)


def test_the_wait_falls_back_to_the_default_port_without_a_port_file(tmp_path):
    """Before `ray start --head` returns, the head has no port file yet."""
    proc, port, _ = _run_wait(tmp_path,
                              expected=1,
                              raylet=_fake_raylet([[0]]),
                              write_port_file=False)
    # The default port passed in is the listening one here.
    assert proc.returncode == 0, proc.stderr
    assert message_utils.decode_payload(proc.stdout) == {'ray_port': port}


def test_the_command_ships_the_script_as_one_shell_word():
    cmd = instance_setup.ray_wait_for_alive_nodes_command(4, 87.25)
    words = shlex.split(cmd)
    code = words[words.index('-c') + 1]
    script_b64 = re.search(r"b64decode\('([A-Za-z0-9+/=]+)'\)", code).group(1)
    assert (base64.b64decode(script_b64).decode() ==
            instance_setup._WAIT_ALIVE_NODES_PY)
    assert words[-6:] == [
        '4', '87.2',
        str(instance_setup._RAY_WAIT_POLL_INTERVAL_SECONDS),
        constants.SKY_RUNTIME_DIR_ENV_VAR_KEY,
        constants.SKY_REMOTE_RAY_PORT_FILE,
        str(constants.SKY_REMOTE_RAY_PORT)
    ]


# --- The provisioner, on a freshly booted K8s cluster. ---


class _Stop(Exception):
    """Stops the setup right after the skylet start."""


def _launch(monkeypatch, head_run, start_skylet, num_nodes=2):
    """Sets up a just-booted K8s cluster of `num_nodes` pods.

    Returns the mocks for starting Ray on the head and on the workers.
    """
    instances = {
        'head': [common.InstanceInfo('head', '10.0.0.1', None, {})],
    }
    for i in range(1, num_nodes):
        instances[f'worker{i}'] = [
            common.InstanceInfo(f'worker{i}', f'10.0.0.{i + 1}', None, {})
        ]
    cluster_info = common.ClusterInfo(instances=instances,
                                      head_instance_id='head',
                                      provider_name='kubernetes',
                                      provider_config={})
    record = common.ProvisionRecord('kubernetes', 'ctx', None, 'cluster',
                                    'head', [], list(instances))
    for name, value in {
            'get_cluster_yaml_dict': mock.Mock(return_value={
                'provider': {},
                'setup_commands': []
            }),
            'get_handle_from_cluster_name': mock.Mock(),
            'update_cluster_handle': mock.Mock(),
    }.items():
        monkeypatch.setattr(provisioner.global_user_state, name, value)
    monkeypatch.setattr(provisioner.provision, 'get_cluster_info',
                        mock.Mock(return_value=cluster_info))
    monkeypatch.setattr(
        provisioner.provision, 'get_command_runners',
        mock.Mock(return_value=[mock.Mock(run=head_run)] +
                  [mock.Mock() for _ in range(num_nodes - 1)]))
    monkeypatch.setattr(provisioner.backend_utils, 'ssh_credential_from_yaml',
                        mock.Mock(return_value={}))
    monkeypatch.setattr(provisioner.rich_utils, 'safe_status',
                        lambda *_: contextlib.nullcontext(mock.Mock()))
    monkeypatch.setattr(provisioner.logs, 'get_logging_agent',
                        mock.Mock(return_value=None))
    for name in ('internal_file_mounts', 'setup_runtime_on_cluster'):
        monkeypatch.setattr(provisioner.instance_setup, name, mock.Mock())
    start_head = mock.Mock()
    start_workers = mock.Mock()
    monkeypatch.setattr(provisioner.instance_setup, 'start_ray_on_head_node',
                        start_head)
    monkeypatch.setattr(provisioner.instance_setup, 'start_ray_on_worker_nodes',
                        start_workers)
    monkeypatch.setattr(provisioner.instance_setup, 'start_skylet_on_head_node',
                        start_skylet)
    provisioner._post_provision_setup(
        SimpleNamespace(cloud=clouds.Kubernetes(), region=None),
        resources_utils.ClusterName('cluster', 'cluster'), '/cluster.yaml',
        record, None)
    return start_head, start_workers


def _wait_output(alive, total, port=_PORT):
    payload = message_utils.encode_payload({'ray_port': port})
    return (f'{payload}SKY_ALIVE_NODES {alive} {total}\n'
            f'{instance_setup.RAY_WAIT_DONE_MARKER}\n')


def test_a_ready_cluster_is_waited_for_once_and_left_alone(monkeypatch):
    calls = []

    def head_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return 0, _wait_output(2, 2), ''

    start_skylet = mock.Mock()
    start_head, start_workers = _launch(monkeypatch, head_run, start_skylet)
    # One blocking call, not a poll loop.
    assert len(calls) == 1
    cmd, kwargs = calls[0]
    assert cmd.startswith(constants.SKY_PYTHON_CMD)
    assert shlex.split(cmd)[-6] == '2'
    assert kwargs['timeout'] > provisioner._K8S_RAY_READY_TIMEOUT_SECONDS
    start_head.assert_not_called()
    start_workers.assert_not_called()
    start_skylet.assert_called_once()


def test_the_skylet_starts_while_the_workers_are_still_joining(monkeypatch):
    """The wait only returns once the skylet has started: they overlap."""
    skylet_started = threading.Event()
    log_paths = []

    def head_run(cmd, **_):
        del cmd
        assert skylet_started.wait(timeout=10), 'skylet waited for Ray'
        return 0, _wait_output(2, 2), ''

    def start_skylet(*_, **__):
        log_paths.append(provision_logging.get_log_path())
        skylet_started.set()

    monkeypatch.setattr(provision_logging.config, 'log_path',
                        provision_logging.pathlib.Path('/provision.log'))
    _launch(monkeypatch, head_run, start_skylet)
    # The skylet thread logs to this provision's log, not the thread
    # default of /dev/null.
    assert log_paths == [provision_logging.pathlib.Path('/provision.log')]


def test_a_dropped_wait_is_retried_not_treated_as_dead_ray(monkeypatch):
    """`kubectl exec` dropping mid-wait (no done marker) must not restart
    Ray on every node."""
    answers = [(255, 'error: stream closed\n', ''), (0, _wait_output(2, 2), '')]

    def head_run(*_, **__):
        return answers.pop(0)

    monkeypatch.setattr(provisioner.time, 'sleep', lambda _: None)
    start_head, start_workers = _launch(monkeypatch, head_run, mock.Mock())
    assert not answers
    start_head.assert_not_called()
    start_workers.assert_not_called()


def test_workers_that_never_join_are_started_without_a_head_restart(
        monkeypatch):

    def head_run(cmd, **_):
        if cmd == instance_setup.RAY_ALIVE_NODE_COUNT_COMMAND:
            return 0, _wait_output(1, 2), ''
        return 0, _wait_output(1, 2), ''

    start_head, start_workers = _launch(monkeypatch, head_run, mock.Mock())
    start_head.assert_not_called()
    start_workers.assert_called_once()
    assert start_workers.call_args.kwargs['no_restart'] is True
    assert start_workers.call_args.kwargs['ray_port'] == _PORT


def test_a_head_without_ray_is_started_after_the_wait(monkeypatch):
    """Past the deadline with no Ray on the head: start it everywhere, as
    before. The skylet is still started exactly once."""
    not_found = (message_utils.encode_payload({'ray_port': _PORT}) +
                 'Ray cluster is not found at 127.0.0.1:20601\n'
                 f'{instance_setup.RAY_WAIT_DONE_MARKER}\n')

    def head_run(cmd, **_):
        if cmd == instance_setup.RAY_ALIVE_NODE_COUNT_COMMAND:
            return 0, _wait_output(1, 1), ''
        return 1, not_found, ''

    start_skylet = mock.Mock()
    start_head, start_workers = _launch(monkeypatch, head_run, start_skylet)
    start_head.assert_called_once()
    start_workers.assert_called_once()
    assert start_workers.call_args.kwargs['no_restart'] is False
    start_skylet.assert_called_once()


def test_a_skylet_failure_still_fails_the_launch(monkeypatch):

    def head_run(*_, **__):
        return 0, _wait_output(2, 2), ''

    with pytest.raises(_Stop):
        _launch(monkeypatch, head_run, mock.Mock(side_effect=_Stop))
