"""Tests for the environment SkyPilot gives `ray start` on each node."""
import re

import pytest

from sky.provision import instance_setup
from sky.skylet import constants

_COMMANDS = {
    'head': instance_setup.ray_head_start_command(None, None),
    'worker': instance_setup.ray_worker_start_command(None,
                                                      None,
                                                      no_restart=False),
}


def _ray_start_env(cmd: str) -> dict:
    """Returns the VAR=value prefixes of the simple command running ray start.

    A prefix only reaches `ray start` (and from there the raylet and its
    workers) when it's part of the same simple command.
    """
    start = f'{constants.SKY_RAY_CMD} start'
    assert cmd.count(start) == 1, cmd
    # The text between the previous command's `;` and `ray start`.
    prefix = cmd.split(start)[0].rsplit('; ', 1)[-1]
    # A value is one word, or shell arithmetic such as $(( 3 * $(nproc) )).
    assign = r'(\w+)=(\$\(\(.*?\)\)(?=\s)|\S+)\s+'
    assert re.fullmatch(f'(?:{assign})+', prefix), prefix
    return dict(re.findall(assign, prefix))


@pytest.mark.parametrize('node', sorted(_COMMANDS))
def test_ray_start_retries_local_connects_every_200ms(node):
    env = _ray_start_env(_COMMANDS[node])
    timeout_ms = int(env['RAY_raylet_client_connect_timeout_milliseconds'])
    attempts = int(env['RAY_raylet_client_num_connect_attempts'])
    assert timeout_ms == 200
    # Ray gives up after about attempts x timeout: keep Ray's default of
    # 10 x 1000 ms, so a slow node isn't failed sooner than before.
    assert timeout_ms * attempts == 10 * 1000


@pytest.mark.parametrize('node', sorted(_COMMANDS))
def test_ray_start_has_no_dashboard(node):
    assert _ray_start_env(_COMMANDS[node])['SKYPILOT_RAY_NO_DASHBOARD'] == '1'
