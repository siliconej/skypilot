"""Tests for reusing a storage check that found no enabled cloud."""
import pytest

from sky import clouds as sky_clouds
from sky import global_user_state
import sky.check as sky_check
from sky.clouds.cloud import CloudCapability
from sky.skylet import constants
from sky.utils.db import db_utils

_WORKSPACE = 'default'
_ALLOWED = ['AWS', 'Kubernetes']


@pytest.fixture(name='env')
def _env_fixture(tmp_path, monkeypatch):
    """A fresh DB, a fake clock and config, and a counted cloud check.

    Set `env['enabled']` to change what later checks find.
    """
    monkeypatch.setenv(constants.SKY_RUNTIME_DIR_ENV_VAR_KEY, str(tmp_path))
    monkeypatch.setattr(
        global_user_state, '_db_manager',
        db_utils.DatabaseManager(
            'state',
            global_user_state.create_table,
            post_init_fn=lambda _: global_user_state._sqlite_supports_returning(
            ),
        ))
    state = {
        'checks': [],
        'enabled': [],
        'fingerprint': 'config-a',
        'now': 1000.0,
        'allowed': list(_ALLOWED),
    }

    def fake_check_capability(capability, quiet=False, workspace=None):
        del quiet
        state['checks'].append(capability)
        global_user_state.set_enabled_clouds(list(state['enabled']), capability,
                                             workspace)
        return {}

    monkeypatch.setattr(sky_check, 'check_capability', fake_check_capability)
    monkeypatch.setattr(sky_check.skypilot_config, 'get_active_workspace',
                        lambda: _WORKSPACE)
    monkeypatch.setattr(sky_check, '_get_workspace_allowed_clouds',
                        lambda _: state['allowed'])
    monkeypatch.setattr(sky_check, '_caller_access_sets', lambda:
                        ({_WORKSPACE}, {_WORKSPACE}))
    monkeypatch.setattr(sky_check, 'current_config_fingerprint',
                        lambda: state['fingerprint'])
    monkeypatch.setattr(sky_check.time, 'time', lambda: state['now'])
    global_user_state.set_allowed_clouds(list(_ALLOWED), _WORKSPACE)
    return state


def _lookup(capability=CloudCapability.STORAGE):
    return sky_check.get_cached_enabled_clouds_or_refresh(capability)


def test_empty_result_is_reused_on_the_same_config(env):
    assert _lookup() == []
    assert _lookup() == []
    assert len(env['checks']) == 1


def test_config_change_reruns_the_check(env):
    _lookup()
    env['fingerprint'] = 'config-b'
    _lookup()
    assert len(env['checks']) == 2
    # The new config's result is the one now reused.
    _lookup()
    assert len(env['checks']) == 2


def test_empty_result_expires(env):
    _lookup()
    env['now'] += sky_check._EMPTY_STORAGE_CHECK_TTL_SECONDS - 1
    _lookup()
    assert len(env['checks']) == 1
    env['now'] += 1
    _lookup()
    assert len(env['checks']) == 2


def test_clock_moving_back_reruns_the_check(env):
    _lookup()
    env['now'] -= 1
    _lookup()
    assert len(env['checks']) == 2


def test_allowed_clouds_change_reruns_the_check(env):
    _lookup()
    env['allowed'] = ['Kubernetes']
    _lookup()
    assert len(env['checks']) == 2


def test_credentials_found_after_expiry_are_used(env):
    _lookup()
    env['enabled'] = ['AWS']
    assert _lookup() == []  # Reuses the empty result until it expires.
    env['now'] += sky_check._EMPTY_STORAGE_CHECK_TTL_SECONDS
    assert _lookup() == [sky_clouds.AWS()]


def test_sky_check_finding_a_cloud_takes_effect_at_once(env):
    _lookup()
    # What `sky check` stores, without going through the lookup.
    global_user_state.set_enabled_clouds(['AWS'], CloudCapability.STORAGE,
                                         _WORKSPACE)
    assert _lookup() == [sky_clouds.AWS()]
    assert len(env['checks']) == 1


def test_non_empty_result_is_not_recorded_as_empty(env):
    env['enabled'] = ['AWS']
    assert _lookup() == [sky_clouds.AWS()]
    assert global_user_state.get_empty_storage_check(_WORKSPACE) is None


def test_compute_check_is_not_reused(env):
    _lookup(CloudCapability.COMPUTE)
    _lookup(CloudCapability.COMPUTE)
    assert env['checks'] == [CloudCapability.COMPUTE] * 2
    assert global_user_state.get_empty_storage_check(_WORKSPACE) is None
