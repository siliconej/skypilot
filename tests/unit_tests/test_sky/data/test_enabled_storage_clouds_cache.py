"""Tests for reusing the enabled storage clouds within a request."""
from unittest import mock

import pytest

from sky import clouds as sky_clouds
from sky import exceptions
from sky.data import storage as storage_lib
from sky.utils import annotations


@pytest.fixture(name='lookups')
def _lookups_fixture(monkeypatch):
    """Counts the enabled-cloud lookups and controls their answers.

    Append to `answers` to change what later lookups return.
    """
    calls = []
    answers = [[]]

    def fake_lookup(capability, raise_if_no_cloud_access=False):
        del capability, raise_if_no_cloud_access
        calls.append(1)
        return list(answers[-1])

    monkeypatch.setattr(storage_lib.sky_check,
                        'get_cached_enabled_clouds_or_refresh', fake_lookup)
    for adaptor in (storage_lib.cloudflare, storage_lib.coreweave,
                    storage_lib.vastdata, storage_lib.huggingface):
        monkeypatch.setattr(adaptor, 'check_storage_credentials', lambda:
                            (False, None))
    monkeypatch.setattr(storage_lib.skypilot_config, 'get_active_workspace',
                        lambda: 'default')
    monkeypatch.setattr(storage_lib.sky_check, 'current_config_fingerprint',
                        lambda: 'config-a')
    return calls, answers


def test_no_storage_cloud_is_looked_up_once_per_request(lookups):
    calls, _ = lookups
    for _ in range(3):
        assert (storage_lib.get_cached_enabled_storage_cloud_names_or_refresh()
                == [])
    assert len(calls) == 1


def test_enabled_clouds_are_reused(lookups):
    calls, answers = lookups
    answers.append([sky_clouds.AWS()])
    for _ in range(2):
        assert (
            storage_lib.get_cached_enabled_storage_cloud_names_or_refresh() == [
                'AWS'
            ])
    assert len(calls) == 1


def test_storage_only_clouds_are_included(lookups, monkeypatch):
    calls, _ = lookups
    monkeypatch.setattr(storage_lib.cloudflare, 'check_storage_credentials',
                        lambda: (True, None))
    assert (storage_lib.get_cached_enabled_storage_cloud_names_or_refresh() == [
        storage_lib.cloudflare.NAME
    ])
    assert (storage_lib.get_cached_enabled_storage_cloud_names_or_refresh() == [
        storage_lib.cloudflare.NAME
    ])
    assert len(calls) == 1


def test_raises_from_the_reused_answer(lookups):
    calls, _ = lookups
    assert not storage_lib.get_cached_enabled_storage_cloud_names_or_refresh()
    with pytest.raises(exceptions.NoCloudAccessError):
        storage_lib.get_cached_enabled_storage_cloud_names_or_refresh(
            raise_if_no_cloud_access=True)
    assert len(calls) == 1


def test_callers_get_their_own_list(lookups):
    _, answers = lookups
    answers.append([sky_clouds.AWS()])
    first = storage_lib.get_cached_enabled_storage_cloud_names_or_refresh()
    first.append('mutated')
    assert (storage_lib.get_cached_enabled_storage_cloud_names_or_refresh() == [
        'AWS'
    ])


def test_a_new_request_looks_again(lookups):
    calls, answers = lookups
    assert not storage_lib.get_cached_enabled_storage_cloud_names_or_refresh()
    answers.append([sky_clouds.GCP()])
    annotations.clear_request_level_cache()
    assert (storage_lib.get_cached_enabled_storage_cloud_names_or_refresh() == [
        'GCP'
    ])
    assert len(calls) == 2


def test_workspaces_are_kept_apart(lookups, monkeypatch):
    calls, answers = lookups
    assert not storage_lib.get_cached_enabled_storage_cloud_names_or_refresh()
    answers.append([sky_clouds.AWS()])
    monkeypatch.setattr(storage_lib.skypilot_config, 'get_active_workspace',
                        lambda: 'team-b')
    assert (storage_lib.get_cached_enabled_storage_cloud_names_or_refresh() == [
        'AWS'
    ])
    assert len(calls) == 2


def test_configs_are_kept_apart(lookups, monkeypatch):
    calls, answers = lookups
    assert not storage_lib.get_cached_enabled_storage_cloud_names_or_refresh()
    answers.append([sky_clouds.AWS()])
    monkeypatch.setattr(storage_lib.sky_check, 'current_config_fingerprint',
                        lambda: 'config-b')
    assert (storage_lib.get_cached_enabled_storage_cloud_names_or_refresh() == [
        'AWS'
    ])
    assert len(calls) == 2


def test_failed_lookup_is_not_reused(lookups, monkeypatch):
    calls, _ = lookups

    def failing_lookup(capability, raise_if_no_cloud_access=False):
        del capability, raise_if_no_cloud_access
        calls.append(1)
        raise ValueError('workspace not readable')

    monkeypatch.setattr(storage_lib.sky_check,
                        'get_cached_enabled_clouds_or_refresh', failing_lookup)
    for _ in range(2):
        with pytest.raises(ValueError):
            storage_lib.get_cached_enabled_storage_cloud_names_or_refresh()
    assert len(calls) == 2


def test_sky_check_fix_is_seen_in_the_same_request(lookups, monkeypatch):
    """`_is_storage_cloud_enabled` re-checks a missing cloud, then looks again.

    The second look must see what the check found, not the earlier answer.
    """
    calls, answers = lookups

    def fake_check_capability(*args, **kwargs):
        del args, kwargs
        answers.append([sky_clouds.AWS()])

    check = mock.Mock(side_effect=fake_check_capability)
    monkeypatch.setattr(storage_lib.sky_check, 'check_capability', check)
    # pylint: disable-next=protected-access
    assert storage_lib._is_storage_cloud_enabled('AWS')
    assert check.call_count == 1
    assert len(calls) == 2


def test_still_disabled_after_sky_check(lookups, monkeypatch):
    calls, _ = lookups
    check = mock.Mock()
    monkeypatch.setattr(storage_lib.sky_check, 'check_capability', check)
    # pylint: disable-next=protected-access
    assert not storage_lib._is_storage_cloud_enabled('AWS')
    assert check.call_count == 1
    assert len(calls) == 2
