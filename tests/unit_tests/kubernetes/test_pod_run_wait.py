"""Tests for the wait between polls in _wait_for_pods_to_run."""
# pylint: disable=protected-access
import types
from unittest import mock

import pytest

from sky.provision.kubernetes import instance


def _pod(name, phase='Running', running=True, deleting=False):
    state = types.SimpleNamespace(running=object() if running else None)
    return types.SimpleNamespace(
        metadata=types.SimpleNamespace(
            name=name, deletion_timestamp=object() if deleting else None),
        status=types.SimpleNamespace(
            phase=phase,
            container_statuses=[types.SimpleNamespace(state=state)]))


def _event(pod, event_type='MODIFIED'):
    return {'type': event_type, 'object': pod}


class _Events:
    """Stands in for the Watch.stream generator; records how far it was read."""

    def __init__(self, events, error=None):
        self._events = iter(events)
        self._error = error
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._events)
        except StopIteration:
            if self._error is not None:
                raise self._error  # pylint: disable=raise-missing-from
            raise

    def close(self):
        self.closed = True


@pytest.fixture(name='sleeps')
def _sleeps(monkeypatch):
    """Records sleeps, with the clock frozen at the start of the wait."""
    recorded = []
    monkeypatch.setattr(instance.time, 'sleep', recorded.append)
    monkeypatch.setattr(instance.time, 'time', lambda: 100.0)
    return recorded


def _wait(monkeypatch, events, waiting):
    stream = mock.MagicMock(return_value=events)
    monkeypatch.setattr(instance.kubernetes, 'watch',
                        lambda context: mock.MagicMock(stream=stream))
    core_api = mock.MagicMock()
    monkeypatch.setattr(instance.kubernetes, 'core_api',
                        lambda context: core_api)
    pod_list = types.SimpleNamespace(metadata=types.SimpleNamespace(
        resource_version='42'))
    instance._wait_for_next_pod_run_poll('ctx', 'ns', 'a=b', pod_list, waiting)
    return stream, core_api


def test_ends_when_the_last_waiting_pod_is_running(monkeypatch, sleeps):
    events = _Events([_event(_pod('p1')), _event(_pod('p2'))])
    _wait(monkeypatch, events, {'p1', 'p2'})
    assert sleeps == []
    assert events.closed


def test_watches_from_the_polls_own_list(monkeypatch, sleeps):
    del sleeps
    stream, core_api = _wait(monkeypatch, _Events([]), {'p1'})
    args, kwargs = stream.call_args
    assert args == (core_api.list_namespaced_pod, 'ns')
    assert kwargs['label_selector'] == 'a=b'
    assert kwargs['resource_version'] == '42'
    assert kwargs['timeout_seconds'] == instance._POD_RUN_POLL_INTERVAL_SECONDS


def test_some_pods_still_waiting_does_not_end_it(monkeypatch, sleeps):
    events = _Events([_event(_pod('p1')), _event(_pod('p3'))])
    _wait(monkeypatch, events, {'p1', 'p2'})
    # The watch ran out, and the rest of the interval is slept off.
    assert sleeps == [instance._POD_RUN_POLL_INTERVAL_SECONDS]
    assert events.closed


@pytest.mark.parametrize(
    'pod',
    [
        _pod('p1', phase='Pending', running=False),
        # Running, but a container isn't, e.g. it's restarting.
        _pod('p1', phase='Running', running=False),
    ])
def test_a_pod_that_is_not_ready_does_not_end_it(monkeypatch, sleeps, pod):
    _wait(monkeypatch, _Events([_event(pod)]), {'p1'})
    assert sleeps == [instance._POD_RUN_POLL_INTERVAL_SECONDS]


@pytest.mark.parametrize('event', [
    _event(_pod('p2'), event_type='DELETED'),
    _event(_pod('p2', deleting=True)),
    _event(_pod('p2', phase='Failed', running=False)),
])
def test_a_pod_going_away_ends_it_for_the_poll_to_report(
        monkeypatch, sleeps, event):
    # p2 isn't even one of the pods being waited on: the poll decides.
    events = _Events([event])
    _wait(monkeypatch, events, {'p1'})
    assert sleeps == []
    assert events.closed


def test_a_watch_error_falls_back_to_the_rest_of_the_interval(
        monkeypatch, sleeps):
    events = _Events([], error=RuntimeError('410 Gone'))
    _wait(monkeypatch, events, {'p1'})
    assert sleeps == [instance._POD_RUN_POLL_INTERVAL_SECONDS]
    assert events.closed


def test_a_failure_to_start_the_watch_falls_back_to_sleeping(
        monkeypatch, sleeps):

    def _raise(context):
        raise RuntimeError('no kubeconfig')

    monkeypatch.setattr(instance.kubernetes, 'watch', _raise)
    pod_list = types.SimpleNamespace(metadata=types.SimpleNamespace(
        resource_version='42'))
    instance._wait_for_next_pod_run_poll('ctx', 'ns', 'a=b', pod_list, {'p1'})
    assert sleeps == [instance._POD_RUN_POLL_INTERVAL_SECONDS]


def test_a_pod_list_without_a_version_falls_back_to_sleeping(
        monkeypatch, sleeps):
    monkeypatch.setattr(instance.kubernetes, 'watch',
                        lambda context: mock.MagicMock())
    monkeypatch.setattr(instance.kubernetes, 'core_api',
                        lambda context: mock.MagicMock())
    pod_list = types.SimpleNamespace(items=[])
    instance._wait_for_next_pod_run_poll('ctx', 'ns', 'a=b', pod_list, {'p1'})
    assert sleeps == [instance._POD_RUN_POLL_INTERVAL_SECONDS]


def test_only_the_rest_of_the_interval_is_slept(monkeypatch):
    sleeps = []
    clock = iter([100.0, 100.75])
    monkeypatch.setattr(instance.time, 'sleep', sleeps.append)
    monkeypatch.setattr(instance.time, 'time', lambda: next(clock))
    _wait(monkeypatch, _Events([]), {'p1'})
    assert sleeps == [pytest.approx(0.25)]
