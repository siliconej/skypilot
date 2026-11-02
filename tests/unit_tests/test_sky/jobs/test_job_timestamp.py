"""Tests that job timestamps keep their precision over gRPC."""
from unittest import mock

import pytest

from sky import exceptions
from sky.jobs import utils as jobs_utils
from sky.schemas.generated import jobsv1_pb2
from sky.skylet import job_lib
from sky.skylet import services
from sky.utils import message_utils

# A current Unix time. As a float it would round to 1793603072.0, 42 s away.
_TIMESTAMP = 1793603114.151565
_ROUNDED_TIMESTAMP = 1793603072.0
_RESPONSES = [
    (False, jobsv1_pb2.GetJobSubmittedTimestampResponse),
    (True, jobsv1_pb2.GetJobEndedTimestampResponse),
]


def _service_response(impl, get_end_time):
    request_type = (jobsv1_pb2.GetJobEndedTimestampRequest if get_end_time else
                    jobsv1_pb2.GetJobSubmittedTimestampRequest)
    method = (impl.GetJobEndedTimestamp
              if get_end_time else impl.GetJobSubmittedTimestamp)
    return method(request_type(job_id=1), mock.MagicMock())


@pytest.mark.parametrize('get_end_time', [False, True])
def test_skylet_sets_both_fields(monkeypatch, get_end_time):
    monkeypatch.setattr(job_lib, 'get_job_submitted_or_ended_timestamp',
                        lambda job_id, ended: _TIMESTAMP)

    response = _service_response(services.JobsServiceImpl(), get_end_time)

    # The new field survives the wire exactly.
    parsed = type(response).FromString(response.SerializeToString())
    assert parsed.timestamp_double == _TIMESTAMP
    # Older clients still read the old field, with its old rounding.
    assert parsed.timestamp == _ROUNDED_TIMESTAMP


@pytest.fixture(name='cluster')
def _cluster(monkeypatch):
    """A gRPC-enabled cluster whose code path fallback returns _TIMESTAMP."""
    handle = mock.MagicMock()
    handle.is_grpc_enabled_with_flag = True
    monkeypatch.setattr(jobs_utils.global_user_state,
                        'get_handle_from_cluster_name', lambda name: handle)
    backend = mock.MagicMock()
    backend.run_on_head.return_value = (
        0, message_utils.encode_payload(_TIMESTAMP), '')
    return backend


@pytest.mark.parametrize('get_end_time,response_type', _RESPONSES)
def test_client_reads_exact_timestamp(monkeypatch, cluster, get_end_time,
                                      response_type):
    response = response_type(timestamp=_TIMESTAMP, timestamp_double=_TIMESTAMP)
    monkeypatch.setattr(jobs_utils.backend_utils, 'invoke_skylet_with_retries',
                        lambda fn: response)

    result = jobs_utils.get_job_timestamp(cluster,
                                          'cluster',
                                          job_id=1,
                                          get_end_time=get_end_time)

    assert result == _TIMESTAMP
    cluster.run_on_head.assert_not_called()


@pytest.mark.parametrize('get_end_time,response_type', _RESPONSES)
def test_client_ignores_rounded_timestamp_from_older_skylet(
        monkeypatch, cluster, get_end_time, response_type):
    # An older skylet doesn't know about timestamp_double.
    response = response_type(timestamp=_TIMESTAMP)
    assert not response.HasField('timestamp_double')
    monkeypatch.setattr(jobs_utils.backend_utils, 'invoke_skylet_with_retries',
                        lambda fn: response)

    result = jobs_utils.get_job_timestamp(cluster,
                                          'cluster',
                                          job_id=1,
                                          get_end_time=get_end_time)

    assert result == _TIMESTAMP
    cluster.run_on_head.assert_called_once()


@pytest.mark.parametrize('get_end_time', [False, True])
def test_client_falls_back_when_method_is_missing(monkeypatch, cluster,
                                                  get_end_time):

    def _raise(fn):
        raise exceptions.SkyletMethodNotImplementedError('not implemented')

    monkeypatch.setattr(jobs_utils.backend_utils, 'invoke_skylet_with_retries',
                        _raise)

    result = jobs_utils.get_job_timestamp(cluster,
                                          'cluster',
                                          job_id=1,
                                          get_end_time=get_end_time)

    assert result == _TIMESTAMP
    cluster.run_on_head.assert_called_once()
