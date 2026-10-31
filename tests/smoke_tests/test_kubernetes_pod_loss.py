"""Smoke tests for a pod deleted while `sky launch` sets up the cluster.

A pod deleted after it started (``kubectl delete pod``, an eviction) never
comes back: SkyPilot pods use ``restartPolicy: Never``. The launch used to
notice only after ~106 s -- the Ray wait ran its whole 90 s timeout, then
starting Ray on the workers retried the missing pod six times -- and then
failed with an error that did not say a pod was gone. It now fails within
about one Ray-wait chunk (10 s) plus a confirmation, with
InstancesLostDuringSetupError naming the pod.
"""
import pytest
from smoke_tests import smoke_tests_utils

# Generous next to the ~12 s it takes, so a slow CI cluster does not flake;
# well short of the ~106 s it took before.
_MAX_SECONDS_TO_FAIL = 45

_SCRIPT = 'tests/smoke_tests/scripts/delete_pod_mid_launch.sh'


@pytest.mark.kubernetes
@pytest.mark.parametrize('target', ['worker', 'head'])
def test_launch_fails_fast_when_pod_deleted(target: str):
    # One cluster per case: the cases may run in parallel.
    name = f'{smoke_tests_utils.get_cluster_name()}-{target[0]}'
    test = smoke_tests_utils.Test(
        f'launch_fails_fast_when_{target}_pod_deleted',
        [
            f'bash {_SCRIPT} {name} {target} {_MAX_SECONDS_TO_FAIL} '
            f'--infra kubernetes --num-nodes 2 '
            f'{smoke_tests_utils.LOW_RESOURCE_ARG} examples/minimal.yaml',
        ],
        teardown=f'sky down -y {name}',
        timeout=smoke_tests_utils.get_timeout('kubernetes'),
    )
    smoke_tests_utils.run_one_test(test)
