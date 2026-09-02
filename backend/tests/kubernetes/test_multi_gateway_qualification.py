"""Opt-in live qualification for the exact two-Gateway topology."""

from __future__ import annotations

import os

import pytest
from support.multi_gateway_qualification import (
    KubernetesMultiGatewayQualificationConfigV1,
    KubernetesMultiGatewayQualificationRunnerV1,
)

from deerflow.deployment.topology import MULTI_GATEWAY_QUALIFICATION_SCOPE
from deerflow.multi_gateway_qualification import (
    MULTI_GATEWAY_QUALIFICATION_SCENARIOS,
)

pytestmark = pytest.mark.kubernetes_contract


def test_exact_two_gateway_kubernetes_qualification() -> None:
    """Run all 16 mandatory scenarios against real cluster dependencies."""

    config = KubernetesMultiGatewayQualificationConfigV1.from_environment(os.environ)
    evidence = KubernetesMultiGatewayQualificationRunnerV1(config).qualify()

    assert evidence.SCOPE == MULTI_GATEWAY_QUALIFICATION_SCOPE
    assert evidence.qualification_id == config.qualification_id
    assert evidence.namespace == config.namespace
    assert evidence.image_digests == config.image_digests
    assert len(evidence.topology_registrations) == 2
    assert tuple(item.scenario_id for item in evidence.scenarios) == (MULTI_GATEWAY_QUALIFICATION_SCENARIOS)
    assert all(item.status == "passed" for item in evidence.scenarios)
