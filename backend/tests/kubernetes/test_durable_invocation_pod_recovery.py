"""Opt-in real-pod qualification for the durable one-replica profile."""

from __future__ import annotations

import os

import pytest
from support.kubernetes_qualification import (
    KubernetesQualificationConfig,
    KubernetesQualificationEvidence,
    KubernetesQualificationRunner,
)

pytestmark = pytest.mark.kubernetes_contract


def test_real_one_replica_pod_recovery_contract() -> None:
    """Qualify every commit-boundary fault against one exact image and chart."""

    config = KubernetesQualificationConfig.from_environment(os.environ)
    evidence = KubernetesQualificationRunner(config).qualify()

    assert evidence.image_digest == config.image_digest
    assert evidence.namespace == config.namespace
    assert tuple(store.component for store in evidence.stores) == ("postgres", "redis")
    assert tuple(item.name for item in evidence.scenarios) == (KubernetesQualificationEvidence.REQUIRED_SCENARIOS)
    assert all(item.status == "passed" for item in evidence.scenarios)
    assert tuple(item.worker_attachments for item in evidence.scenarios) == (1, 0, 1, 1, 1, 1)
    assert evidence.scenarios[-2].termination_mode == "graceful"
    assert evidence.scenarios[-1].termination_mode == "forced_deadline"
