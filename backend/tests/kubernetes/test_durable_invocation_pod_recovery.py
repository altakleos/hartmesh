"""Opt-in real-pod qualification for the durable one-replica profile."""

from __future__ import annotations

import os

import pytest
from support.kubernetes_qualification import (
    KubernetesAcceptedSkillQualificationConfigV2,
    KubernetesAcceptedSkillQualificationRunnerV2,
    KubernetesQualificationConfig,
    KubernetesQualificationEvidence,
    KubernetesQualificationRunner,
)

from deerflow.qualification_evidence import (
    ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2,
    KubernetesAcceptedSkillQualificationEvidenceV2,
)

pytestmark = pytest.mark.kubernetes_contract


def test_real_one_replica_pod_recovery_contract() -> None:
    """Qualify every commit-boundary fault against one exact image and chart."""

    if os.environ.get("DEERFLOW_TEST_KUBERNETES_SCOPE") == (ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2):
        config_v2 = KubernetesAcceptedSkillQualificationConfigV2.from_environment(
            os.environ,
        )
        evidence_v2 = KubernetesAcceptedSkillQualificationRunnerV2(
            config_v2,
        ).qualify()
        assert evidence_v2.gateway_image_digest == config_v2.image_digest
        assert evidence_v2.environment.namespace == config_v2.namespace
        assert evidence_v2.environment.gateway_node != (evidence_v2.environment.sandbox_node)
        assert evidence_v2.material.file_count > 0
        assert tuple(item.name for item in evidence_v2.scenarios) == (KubernetesAcceptedSkillQualificationEvidenceV2.REQUIRED_SCENARIOS)
        assert all(item.status == "passed" for item in evidence_v2.scenarios)
        return

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
