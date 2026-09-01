from __future__ import annotations

from types import SimpleNamespace

import pytest
from deerflow_extension_api import ReplicaSafety

from deerflow.deployment.topology import TopologyError, validate_extension_replica_safety


@pytest.mark.parametrize(
    "value",
    [
        "stateless_replica_safe",
        "shared_store_fenced",
        "singleton_leased",
        "single_replica_only",
        "unclassified",
    ],
)
def test_public_extension_api_pins_all_replica_safety_categories(value: str) -> None:
    assert ReplicaSafety(value).value == value


def test_multi_gateway_rejects_unclassified_and_single_replica_services() -> None:
    for classification in (ReplicaSafety.UNCLASSIFIED, ReplicaSafety.SINGLE_REPLICA_ONLY):
        loaded = SimpleNamespace(services=(("extension:install", SimpleNamespace(replica_safety=classification)),))
        with pytest.raises(TopologyError) as exc_info:
            validate_extension_replica_safety(loaded)
        assert exc_info.value.code == "topology_extension_not_replica_safe"


def test_fenced_and_leased_services_require_health_and_fence_evidence() -> None:
    for classification in (ReplicaSafety.SHARED_STORE_FENCED, ReplicaSafety.SINGLETON_LEASED):
        incomplete = SimpleNamespace(services=(("extension:install", SimpleNamespace(replica_safety=classification)),))
        with pytest.raises(TopologyError):
            validate_extension_replica_safety(incomplete)

        complete = SimpleNamespace(
            services=(
                (
                    "extension:install",
                    SimpleNamespace(
                        replica_safety=classification,
                        replica_safety_health_capability_id="audit.health",
                        replica_safety_fence_evidence_kind="audit.lease.v1",
                    ),
                ),
            )
        )
        validate_extension_replica_safety(complete)


def test_stateless_service_is_replica_safe_without_shared_authority() -> None:
    loaded = SimpleNamespace(services=(("extension:install", SimpleNamespace(replica_safety=ReplicaSafety.STATELESS_REPLICA_SAFE)),))
    validate_extension_replica_safety(loaded)
