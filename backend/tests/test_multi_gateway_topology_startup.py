from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from deerflow.deployment.topology import (
    MULTI_GATEWAY_PROFILE,
    TopologyStartupFactsV1,
    build_topology_fingerprint,
)


def _environment() -> dict[str, str]:
    return {
        "DEER_FLOW_REPLICA_ID": "gateway-0",
        "DEER_FLOW_TOPOLOGY_IMAGE_DIGESTS": json.dumps(
            {
                "gateway": f"sha256:{'a' * 64}",
                "frontend": f"sha256:{'f' * 64}",
                "nginx": f"sha256:{'1' * 64}",
                "provisioner": f"sha256:{'b' * 64}",
                "sandbox": f"sha256:{'c' * 64}",
                "postgres": f"sha256:{'2' * 64}",
                "redis": f"sha256:{'3' * 64}",
            }
        ),
        "DEER_FLOW_TOPOLOGY_CONFIG_DIGEST": f"sha256:{'d' * 64}",
        "DEER_FLOW_TOPOLOGY_DATABASE_SCHEMA_REF": f"schema:sha256:{'e' * 64}",
    }


def test_startup_facts_parse_only_bounded_redacted_environment() -> None:
    facts = TopologyStartupFactsV1.from_environment(_environment())
    assert facts.replica_id == "gateway-0"
    assert facts.image_digests["gateway"].startswith("sha256:")
    assert "DATABASE" not in repr(facts.image_digests)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("DEER_FLOW_REPLICA_ID", "bad replica"),
        ("DEER_FLOW_TOPOLOGY_IMAGE_DIGESTS", "{}"),
        ("DEER_FLOW_TOPOLOGY_IMAGE_DIGESTS", "not-json"),
        ("DEER_FLOW_TOPOLOGY_CONFIG_DIGEST", "secret=value"),
        ("DEER_FLOW_TOPOLOGY_DATABASE_SCHEMA_REF", "public"),
    ],
)
def test_startup_facts_reject_missing_or_unbounded_values(key: str, value: str) -> None:
    environment = _environment()
    environment[key] = value
    with pytest.raises(ValueError):
        TopologyStartupFactsV1.from_environment(environment)

    environment = _environment()
    del environment[key]
    with pytest.raises(ValueError):
        TopologyStartupFactsV1.from_environment(environment)


def test_fingerprint_builder_binds_runtime_manifests_and_expected_head() -> None:
    manifest = SimpleNamespace(
        artifact_manifest_digest="f" * 64,
        extension_configuration_digest="0" * 64,
        digest="1" * 64,
    )
    config = SimpleNamespace(
        deployment=SimpleNamespace(profile=MULTI_GATEWAY_PROFILE),
        sandbox=SimpleNamespace(accepted_skill_projection_profile="rwx_verified_copy_v2"),
    )
    fingerprint = build_topology_fingerprint(
        facts=TopologyStartupFactsV1.from_environment(_environment()),
        tenant_digest="2" * 64,
        redis_namespace_digest="3" * 64,
        capability_manifest=manifest,
        config=config,
    )
    assert fingerprint.migration_head == "0027_multi_gateway_topology"
    assert fingerprint.extension_artifact_digest == f"sha256:{'f' * 64}"
    assert fingerprint.redis_namespace_digest == f"sha256:{'3' * 64}"
    assert fingerprint.capability_manifest_digest == "1" * 64
