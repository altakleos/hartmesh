from __future__ import annotations

import inspect
import json
from types import SimpleNamespace

import pytest

from app.gateway import app as gateway_app
from app.gateway import deps as gateway_deps
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
        mcp_task_replay_keyring_confirmation_version=1,
        mcp_task_replay_keyring_confirmation_digest=f"sha256:{'4' * 64}",
        execution_policy_keyring_confirmation_version=1,
        execution_policy_keyring_confirmation_digest=f"sha256:{'5' * 64}",
    )
    assert fingerprint.migration_head == "0036_execution_policy_state"
    assert fingerprint.extension_artifact_digest == f"sha256:{'f' * 64}"
    assert fingerprint.redis_namespace_digest == f"sha256:{'3' * 64}"
    assert fingerprint.capability_manifest_digest == "1" * 64
    assert fingerprint.mcp_task_replay_keyring_confirmation_version == 1
    assert fingerprint.mcp_task_replay_keyring_confirmation_digest == f"sha256:{'4' * 64}"
    assert fingerprint.execution_policy_keyring_confirmation_version == 1
    assert fingerprint.execution_policy_keyring_confirmation_digest == f"sha256:{'5' * 64}"


def test_exact_two_run_store_clock_gate_precedes_worker_activity() -> None:
    source = inspect.getsource(gateway_deps.langgraph_runtime)

    initialized = source.index(
        "await app.state.run_store.initialize_lifecycle()",
    )
    validated = source.index(
        "validate_multi_gateway_run_store(app.state.run_store)",
    )
    heartbeat_started = source.index(
        "await app.state.run_manager.start_heartbeat()",
    )
    recovery_started = source.index(
        "await app.state.run_manager.reconcile_orphaned_inflight_runs(",
    )

    assert initialized < validated < heartbeat_started < recovery_started


def test_exact_two_readiness_checks_live_run_store_clock_authority() -> None:
    source = inspect.getsource(gateway_app.create_app)

    assert "multi_gateway_run_store_ready" in source
    assert 'getattr(app.state, "run_store", None)' in source
