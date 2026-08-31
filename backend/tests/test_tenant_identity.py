from __future__ import annotations

from dataclasses import FrozenInstanceError
from types import MappingProxyType

import pytest
from deerflow_extension_api import (
    EffectiveSubjectV1,
    InvocationIdentityV1,
    OriginContributionRequestV1,
    PrincipalProjectionV1,
    ResolvedAgentRevisionReferenceV1,
    RunContextContributionRequestV1,
    SealedOriginV1,
)

from deerflow.config.deployment_config import DeploymentConfig
from deerflow.runtime.tenant_identity import (
    LegacyRedisPrefixRecordV1,
    RedisTenantComponent,
    TenantIdentityError,
    TenantIdentityV1,
    TenantSubsystem,
    redis_component_key_prefix,
    redis_component_match_pattern,
    tenant_admission_scope,
)


def _resolve(
    *,
    profile: str = "local_development",
    tenant_id: str | None = None,
    environ: dict[str, str] | None = None,
) -> TenantIdentityV1:
    return TenantIdentityV1.resolve(
        deployment_config=DeploymentConfig(
            profile=profile,
            tenant_id=tenant_id,
        ),
        environ=MappingProxyType(environ or {}),
    )


def test_local_development_defaults_to_the_documented_local_identity() -> None:
    identity = _resolve()

    assert identity.version == 1
    assert identity.canonical_id == "local"
    assert identity.digest == "fd1e0d1ead4a5e206a1ada1acb0a795d78857d325ec031014cb9bb99dff2abb9"
    assert identity.public_ref == "tenant-fd1e0d1ead4a5e20"


def test_environment_tenant_takes_exact_precedence_over_yaml() -> None:
    identity = _resolve(
        tenant_id="yaml-tenant",
        environ={"DEER_FLOW_TENANT_ID": "environment-tenant"},
    )

    assert identity.canonical_id == "environment-tenant"


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "Acme",
        "-acme",
        "acme-",
        "acme_tenant",
        "a" * 64,
        "tenant.example",
    ],
)
def test_invalid_operator_identity_fails_without_sanitizing(value: str) -> None:
    with pytest.raises(TenantIdentityError) as error:
        _resolve(tenant_id=value)

    assert error.value.code == "tenant_identity_invalid"


def test_empty_environment_override_is_invalid_instead_of_falling_back() -> None:
    with pytest.raises(TenantIdentityError) as error:
        _resolve(
            tenant_id="yaml-tenant",
            environ={"DEER_FLOW_TENANT_ID": ""},
        )

    assert error.value.code == "tenant_identity_invalid"


@pytest.mark.parametrize("tenant_id", [None, "local"])
def test_durable_production_requires_an_explicit_non_local_identity(
    tenant_id: str | None,
) -> None:
    with pytest.raises(TenantIdentityError) as error:
        _resolve(profile="durable_production", tenant_id=tenant_id)

    assert error.value.code == "tenant_identity_required"


def test_namespace_projections_are_stable_and_grammar_specific() -> None:
    identity = _resolve(tenant_id="acme")

    redis = identity.namespace(TenantSubsystem.REDIS)
    opensandbox = identity.namespace(TenantSubsystem.OPENSANDBOX)
    mcp_tasks = identity.namespace(TenantSubsystem.MCP_TASKS)

    assert identity.digest == "e88e13d0bac8805e78a2524daf2df88924646f5d9baa041e23c884172d2a5328"
    assert identity.public_ref == "tenant-e88e13d0bac8805e"
    assert redis.key_prefix == "hm:v1:tenant-e88e13d0bac8805e:redis:"
    assert opensandbox.key_prefix == "hm-v1-e88e13d0bac8805e-opensandbox"
    assert mcp_tasks.key_prefix == "hm-v1-e88e13d0bac8805e-mcp-tasks"
    assert redis.metadata_ref == opensandbox.metadata_ref == identity.public_ref
    assert len({redis.digest, opensandbox.digest, mcp_tasks.digest}) == 3


def test_persisted_reference_never_contains_the_operator_readable_id() -> None:
    reference = _resolve(tenant_id="customer-readable-name").to_persisted_reference()

    assert reference.to_json() == {
        "version": 1,
        "public_ref": "tenant-d25d6d3e435cafee",
        "digest": "d25d6d3e435cafee9cbb0925350695cf31a9f2316658a580babf91f06bf1a6d9",
    }
    assert "customer-readable-name" not in str(reference.to_json())


def test_extension_contributor_requests_observe_only_an_immutable_safe_reference() -> None:
    reference = _resolve(tenant_id="customer-readable-name").to_persisted_reference()
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(
            kind="human",
            subject_id="user-1",
            role="member",
        )
    )
    origin_request = OriginContributionRequestV1(
        source_kind="http",
        identity=identity,
        tenant=reference,
    )
    context_request = RunContextContributionRequestV1(
        principal=PrincipalProjectionV1(identity=identity),
        origin=SealedOriginV1(source_kind="http"),
        thread_id="thread-1",
        agent_revision=ResolvedAgentRevisionReferenceV1(
            agent_id="lead_agent",
            digest="a" * 64,
        ),
        tenant=reference,
    )

    assert origin_request.tenant is reference
    assert context_request.tenant is reference
    assert "customer-readable-name" not in repr(origin_request)

    with pytest.raises(FrozenInstanceError):
        origin_request.tenant = None  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        context_request.tenant.public_ref = "tenant-0000000000000000"  # type: ignore[misc,union-attr]


def test_two_tenants_have_disjoint_subsystem_namespaces() -> None:
    tenant_a = _resolve(tenant_id="tenant-a")
    tenant_b = _resolve(tenant_id="tenant-b")

    for subsystem in TenantSubsystem:
        assert tenant_a.namespace(subsystem).key_prefix != tenant_b.namespace(subsystem).key_prefix
        assert tenant_a.namespace(subsystem).digest != tenant_b.namespace(subsystem).digest


def test_identical_base_admission_scopes_are_disjoint_across_tenants() -> None:
    base_scope = "http:v1:sha256:" + ("a" * 64)
    tenant_a = _resolve(tenant_id="tenant-a").to_persisted_reference()
    tenant_b = _resolve(tenant_id="tenant-b").to_persisted_reference()

    assert tenant_admission_scope(tenant_a, base_scope) != tenant_admission_scope(
        tenant_b,
        base_scope,
    )


def test_all_redis_component_prefixes_come_from_the_tenant_projection() -> None:
    namespace = _resolve(tenant_id="acme").namespace(TenantSubsystem.REDIS)

    assert redis_component_key_prefix(namespace, RedisTenantComponent.STREAM_BRIDGE) == ("hm:v1:tenant-e88e13d0bac8805e:redis")
    assert (
        redis_component_key_prefix(
            namespace,
            RedisTenantComponent.CHECKPOINT_CACHE,
        )
        == "hm:v1:tenant-e88e13d0bac8805e:redis:ckpt-hist:v1"
    )
    assert (
        redis_component_key_prefix(
            namespace,
            RedisTenantComponent.SANDBOX_OWNERSHIP,
        )
        == "hm:v1:tenant-e88e13d0bac8805e:redis:deerflow:sandbox:owner"
    )


def test_redis_component_prefixes_are_disjoint_across_tenants() -> None:
    tenant_a = _resolve(tenant_id="tenant-a").namespace(TenantSubsystem.REDIS)
    tenant_b = _resolve(tenant_id="tenant-b").namespace(TenantSubsystem.REDIS)

    for component in RedisTenantComponent:
        assert redis_component_key_prefix(
            tenant_a,
            component,
        ) != redis_component_key_prefix(tenant_b, component)


def test_matching_legacy_redis_prefix_is_accepted_during_compatibility_window() -> None:
    namespace = _resolve(tenant_id="acme").namespace(TenantSubsystem.REDIS)
    expected = "hm:v1:tenant-e88e13d0bac8805e:redis:ckpt-hist:v1"

    assert (
        redis_component_key_prefix(
            namespace,
            RedisTenantComponent.CHECKPOINT_CACHE,
            configured_prefix=expected,
            configured_field="database.checkpoint_cache.key_prefix",
        )
        == expected
    )


def test_exact_operator_recorded_legacy_redis_prefix_is_selected() -> None:
    namespace = _resolve(tenant_id="acme").namespace(TenantSubsystem.REDIS)
    legacy = LegacyRedisPrefixRecordV1(
        checkpoint_cache="legacy-acme:checkpoint",
    )

    assert (
        redis_component_key_prefix(
            namespace,
            RedisTenantComponent.CHECKPOINT_CACHE,
            configured_prefix="legacy-acme:checkpoint",
            configured_field="database.checkpoint_cache.key_prefix",
            legacy_record=legacy,
        )
        == "legacy-acme:checkpoint"
    )


def test_unrecorded_legacy_redis_prefix_still_fails_closed() -> None:
    namespace = _resolve(tenant_id="acme").namespace(TenantSubsystem.REDIS)
    legacy = LegacyRedisPrefixRecordV1(
        checkpoint_cache="recorded:checkpoint",
    )

    with pytest.raises(TenantIdentityError) as error:
        redis_component_key_prefix(
            namespace,
            RedisTenantComponent.CHECKPOINT_CACHE,
            configured_prefix="different:checkpoint",
            configured_field="database.checkpoint_cache.key_prefix",
            legacy_record=legacy,
        )

    assert error.value.code == "tenant_namespace_conflict"


def test_conflicting_legacy_redis_prefix_names_the_field() -> None:
    namespace = _resolve(tenant_id="acme").namespace(TenantSubsystem.REDIS)

    with pytest.raises(TenantIdentityError) as error:
        redis_component_key_prefix(
            namespace,
            RedisTenantComponent.STREAM_BRIDGE,
            configured_prefix="another-release",
            configured_field="DEER_FLOW_STREAM_BRIDGE_KEY_PREFIX",
        )

    assert error.value.code == "tenant_namespace_conflict"
    assert "DEER_FLOW_STREAM_BRIDGE_KEY_PREFIX" in str(error.value)


def test_redis_inventory_patterns_cover_only_namespaced_key_families() -> None:
    namespace = _resolve(tenant_id="acme").namespace(TenantSubsystem.REDIS)

    assert redis_component_match_pattern(
        namespace,
        RedisTenantComponent.STREAM_BRIDGE,
    ) == ("hm:v1:tenant-e88e13d0bac8805e:redis:deerflow:stream_bridge:*")
    assert redis_component_match_pattern(
        namespace,
        RedisTenantComponent.SANDBOX_OWNERSHIP,
    ).startswith(namespace.key_prefix)


def test_e2b_capacity_ledger_reuses_central_sandbox_ownership_projection() -> None:
    from deerflow.community.e2b_sandbox.capacity import make_e2b_capacity_store
    from deerflow.config.sandbox_config import SandboxOwnershipConfig

    namespace = _resolve(tenant_id="tenant-a").namespace(TenantSubsystem.REDIS)
    expected = redis_component_key_prefix(
        namespace,
        RedisTenantComponent.SANDBOX_OWNERSHIP,
    )
    store = make_e2b_capacity_store(
        SandboxOwnershipConfig(
            type="redis",
            redis_url="redis://127.0.0.1:1/0",
        ),
        hard_limit=1,
        tenant_namespace=namespace,
    )

    assert store is not None
    try:
        assert store.key == f"{expected}:e2b-capacity"
    finally:
        store.close()
