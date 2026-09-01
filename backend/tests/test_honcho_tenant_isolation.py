"""Tenant-isolation contract for the host projection and portable Honcho IDs."""

from __future__ import annotations

import re
from types import MappingProxyType, SimpleNamespace

import pytest

from deerflow.agents.memory.backends.honcho.config import (
    HONCHO_ID_MAX_LENGTH,
    HonchoConfig,
    HonchoIdentityResolver,
)
from deerflow.agents.memory.backends.honcho.honcho_manager import HonchoMemoryManager
from deerflow.agents.memory.honcho_tenant import (
    HARTMESH_TENANT_CONFIG_KEY,
    project_honcho_backend_config,
)
from deerflow.agents.memory.manager import get_memory_manager, reset_memory_manager
from deerflow.config.memory_config import (
    MemoryConfig,
    get_memory_config,
    load_memory_config_from_dict,
    set_memory_config,
)
from deerflow.runtime.tenant_identity import TenantIdentityV1


def _identity(tenant_id: str) -> TenantIdentityV1:
    return TenantIdentityV1.from_canonical_id(tenant_id)


def _projected_config(
    tenant_id: str,
    backend_config: dict[str, object] | None = None,
    *,
    profile: str = "durable_production",
) -> dict[str, object]:
    return project_honcho_backend_config(
        backend_config or {},
        tenant_identity=_identity(tenant_id),
        deployment_profile=profile,
    )


def test_host_projection_is_derived_from_server_tenant_without_raw_id() -> None:
    alpha = _projected_config("customer-alpha")
    beta = _projected_config("customer-beta")

    alpha_projection = alpha[HARTMESH_TENANT_CONFIG_KEY]
    beta_projection = beta[HARTMESH_TENANT_CONFIG_KEY]
    assert isinstance(alpha_projection, dict)
    assert isinstance(beta_projection, dict)
    assert alpha_projection["workspace_namespace"] != beta_projection["workspace_namespace"]
    assert alpha_projection["isolation_mode"] == "tenant_user"
    assert "customer-alpha" not in repr(alpha_projection)
    assert "customer-beta" not in repr(beta_projection)


def test_host_projection_replaces_a_forged_reserved_value(caplog: pytest.LogCaptureFixture) -> None:
    projected = _projected_config(
        "customer-alpha",
        {
            HARTMESH_TENANT_CONFIG_KEY: {
                "tenant_public_ref": "attacker-selected",
            }
        },
    )

    projection = projected[HARTMESH_TENANT_CONFIG_KEY]
    assert isinstance(projection, dict)
    assert projection["tenant_public_ref"] == _identity("customer-alpha").public_ref
    assert "attacker-selected" not in caplog.text
    assert "reserved Honcho tenant projection" in caplog.text


def test_runtime_config_surface_drops_reserved_projection(caplog: pytest.LogCaptureFixture) -> None:
    previous = get_memory_config()
    try:
        load_memory_config_from_dict(
            {
                "manager_class": "honcho",
                "backend_config": {
                    HARTMESH_TENANT_CONFIG_KEY: {"tenant_public_ref": "attacker-selected"},
                },
            }
        )
        assert HARTMESH_TENANT_CONFIG_KEY not in get_memory_config().backend_config
        assert "attacker-selected" not in caplog.text
        assert "reserved Honcho tenant projection" in caplog.text
    finally:
        set_memory_config(previous)


def test_typed_memory_config_drops_reserved_projection() -> None:
    config = MemoryConfig(
        manager_class="honcho",
        backend_config={
            HARTMESH_TENANT_CONFIG_KEY: {
                "tenant_public_ref": "attacker-selected",
            }
        },
    )

    assert HARTMESH_TENANT_CONFIG_KEY not in config.backend_config


def test_typed_memory_config_drops_reserved_projection_from_any_mapping() -> None:
    config = MemoryConfig(
        manager_class="honcho",
        backend_config=MappingProxyType(
            {
                HARTMESH_TENANT_CONFIG_KEY: {
                    "tenant_public_ref": "attacker-selected",
                }
            }
        ),
    )

    assert HARTMESH_TENANT_CONFIG_KEY not in config.backend_config


def test_matching_legacy_prefix_is_accepted_but_conflict_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    projected = _projected_config("customer-alpha")
    projection = projected[HARTMESH_TENANT_CONFIG_KEY]
    assert isinstance(projection, dict)
    namespace = projection["workspace_namespace"]

    matching = _projected_config(
        "customer-alpha",
        {"workspace_prefix": namespace},
    )
    assert matching["workspace_prefix"] == namespace
    assert "workspace_prefix is deprecated" in caplog.text

    with pytest.raises(ValueError, match="honcho_workspace_namespace_conflict"):
        _projected_config(
            "customer-alpha",
            {"workspace_prefix": "operator-selected-"},
        )


def test_production_workspace_override_must_equal_that_users_derived_workspace() -> None:
    base = _projected_config("customer-alpha")
    resolver = HonchoIdentityResolver(HonchoConfig.from_backend_config(base))
    expected = resolver.workspace("alice")
    assert expected is not None

    accepted = _projected_config(
        "customer-alpha",
        {"workspace_overrides": {"alice": expected}},
    )
    assert accepted["workspace_overrides"] == {"alice": expected}

    with pytest.raises(ValueError, match="honcho_shared_workspace_forbidden"):
        _projected_config(
            "customer-alpha",
            {"workspace_overrides": {"alice": "shared"}},
        )


def test_local_shared_workspace_requires_loud_opt_in_and_reports_mode(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with pytest.raises(ValueError, match="honcho_shared_workspace_forbidden"):
        _projected_config(
            "local",
            {"workspace_overrides": {"alice": "shared"}},
            profile="local_development",
        )

    projected = _projected_config(
        "local",
        {
            "workspace_overrides": {"alice": "shared", "bob": "shared"},
            "allow_local_shared_workspaces": True,
        },
        profile="local_development",
    )
    projection = projected[HARTMESH_TENANT_CONFIG_KEY]
    assert isinstance(projection, dict)
    assert projection["isolation_mode"] == "local_explicit_shared"
    assert "local explicit shared-workspace mode" in caplog.text


@pytest.mark.parametrize("configured", ["true", 1])
def test_local_shared_workspace_opt_in_requires_a_literal_boolean(
    configured: object,
) -> None:
    with pytest.raises(ValueError, match="honcho_shared_workspace_forbidden"):
        _projected_config(
            "local",
            {
                "workspace_overrides": {"alice": "shared"},
                "allow_local_shared_workspaces": configured,
            },
            profile="local_development",
        )


def test_production_rejects_case_variant_plain_http_with_api_key() -> None:
    with pytest.raises(ValueError, match="honcho_tenant_projection_invalid"):
        _projected_config(
            "customer-alpha",
            {
                "base_url": "HTTP://honcho.example",
                "api_key": "provider-secret",
                "allow_insecure_http": True,
            },
        )


def test_portable_projection_validation_rejects_forged_or_malformed_values() -> None:
    valid = _projected_config("customer-alpha")[HARTMESH_TENANT_CONFIG_KEY]
    assert isinstance(valid, dict)

    with pytest.raises(ValueError, match="honcho_tenant_projection_invalid"):
        HonchoConfig.from_backend_config(
            {
                HARTMESH_TENANT_CONFIG_KEY: {
                    **valid,
                    "tenant_digest": "0" * 64,
                }
            }
        )


@pytest.mark.parametrize("forged_version", [True, 1.0, "1"])
def test_portable_projection_version_is_a_strict_integer(
    forged_version: object,
) -> None:
    valid = _projected_config("customer-alpha")[HARTMESH_TENANT_CONFIG_KEY]
    assert isinstance(valid, dict)

    with pytest.raises(ValueError, match="honcho_tenant_projection_invalid"):
        HonchoConfig.from_backend_config(
            {
                HARTMESH_TENANT_CONFIG_KEY: {
                    **valid,
                    "version": forged_version,
                }
            }
        )
    with pytest.raises(ValueError, match="honcho_tenant_projection_invalid"):
        HonchoConfig.from_backend_config(
            {
                HARTMESH_TENANT_CONFIG_KEY: {
                    **valid,
                    "unexpected": True,
                }
            }
        )
    with pytest.raises(ValueError, match="honcho_tenant_projection_invalid"):
        HonchoConfig.from_backend_config(
            {
                HARTMESH_TENANT_CONFIG_KEY: {
                    **valid,
                    "workspace_namespace": "attacker-selected-",
                }
            }
        )


def test_two_tenants_resolve_disjoint_bounded_identity_scopes() -> None:
    raw_user = "same.user@example.com"
    raw_thread = "same.thread/1"
    alpha = HonchoIdentityResolver(HonchoConfig.from_backend_config(_projected_config("customer-alpha")))
    beta = HonchoIdentityResolver(HonchoConfig.from_backend_config(_projected_config("customer-beta")))

    alpha_ids = {
        alpha.workspace(raw_user),
        alpha.user_peer(raw_user),
        alpha.assistant_peer(),
        alpha.session(raw_thread),
    }
    beta_ids = {
        beta.workspace(raw_user),
        beta.user_peer(raw_user),
        beta.assistant_peer(),
        beta.session(raw_thread),
    }
    assert alpha_ids.isdisjoint(beta_ids)
    for value in alpha_ids | beta_ids:
        assert isinstance(value, str)
        assert 0 < len(value) <= HONCHO_ID_MAX_LENGTH
        assert re.fullmatch(r"[A-Za-z0-9_-]+", value)


def test_sanitized_user_and_thread_collisions_remain_distinct() -> None:
    resolver = HonchoIdentityResolver(HonchoConfig.from_backend_config(_projected_config("customer-alpha")))

    assert resolver.workspace("user.name@example.com") != resolver.workspace("user-name@example.com")
    assert resolver.user_peer("user.name@example.com") != resolver.user_peer("user-name@example.com")
    assert resolver.session("thread.1") != resolver.session("thread-1")


def test_peer_overrides_cannot_use_assistant_namespace_or_collide() -> None:
    projected = _projected_config("customer-alpha")
    config = HonchoConfig.from_backend_config(projected)
    resolver = HonchoIdentityResolver(config)
    prefix = f"hm-u-{config.tenant.tenant_digest[:12]}-"

    with pytest.raises(ValueError, match="honcho_identity_collision"):
        HonchoConfig.from_backend_config(
            {
                **projected,
                "user_peer_overrides": {
                    "alice": resolver.assistant_peer(),
                },
            }
        )
    with pytest.raises(ValueError, match="honcho_identity_collision"):
        HonchoConfig.from_backend_config(
            {
                **projected,
                "user_peer_overrides": {
                    "alice": f"{prefix}shared",
                    "bob": f"{prefix}shared",
                },
            }
        )


def test_local_shared_peer_override_cannot_collide_with_another_derived_user() -> None:
    shared = "explicit-local-shared"
    projected = _projected_config(
        "customer-alpha",
        {
            "allow_local_shared_workspaces": True,
            "workspace_overrides": {"alice": shared, "bob": shared},
        },
        profile="local_development",
    )
    resolver = HonchoIdentityResolver(HonchoConfig.from_backend_config(projected))

    with pytest.raises(ValueError, match="honcho_identity_collision"):
        HonchoConfig.from_backend_config(
            {
                **projected,
                "user_peer_overrides": {
                    "alice": resolver.user_peer("bob"),
                },
            }
        )


def test_safe_diagnostics_expose_only_tenant_pseudonyms() -> None:
    raw_user = "alice@example.com"
    resolver = HonchoIdentityResolver(HonchoConfig.from_backend_config(_projected_config("customer-alpha")))

    diagnostics = dict(resolver.safe_diagnostics())
    rendered = repr(diagnostics)
    assert diagnostics["isolation_mode"] == "tenant_user"
    assert diagnostics["tenant_public_ref"] == _identity("customer-alpha").public_ref
    assert "customer-alpha" not in rendered
    assert raw_user not in rendered


class _RecordingHonchoClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def get_or_create_peer(self, workspace: str, peer_id: str) -> None:
        self.calls.append(("peer", (workspace, peer_id)))

    def get_or_create_session(self, workspace: str, session_id: str) -> None:
        self.calls.append(("session", (workspace, session_id)))

    def set_session_peers(self, workspace: str, session_id: str, peer_ids: list[str]) -> None:
        self.calls.append(("session_peers", (workspace, session_id, tuple(peer_ids))))

    def add_messages(self, workspace: str, session_id: str, messages: list[dict[str, str]]) -> None:
        self.calls.append(("messages", (workspace, session_id, tuple((item["peer_id"], item["content"]) for item in messages))))

    def working_representation(self, workspace: str, peer_id: str, *, max_conclusions: int = 25) -> str:
        self.calls.append(("representation", (workspace, peer_id, max_conclusions)))
        return "bounded context"

    def search(self, workspace: str, query: str, *, limit: int = 5) -> list[dict[str, object]]:
        self.calls.append(("search", (workspace, query, limit)))
        return [{"content": "bounded hit"}]

    def close(self) -> None:
        return None


def test_manager_routes_every_operation_through_the_same_tenant_resolver() -> None:
    backend_config = _projected_config("customer-alpha")
    manager = HonchoMemoryManager.from_config(backend_config)
    client = _RecordingHonchoClient()
    manager._client = client
    resolver = HonchoIdentityResolver(manager._config)

    manager.add(
        "thread.1",
        [
            SimpleNamespace(type="human", content="hello"),
            SimpleNamespace(type="ai", content="hi"),
        ],
        user_id="alice@example.com",
    )
    assert manager.get_context("alice@example.com") == "bounded context"
    assert manager.search("query", user_id="alice@example.com")[0]["content"] == "bounded hit"
    assert manager.get_memory(user_id="alice@example.com")["user"]["workContext"]["summary"] == "bounded context"

    expected_workspace = resolver.workspace("alice@example.com")
    assert expected_workspace is not None
    workspace_calls = [args[0] for _name, args in client.calls]
    assert set(workspace_calls) == {expected_workspace}
    assert ("peer", (expected_workspace, resolver.user_peer("alice@example.com"))) in client.calls
    assert ("peer", (expected_workspace, resolver.assistant_peer())) in client.calls
    assert ("session", (expected_workspace, resolver.session("thread.1"))) in client.calls


def test_factory_injects_only_the_frozen_host_tenant_projection() -> None:
    previous = get_memory_config()
    reset_memory_manager()
    try:
        set_memory_config(
            MemoryConfig(
                manager_class="honcho",
                backend_config={
                    "base_url": "http://honcho.test",
                    HARTMESH_TENANT_CONFIG_KEY: {"tenant_public_ref": "attacker-selected"},
                },
            )
        )
        manager = get_memory_manager(
            tenant_identity=_identity("customer-alpha"),
            deployment_profile="durable_production",
        )
        assert isinstance(manager, HonchoMemoryManager)
        assert manager._config.tenant is not None
        assert manager._config.tenant.tenant_public_ref == _identity("customer-alpha").public_ref
        assert "attacker-selected" not in repr(manager._config.tenant)
    finally:
        reset_memory_manager()
        set_memory_config(previous)


def test_factory_rejects_durable_honcho_without_host_projection() -> None:
    previous = get_memory_config()
    reset_memory_manager()
    try:
        set_memory_config(
            MemoryConfig(
                manager_class="honcho",
                backend_config={"base_url": "http://honcho.test"},
            )
        )
        with pytest.raises(ValueError, match="honcho_tenant_projection_required"):
            get_memory_manager(deployment_profile="durable_production")
    finally:
        reset_memory_manager()
        set_memory_config(previous)


def test_factory_rejects_reusing_a_cached_unscoped_honcho_manager_for_gateway() -> None:
    previous = get_memory_config()
    reset_memory_manager()
    try:
        set_memory_config(
            MemoryConfig(
                manager_class="honcho",
                backend_config={"base_url": "http://honcho.test"},
            )
        )
        assert isinstance(get_memory_manager(), HonchoMemoryManager)

        with pytest.raises(ValueError, match="honcho_tenant_projection_required"):
            get_memory_manager(
                tenant_identity=_identity("customer-alpha"),
                deployment_profile="durable_production",
            )
    finally:
        reset_memory_manager()
        set_memory_config(previous)


def test_factory_rejects_cached_unscoped_honcho_in_durable_profile_without_identity() -> None:
    previous = get_memory_config()
    reset_memory_manager()
    try:
        set_memory_config(
            MemoryConfig(
                manager_class="honcho",
                backend_config={"base_url": "http://honcho.test"},
            )
        )
        assert isinstance(get_memory_manager(), HonchoMemoryManager)

        with pytest.raises(ValueError, match="honcho_tenant_projection_required"):
            get_memory_manager(deployment_profile="durable_production")
    finally:
        reset_memory_manager()
        set_memory_config(previous)


def test_factory_rejects_unknown_deployment_profile_for_honcho() -> None:
    previous = get_memory_config()
    reset_memory_manager()
    try:
        set_memory_config(
            MemoryConfig(
                manager_class="honcho",
                backend_config={"base_url": "http://honcho.test"},
            )
        )

        with pytest.raises(ValueError, match="unknown deployment profile"):
            get_memory_manager(deployment_profile="typo_profile")
    finally:
        reset_memory_manager()
        set_memory_config(previous)
