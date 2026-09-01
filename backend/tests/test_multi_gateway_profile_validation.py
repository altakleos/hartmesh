from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from deerflow.deployment.topology import (
    MULTI_GATEWAY_PROFILE,
    MULTI_GATEWAY_QUALIFICATION_SCOPE,
    TopologyError,
    validate_multi_gateway_config,
)


def _ns(**values):
    return SimpleNamespace(**values)


def _valid_config():
    providers = {
        name: _ns(enabled=False)
        for name in (
            "slack",
            "telegram",
            "discord",
            "feishu",
            "dingtalk",
            "wechat",
            "wecom",
            "buzz",
        )
    }
    return _ns(
        deployment=_ns(profile=MULTI_GATEWAY_PROFILE, tenant_id="tenant-a"),
        database=_ns(
            backend="postgres",
            command_timeout=30,
            postgres_schema="tenant_a",
            checkpoint_cache=_ns(type="redis"),
        ),
        checkpointer=_ns(type="postgres", postgres_schema="tenant_a"),
        run_events=_ns(backend="db"),
        agent_storage=_ns(backend="db"),
        stream_bridge=_ns(type="redis"),
        scheduler=_ns(enabled=True, multi_instance=True),
        mcp_tasks=_ns(enabled=True),
        run_ownership=_ns(heartbeat_enabled=True),
        dedupe_storage=_ns(backend="postgres"),
        sandbox=_ns(
            use="deerflow.community.aio_sandbox:AioSandboxProvider",
            image=f"registry.example/sandbox@sha256:{'a' * 64}",
            ownership=_ns(type="redis"),
            provisioner_url="http://deer-flow-provisioner:8002",
            provisioner_api_key=None,
            provisioner_service_account_token_file="/var/run/secrets/tokens/provisioner",
            accepted_skill_projection_profile="rwx_verified_copy_v2",
            accepted_materialization_profile="disabled",
        ),
        channel_connections=_ns(enabled=False, **providers),
        model_extra={"channels": {}},
        plugins=[],
    )


def _validate(config) -> None:
    validate_multi_gateway_config(
        config,
        qualification_scopes=frozenset(),
        webhook_route_enabled=False,
        qualification_candidate=True,
    )


def test_exact_profile_accepts_only_the_qualified_shared_surface() -> None:
    _validate(_valid_config())


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (lambda c: setattr(c.deployment, "profile", "durable_production"), "topology_profile_unsupported"),
        (lambda c: setattr(c.database, "backend", "sqlite"), "topology_dependency_not_shared"),
        (lambda c: setattr(c.database, "command_timeout", None), "topology_dependency_not_shared"),
        (lambda c: setattr(c.database.checkpoint_cache, "type", "memory"), "topology_dependency_not_shared"),
        (lambda c: setattr(c.checkpointer, "type", "memory"), "topology_dependency_not_shared"),
        (lambda c: setattr(c.run_events, "backend", "jsonl"), "topology_dependency_not_shared"),
        (lambda c: setattr(c.agent_storage, "backend", "file"), "topology_dependency_not_shared"),
        (lambda c: setattr(c.stream_bridge, "type", "memory"), "topology_dependency_not_shared"),
        (lambda c: setattr(c.scheduler, "enabled", False), "topology_dependency_not_shared"),
        (lambda c: setattr(c.scheduler, "multi_instance", False), "topology_dependency_not_shared"),
        (lambda c: setattr(c.mcp_tasks, "enabled", False), "topology_dependency_not_shared"),
        (lambda c: setattr(c.run_ownership, "heartbeat_enabled", False), "topology_dependency_not_shared"),
        (lambda c: setattr(c.dedupe_storage, "backend", "memory"), "topology_dependency_not_shared"),
        (lambda c: setattr(c.sandbox, "use", "deerflow.community.opensandbox:OpenSandboxProvider"), "topology_dependency_not_shared"),
        (lambda c: setattr(c.sandbox.ownership, "type", "memory"), "topology_dependency_not_shared"),
        (lambda c: setattr(c.sandbox, "accepted_skill_projection_profile", "disabled"), "topology_dependency_not_shared"),
        (lambda c: setattr(c.channel_connections.slack, "enabled", True), "topology_channel_not_replica_safe"),
        (lambda c: setattr(c.channel_connections, "enabled", True), "topology_channel_not_replica_safe"),
        (lambda c: c.model_extra.update({"channels": {"github": {"enabled": True}}}), "topology_channel_not_replica_safe"),
    ],
)
def test_profile_rejects_each_unsupported_surface(mutate, code: str) -> None:
    config = deepcopy(_valid_config())
    mutate(config)
    with pytest.raises(TopologyError) as exc_info:
        _validate(config)
    assert exc_info.value.code == code


def test_operator_asserted_scope_cannot_unlock_the_unqualified_profile() -> None:
    with pytest.raises(TopologyError) as exc_info:
        validate_multi_gateway_config(
            _valid_config(),
            qualification_scopes=frozenset({MULTI_GATEWAY_QUALIFICATION_SCOPE}),
            webhook_route_enabled=False,
        )
    assert exc_info.value.code == "topology_qualification_missing"


def test_profile_candidate_allows_only_the_live_harness_to_supply_missing_scope() -> None:
    validate_multi_gateway_config(
        _valid_config(),
        qualification_scopes=frozenset(),
        webhook_route_enabled=False,
        qualification_candidate=True,
    )


def test_profile_rejects_webhook_route_and_arbitrary_extensions() -> None:
    with pytest.raises(TopologyError) as exc_info:
        validate_multi_gateway_config(
            _valid_config(),
            qualification_scopes=frozenset({MULTI_GATEWAY_QUALIFICATION_SCOPE}),
            webhook_route_enabled=True,
            qualification_candidate=True,
        )
    assert exc_info.value.code == "topology_channel_not_replica_safe"

    config = _valid_config()
    config.plugins = [_ns(enabled=True, name="third-party", package="third-party", use="third_party:install")]
    with pytest.raises(TopologyError) as exc_info:
        _validate(config)
    assert exc_info.value.code == "topology_extension_not_replica_safe"


def test_profile_allows_only_the_first_party_governance_extension_as_nonempty() -> None:
    config = _valid_config()
    config.plugins = [
        _ns(
            enabled=True,
            name="governance",
            package="hartmesh-governance-extension",
            use="hartmesh_governance_extension:install",
        )
    ]
    _validate(config)
