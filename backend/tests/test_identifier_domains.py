"""Cross-boundary compatibility tests for Hartmesh identifier domains."""

from __future__ import annotations

import pytest


def _constraints_request(thread_id: str):
    from deerflow_extension_api import (
        ConstraintProjectionRequestV2,
        EffectiveSubjectV1,
        InvocationIdentityV1,
        ResolvedAgentRevisionReferenceV1,
        ResolvedProfileRevisionReferenceV1,
        SealedOriginV1,
    )

    digest = "a" * 64
    return ConstraintProjectionRequestV2(
        identity=InvocationIdentityV1(
            effective_subject=EffectiveSubjectV1(
                kind="human",
                subject_id="user-1",
            )
        ),
        origin=SealedOriginV1(source_kind="http", digest=digest),
        policy_lookup_references=(),
        thread_id=thread_id,
        external_key_reference=None,
        agent_revision=ResolvedAgentRevisionReferenceV1(
            agent_id="agent",
            digest=digest,
        ),
        profile_revision=ResolvedProfileRevisionReferenceV1(
            profile_id="default",
            digest=digest,
        ),
        request_digest=digest,
        trusted_context_digest=digest,
        extension_manifest_digest=digest,
        extension_generation=1,
        host_max_total_subagents=1,
    )


@pytest.mark.parametrize(
    "agent_id",
    [
        "1bot",
        "A",
        "agent-9",
        "a" * 128,
    ],
)
def test_agent_ids_supported_by_host_are_supported_by_public_contracts(agent_id: str) -> None:
    from deerflow_extension_api import ResolvedAgentRevisionReferenceV1
    from deerflow_runtime_api import GraphInputV1, InvocationEnsureRequest, InvocationOptionsV1

    from deerflow.config.agents_config import AgentConfig, validate_agent_name

    canonical = agent_id.lower()
    assert validate_agent_name(agent_id) == canonical
    assert AgentConfig(name=agent_id).name == canonical
    assert ResolvedAgentRevisionReferenceV1(agent_id=agent_id, digest="a" * 64).agent_id == canonical
    request = InvocationEnsureRequest(
        external_key="request-1",
        thread_id="thread-1",
        agent_hint=agent_id,
        input=GraphInputV1(value={"messages": []}),
        options=InvocationOptionsV1(),
    )
    assert request.agent_hint == canonical


def test_reserved_lead_agent_remains_a_resolved_runtime_identity() -> None:
    from deerflow_extension_api import ResolvedAgentRevisionReferenceV1
    from deerflow_runtime_api import GraphInputV1, InvocationEnsureRequest, InvocationOptionsV1

    assert (
        ResolvedAgentRevisionReferenceV1(
            agent_id="lead_agent",
            digest="a" * 64,
        ).agent_id
        == "lead_agent"
    )
    assert (
        InvocationEnsureRequest(
            external_key="request-1",
            thread_id="thread-1",
            agent_hint="lead_agent",
            input=GraphInputV1(value={"messages": []}),
            options=InvocationOptionsV1(),
        ).agent_hint
        == "lead_agent"
    )


def test_legacy_channel_agent_alias_is_explicit_and_actionable(caplog) -> None:
    from app.channels.manager import _normalize_custom_agent_name

    with caplog.at_level("WARNING"):
        assert _normalize_custom_agent_name(" Mobile_Agent ") == "mobile-agent"

    assert "canonicalized to 'mobile-agent'" in caplog.text
    assert "update the channel session" in caplog.text


@pytest.mark.parametrize(
    "agent_id",
    [
        "",
        "-agent",
        "_agent",
        "agent_name",
        "agent.name",
        "agent/name",
        "a" * 129,
        "agént",
        "agent\nname",
    ],
)
def test_agent_id_rejection_is_identical_across_host_and_public_contracts(agent_id: str) -> None:
    from deerflow_extension_api import ResolvedAgentRevisionReferenceV1
    from deerflow_runtime_api import GraphInputV1, InvocationEnsureRequest, InvocationOptionsV1
    from pydantic import ValidationError

    from deerflow.config.agents_config import AgentConfig, validate_agent_name

    with pytest.raises(ValueError):
        validate_agent_name(agent_id)
    with pytest.raises(ValidationError):
        AgentConfig(name=agent_id)
    with pytest.raises(ValueError):
        ResolvedAgentRevisionReferenceV1(agent_id=agent_id, digest="a" * 64)
    with pytest.raises(ValueError):
        InvocationEnsureRequest(
            external_key="request-1",
            thread_id="thread-1",
            agent_hint=agent_id,
            input=GraphInputV1(value={"messages": []}),
            options=InvocationOptionsV1(),
        )


@pytest.mark.parametrize(
    "thread_id",
    ["a", "_thread", "-thread", "A_9-z", "t" * 64],
)
def test_canonical_thread_ids_cross_runtime_and_constraints_v2(thread_id: str) -> None:
    from deerflow_runtime_api import (
        ContextInvocationsQuery,
        GraphInputV1,
        InvocationEnsureRequest,
        InvocationOptionsV1,
    )

    from deerflow.utils.thread_id import validate_thread_id

    assert validate_thread_id(thread_id) == thread_id
    assert _constraints_request(thread_id).thread_id == thread_id
    assert ContextInvocationsQuery(thread_id=thread_id).thread_id == thread_id
    assert (
        InvocationEnsureRequest(
            external_key="request-1",
            thread_id=thread_id,
            agent_hint=None,
            input=GraphInputV1(value={"messages": []}),
            options=InvocationOptionsV1(),
        ).thread_id
        == thread_id
    )


@pytest.mark.parametrize(
    "thread_id",
    ["", "t" * 65, ".thread", "thread/name", "thréad", "thread\nname"],
)
def test_noncanonical_thread_ids_fail_at_every_new_invocation_boundary(
    thread_id: str,
) -> None:
    from deerflow_runtime_api import (
        ContextInvocationsQuery,
        GraphInputV1,
        InvocationEnsureRequest,
        InvocationOptionsV1,
    )

    from deerflow.utils.thread_id import validate_thread_id

    with pytest.raises(ValueError):
        validate_thread_id(thread_id)
    with pytest.raises(ValueError):
        _constraints_request(thread_id)
    with pytest.raises(ValueError):
        ContextInvocationsQuery(thread_id=thread_id)
    with pytest.raises(ValueError):
        InvocationEnsureRequest(
            external_key="request-1",
            thread_id=thread_id,
            agent_hint=None,
            input=GraphInputV1(value={"messages": []}),
            options=InvocationOptionsV1(),
        )


@pytest.mark.parametrize(
    "profile_id",
    ["default", "Profile V2", "provider:model/name", "模型", "p" * 128],
)
def test_model_profile_ids_cross_configuration_and_portable_contracts(
    profile_id: str,
) -> None:
    from deerflow_extension_api import ResolvedProfileRevisionReferenceV1
    from deerflow_runtime_api import InvocationOptionsV1

    from deerflow.config.agents_config import AgentConfig
    from deerflow.config.model_config import ModelConfig

    assert ModelConfig(name=profile_id, use="package:Provider", model="provider-model").name == profile_id
    assert AgentConfig(name="agent", model=profile_id).model == profile_id
    assert ResolvedProfileRevisionReferenceV1(profile_id=profile_id, digest="a" * 64).profile_id == profile_id
    assert InvocationOptionsV1(model_name=profile_id).model_name == profile_id


@pytest.mark.parametrize(
    "profile_id",
    ["", "p" * 129, "é" * 65, "profile\nname", "profile\x00name", "profile\x7fname"],
)
def test_invalid_model_profile_ids_fail_during_configuration_preflight(
    profile_id: str,
) -> None:
    from deerflow_extension_api import ResolvedProfileRevisionReferenceV1
    from deerflow_runtime_api import InvocationOptionsV1
    from pydantic import ValidationError

    from deerflow.config.agents_config import AgentConfig
    from deerflow.config.model_config import ModelConfig

    with pytest.raises(ValidationError, match="128 UTF-8 bytes|model profile identifier"):
        ModelConfig(name=profile_id, use="package:Provider", model="provider-model")
    with pytest.raises(ValidationError, match="128 UTF-8 bytes|model profile identifier"):
        AgentConfig(name="agent", model=profile_id)
    with pytest.raises(ValueError, match="128 UTF-8 bytes|model profile identifier"):
        ResolvedProfileRevisionReferenceV1(profile_id=profile_id, digest="a" * 64)
    with pytest.raises(ValueError, match="128 UTF-8 bytes|model profile identifier"):
        InvocationOptionsV1(model_name=profile_id)


def test_overlong_model_profile_preflight_has_actionable_migration_guidance() -> None:
    from pydantic import ValidationError

    from deerflow.config.app_config import AppConfig
    from deerflow.config.model_config import ModelConfig

    with pytest.raises(ValidationError) as caught:
        ModelConfig(name="p" * 129, use="package:Provider", model="provider-model")

    message = str(caught.value).casefold()
    assert "rename" in message
    assert "models[].name" in message
    assert "no truncation" in message

    with pytest.raises(ValidationError, match=r"models\[\]\.name"):
        AppConfig.model_validate(
            {
                "models": [
                    {
                        "name": "p" * 129,
                        "use": "package:Provider",
                        "model": "provider-model",
                    }
                ]
            }
        )


def _mcp_projection(*, server_name: str = "search", tool_name: str = "lookup"):
    from deerflow_extension_api import (
        McpCallProjectionV1,
        PrincipalProjectionV1,
        ResolvedAgentRevisionReferenceV1,
        SealedOriginV1,
    )

    digest = "a" * 64
    return McpCallProjectionV1(
        principal=PrincipalProjectionV1(user_id="user-1"),
        origin=SealedOriginV1(source_kind="http", digest=digest),
        thread_id="_thread",
        run_id="run-1",
        agent_revision=ResolvedAgentRevisionReferenceV1(agent_id="1bot", digest=digest),
        extension_generation=1,
        server_name=server_name,
        tool_name=tool_name,
        arguments_digest=digest,
    )


@pytest.mark.parametrize("server_name", ["search", "_search", "-search", "Search Cluster", "検索", "s" * 128])
def test_mcp_server_ids_cross_configuration_and_call_contract(server_name: str) -> None:
    from app.gateway.routers.mcp import McpConfigUpdateRequest
    from deerflow.config.extensions_config import ExtensionsConfig

    config = ExtensionsConfig.model_validate({"mcpServers": {server_name: {"tool_name_prefix": False}}})
    assert server_name in config.mcp_servers
    assert server_name in McpConfigUpdateRequest.model_validate({"mcp_servers": {server_name: {"tool_name_prefix": False}}}).mcp_servers
    assert _mcp_projection(server_name=server_name).server_name == server_name


@pytest.mark.parametrize("server_name", ["", "s" * 129, "é" * 65, "server\nname", "server\x00name"])
def test_invalid_mcp_server_ids_fail_during_config_preflight(server_name: str) -> None:
    from pydantic import ValidationError

    from app.gateway.routers.mcp import McpConfigUpdateRequest
    from deerflow.config.extensions_config import ExtensionsConfig

    with pytest.raises(ValidationError, match="MCP server identifier|128 UTF-8 bytes"):
        ExtensionsConfig.model_validate({"mcpServers": {server_name: {"tool_name_prefix": False}}})
    with pytest.raises(ValueError, match="MCP server identifier|128 UTF-8 bytes"):
        _mcp_projection(server_name=server_name)
    with pytest.raises(ValidationError, match="MCP server identifier|128 UTF-8 bytes"):
        McpConfigUpdateRequest.model_validate({"mcp_servers": {server_name: {"tool_name_prefix": False}}})


def test_non_tool_compatible_mcp_server_requires_prefix_opt_out() -> None:
    from pydantic import ValidationError

    from deerflow.config.extensions_config import ExtensionsConfig

    with pytest.raises(ValidationError, match="tool_name_prefix=false"):
        ExtensionsConfig.model_validate({"mcpServers": {"Search Cluster": {}}})

    with pytest.raises(ValidationError, match="tool_name_prefix=false"):
        ExtensionsConfig.model_validate({"mcpServers": {"s" * 127: {}}})


@pytest.mark.parametrize("tool_name", ["lookup", "_lookup", "-lookup", "Lookup_9", "t" * 128])
def test_mcp_tool_ids_match_the_host_load_boundary(tool_name: str) -> None:
    from deerflow.mcp.tools import is_valid_mcp_tool_name

    assert is_valid_mcp_tool_name(tool_name)
    assert _mcp_projection(tool_name=tool_name).tool_name == tool_name


@pytest.mark.parametrize("tool_name", ["", "t" * 129, "tool.name", "tool name", "tøøl", "tool\nname"])
def test_invalid_mcp_tool_ids_fail_at_host_and_public_boundaries(tool_name: str) -> None:
    from deerflow.mcp.tools import is_valid_mcp_tool_name

    assert not is_valid_mcp_tool_name(tool_name)
    with pytest.raises(ValueError, match="MCP tool identifier|1-128"):
        _mcp_projection(tool_name=tool_name)
