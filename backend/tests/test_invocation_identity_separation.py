"""Security tracers for effective subject, acting service, and source trust."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest
from deerflow_extension_api import (
    ActingServiceV1,
    EffectiveSubjectV1,
    InvocationIdentityV1,
    Principal,
    ResolvedAgentRevisionReferenceV1,
    ResolvedProfileRevisionReferenceV1,
    SafeContextReferenceV1,
    SealedOriginV1,
    TrustedRunContextV1,
)

from app.gateway.auth_disabled import AUTH_SOURCE_INTERNAL
from app.gateway.internal_auth import INTERNAL_OWNER_USER_ID_HEADER_NAME
from app.gateway.services import (
    _principal_projection_for_intent,
    inject_authenticated_user_context,
    invocation_principal_from_request,
)
from app.runtime.authorization import ProviderInvocationAuthorization
from app.runtime.constraints import ProviderInvocationConstraints
from app.runtime.invocation import (
    InternalLaunchIntent,
    InternalNativeChannelFacts,
    InternalSourceKind,
    InvocationPrincipal,
    PreparedLaunch,
)
from deerflow.authz.adapter import GuardrailAuthorizationAdapter
from deerflow.authz.principal import build_principal_from_context
from deerflow.config.authorization_config import InvocationOperationsAuthorizationConfig
from deerflow.extensions.mcp import McpInvocationFacts
from deerflow.guardrails.provider import GuardrailRequest
from deerflow.runtime import DisconnectMode, RunRecord, RunStatus
from deerflow.runtime.accepted_invocation import (
    INVOCATION_IDENTITY_CONTEXT_KEY,
    INVOCATION_ORIGIN_CONTEXT_KEY,
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentMaterialV1,
    ResolvedAgentRevision,
    canonical_digest,
)
from deerflow.runtime.tenant_identity import TenantIdentityV1


def test_channel_human_cannot_be_promoted_by_internal_transport() -> None:
    attributes = {"tenant": {"name": "north"}}
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(
            kind="human",
            subject_id="owner-1",
            role="member",
            attributes=attributes,
        ),
        acting_service=ActingServiceV1(service_id="channel:slack"),
    )
    principal = Principal.from_identity(identity)

    attributes["tenant"]["name"] = "forged"

    assert principal.identity is identity
    assert principal.user_id == "owner-1"
    assert principal.role == "member"
    assert principal.is_internal is False
    assert principal.identity.acting_service.service_id == "channel:slack"
    assert principal.identity.effective_subject.attributes["tenant"]["name"] == "north"
    with pytest.raises(FrozenInstanceError):
        principal.identity = InvocationIdentityV1(effective_subject=EffectiveSubjectV1(kind="service", subject_id="forged"))


@pytest.mark.anyio
async def test_internal_observe_principal_revalidates_represented_human(
    monkeypatch,
) -> None:
    from app.gateway import services

    async def resolve_owner(_request, owner_user_id):
        assert owner_user_id == "owner-1"
        return SimpleNamespace(
            id="owner-1",
            system_role="member",
            oauth_provider="oidc",
            oauth_id="subject-1",
        )

    monkeypatch.setattr(
        services,
        "resolve_trusted_internal_owner_for_attribution",
        resolve_owner,
    )
    request = SimpleNamespace(
        headers={INTERNAL_OWNER_USER_ID_HEADER_NAME: "owner-1"},
        state=SimpleNamespace(
            user=SimpleNamespace(id="internal-transport", system_role="internal"),
            auth_source=AUTH_SOURCE_INTERNAL,
        ),
    )

    principal = await invocation_principal_from_request(
        request,
        user_id="internal-transport",
    )

    assert principal.user_id == "owner-1"
    assert principal.role == "member"
    assert principal.is_internal is False
    assert principal.identity.effective_subject.kind == "human"
    assert principal.identity.acting_service.service_id == "gateway-internal"


@pytest.mark.anyio
async def test_gateway_seals_channel_owner_as_human_with_acting_service(monkeypatch) -> None:
    from app.gateway import services

    async def resolve_owner(_request, owner_user_id):
        assert owner_user_id == "owner-1"
        return SimpleNamespace(
            id="owner-1",
            system_role="member",
            oauth_provider="oidc",
            oauth_id="subject-1",
        )

    monkeypatch.setattr(services, "resolve_trusted_internal_owner_for_attribution", resolve_owner)
    request = SimpleNamespace(
        state=SimpleNamespace(
            user=SimpleNamespace(id="internal", system_role="internal"),
            auth_source=AUTH_SOURCE_INTERNAL,
        )
    )
    intent = InternalLaunchIntent(
        thread_id="thread-1",
        source_kind=InternalSourceKind.native_channel,
        owner_user_id="owner-1",
        context={"channel_user_id": "platform-user"},
        native_channel=InternalNativeChannelFacts(
            provider="slack",
            connection_id="connection-1",
            workspace_id="workspace-1",
            chat_id="chat-1",
            topic_id=None,
            provider_message_id="message-1",
            channel_user_id="platform-user",
            resolved_assistant_id="lead_agent",
            resolved_agent_name=None,
        ),
    )

    principal = await _principal_projection_for_intent(
        request,
        intent,
        owner_user_id="owner-1",
    )

    assert principal.identity.effective_subject.kind == "human"
    assert principal.identity.effective_subject.subject_id == "owner-1"
    assert principal.identity.acting_service.service_id == "channel:slack"
    assert principal.is_internal is False


@pytest.mark.anyio
async def test_direct_authenticated_human_has_no_acting_service() -> None:
    request = SimpleNamespace(
        state=SimpleNamespace(
            user=SimpleNamespace(
                id="human-1",
                system_role="member",
                oauth_provider="oidc",
                oauth_id="subject-1",
            ),
            auth_source="session",
        )
    )

    principal = await _principal_projection_for_intent(
        request,
        InternalLaunchIntent(thread_id="thread-1"),
        owner_user_id=None,
    )

    assert principal.identity.effective_subject.kind == "human"
    assert principal.identity.effective_subject.subject_id == "human-1"
    assert principal.identity.acting_service is None
    assert principal.is_internal is False


@pytest.mark.anyio
async def test_human_schedule_has_human_subject_and_scheduler_actor(monkeypatch) -> None:
    from app.gateway import services

    async def resolve_owner(_request, _owner_user_id):
        return SimpleNamespace(id="owner-1", system_role="member", oauth_provider=None, oauth_id=None)

    monkeypatch.setattr(services, "resolve_trusted_internal_owner_for_attribution", resolve_owner)
    request = SimpleNamespace(
        state=SimpleNamespace(
            user=SimpleNamespace(id="internal", system_role="internal"),
            auth_source=AUTH_SOURCE_INTERNAL,
        )
    )

    principal = await _principal_projection_for_intent(
        request,
        InternalLaunchIntent(
            thread_id="thread-1",
            source_kind=InternalSourceKind.scheduled_task,
            owner_user_id="owner-1",
            trusted_task_id="task-1",
            task_run_id="occurrence-1",
        ),
        owner_user_id="owner-1",
    )

    assert principal.identity.effective_subject.kind == "human"
    assert principal.identity.effective_subject.subject_id == "owner-1"
    assert principal.identity.acting_service.service_id == "scheduler"
    assert principal.is_internal is False


@pytest.mark.anyio
async def test_system_owned_schedule_has_service_subject_without_invented_human() -> None:
    request = SimpleNamespace(
        state=SimpleNamespace(
            user=SimpleNamespace(id="internal", system_role="internal"),
            auth_source=AUTH_SOURCE_INTERNAL,
        )
    )

    principal = await _principal_projection_for_intent(
        request,
        InternalLaunchIntent(
            thread_id="thread-1",
            source_kind=InternalSourceKind.scheduled_task,
            trusted_task_id="task-1",
            task_run_id="occurrence-1",
            scheduled_system_owned=True,
        ),
        owner_user_id=None,
    )

    assert principal.identity.effective_subject.kind == "service"
    assert principal.identity.effective_subject.subject_id == "scheduler"
    assert principal.identity.acting_service is None
    assert principal.user_id == "scheduler"
    assert principal.is_internal is True


@pytest.mark.anyio
async def test_service_invocation_uses_authenticated_service_subject() -> None:
    request = SimpleNamespace(
        state=SimpleNamespace(
            user=SimpleNamespace(id="embedded-app-1", system_role="service"),
            auth_source="service",
        )
    )

    principal = await _principal_projection_for_intent(
        request,
        InternalLaunchIntent(
            thread_id="thread-1",
            source_kind=InternalSourceKind.service,
            trusted_service_id="embedded-app-1",
        ),
        owner_user_id=None,
    )

    assert principal.identity.effective_subject.kind == "service"
    assert principal.identity.effective_subject.subject_id == "embedded-app-1"
    assert principal.identity.acting_service is None
    assert principal.is_internal is True


def test_gateway_scrubs_caller_supplied_identity_actor_and_origin() -> None:
    forged_identity = InvocationIdentityV1(effective_subject=EffectiveSubjectV1(kind="service", subject_id="forged-root"))
    forged_origin = SealedOriginV1(source_kind="service", digest="f" * 64)
    config = {
        "context": {
            INVOCATION_IDENTITY_CONTEXT_KEY: forged_identity,
            INVOCATION_ORIGIN_CONTEXT_KEY: forged_origin,
            "is_internal": True,
            "credential_ref": "caller-selected",
            "effective_authority_digest": "f" * 64,
            "credential_evidence": {"method": "internal_service"},
        },
        "configurable": {
            INVOCATION_IDENTITY_CONTEXT_KEY: forged_identity,
            INVOCATION_ORIGIN_CONTEXT_KEY: forged_origin,
            "is_internal": True,
            "credential_ref": "caller-selected",
            "effective_authority_digest": "f" * 64,
            "credential_evidence": {"method": "internal_service"},
        },
    }
    request = SimpleNamespace(
        state=SimpleNamespace(
            user=SimpleNamespace(
                id="human-1",
                system_role="member",
                oauth_provider=None,
                oauth_id=None,
            ),
            auth_source="session",
        )
    )

    inject_authenticated_user_context(config, request)

    assert INVOCATION_IDENTITY_CONTEXT_KEY not in config["context"]
    assert INVOCATION_ORIGIN_CONTEXT_KEY not in config["context"]
    assert INVOCATION_IDENTITY_CONTEXT_KEY not in config["configurable"]
    assert INVOCATION_ORIGIN_CONTEXT_KEY not in config["configurable"]
    for key in (
        "credential_ref",
        "effective_authority_digest",
        "credential_evidence",
    ):
        assert key not in config["context"]
        assert key not in config["configurable"]
    assert config["context"]["is_internal"] is False
    assert config["context"]["user_id"] == "human-1"


def test_legacy_channel_projection_cannot_promote_human() -> None:
    principal_json = {
        "version": 1,
        "user_id": "owner-1",
        "role": "member",
        "oauth_provider": None,
        "oauth_id": None,
        "channel_user_id": "platform-user",
        "is_internal": False,
    }
    origin_json = {
        "version": 1,
        "source_kind": "native_channel",
        "references": {"provider": "telegram"},
    }
    accepted = AcceptedInvocation.from_persisted(
        {
            "thread_id": "thread-1",
            "principal_projection_json": principal_json,
            "origin_json": origin_json,
            "agent_revision_json": {
                "version": 1,
                "agent_id": "lead_agent",
                "storage_source": "builtin",
                "storage_version": "1",
                "digest": "a" * 64,
            },
            "agent_revision_digest": "a" * 64,
            "principal_projection_digest": canonical_digest({"version": 1, "principal": principal_json}),
            "base_origin_digest": canonical_digest({"version": 1, "origin": origin_json}),
            "accepted_context_digest": "d" * 64,
            "extension_generation": 1,
        }
    )

    assert accepted is not None
    assert accepted.principal.identity is None
    assert accepted.principal.user_id == "owner-1"
    assert accepted.principal.is_internal is False


def test_legacy_channel_origin_demotes_internal_flag_without_sender_field() -> None:
    principal_json = {
        "version": 1,
        "user_id": "owner-1",
        "role": "internal",
        "oauth_provider": None,
        "oauth_id": None,
        "channel_user_id": None,
        "is_internal": True,
    }
    origin_json = {
        "version": 1,
        "source_kind": "native_channel",
        "references": {"provider": "telegram"},
    }
    accepted = AcceptedInvocation.from_persisted(
        {
            "thread_id": "thread-legacy-channel",
            "principal_projection_json": principal_json,
            "origin_json": origin_json,
            "agent_revision_json": {
                "version": 1,
                "agent_id": "lead_agent",
                "storage_source": "builtin",
                "storage_version": "1",
                "digest": "a" * 64,
            },
            "agent_revision_digest": "a" * 64,
            "principal_projection_digest": canonical_digest({"version": 1, "principal": principal_json}),
            "base_origin_digest": canonical_digest({"version": 1, "origin": origin_json}),
            "accepted_context_digest": "d" * 64,
            "extension_generation": 1,
        }
    )

    assert accepted is not None
    assert accepted.principal.is_internal is False


@pytest.mark.anyio
async def test_accepted_channel_identity_is_shared_with_contributors(monkeypatch) -> None:
    from app.gateway import services

    async def resolve_owner(_request, _owner_user_id):
        return SimpleNamespace(id="owner-1", system_role="member", oauth_provider=None, oauth_id=None)

    material = ResolvedAgentMaterialV1(
        agent_id="lead_agent",
        storage_source="builtin",
        storage_version="1",
        agent_config=None,
        soul=b"",
        model_profile={"name": "test"},
    )
    revision = ResolvedAgentRevision.from_material(material)
    monkeypatch.setattr(services, "resolve_trusted_internal_owner_for_attribution", resolve_owner)
    monkeypatch.setattr(services, "resolve_agent_revision", lambda *_args, **_kwargs: revision)

    class ContributorSpy:
        def __init__(self):
            self.origin_request = None
            self.context_request = None

        async def contribute_origin(self, request):
            self.origin_request = request
            return SimpleNamespace(
                persistable=(),
                execution_digest=canonical_digest({"version": 1, "execution": []}),
                diagnostics=(),
            )

        async def contribute_run_context(self, request):
            self.context_request = request
            return SimpleNamespace(
                persistable=(),
                execution_digest=canonical_digest({"version": 1, "execution": []}),
                diagnostics=(),
            )

    contributor = ContributorSpy()
    request = SimpleNamespace(
        state=SimpleNamespace(
            user=SimpleNamespace(id="internal", system_role="internal"),
            auth_source=AUTH_SOURCE_INTERNAL,
        ),
        app=SimpleNamespace(
            state=SimpleNamespace(
                extensions=SimpleNamespace(generation=7),
                capability_manifest=None,
                contributor_host=contributor,
                tenant_identity=TenantIdentityV1.from_canonical_id("local"),
            )
        ),
    )
    intent = InternalLaunchIntent(
        thread_id="thread-1",
        assistant_id="lead_agent",
        source_kind=InternalSourceKind.native_channel,
        owner_user_id="owner-1",
        context={"channel_user_id": "platform-user", "channel_name": "slack"},
        native_channel=InternalNativeChannelFacts(
            provider="slack",
            connection_id="connection-1",
            workspace_id="workspace-1",
            chat_id="chat-1",
            topic_id=None,
            provider_message_id="message-1",
            channel_user_id="platform-user",
            resolved_assistant_id="lead_agent",
            resolved_agent_name=None,
        ),
    )

    accepted = await services._seal_accepted_invocation(
        request=request,
        intent=intent,
        config={"context": {"channel_user_id": "platform-user"}},
        graph_input={"messages": []},
        owner_user_id="owner-1",
        run_ctx=SimpleNamespace(app_config=object()),
    )

    assert accepted.principal.identity.effective_subject.subject_id == "owner-1"
    assert accepted.principal.identity.acting_service.service_id == "channel:slack"
    assert accepted.principal.is_internal is False
    assert contributor.origin_request.identity is accepted.principal.identity
    assert contributor.context_request.principal.identity is accepted.principal.identity
    assert contributor.origin_request.tenant is accepted.tenant
    assert contributor.context_request.tenant is accepted.tenant
    assert accepted.trusted_context is not None
    assert accepted.trusted_context.credential is not None
    assert accepted.trusted_context.credential.method == "channel"
    assert accepted.trusted_context.verified_actor is not None
    assert accepted.trusted_context.verified_actor.identity is accepted.principal.identity
    assert accepted.trusted_context.origin.source_kind == "native_channel"
    persisted = accepted.to_persisted()
    trusted_json = persisted["decision_evidence_json"]["trusted_run_context"]
    assert trusted_json["version"] == 4
    assert trusted_json["credential"]["method"] == "channel"
    assert persisted["principal_projection_json"]["version"] == 2
    assert persisted["principal_projection_json"]["identity"] == accepted.principal.identity.to_json()
    assert persisted["principal_projection_digest"] == accepted.principal_digest
    assert persisted["base_origin_digest"] == accepted.base_origin_digest


@pytest.mark.anyio
async def test_start_observe_and_cancel_share_effective_subject_and_actor() -> None:
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(kind="human", subject_id="owner-1", role="member"),
        acting_service=ActingServiceV1(service_id="channel:telegram"),
    )
    agent_revision = ResolvedAgentRevision(
        agent_id="lead_agent",
        digest="a" * 64,
        storage_source="builtin",
        storage_version="1",
    )
    trusted_context = TrustedRunContextV1(
        identity=identity,
        tenant=__import__("deerflow.runtime.tenant_identity", fromlist=["TenantIdentityV1"]).TenantIdentityV1.from_canonical_id("local").to_persisted_reference(),
        origin=SealedOriginV1(source_kind="native_channel", digest="c" * 64),
        thread_id="thread-1",
        external_key_reference=None,
        agent_revision=ResolvedAgentRevisionReferenceV1(
            agent_id=agent_revision.agent_id,
            digest=agent_revision.digest,
        ),
        profile_revision=ResolvedProfileRevisionReferenceV1(
            profile_id="default",
            digest="d" * 64,
        ),
        extension_generation=1,
        extension_manifest_digest=None,
    )
    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(identity=identity, channel_user_id="telegram-user"),
        origin=InvocationOrigin(
            source_kind="native_channel",
            references={"provider": "telegram", "channel_user_id": "telegram-user"},
        ),
        thread_id="thread-1",
        context_references={},
        agent_revision=agent_revision,
        normalized_input={"messages": []},
        execution_options={},
        extension_generation=1,
        contributor_execution_digest="b" * 64,
        tenant=trusted_context.tenant,
        trusted_context=trusted_context,
    )

    class Provider:
        name = "capture"

        def __init__(self):
            self.requests = []

        async def aauthorize(self, request):
            from deerflow_extension_api import AuthzDecision

            self.requests.append(request)
            return AuthzDecision(allow=True)

    provider = Provider()
    authorization = ProviderInvocationAuthorization(
        InvocationOperationsAuthorizationConfig(
            start_enabled=True,
            observe_enabled=True,
            cancel_enabled=True,
        ),
        lambda: SimpleNamespace(generation=3, provider=provider),
    )

    async def worker(_record):
        return None

    launch = PreparedLaunch(
        thread_id="thread-1",
        assistant_id="lead_agent",
        on_disconnect=DisconnectMode.cancel,
        metadata={},
        kwargs={},
        multitask_strategy="reject",
        model_name=None,
        user_id="owner-1",
        worker=worker,
        accepted_invocation=accepted,
        request_digest="c" * 64,
        request_digest_version="sha256-canonical-json-v1",
    )
    record = RunRecord(
        run_id="run-1",
        thread_id="thread-1",
        assistant_id="lead_agent",
        status=RunStatus.pending,
        on_disconnect=DisconnectMode.cancel,
        user_id="owner-1",
        created_at="2026-08-07T00:00:00+00:00",
        updated_at="2026-08-07T00:00:00+00:00",
        accepted_invocation=accepted,
        state_version=1,
    )
    principal = InvocationPrincipal(identity=identity, channel_user_id="telegram-user")

    await authorization.authorize_start(launch)
    await authorization.authorize_observe(record, principal)
    await authorization.authorize_cancel(record, principal)

    assert [request.action for request in provider.requests] == ["start", "observe", "cancel"]
    assert all(request.principal.identity is identity for request in provider.requests)
    assert all(request.principal.is_internal is False for request in provider.requests)
    assert all(request.principal.identity.acting_service.service_id == "channel:telegram" for request in provider.requests)
    assert all(request.trusted_context is trusted_context for request in provider.requests)


def test_accepted_identity_overrides_legacy_internal_runtime_flag() -> None:
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(kind="human", subject_id="owner-1", role="member"),
        acting_service=ActingServiceV1(service_id="channel:slack"),
    )

    principal = build_principal_from_context(
        {
            "user_id": "internal-worker",
            "user_role": "internal",
            "is_internal": True,
            "channel_user_id": "platform-user",
            INVOCATION_IDENTITY_CONTEXT_KEY: identity,
        },
        default_role="user",
    )

    assert principal.identity is identity
    assert principal.user_id == "owner-1"
    assert principal.role == "member"
    assert principal.is_internal is False


def test_tool_authorization_receives_identity_and_final_origin() -> None:
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(kind="human", subject_id="owner-1", role="member"),
        acting_service=ActingServiceV1(service_id="channel:slack"),
    )
    origin = SealedOriginV1(
        source_kind="native_channel",
        references=(
            SafeContextReferenceV1(
                key="provider",
                value="slack",
                storage_class="persistable",
                purpose="correlation",
            ),
        ),
        digest="a" * 64,
    )
    adapter = GuardrailAuthorizationAdapter(SimpleNamespace(), default_role="user")

    request = adapter._to_authz(
        GuardrailRequest(
            tool_name="mcp__demo__read",
            tool_input={},
            user_id="internal-worker",
            user_role="internal",
            is_internal=True,
            identity=identity,
            origin=origin,
        )
    )

    assert request.principal.identity is identity
    assert request.principal.is_internal is False
    assert request.context["origin"] is origin


@pytest.mark.anyio
async def test_constraint_projection_receives_identity_and_final_origin() -> None:
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(kind="human", subject_id="owner-1", role="member"),
        acting_service=ActingServiceV1(service_id="scheduler"),
    )
    agent_revision = ResolvedAgentRevision(
        agent_id="lead_agent",
        digest="a" * 64,
        storage_source="builtin",
        storage_version="1",
    )
    trusted_context = TrustedRunContextV1(
        identity=identity,
        tenant=__import__("deerflow.runtime.tenant_identity", fromlist=["TenantIdentityV1"]).TenantIdentityV1.from_canonical_id("local").to_persisted_reference(),
        origin=SealedOriginV1(source_kind="scheduled_task", digest="d" * 64),
        thread_id="thread-1",
        external_key_reference="raw:occurrence-1",
        agent_revision=ResolvedAgentRevisionReferenceV1(
            agent_id=agent_revision.agent_id,
            digest=agent_revision.digest,
        ),
        profile_revision=ResolvedProfileRevisionReferenceV1(
            profile_id="default",
            digest="e" * 64,
        ),
        extension_generation=1,
        extension_manifest_digest=None,
    )
    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(identity=identity),
        origin=InvocationOrigin(
            source_kind="scheduled_task",
            references={"task_id": "task-1", "task_run_id": "occurrence-1"},
        ),
        thread_id="thread-1",
        context_references={},
        agent_revision=agent_revision,
        normalized_input={"messages": []},
        execution_options={},
        extension_generation=1,
        contributor_execution_digest="b" * 64,
        tenant=trusted_context.tenant,
        trusted_context=trusted_context,
    )

    class Host:
        request = None

        async def project(self, request, **_kwargs):
            self.request = request
            return None

    async def worker(_record):
        return None

    host = Host()
    launch = PreparedLaunch(
        thread_id="thread-1",
        assistant_id="lead_agent",
        on_disconnect=DisconnectMode.cancel,
        metadata={},
        kwargs={},
        multitask_strategy="reject",
        model_name=None,
        user_id="owner-1",
        worker=worker,
        accepted_invocation=accepted,
        request_digest="c" * 64,
        request_digest_version="sha256-canonical-json-v1",
    )

    await ProviderInvocationConstraints(host).project(launch)

    assert host.request.identity is identity
    assert host.request.origin.source_kind == "scheduled_task"
    assert host.request.origin is trusted_context.origin
    assert host.request.trusted_context is trusted_context


def test_mcp_facts_retain_effective_subject_actor_and_origin() -> None:
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(kind="human", subject_id="owner-1", role="member"),
        acting_service=ActingServiceV1(service_id="channel:telegram"),
    )
    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(identity=identity, channel_user_id="telegram-user"),
        origin=InvocationOrigin(
            source_kind="native_channel",
            references={"provider": "telegram", "channel_user_id": "telegram-user"},
        ),
        thread_id="thread-1",
        context_references={},
        agent_revision=ResolvedAgentRevision(
            agent_id="lead_agent",
            digest="a" * 64,
            storage_source="builtin",
            storage_version="1",
        ),
        normalized_input={},
        execution_options={},
        extension_generation=4,
        contributor_execution_digest="b" * 64,
        tenant=__import__("deerflow.runtime.tenant_identity", fromlist=["TenantIdentityV1"]).TenantIdentityV1.from_canonical_id("local").to_persisted_reference(),
    )

    facts = McpInvocationFacts.from_accepted(accepted, run_id="run-1")

    assert facts.principal.identity is identity
    assert facts.principal.is_internal is False
    assert facts.origin.source_kind == "native_channel"
    assert facts.origin.digest == accepted.base_origin_digest
    assert facts.extension_manifest_digest == accepted.extension_manifest_digest
