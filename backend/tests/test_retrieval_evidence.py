from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from deerflow_extension_api import (
    CredentialEvidenceV1,
    EffectiveSubjectV1,
    InvocationIdentityV1,
    ResolvedAgentRevisionReferenceV1,
    ResolvedProfileRevisionReferenceV1,
    SealedOriginV1,
    TenantReferenceV1,
    TrustedRunContextV1,
    effective_authority_digest_v1,
)

from deerflow.retrieval import (
    AcceptedRetrievalRequest,
    EvidenceBearingRetrievalService,
    ProviderRetrievalItem,
    ProviderRetrievalResponse,
    ResolvedRetrievalCredentialV1,
    RetrievalEvidenceError,
    RetrievalObservationV1,
    RetrievalPolicyDenied,
    RetrievalPolicyV1,
    RetrievalProviderError,
    RetrievalRequestConstraintsV1,
    RetrievalToolDeclarationV1,
    TenantProviderConcurrencyLimiter,
    accepted_retrieval_request_from_active,
    active_retrieval_draft_context,
    normalize_web_source_reference,
    run_blocking_provider_call,
    validate_retrieval_pair,
)
from deerflow.runtime.accepted_invocation import (
    TRUSTED_RUN_CONTEXT_KEY,
    ResolvedAgentMaterialV1,
)
from deerflow.runtime.agent_revision import RESOLVED_AGENT_MATERIAL_CONTEXT_KEY
from deerflow.runtime.tool_evidence import (
    DurableToolReceiptV1,
    ToolAttemptContextV1,
    canonical_digest,
)


def test_policy_narrowing_and_source_normalization_are_private_and_deterministic() -> None:
    policy = RetrievalPolicyV1(
        allowed_providers=("serply",),
        allowed_endpoint_origins=("https://api.serply.io",),
        web_domain_allowlist=("example.com",),
        web_domain_denylist=("private.example.com",),
        max_recency_days=30,
        max_results=10,
        max_item_bytes=1_024,
        max_aggregate_bytes=4_096,
        timeout_ms=5_000,
        allow_redirects=False,
    )
    requested = RetrievalRequestConstraintsV1(
        provider_id="serply",
        endpoint="https://api.serply.io/v1/search/",
        domains=("docs.example.com",),
        recency_days=7,
        max_results=3,
        max_item_bytes=512,
        max_aggregate_bytes=2_048,
        timeout_ms=2_000,
        allow_redirects=False,
    )

    effective = policy.narrow(requested)

    assert effective.max_results == 3
    assert effective.recency_days == 7
    assert effective.policy_digest == policy.digest
    assert (
        normalize_web_source_reference(
            "HTTPS://user:password@Docs.Example.COM:443/a%20b?utm_source=secret&q=private#fragment",
            allowed_schemes=effective.source_schemes,
            allowed_domains=effective.domains,
            denied_domains=policy.web_domain_denylist,
        )
        == "https://docs.example.com"
    )
    portable = json.dumps(effective.to_safe_projection(), sort_keys=True)
    assert "password" not in portable
    assert "private" not in portable
    assert "utm_source" not in portable
    assert "q=" not in portable

    with pytest.raises(RetrievalPolicyDenied, match="retrieval_policy_provider_denied"):
        policy.narrow(
            RetrievalRequestConstraintsV1(
                provider_id="tencent_wsa",
                endpoint="https://api.wsa.cloud.tencent.com/SearchPro",
            )
        )

    with pytest.raises(RetrievalEvidenceError, match="retrieval_source_invalid"):
        normalize_web_source_reference(
            "https://example.com/bad%2Gpath",
            allowed_domains=("example.com",),
        )


def test_policy_digest_cannot_be_used_as_a_collection_selector_oracle() -> None:
    shared = {
        "allowed_providers": ("ragflow",),
        "allowed_endpoint_origins": ("https://ragflow.example.com",),
        "collection_public_refs": ("tenant-collection-1",),
        "source_schemes": ("ragflow-doc",),
    }
    first = RetrievalPolicyV1(
        **shared,
        allowed_collections=("finance",),
    )
    second = RetrievalPolicyV1(
        **shared,
        allowed_collections=("secret-dataset-uuid",),
    )

    # Exact private deployment material is anchored by the accepted tool-plane
    # digests. The portable policy commitment intentionally uses only its safe
    # public projection, so a low-entropy selector dictionary is not an oracle.
    assert first.digest == second.digest


def test_domain_selectors_are_not_a_portable_policy_or_projection_oracle() -> None:
    shared = {
        "allowed_providers": ("serply",),
        "allowed_endpoint_origins": ("https://api.serply.io",),
    }
    first = RetrievalPolicyV1(
        **shared,
        web_domain_allowlist=("confidential-customer.example",),
        web_domain_denylist=("blocked.confidential-customer.example",),
    )
    second = RetrievalPolicyV1(
        **shared,
        web_domain_allowlist=("another-private-tenant.example",),
        web_domain_denylist=("blocked.another-private-tenant.example",),
    )

    assert first.digest == second.digest
    portable = json.dumps(
        first.narrow(
            RetrievalRequestConstraintsV1(
                provider_id="serply",
                endpoint="https://api.serply.io/v1/search",
            )
        ).to_safe_projection(),
        sort_keys=True,
    )
    assert "confidential-customer" not in portable
    assert "domain_scope" in portable


def test_web_source_reference_is_origin_only_when_path_reflects_query() -> None:
    assert (
        normalize_web_source_reference(
            "https://docs.example.com/search/reset-password?query=reset-password#result",
            allowed_domains=("example.com",),
        )
        == "https://docs.example.com"
    )


@pytest.mark.parametrize(
    "public_ref",
    (
        "contains:delimiter",
        "contains whitespace",
        "contains/slash",
        "\N{SNOWMAN}",
    ),
)
def test_collection_public_references_use_a_closed_safe_grammar(
    public_ref: str,
) -> None:
    with pytest.raises(
        RetrievalEvidenceError,
        match="retrieval_collection_refs_invalid",
    ):
        RetrievalPolicyV1(
            allowed_providers=("ragflow",),
            allowed_endpoint_origins=("https://ragflow.example.com",),
            allowed_collections=("private-dataset-id",),
            collection_public_refs=(public_ref,),
            source_schemes=("ragflow-doc",),
        )


@pytest.mark.parametrize(
    ("override", "error_code"),
    [
        ({"collections": ("other",)}, "collection_denied"),
        ({"domains": ("other.example",)}, "domain_denied"),
        ({"domains": ("private.example.com",)}, "domain_denied"),
        ({"recency_days": 31}, "recency_denied"),
        ({"max_results": 6}, "max_results_denied"),
        ({"max_item_bytes": 1_025}, "max_item_bytes_denied"),
        ({"max_aggregate_bytes": 4_097}, "max_aggregate_bytes_denied"),
        ({"timeout_ms": 5_001}, "timeout_ms_denied"),
        ({"allow_redirects": True}, "redirect_denied"),
        ({"accept_partial": True}, "partial_denied"),
        ({"source_schemes": ("http",)}, "source_scheme_denied"),
    ],
)
def test_caller_cannot_broaden_any_retrieval_policy_dimension(
    override: dict[str, object],
    error_code: str,
) -> None:
    policy = RetrievalPolicyV1(
        allowed_providers=("serply",),
        allowed_endpoint_origins=("https://api.serply.io",),
        allowed_collections=("finance",),
        collection_public_refs=("collection-public-1",),
        web_domain_allowlist=("example.com",),
        web_domain_denylist=("private.example.com",),
        max_recency_days=30,
        max_results=5,
        max_item_bytes=1_024,
        max_aggregate_bytes=4_096,
        timeout_ms=5_000,
        source_schemes=("https",),
    )
    requested: dict[str, object] = {
        "provider_id": "serply",
        "endpoint": "https://api.serply.io/search",
        "collections": ("finance",),
        "domains": ("docs.example.com",),
        "recency_days": 7,
        "max_results": 2,
        "max_item_bytes": 512,
        "max_aggregate_bytes": 2_048,
        "timeout_ms": 2_000,
        "allow_redirects": False,
        "accept_partial": False,
        "source_schemes": ("https",),
    }
    requested.update(override)

    with pytest.raises(
        RetrievalPolicyDenied,
        match=f"retrieval_policy_{error_code}",
    ):
        policy.narrow(RetrievalRequestConstraintsV1(**requested))  # type: ignore[arg-type]


def _started_receipt(
    *,
    tenant_marker: str = "d",
    attempt: int = 1,
) -> DurableToolReceiptV1:
    return DurableToolReceiptV1.started(
        context=ToolAttemptContextV1(
            run_id="run-1",
            execution_task_id="run-1",
            execution_kind="lead",
            subagent_name=None,
            tool_call_id="call-1",
            attempt=attempt,
            owner_id="worker-1",
            lease_epoch=4,
            agent_revision_digest="a" * 64,
            assembly_fingerprint="b" * 64,
            extension_generation=2,
            subagent_catalog_digest="c" * 64,
            subagent_definition_digest=None,
            tenant=TenantReferenceV1(
                version=1,
                public_ref="tenant-" + tenant_marker * 16,
                digest=tenant_marker * 64,
            ),
        ),
        tool_name="web_search",
        request_projection_digest="e" * 64,
    )


def _accepted_request(
    *,
    query: str = "quarterly acquisition codename",
    credential_secret: object = "bearer-super-secret",
    max_results: int = 2,
    max_aggregate_bytes: int = 4_096,
    accept_partial: bool = False,
    timeout_ms: int = 2_000,
) -> AcceptedRetrievalRequest:
    receipt = _started_receipt()
    policy = RetrievalPolicyV1(
        allowed_providers=("serply",),
        allowed_endpoint_origins=("https://api.serply.io",),
        web_domain_allowlist=("example.com",),
        max_results=5,
        max_item_bytes=1_024,
        max_aggregate_bytes=max_aggregate_bytes,
        timeout_ms=timeout_ms,
        source_schemes=("https",),
        accept_partial=accept_partial,
    )
    return AcceptedRetrievalRequest(
        thread_id="thread-1",
        tenant=receipt.context.tenant,
        receipt=receipt,
        actor_ref="actor-" + "f" * 32,
        provider_id="serply",
        tool_kind="web_search",
        adapter_capability_version="serply-http-v1",
        query=query,
        credential=ResolvedRetrievalCredentialV1(
            provider_id="serply",
            selector_ref="serply-primary",
            secret=credential_secret,
        ),
        policy=policy,
        requested_constraints=RetrievalRequestConstraintsV1(
            provider_id="serply",
            endpoint="https://api.serply.io/v1/search/",
            max_results=max_results,
            accept_partial=accept_partial,
        ),
        tool_plane_base_revision_digest="1" * 64,
        tool_plane_user_overlay_digest="2" * 64,
        tool_plane_projection_digest="3" * 64,
        tool_plane_effective_digest="4" * 64,
    )


@pytest.mark.anyio
async def test_provider_concurrency_is_bounded_per_tenant_and_provider() -> None:
    class TrackingProvider:
        def __init__(self) -> None:
            self.active = 0
            self.maximum_active = 0

        async def search(self, _request):
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
            try:
                await asyncio.sleep(0.02)
                return ProviderRetrievalResponse(
                    candidate_result={"results": []},
                    items=(),
                )
            finally:
                self.active -= 1

    provider = TrackingProvider()
    service = EvidenceBearingRetrievalService(
        concurrency_limiter=TenantProviderConcurrencyLimiter(
            max_concurrency=1,
        )
    )

    await asyncio.gather(
        service.retrieve(_accepted_request(), provider),
        service.retrieve(_accepted_request(), provider),
    )

    assert provider.maximum_active == 1


@pytest.mark.anyio
async def test_timed_out_blocking_calls_keep_their_concurrency_permit() -> None:
    import threading
    import time

    lock = threading.Lock()
    active = 0
    maximum_active = 0

    def blocking_search() -> ProviderRetrievalResponse:
        nonlocal active, maximum_active
        with lock:
            active += 1
            maximum_active = max(maximum_active, active)
        try:
            time.sleep(0.04)
            return ProviderRetrievalResponse(candidate_result=[], items=())
        finally:
            with lock:
                active -= 1

    class BlockingProvider:
        async def search(self, _request):
            return await run_blocking_provider_call(blocking_search)

    service = EvidenceBearingRetrievalService(
        concurrency_limiter=TenantProviderConcurrencyLimiter(max_concurrency=1),
    )
    outcomes = await asyncio.gather(
        *(service.retrieve(_accepted_request(timeout_ms=5), BlockingProvider()) for _ in range(3)),
        return_exceptions=True,
    )

    assert maximum_active == 1
    assert all(isinstance(outcome, RetrievalProviderError) and outcome.status == "timeout" for outcome in outcomes)


@pytest.mark.anyio
async def test_repeated_cancellation_keeps_blocking_call_concurrency_permit() -> None:
    import threading

    lock = threading.Lock()
    release = threading.Event()
    first_started = threading.Event()
    second_started = threading.Event()
    active = 0
    maximum_active = 0
    call_count = 0

    def blocking_search() -> ProviderRetrievalResponse:
        nonlocal active, maximum_active, call_count
        with lock:
            call_count += 1
            current_call = call_count
            active += 1
            maximum_active = max(maximum_active, active)
        (first_started if current_call == 1 else second_started).set()
        try:
            if not release.wait(timeout=1):
                raise TimeoutError
            return ProviderRetrievalResponse(candidate_result=[], items=())
        finally:
            with lock:
                active -= 1

    class BlockingProvider:
        async def search(self, _request):
            return await run_blocking_provider_call(blocking_search)

    service = EvidenceBearingRetrievalService(
        concurrency_limiter=TenantProviderConcurrencyLimiter(max_concurrency=1),
    )
    first = asyncio.create_task(service.retrieve(_accepted_request(timeout_ms=2_000), BlockingProvider()))
    assert await asyncio.to_thread(first_started.wait, 0.5)
    second = asyncio.create_task(service.retrieve(_accepted_request(timeout_ms=2_000), BlockingProvider()))

    first.cancel()
    await asyncio.sleep(0)
    first.cancel()
    done, _ = await asyncio.wait({first}, timeout=0.05)
    permit_released_early = first in done
    second_overlapped = second_started.is_set()
    release.set()
    outcomes = await asyncio.gather(first, second, return_exceptions=True)

    assert not permit_released_early
    assert not second_overlapped
    assert maximum_active == 1
    assert isinstance(outcomes[0], asyncio.CancelledError)


def _trusted_runtime_context(tenant: TenantReferenceV1) -> dict[str, object]:
    credential = CredentialEvidenceV1(
        method="channel",
        credential_ref=None,
        effective_authority_digest=effective_authority_digest_v1(("runs:create",)),
        authority_categories=("runs",),
    )
    return {
        TRUSTED_RUN_CONTEXT_KEY: TrustedRunContextV1(
            identity=InvocationIdentityV1(
                effective_subject=EffectiveSubjectV1(
                    kind="human",
                    subject_id="user-1",
                    role="member",
                )
            ),
            credential=credential,
            tenant=tenant,
            origin=SealedOriginV1(source_kind="web", digest="a" * 64),
            thread_id="thread-1",
            external_key_reference=None,
            agent_revision=ResolvedAgentRevisionReferenceV1(
                agent_id="lead-agent",
                digest="b" * 64,
            ),
            profile_revision=ResolvedProfileRevisionReferenceV1(
                profile_id="default",
                digest="c" * 64,
            ),
            extension_generation=1,
            extension_manifest_digest="d" * 64,
            run_id="run-1",
        ),
        "accepted_tool_plane_revision": {
            "base_revision_digest": "1" * 64,
            "user_overlay_digest": "2" * 64,
            "projection_digest": "3" * 64,
            "effective_digest": "4" * 64,
        },
    }


def test_active_request_uses_trusted_actor_and_publishes_policy_denial() -> None:
    receipt = _started_receipt()
    tenant = receipt.context.tenant
    assert isinstance(tenant, TenantReferenceV1)
    runtime_context = _trusted_runtime_context(tenant)
    declaration = RetrievalToolDeclarationV1(
        provider_id="serply",
        tool_kind="web_search",
        adapter_capability_version="serply-http-v1",
    )
    policy = RetrievalPolicyV1(
        allowed_providers=("serply",),
        allowed_endpoint_origins=("https://api.serply.io",),
    )

    with active_retrieval_draft_context(
        receipt,
        declaration,
        runtime_context,
    ) as handoff:
        with pytest.raises(
            RetrievalPolicyDenied,
            match="retrieval_policy_endpoint_denied",
        ):
            accepted_retrieval_request_from_active(
                query="password reset",
                credential=ResolvedRetrievalCredentialV1(
                    provider_id="serply",
                    selector_ref="primary-secret-selector",
                    secret="credential-secret",
                ),
                policy=policy,
                requested_constraints=RetrievalRequestConstraintsV1(
                    provider_id="serply",
                    endpoint="https://attacker.invalid/search",
                ),
            )

    assert handoff.draft is not None
    assert handoff.draft.provider_status == "policy_denied"
    portable = json.dumps(handoff.draft.to_event_projection(), sort_keys=True)
    for forbidden in (
        "password reset",
        "attacker.invalid",
        "primary-secret-selector",
        "credential-secret",
    ):
        assert forbidden not in portable


def test_mcp_tools_are_linked_only_by_explicit_retrieval_declaration() -> None:
    from deerflow.retrieval import (
        RETRIEVAL_TOOL_METADATA_KEY,
        retrieval_tool_declaration,
    )

    arbitrary = SimpleNamespace(
        name="search_everything",
        metadata={"deerflow_mcp": True},
    )
    assert retrieval_tool_declaration(arbitrary) is None

    recognized = SimpleNamespace(
        name="search_everything",
        metadata={
            "deerflow_mcp": True,
            RETRIEVAL_TOOL_METADATA_KEY: RetrievalToolDeclarationV1(
                provider_id="mcp",
                tool_kind="web_search",
                adapter_capability_version="mcp-sanitized-v1",
                mcp_evidence_ref="mcp-lineage:public-reference",
            ).to_metadata(),
        },
    )
    declaration = retrieval_tool_declaration(recognized)
    assert declaration is not None
    assert declaration.mcp_evidence_ref == "mcp-lineage:public-reference"


@pytest.mark.anyio
async def test_serply_adapter_uses_common_service_and_accepted_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from deerflow.community.serply import tools as serply_tools
    from deerflow.retrieval import retrieval_tool_declaration

    receipt = _started_receipt()
    tenant = receipt.context.tenant
    assert isinstance(tenant, TenantReferenceV1)
    runtime_context = _trusted_runtime_context(tenant)
    accepted_config = SimpleNamespace(
        model_extra={
            "api_key": "provider-secret",
            "max_results": 5,
            "allowed_domains": ["example.com"],
            "timeout": 3,
        }
    )
    accepted_app_config = SimpleNamespace(
        get_tool_config=lambda _name: accepted_config,
    )
    runtime_context[RESOLVED_AGENT_MATERIAL_CONTEXT_KEY] = ResolvedAgentMaterialV1(
        agent_id="default",
        storage_source="builtin",
        storage_version="v1",
        agent_config=None,
        soul="",
        model_profile={},
        app_config=accepted_app_config,
    )
    changed_live_config = SimpleNamespace(
        model_extra={
            "api_key": "changed-live-secret",
            "max_results": 1,
            "allowed_domains": ["attacker.invalid"],
            "timeout": 1,
        }
    )
    monkeypatch.setattr(
        serply_tools,
        "get_app_config",
        lambda: SimpleNamespace(
            get_tool_config=lambda _name: changed_live_config,
        ),
    )
    calls: list[dict[str, object]] = []

    def fake_get(
        path,
        api_key,
        query,
        params,
        timeout_seconds=30,
        max_response_bytes=8 * 1024 * 1024,
        strict_response=False,
    ):
        del max_response_bytes, strict_response
        calls.append(
            {
                "path": path,
                "api_key": api_key,
                "query": query,
                "params": params,
                "timeout_seconds": timeout_seconds,
            }
        )
        return (
            {
                "results": [
                    {
                        "title": "private title",
                        "link": "https://example.com/report?q=private#fragment",
                        "description": "private snippet",
                    }
                ]
            },
            None,
        )

    monkeypatch.setattr(serply_tools, "_serply_get", fake_get)
    declaration = retrieval_tool_declaration(serply_tools.web_search_tool)
    assert declaration is not None
    with active_retrieval_draft_context(
        receipt,
        declaration,
        runtime_context,
    ) as handoff:
        result = await serply_tools._web_search_with_evidence(
            "private query",
            max_results=2,
        )

    assert "private snippet" in result
    assert calls == [
        {
            "path": "search",
            "api_key": "provider-secret",
            "query": "private query",
            "params": {"q": "private query", "num": 2},
            "timeout_seconds": 3,
        }
    ]
    assert handoff.draft is not None
    assert handoff.draft.source_references == ("https://example.com",)
    portable = json.dumps(handoff.draft.to_event_projection(), sort_keys=True)
    for forbidden in (
        "private query",
        "provider-secret",
        "private title",
        "private snippet",
        "q=private",
    ):
        assert forbidden not in portable


@pytest.mark.anyio
async def test_provider_service_builds_a_query_and_credential_free_draft() -> None:
    raw_query = "quarterly acquisition codename"
    credential_secret = "bearer-super-secret"
    request = _accepted_request(
        query=raw_query,
        credential_secret=credential_secret,
    )
    receipt = request.receipt

    class FakeProvider:
        calls = 0

        async def search(self, provider_request):
            self.calls += 1
            assert provider_request.query == raw_query
            assert provider_request.credential.secret == credential_secret
            return ProviderRetrievalResponse(
                candidate_result={
                    "query": raw_query,
                    "results": [
                        {
                            "title": "Confidential result title",
                            "url": "https://example.com/report?tracking=private",
                            "content": "sensitive result body",
                        }
                    ],
                },
                items=(
                    ProviderRetrievalItem(
                        source_locator="https://example.com/report?tracking=private#part",
                        content="sensitive result body",
                    ),
                ),
                safe_request_ref="request-opaque-1",
            )

    provider = FakeProvider()
    candidate = await EvidenceBearingRetrievalService().retrieve(request, provider)

    assert provider.calls == 1
    assert candidate.result["query"] == raw_query
    assert candidate.draft.source_references == ("https://example.com",)
    portable = json.dumps(candidate.draft.to_event_projection(), sort_keys=True)
    for forbidden in (
        raw_query,
        credential_secret,
        "serply-primary",
        "Confidential result title",
        "sensitive result body",
        "tracking=private",
    ):
        assert forbidden not in portable

    with pytest.raises(RetrievalEvidenceError, match="retrieval_source_invalid"):
        replace(
            candidate.draft,
            source_references=("https://example.com/report?query=quarterly-acquisition-codename",),
        )

    with pytest.raises(
        RetrievalEvidenceError,
        match="retrieval_source_count_invalid",
    ):
        replace(
            candidate.draft,
            source_count=0,
            source_references=(),
        )

    forged_failure = replace(
        candidate.draft,
        provider_status="timeout",
        safe_reason="timeout",
        result_count=0,
        source_count=0,
        source_references=(),
    )
    terminal = receipt.outcome(
        phase="succeeded",
        result_projection_digest="f" * 64,
        result_kind="tool_message",
        safe_error_code=None,
    )
    for mismatched_draft in (
        replace(candidate.draft, run_id="run-other"),
        replace(
            candidate.draft,
            tenant_ref="tenant-" + "9" * 16,
            tenant_digest="9" * 64,
        ),
    ):
        mismatched_observation = RetrievalObservationV1(
            draft=mismatched_draft,
            receipt_phase="succeeded",
            result_projection_digest=terminal.result_projection_digest,
            result_kind=terminal.result_kind,
            safe_terminal_reason=None,
            terminal_at=terminal.occurred_at,
        )
        with pytest.raises(
            RetrievalEvidenceError,
            match="retrieval_pair_mismatch",
        ):
            validate_retrieval_pair(
                terminal.to_event_body(),
                mismatched_observation.to_event_body(),
            )

    with pytest.raises(
        RetrievalEvidenceError,
        match="retrieval_terminal_status_mismatch",
    ):
        RetrievalObservationV1.finalize(terminal, forged_failure)
    assert candidate.draft.policy_digest == request.policy.digest
    assert candidate.draft.receipt_id == receipt.receipt_id
    assert candidate.draft.draft_digest == canonical_digest(
        {
            "domain": "hartmesh/retrieval-draft/v1",
            "projection": candidate.draft.to_event_projection(),
        }
    )


def test_policy_denial_and_missing_credentials_happen_before_provider_access() -> None:
    policy = RetrievalPolicyV1(
        allowed_providers=("serply",),
        allowed_endpoint_origins=("https://api.serply.io",),
    )
    receipt = _started_receipt()

    with pytest.raises(RetrievalPolicyDenied, match="retrieval_policy_endpoint_denied"):
        AcceptedRetrievalRequest(
            thread_id="thread-1",
            tenant=receipt.context.tenant,
            receipt=receipt,
            actor_ref="actor-1",
            provider_id="serply",
            tool_kind="web_search",
            adapter_capability_version="serply-http-v1",
            query="private query",
            credential=ResolvedRetrievalCredentialV1(
                provider_id="serply",
                selector_ref="primary",
                secret="secret",
            ),
            policy=policy,
            requested_constraints=RetrievalRequestConstraintsV1(
                provider_id="serply",
                endpoint="https://attacker.invalid/search",
            ),
            tool_plane_base_revision_digest="1" * 64,
            tool_plane_user_overlay_digest="2" * 64,
            tool_plane_projection_digest="3" * 64,
            tool_plane_effective_digest="4" * 64,
        )

    with pytest.raises(ValueError, match="retrieval_credential_unavailable"):
        _accepted_request(credential_secret=None)


@pytest.mark.anyio
async def test_provider_failure_is_safe_and_publishes_one_bounded_draft(caplog) -> None:
    raw_query = "secret query in unsafe provider exception"
    raw_error = f"provider echoed {raw_query} with bearer-super-secret"
    request = _accepted_request(query=raw_query)
    declaration = RetrievalToolDeclarationV1(
        provider_id="serply",
        tool_kind="web_search",
        adapter_capability_version="serply-http-v1",
    )

    class ExplodingProvider:
        async def search(self, _provider_request):
            raise RuntimeError(raw_error)

    with active_retrieval_draft_context(
        request.receipt,
        declaration,
        {},
    ) as handoff:
        with pytest.raises(
            RetrievalProviderError,
            match="retrieval_provider_unavailable",
        ):
            await EvidenceBearingRetrievalService().retrieve(
                request,
                ExplodingProvider(),
            )

    assert handoff.draft is not None
    assert handoff.draft.provider_status == "provider_unavailable"
    serialized = json.dumps(handoff.draft.to_event_projection(), sort_keys=True)
    assert raw_query not in serialized
    assert raw_error not in serialized
    assert "bearer-super-secret" not in serialized
    assert raw_error not in caplog.text


@pytest.mark.anyio
@pytest.mark.parametrize(
    "status",
    [
        "provider_unavailable",
        "rate_limited",
        "authentication_failed",
        "configuration_error",
    ],
)
async def test_provider_port_failure_categories_are_preserved(status: str) -> None:
    request = _accepted_request()
    declaration = RetrievalToolDeclarationV1(
        provider_id="serply",
        tool_kind="web_search",
        adapter_capability_version="serply-http-v1",
    )

    class FailingProvider:
        async def search(self, _provider_request):
            raise RetrievalProviderError(status)

    with active_retrieval_draft_context(
        request.receipt,
        declaration,
        {},
    ) as handoff:
        with pytest.raises(RetrievalProviderError, match=f"retrieval_{status}"):
            await EvidenceBearingRetrievalService().retrieve(
                request,
                FailingProvider(),
            )

    assert handoff.draft is not None
    assert handoff.draft.provider_status == status
    assert handoff.draft.result_count == 0
    assert handoff.draft.source_references == ()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("response", "expected_status"),
    [
        (
            ProviderRetrievalResponse(candidate_result=[], items=()),
            "empty",
        ),
        (
            ProviderRetrievalResponse(
                candidate_result=["partial"],
                items=(
                    ProviderRetrievalItem(
                        source_locator="https://example.com/partial",
                        content="partial",
                    ),
                ),
                partial=True,
            ),
            "partial",
        ),
    ],
)
async def test_provider_contract_empty_and_partial_statuses(
    response: ProviderRetrievalResponse,
    expected_status: str,
) -> None:
    request = _accepted_request(accept_partial=expected_status == "partial")

    class FakeProvider:
        async def search(self, _provider_request):
            return response

    candidate = await EvidenceBearingRetrievalService().retrieve(
        request,
        FakeProvider(),
    )

    assert candidate.draft.provider_status == expected_status
    assert candidate.draft.partial is (expected_status == "partial")


@pytest.mark.anyio
async def test_unaccepted_partial_response_is_rejected_as_unsafe() -> None:
    request = _accepted_request(accept_partial=False)
    declaration = RetrievalToolDeclarationV1(
        provider_id="serply",
        tool_kind="web_search",
        adapter_capability_version="serply-http-v1",
    )

    class PartialProvider:
        async def search(self, _provider_request):
            return ProviderRetrievalResponse(
                candidate_result=["partial"],
                items=(
                    ProviderRetrievalItem(
                        source_locator="https://example.com/partial",
                        content="partial",
                    ),
                ),
                partial=True,
            )

    with active_retrieval_draft_context(
        request.receipt,
        declaration,
        {},
    ) as handoff:
        with pytest.raises(
            RetrievalProviderError,
            match="retrieval_unsafe_response",
        ):
            await EvidenceBearingRetrievalService().retrieve(
                request,
                PartialProvider(),
            )
    assert handoff.draft is not None
    assert handoff.draft.provider_status == "unsafe_response"


@pytest.mark.anyio
async def test_timeout_and_oversized_responses_use_finite_safe_categories() -> None:
    request = replace(
        _accepted_request(),
        policy=replace(_accepted_request().policy, timeout_ms=1),
    )
    declaration = RetrievalToolDeclarationV1(
        provider_id="serply",
        tool_kind="web_search",
        adapter_capability_version="serply-http-v1",
    )

    class SlowProvider:
        async def search(self, _provider_request):
            await asyncio.sleep(0.05)
            raise AssertionError("unreachable")

    with active_retrieval_draft_context(
        request.receipt,
        declaration,
        {},
    ) as timeout_handoff:
        with pytest.raises(RetrievalProviderError, match="retrieval_timeout"):
            await EvidenceBearingRetrievalService().retrieve(request, SlowProvider())
    assert timeout_handoff.draft is not None
    assert timeout_handoff.draft.provider_status == "timeout"

    class OversizedProvider:
        async def search(self, _provider_request):
            return ProviderRetrievalResponse(
                candidate_result="x" * 5_000,
                items=(
                    ProviderRetrievalItem(
                        source_locator="https://example.com/large",
                        content="x" * 2_000,
                    ),
                ),
            )

    with active_retrieval_draft_context(
        request.receipt,
        declaration,
        {},
    ) as oversized_handoff:
        with pytest.raises(
            RetrievalProviderError,
            match="retrieval_oversized_response",
        ):
            await EvidenceBearingRetrievalService().retrieve(
                request,
                OversizedProvider(),
            )
    assert oversized_handoff.draft is not None
    assert oversized_handoff.draft.provider_status == "oversized_response"


@pytest.mark.anyio
async def test_malformed_provider_response_is_rejected_without_copying_it() -> None:
    request = _accepted_request()
    declaration = RetrievalToolDeclarationV1(
        provider_id="serply",
        tool_kind="web_search",
        adapter_capability_version="serply-http-v1",
    )
    private_body = "provider body that must not persist"

    class MalformedProvider:
        async def search(self, _provider_request):
            return {"body": private_body}

    with active_retrieval_draft_context(
        request.receipt,
        declaration,
        {},
    ) as handoff:
        with pytest.raises(
            RetrievalProviderError,
            match="retrieval_unsafe_response",
        ):
            await EvidenceBearingRetrievalService().retrieve(
                request,
                MalformedProvider(),
            )

    assert handoff.draft is not None
    assert private_body not in json.dumps(handoff.draft.to_event_projection())


@pytest.mark.anyio
async def test_provider_content_type_is_enforced_before_candidate_acceptance() -> None:
    request = _accepted_request()
    declaration = RetrievalToolDeclarationV1(
        provider_id="serply",
        tool_kind="web_search",
        adapter_capability_version="serply-http-v1",
    )

    class HtmlProvider:
        async def search(self, _provider_request):
            return ProviderRetrievalResponse(
                candidate_result={"results": []},
                items=(),
                content_type="text/html",
            )

    with active_retrieval_draft_context(
        request.receipt,
        declaration,
        {},
    ) as handoff:
        with pytest.raises(
            RetrievalProviderError,
            match="retrieval_unsafe_response",
        ):
            await EvidenceBearingRetrievalService().retrieve(
                request,
                HtmlProvider(),
            )

    assert handoff.draft is not None
    assert handoff.draft.provider_status == "unsafe_response"


@pytest.mark.anyio
async def test_internal_document_references_are_tenant_scoped_and_pseudonymous() -> None:
    async def retrieve_for_tenant(marker: str):
        receipt = _started_receipt(tenant_marker=marker)
        policy = RetrievalPolicyV1(
            allowed_providers=("ragflow",),
            allowed_endpoint_origins=("https://ragflow.example.com",),
            allowed_collections=("private-dataset-id",),
            collection_public_refs=(f"tenant-{marker * 16}-collection-1",),
            max_results=2,
            source_schemes=("ragflow-doc",),
        )
        request = AcceptedRetrievalRequest(
            thread_id="thread-1",
            tenant=receipt.context.tenant,
            receipt=receipt,
            actor_ref="actor-1",
            provider_id="ragflow",
            tool_kind="knowledge_search",
            adapter_capability_version="ragflow-http-v1",
            query="private employee question",
            credential=ResolvedRetrievalCredentialV1(
                provider_id="ragflow",
                selector_ref="ragflow-primary",
                secret="credential-secret",
            ),
            policy=policy,
            requested_constraints=RetrievalRequestConstraintsV1(
                provider_id="ragflow",
                endpoint="https://ragflow.example.com/api/retrieval",
                collections=("private-dataset-id",),
                max_results=2,
                source_schemes=("ragflow-doc",),
            ),
            tool_plane_base_revision_digest="1" * 64,
            tool_plane_user_overlay_digest="2" * 64,
            tool_plane_projection_digest="3" * 64,
            tool_plane_effective_digest="4" * 64,
        )

        class RagflowProvider:
            async def search(self, _provider_request):
                return ProviderRetrievalResponse(
                    candidate_result="private document text",
                    items=(
                        ProviderRetrievalItem(
                            collection_selector="private-dataset-id",
                            document_selector="employee-record-7",
                            content="private document text",
                        ),
                    ),
                )

        return await EvidenceBearingRetrievalService().retrieve(
            request,
            RagflowProvider(),
        )

    first = await retrieve_for_tenant("d")
    second = await retrieve_for_tenant("e")

    first_ref = first.draft.source_references[0]
    second_ref = second.draft.source_references[0]
    assert first_ref.startswith("ragflow-doc:tenant-dddddddddddddddd-collection-1:")
    assert first_ref != second_ref
    portable = json.dumps(first.draft.to_event_projection(), sort_keys=True)
    for private in (
        "private-dataset-id",
        "employee-record-7",
        "private document text",
        "private employee question",
        "credential-secret",
    ):
        assert private not in portable


@pytest.mark.anyio
async def test_common_query_dictionary_has_no_matchable_portable_identifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    common_queries = (
        "weather",
        "benefits",
        "quarterly results",
        "reset password",
    )

    from deerflow.community.serply.tools import _SerplyRetrievalProvider
    from deerflow.runtime.tool_evidence import digest_result_projection

    def empty_serply(*_args, **_kwargs):
        return {}, None

    monkeypatch.setattr(
        "deerflow.community.serply.tools._serply_get",
        empty_serply,
    )
    result_digests: set[str] = set()

    for query in common_queries:
        provider = _SerplyRetrievalProvider(vertical="search", extras={})
        candidate = await EvidenceBearingRetrievalService().retrieve(_accepted_request(query=query), provider)
        portable = json.dumps(candidate.draft.to_event_projection(), sort_keys=True)
        assert query not in portable
        assert hashlib.sha256(query.encode()).hexdigest() not in portable
        result_digests.add(digest_result_projection(candidate.result, result_kind="str", status="success"))

    assert len(result_digests) == 1


@pytest.mark.anyio
async def test_safe_constraint_projection_rejects_free_form_fields() -> None:
    class EmptyProvider:
        async def search(self, _provider_request):
            return ProviderRetrievalResponse(candidate_result=[], items=())

    candidate = await EvidenceBearingRetrievalService().retrieve(
        _accepted_request(),
        EmptyProvider(),
    )

    with pytest.raises(ValueError, match="retrieval_constraints_invalid"):
        replace(
            candidate.draft,
            safe_constraints={
                "version": 1,
                "provider_id": "serply",
                "raw_query": "do not persist me",
            },
        )
