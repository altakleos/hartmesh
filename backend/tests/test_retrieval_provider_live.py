"""Opt-in live qualification for evidence-bearing retrieval adapters.

Run one provider explicitly; a selected provider with missing credentials is a
failed qualification, while an unselected module is skipped and proves
nothing::

    DEER_FLOW_RUN_LIVE_TESTS=1 \
    DEER_FLOW_RETRIEVAL_QUALIFICATION_PROVIDER=duckduckgo \
    uv run pytest tests/test_retrieval_provider_live.py -v -s
"""

from __future__ import annotations

import json
import os

import pytest
from deerflow_extension_api import TenantReferenceV1

from deerflow.retrieval import (
    AcceptedRetrievalRequest,
    EvidenceBearingRetrievalService,
    ResolvedRetrievalCredentialV1,
    RetrievalPolicyV1,
    RetrievalRequestConstraintsV1,
)
from deerflow.runtime.tool_evidence import DurableToolReceiptV1, ToolAttemptContextV1

pytestmark = pytest.mark.live

_PROVIDER = os.environ.get(
    "DEER_FLOW_RETRIEVAL_QUALIFICATION_PROVIDER",
    "",
).strip()
if os.environ.get("CI"):
    pytest.skip("live retrieval qualification is disabled in CI", allow_module_level=True)
if os.environ.get("DEER_FLOW_RUN_LIVE_TESTS") != "1":
    pytest.skip(
        "set DEER_FLOW_RUN_LIVE_TESTS=1 to run live retrieval qualification",
        allow_module_level=True,
    )
if not _PROVIDER:
    pytest.skip(
        "set DEER_FLOW_RETRIEVAL_QUALIFICATION_PROVIDER; no provider is qualified",
        allow_module_level=True,
    )
if _PROVIDER not in {"duckduckgo", "ragflow", "serply", "tencent_wsa"}:
    raise RuntimeError("unsupported retrieval qualification provider")


def _credential(name: str) -> str:
    value = os.environ.get(name)
    if not isinstance(value, str) or not value.strip():
        pytest.fail(
            f"selected provider {_PROVIDER!r} is unqualified: {name} is missing",
            pytrace=False,
        )
    return value.strip()


def _receipt(provider_id: str) -> DurableToolReceiptV1:
    tenant = TenantReferenceV1(
        version=1,
        public_ref="tenant-" + "d" * 16,
        digest="d" * 64,
    )
    return DurableToolReceiptV1.started(
        context=ToolAttemptContextV1(
            run_id="retrieval-live-qualification",
            execution_task_id="retrieval-live-qualification",
            execution_kind="lead",
            subagent_name=None,
            tool_call_id=f"call-{provider_id}",
            attempt=1,
            owner_id="qualification-worker",
            lease_epoch=1,
            agent_revision_digest="a" * 64,
            assembly_fingerprint="b" * 64,
            extension_generation=1,
            subagent_catalog_digest="c" * 64,
            subagent_definition_digest=None,
            tenant=tenant,
        ),
        tool_name="knowledge_search" if provider_id == "ragflow" else "web_search",
        request_projection_digest="e" * 64,
    )


def _request(
    *,
    provider_id: str,
    endpoint: str,
    credential: object,
    adapter_version: str,
    collections: tuple[str, ...] = (),
    collection_refs: tuple[str, ...] = (),
    source_schemes: tuple[str, ...] = ("http", "https"),
) -> AcceptedRetrievalRequest:
    receipt = _receipt(provider_id)
    query = os.environ.get(
        "DEER_FLOW_RETRIEVAL_QUALIFICATION_QUERY",
        "OpenAI official documentation",
    )
    policy = RetrievalPolicyV1(
        allowed_providers=(provider_id,),
        allowed_endpoint_origins=(endpoint,),
        allowed_collections=collections,
        collection_public_refs=collection_refs,
        max_results=3,
        max_item_bytes=32 * 1024,
        max_aggregate_bytes=128 * 1024,
        timeout_ms=30_000,
        source_schemes=source_schemes,  # type: ignore[arg-type]
    )
    return AcceptedRetrievalRequest(
        thread_id="retrieval-live-qualification",
        tenant=receipt.context.tenant,
        receipt=receipt,
        actor_ref="actor-" + "f" * 32,
        provider_id=provider_id,
        tool_kind=("knowledge_search" if provider_id == "ragflow" else "web_search"),
        adapter_capability_version=adapter_version,
        query=query,
        credential=ResolvedRetrievalCredentialV1(
            provider_id=provider_id,
            selector_ref=f"qualification-{provider_id}",
            secret=credential,
        ),
        policy=policy,
        requested_constraints=RetrievalRequestConstraintsV1(
            provider_id=provider_id,
            endpoint=endpoint,
            collections=collections,
            max_results=3,
            max_item_bytes=32 * 1024,
            max_aggregate_bytes=128 * 1024,
            timeout_ms=30_000,
            source_schemes=source_schemes,
        ),
        tool_plane_base_revision_digest="1" * 64,
        tool_plane_user_overlay_digest="2" * 64,
        tool_plane_projection_digest="3" * 64,
        tool_plane_effective_digest="4" * 64,
    )


def _provider_and_request():
    if _PROVIDER == "duckduckgo":
        from deerflow.community.ddg_search.tools import (
            _DuckDuckGoRetrievalProvider,
        )

        return (
            _DuckDuckGoRetrievalProvider(
                region="wt-wt",
                safesearch="moderate",
                time_range=None,
            ),
            _request(
                provider_id="duckduckgo",
                endpoint="https://html.duckduckgo.com/html/",
                credential=True,
                adapter_version="ddgs-controlled-http-v1",
            ),
        )
    if _PROVIDER == "serply":
        from deerflow.community.serply.tools import _SerplyRetrievalProvider

        return (
            _SerplyRetrievalProvider(vertical="search", extras={}),
            _request(
                provider_id="serply",
                endpoint="https://api.serply.io/v1/search/",
                credential=_credential("SERPLY_API_KEY"),
                adapter_version="serply-http-v1",
            ),
        )
    if _PROVIDER == "tencent_wsa":
        from deerflow.community.tencent_wsa.tools import (
            _TencentWsaRetrievalProvider,
        )

        return (
            _TencentWsaRetrievalProvider(extras={}),
            _request(
                provider_id="tencent_wsa",
                endpoint="https://api.wsa.cloud.tencent.com/SearchPro",
                credential=_credential("TENCENTCLOUD_WSA_APIKEY"),
                adapter_version="tencent-wsa-http-v1",
            ),
        )

    from deerflow.community.ragflow.client import RAGFlowClient
    from deerflow.community.ragflow.tools import (
        _RAGFlowRetrievalProvider,
        _RAGFlowRetrievalSettings,
    )

    base_url = _credential("RAGFLOW_BASE_URL")
    api_key = _credential("RAGFLOW_API_KEY")
    datasets = tuple(item.strip() for item in _credential("RAGFLOW_DATASETS").split(",") if item.strip())
    if not datasets:
        pytest.fail(
            "selected provider 'ragflow' is unqualified: RAGFLOW_DATASETS is empty",
            pytrace=False,
        )
    settings = _RAGFlowRetrievalSettings(
        base_url=base_url,
        api_key=api_key,
        datasets=list(datasets),
        page_size=3,
        max_chars_per_chunk=8_192,
        max_total_chars=32_768,
        timeout=30,
    )
    collection_refs = tuple(f"tenant-{'d' * 16}-ragflow-{index}" for index in range(1, len(datasets) + 1))
    return (
        _RAGFlowRetrievalProvider(
            settings=settings,
            client=RAGFlowClient(
                base_url=base_url,
                api_key=api_key,
                timeout=30,
                max_response_bytes=128 * 1024,
            ),
        ),
        _request(
            provider_id="ragflow",
            endpoint=base_url,
            credential=api_key,
            adapter_version="ragflow-http-v1",
            collections=datasets,
            collection_refs=collection_refs,
            source_schemes=("ragflow-doc",),
        ),
    )


@pytest.mark.anyio
async def test_selected_retrieval_provider_normalizes_live_response() -> None:
    provider, request = _provider_and_request()

    candidate = await EvidenceBearingRetrievalService().retrieve(
        request,
        provider,
    )

    draft = candidate.draft
    assert draft.provider_id == _PROVIDER
    assert draft.provider_status in {"success", "empty"}
    assert draft.result_count <= 3
    portable = json.dumps(draft.to_event_projection(), sort_keys=True)
    assert request.query not in portable
    assert request.credential.selector_ref not in portable
    if isinstance(request.credential.secret, str):
        assert request.credential.secret not in portable
    for reference in draft.source_references:
        if reference.startswith("http"):
            assert "?" not in reference
            assert "#" not in reference
