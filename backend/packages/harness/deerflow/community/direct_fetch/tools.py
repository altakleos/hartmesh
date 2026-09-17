"""``web_fetch`` without a key: the Gateway reads the page itself.

The tool is the profile's keyless default (deploy/compose/config.yaml). It
resolves, checks and pins every address it touches (see ``client.py``),
extracts the article and returns it as Markdown, and stamps its own typed
outcome on the result: ``deerflow_tool_meta`` with ``error_scope`` set from
the transport it saw, never inferred from the words of the result. A refusal
is one bounded sentence that names the address and says whether to try
another source.

When the run carries an active retrieval handoff (a durable run under the
evidence-bearing retrieval contract), the fetch goes through
``EvidenceBearingRetrievalService`` like the declared search providers do:
one item, the page's origin as its source reference, the same bounded
observation the receipt ledger already records.
"""

from __future__ import annotations

import asyncio
from typing import Annotated, Any
from urllib.parse import urlsplit

from langchain.tools import InjectedToolCallId, tool
from langgraph.types import Command

from deerflow.community.direct_fetch.client import DEFAULT_TIMEOUT_SECONDS, DirectFetchClient, FetchedPage
from deerflow.community.web_fetch_outcome import FetchRefusal, describe_refusal, refusal_meta, stamped_result, success_meta
from deerflow.config import get_app_config
from deerflow.retrieval import (
    RETRIEVAL_TOOL_METADATA_KEY,
    EvidenceBearingRetrievalService,
    ProviderRetrievalItem,
    ProviderRetrievalRequest,
    ProviderRetrievalResponse,
    ResolvedRetrievalCredentialV1,
    RetrievalPolicyV1,
    RetrievalProviderError,
    RetrievalRequestConstraintsV1,
    RetrievalToolDeclarationV1,
    accepted_retrieval_app_config_from_active,
    accepted_retrieval_request_from_active,
    get_active_retrieval_handoff,
)
from deerflow.utils.readability import ReadabilityExtractor

__all__ = ["PROVIDER_ID", "MAX_RESULT_CHARS", "web_fetch_tool"]

PROVIDER_ID = "direct_http"
#: What the model reads of a page; the same bound the hosted reader had.
MAX_RESULT_CHARS = 4096
_readability = ReadabilityExtractor()


def _coerce_timeout(value: object, default: float) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def _coerce_bool(value: object, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def _client_from_config(app_config: Any) -> DirectFetchClient:
    timeout = DEFAULT_TIMEOUT_SECONDS
    trust_env = True
    config = app_config.get_tool_config("web_fetch")
    if config is not None:
        extra = config.model_extra or {}
        timeout = _coerce_timeout(extra.get("timeout"), timeout)
        trust_env = _coerce_bool(extra.get("trust_env"), trust_env)
    return DirectFetchClient(timeout_seconds=timeout, trust_env=trust_env)


async def _extract(page: FetchedPage) -> str:
    if page.content_type == "text/plain":
        return page.text[:MAX_RESULT_CHARS]
    article = await asyncio.to_thread(_readability.extract_article, page.text)
    return article.to_markdown()[:MAX_RESULT_CHARS]


class _DirectFetchProvider:
    """The client as a retrieval provider: the query is the address."""

    def __init__(self, client: DirectFetchClient) -> None:
        self._client = client
        self.refusal: FetchRefusal | None = None

    async def search(self, request: ProviderRetrievalRequest) -> ProviderRetrievalResponse:
        outcome = await self._client.fetch(request.query)
        if isinstance(outcome, FetchRefusal):
            self.refusal = outcome
            raise RetrievalProviderError(_provider_status(outcome))
        text = await _extract(outcome)
        return ProviderRetrievalResponse(
            candidate_result=text,
            items=(ProviderRetrievalItem(source_locator=outcome.url, content=text),),
            content_type="application/vnd.deerflow.retrieval+json",
            result_count=1,
        )


def _provider_status(refusal: FetchRefusal) -> str:
    if refusal.error_type == "auth":
        return "authentication_failed"
    if refusal.error_type == "rate_limited":
        return "rate_limited"
    if refusal.error_type == "transient" and refusal.status_code is None:
        return "timeout" if "in time" in refusal.reason else "provider_unavailable"
    if refusal.error_type == "unsupported":
        return "unsafe_response" if "larger" not in refusal.reason else "oversized_response"
    return "provider_unavailable"


async def _fetch_with_evidence(url: str, client: DirectFetchClient) -> tuple[str, dict[str, Any]]:
    app_config = accepted_retrieval_app_config_from_active()
    config = app_config.get_tool_config("web_fetch")
    extra = dict((config.model_extra or {}) if config is not None else {})
    timeout_ms = int(_coerce_timeout(extra.get("timeout"), DEFAULT_TIMEOUT_SECONDS) * 1000)
    parsed = urlsplit(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    policy = RetrievalPolicyV1(
        allowed_providers=(PROVIDER_ID,),
        allowed_endpoint_origins=(origin,),
        max_results=1,
        max_item_bytes=64 * 1024,
        max_aggregate_bytes=64 * 1024,
        timeout_ms=timeout_ms,
        allow_redirects=True,
        source_schemes=("http", "https"),
    )
    accepted = accepted_retrieval_request_from_active(
        query=url,
        credential=ResolvedRetrievalCredentialV1(provider_id=PROVIDER_ID, selector_ref="direct-anonymous", secret=None),
        policy=policy,
        requested_constraints=RetrievalRequestConstraintsV1(provider_id=PROVIDER_ID, endpoint=origin, max_results=1, timeout_ms=timeout_ms, allow_redirects=True),
    )
    provider = _DirectFetchProvider(client)
    try:
        candidate = await EvidenceBearingRetrievalService().retrieve(accepted, provider)
    except RetrievalProviderError:
        if provider.refusal is None:
            raise
        return describe_refusal(url, provider.refusal), refusal_meta(provider.refusal)
    if not isinstance(candidate.result, str):
        raise RetrievalProviderError("unsafe_response")
    return candidate.result, success_meta()


@tool("web_fetch", parse_docstring=True)
async def web_fetch_tool(url: str, tool_call_id: Annotated[str, InjectedToolCallId] = "") -> str | Command:
    """Fetch the contents of a web page at a given URL.
    Only fetch EXACT URLs that have been provided directly by the user or have been returned in results from the web_search and web_fetch tools.
    This tool can NOT access content that requires authentication, such as private Google Docs or pages behind login walls.
    Do NOT add www. to URLs that do NOT have them.
    URLs must include the schema: https://example.com is a valid URL while example.com is an invalid URL.

    Args:
        url: The URL to fetch the contents of.
    """
    if get_active_retrieval_handoff() is not None:
        app_config = accepted_retrieval_app_config_from_active()
        text, meta = await _fetch_with_evidence(url, _client_from_config(app_config))
        return stamped_result(text, meta, tool_call_id)
    client = _client_from_config(get_app_config())
    outcome = await client.fetch(url)
    if isinstance(outcome, FetchRefusal):
        return stamped_result(describe_refusal(url, outcome), refusal_meta(outcome), tool_call_id)
    return stamped_result(await _extract(outcome), success_meta(), tool_call_id)


web_fetch_tool.metadata = {
    **(web_fetch_tool.metadata or {}),
    RETRIEVAL_TOOL_METADATA_KEY: RetrievalToolDeclarationV1(
        provider_id=PROVIDER_ID,
        tool_kind="web_fetch",
        adapter_capability_version="direct-http-pinned-v1",
        protected_argument_fields=("url",),
    ).to_metadata(),
}
