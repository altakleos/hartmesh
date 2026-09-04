"""
Web Search Tool - Search the web using DuckDuckGo (no API key required).
"""

import asyncio
import json
import logging

from langchain.tools import tool

from deerflow.community.search_time_range import DDGS_TIMELIMIT_BY_TIME_RANGE, SearchTimeRange
from deerflow.config import get_app_config
from deerflow.retrieval import (
    RETRIEVAL_TOOL_METADATA_KEY,
    EvidenceBearingRetrievalService,
    ProviderRetrievalItem,
    ProviderRetrievalResponse,
    ResolvedRetrievalCredentialV1,
    RetrievalPolicyV1,
    RetrievalProviderError,
    RetrievalRequestConstraintsV1,
    RetrievalToolDeclarationV1,
    accepted_retrieval_app_config_from_active,
    accepted_retrieval_request_from_active,
    get_active_retrieval_handoff,
    run_blocking_provider_call,
)
from deerflow.retrieval.provider_config import (
    configured_domains,
    configured_int,
    configured_timeout_ms,
)

logger = logging.getLogger(__name__)

DEFAULT_BACKEND = "auto"
DEFAULT_REGION = "wt-wt"
DEFAULT_SAFESEARCH = "moderate"
DEFAULT_WIKIPEDIA_REGION = "us-en"
_DDG_EVIDENCE_SEARCH_URL = "https://html.duckduckgo.com/html/"
_DDG_EVIDENCE_ORIGIN = "https://html.duckduckgo.com"

WIKIPEDIA_BACKENDS = {"auto", "all", "wikipedia"}
# ddgs 9.14.1: enabled text engines whose implementations honor ``timelimit``.
# Google and Bing also implement it but are disabled upstream in this release.
TIME_RANGE_CAPABLE_BACKENDS = ("brave", "duckduckgo", "yahoo")
DEFAULT_TIME_RANGE_BACKEND = ",".join(TIME_RANGE_CAPABLE_BACKENDS)
WIKIPEDIA_LANGUAGE_ALIASES = {
    "jp": "ja",
    "kr": "ko",
    "tzh": "zh",
    "wt": "en",
}


def _normalize_backend(backend: str | list[str] | tuple[str, ...] | None) -> str:
    if backend is None:
        return DEFAULT_BACKEND
    if isinstance(backend, (list, tuple)):
        return ",".join(str(part).strip() for part in backend if str(part).strip()) or DEFAULT_BACKEND
    return str(backend).strip() or DEFAULT_BACKEND


def _normalize_setting(value: str | None, default: str) -> str:
    return str(value).strip() if value else default


def _resolve_time_range_backend(backend: str | list[str] | tuple[str, ...] | None) -> str:
    """Exclude DDGS text backends that ignore the native time limit."""
    normalized_backend = _normalize_backend(backend)
    configured_backends = [part.strip().lower() for part in normalized_backend.split(",") if part.strip()]
    if any(part in {"auto", "all"} for part in configured_backends):
        return DEFAULT_TIME_RANGE_BACKEND

    supported_backends = [part for part in configured_backends if part in TIME_RANGE_CAPABLE_BACKENDS]
    excluded_backends = [part for part in configured_backends if part not in TIME_RANGE_CAPABLE_BACKENDS]
    if excluded_backends:
        logger.warning("Ignoring DDGS backends without time-range support: %s", ", ".join(excluded_backends))
    return ",".join(supported_backends) or DEFAULT_TIME_RANGE_BACKEND


def _backend_includes_wikipedia(backend: str | list[str] | tuple[str, ...] | None) -> bool:
    backend = _normalize_backend(backend)
    return any(part.strip().lower() in WIKIPEDIA_BACKENDS for part in backend.split(","))


def _contains_codepoint(query: str, ranges: tuple[tuple[int, int], ...]) -> bool:
    return any(start <= ord(char) <= end for char in query for start, end in ranges)


def _infer_wikipedia_region(query: str) -> str:
    """Pick a valid Wikipedia language region when DDGS' worldwide region is used."""
    if _contains_codepoint(query, ((0x3040, 0x30FF), (0x31F0, 0x31FF))):
        return "jp-ja"
    if _contains_codepoint(query, ((0xAC00, 0xD7AF), (0x1100, 0x11FF), (0x3130, 0x318F))):
        return "kr-ko"
    if _contains_codepoint(query, ((0x3400, 0x9FFF),)):
        return "cn-zh"
    if _contains_codepoint(query, ((0x0400, 0x04FF),)):
        return "ru-ru"
    if _contains_codepoint(query, ((0x0370, 0x03FF),)):
        return "gr-el"
    if _contains_codepoint(query, ((0x0590, 0x05FF),)):
        return "il-he"
    if _contains_codepoint(query, ((0x0600, 0x06FF),)):
        return "xa-ar"
    return DEFAULT_WIKIPEDIA_REGION


def _resolve_ddgs_region(query: str, region: str | None, backend: str | list[str] | tuple[str, ...] | None) -> str:
    """
    DDGS' wikipedia engine treats the second part of region as a Wikipedia
    subdomain. Its default worldwide region, wt-wt, becomes wt.wikipedia.org.
    """
    normalized_region = _normalize_setting(region, DEFAULT_REGION).lower()
    if not _backend_includes_wikipedia(backend):
        return normalized_region

    if normalized_region == DEFAULT_REGION:
        return _infer_wikipedia_region(query)

    if "-" not in normalized_region:
        return DEFAULT_WIKIPEDIA_REGION

    country, language = normalized_region.split("-", 1)
    return f"{country}-{WIKIPEDIA_LANGUAGE_ALIASES.get(language, language)}"


def _search_text(
    query: str,
    max_results: int = 5,
    region: str | None = DEFAULT_REGION,
    safesearch: str | None = DEFAULT_SAFESEARCH,
    backend: str | list[str] | tuple[str, ...] | None = DEFAULT_BACKEND,
    time_range: SearchTimeRange | None = None,
    timeout_seconds: float = 30,
    raise_errors: bool = False,
) -> list[dict]:
    """
    Execute text search using DuckDuckGo.

    Args:
        query: Search keywords
        max_results: Maximum number of results
        region: Search region
        safesearch: Safe search level
        backend: DDGS backend(s), e.g. "auto", "duckduckgo", or "duckduckgo,brave"
        time_range: Optional relative publication/update window

    Returns:
        List of search results
    """
    try:
        from ddgs import DDGS
    except ImportError:
        logger.error("ddgs library not installed. Run: pip install ddgs")
        if raise_errors:
            raise RetrievalProviderError("configuration_error") from None
        return []

    ddgs = DDGS(timeout=timeout_seconds)

    try:
        backend = _resolve_time_range_backend(backend) if time_range is not None else _normalize_backend(backend)
        safesearch = _normalize_setting(safesearch, DEFAULT_SAFESEARCH)
        effective_region = _resolve_ddgs_region(query, region, backend)
        search_kwargs: dict[str, object] = {
            "region": effective_region,
            "safesearch": safesearch,
            "max_results": max_results,
            "backend": backend,
        }
        if time_range is not None:
            search_kwargs["timelimit"] = DDGS_TIMELIMIT_BY_TIME_RANGE[time_range]
        results = ddgs.text(query, **search_kwargs)
        return list(results) if results else []

    except Exception as e:
        logger.error("DDGS web search failed (%s)", type(e).__name__)
        if raise_errors:
            status = "timeout" if type(e).__name__ == "TimeoutException" else "provider_unavailable"
            raise RetrievalProviderError(status) from None
        return []


def _search_duckduckgo_evidence(
    query: str,
    *,
    max_results: int,
    region: str,
    safesearch: str,
    time_range: SearchTimeRange | None,
    timeout_seconds: float,
    max_response_bytes: int = 8 * 1024 * 1024,
) -> list[dict[str, str]]:
    """Call the pinned DDG HTML engine through a fail-closed transport.

    The public DDGS aggregator does not expose its destination or redirect
    policy. Evidence-bearing calls therefore instantiate its pinned HTML
    parser directly, verify the SDK endpoint before I/O, and replace the
    underlying client with one that is HTTPS-only and never follows redirects.
    """

    try:
        import primp
        from ddgs.engines.duckduckgo import Duckduckgo
        from ddgs.exceptions import DDGSException, RatelimitException, TimeoutException
        from ddgs.http_client import Response
    except ImportError:
        logger.error("DDGS evidence transport is unavailable")
        raise RetrievalProviderError("configuration_error") from None

    if Duckduckgo.search_url != _DDG_EVIDENCE_SEARCH_URL or Duckduckgo.search_method != "POST":
        logger.error("DDGS evidence endpoint contract changed")
        raise RetrievalProviderError("configuration_error")

    network_client = primp.Client(
        timeout=timeout_seconds,
        follow_redirects=False,
        https_only=True,
        verify=True,
        impersonate="random",
        impersonate_os="random",
    )

    class _ControlledHttpClient:
        def request(self, method: str, url: str, *args, **kwargs):
            if method != "POST" or url != _DDG_EVIDENCE_SEARCH_URL:
                raise RetrievalProviderError("configuration_error")
            kwargs["follow_redirects"] = False
            try:
                response = network_client.request(method, url, *args, **kwargs)
            except primp.TimeoutError:
                raise RetrievalProviderError("timeout") from None
            except Exception:
                raise RetrievalProviderError("provider_unavailable") from None
            if response.status_code == 429:
                raise RetrievalProviderError("rate_limited")
            if response.status_code != 200:
                raise RetrievalProviderError("provider_unavailable")
            content = response.content
            if not isinstance(content, bytes) or len(content) > max_response_bytes:
                raise RetrievalProviderError("oversized_response")
            content_type = response.headers.get("content-type", "")
            if not isinstance(content_type, str) or content_type.split(";", 1)[0].strip().lower() not in {"text/html", "application/xhtml+xml"}:
                raise RetrievalProviderError("unsafe_response")
            return Response(response)

    engine = Duckduckgo(timeout=timeout_seconds)
    engine.http_client = _ControlledHttpClient()
    try:
        raw_results = engine.search(
            query,
            region=region,
            safesearch=safesearch,
            timelimit=(DDGS_TIMELIMIT_BY_TIME_RANGE[time_range] if time_range is not None else None),
        )
    except RetrievalProviderError:
        raise
    except TimeoutException:
        logger.error("DDGS evidence request timed out")
        raise RetrievalProviderError("timeout") from None
    except RatelimitException:
        logger.error("DDGS evidence request was rate limited")
        raise RetrievalProviderError("rate_limited") from None
    except DDGSException:
        logger.error("DDGS evidence request failed")
        raise RetrievalProviderError("provider_unavailable") from None
    except Exception:
        logger.error("DDGS evidence request failed")
        raise RetrievalProviderError("provider_unavailable") from None

    results: list[dict[str, str]] = []
    # Keep one bounded sentinel beyond the public limit so the adapter can
    # prove whether it discarded provider results.
    for raw in (raw_results or [])[: max_results + 1]:
        if isinstance(raw, dict):
            title = raw.get("title", "")
            href = raw.get("href", raw.get("link", ""))
            body = raw.get("body", raw.get("snippet", ""))
        else:
            title = getattr(raw, "title", "")
            href = getattr(raw, "href", "")
            body = getattr(raw, "body", "")
        results.append(
            {
                "title": title if isinstance(title, str) else "",
                "href": href if isinstance(href, str) else "",
                "body": body if isinstance(body, str) else "",
            }
        )
    return results


@tool("web_search", parse_docstring=True)
def web_search_tool(
    query: str,
    max_results: int = 5,
    time_range: SearchTimeRange | None = None,
) -> str:
    """Search the web for information. Use this tool to find current information, news, articles, and facts from the internet.

    Args:
        query: Search keywords describing what you want to find. Be specific for better results.
        max_results: Maximum number of results to return. Default is 5.
        time_range: Optional relative publication/update window. Use only when the request requires recent results.
    """
    config = get_app_config().get_tool_config("web_search")
    region = DEFAULT_REGION
    safesearch = DEFAULT_SAFESEARCH
    backend = DEFAULT_BACKEND

    if config is not None:
        # Override tool call defaults from config if set.
        max_results = config.model_extra.get("max_results", max_results)
        region = config.model_extra.get("region", region)
        safesearch = config.model_extra.get("safesearch", safesearch)
        backend = config.model_extra.get("backend", backend)

    results = _search_text(
        query=query,
        max_results=max_results,
        region=region,
        safesearch=safesearch,
        backend=backend,
        time_range=time_range,
    )

    if not results:
        return json.dumps({"error": "No results found", "query": query}, ensure_ascii=False)

    normalized_results = [
        {
            "title": r.get("title", ""),
            "url": r.get("href", r.get("link", "")),
            "content": r.get("body", r.get("snippet", "")),
        }
        for r in results
    ]

    output = {
        "query": query,
        "total_results": len(normalized_results),
        "results": normalized_results,
    }

    return json.dumps(output, indent=2, ensure_ascii=False)


_RECENCY_DAYS = {
    "day": 1,
    "week": 7,
    "month": 31,
    "year": 366,
}


class _DuckDuckGoRetrievalProvider:
    def __init__(
        self,
        *,
        region: str,
        safesearch: str,
        time_range: SearchTimeRange | None,
    ) -> None:
        self._region = region
        self._safesearch = safesearch
        self._time_range = time_range

    async def search(self, request) -> ProviderRetrievalResponse:
        results = await run_blocking_provider_call(
            _search_duckduckgo_evidence,
            request.query,
            max_results=request.constraints.max_results,
            region=self._region,
            safesearch=self._safesearch,
            time_range=self._time_range,
            timeout_seconds=request.constraints.timeout_ms / 1_000,
            max_response_bytes=request.constraints.max_aggregate_bytes,
        )
        normalized_results = [
            {
                "title": result.get("title", ""),
                "url": result.get("href", result.get("link", "")),
                "content": result.get("body", result.get("snippet", "")),
            }
            for result in results[: request.constraints.max_results]
            if isinstance(result, dict)
        ]
        if normalized_results:
            candidate = json.dumps(
                {
                    "total_results": len(normalized_results),
                    "results": normalized_results,
                },
                indent=2,
                ensure_ascii=False,
            )
        else:
            candidate = json.dumps(
                {"error": "No results found"},
                ensure_ascii=False,
            )
        items = tuple(
            ProviderRetrievalItem(
                source_locator=result["url"],
                content=result.get("content", ""),
            )
            for result in normalized_results
            if isinstance(result.get("url"), str) and result["url"]
        )
        return ProviderRetrievalResponse(
            candidate_result=candidate,
            items=items,
            result_count=len(normalized_results),
            truncated=len(results) > request.constraints.max_results,
        )


async def _web_search_with_evidence(
    query: str,
    max_results: int = 5,
    time_range: SearchTimeRange | None = None,
) -> str:
    if get_active_retrieval_handoff() is None:
        return await asyncio.to_thread(
            web_search_tool.func,
            query,
            max_results,
            time_range,
        )
    app_config = accepted_retrieval_app_config_from_active()
    config = app_config.get_tool_config("web_search")
    extra = dict((config.model_extra or {}) if config is not None else {})
    server_max_results = configured_int(
        extra,
        "max_results",
        default=5,
        maximum=50,
    )
    if not isinstance(max_results, int) or isinstance(max_results, bool) or not 1 <= max_results <= 50:
        raise RetrievalProviderError("configuration_error")
    region = str(extra.get("region", DEFAULT_REGION))
    safesearch = str(extra.get("safesearch", DEFAULT_SAFESEARCH))
    # Evidence-bearing DDGS calls use one known network provider. The direct
    # compatibility path may still use its configured multi-backend mode.
    recency_days = _RECENCY_DAYS.get(str(time_range)) if time_range else None
    timeout_ms = configured_timeout_ms(extra, default_seconds=30)
    policy = RetrievalPolicyV1(
        allowed_providers=("duckduckgo",),
        allowed_endpoint_origins=(_DDG_EVIDENCE_ORIGIN,),
        web_domain_allowlist=configured_domains(extra, "allowed_domains"),
        web_domain_denylist=configured_domains(extra, "denied_domains"),
        max_recency_days=366,
        max_results=server_max_results,
        max_item_bytes=configured_int(
            extra,
            "max_item_bytes",
            default=16 * 1024,
            maximum=1024 * 1024,
        ),
        max_aggregate_bytes=configured_int(
            extra,
            "max_total_bytes",
            default=64 * 1024,
            maximum=8 * 1024 * 1024,
        ),
        timeout_ms=timeout_ms,
        allow_redirects=False,
        source_schemes=("http", "https"),
    )
    accepted = accepted_retrieval_request_from_active(
        query=query.strip(),
        credential=ResolvedRetrievalCredentialV1(
            provider_id="duckduckgo",
            selector_ref="duckduckgo-anonymous",
            secret=True,
        ),
        policy=policy,
        requested_constraints=RetrievalRequestConstraintsV1(
            provider_id="duckduckgo",
            endpoint=_DDG_EVIDENCE_SEARCH_URL,
            recency_days=recency_days,
            max_results=max_results,
            timeout_ms=timeout_ms,
        ),
    )
    candidate = await EvidenceBearingRetrievalService().retrieve(
        accepted,
        _DuckDuckGoRetrievalProvider(
            region=region,
            safesearch=safesearch,
            time_range=time_range,
        ),
    )
    if not isinstance(candidate.result, str):
        raise RetrievalProviderError("unsafe_response")
    return candidate.result


web_search_tool.coroutine = _web_search_with_evidence
web_search_tool.metadata = {
    **(web_search_tool.metadata or {}),
    RETRIEVAL_TOOL_METADATA_KEY: RetrievalToolDeclarationV1(
        provider_id="duckduckgo",
        tool_kind="web_search",
        adapter_capability_version="ddgs-controlled-http-v1",
    ).to_metadata(),
}
