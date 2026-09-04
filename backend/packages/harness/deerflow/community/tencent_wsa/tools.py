"""Web search tool powered by Tencent Cloud Web Search API (WSA)."""

import asyncio
import json
import logging
import os
from collections.abc import Mapping
from typing import Any

import httpx
from langchain.tools import tool

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
    validate_json_content_type,
    validate_response_body_size,
)

logger = logging.getLogger(__name__)

_SEARCH_ENDPOINT = "https://api.wsa.cloud.tencent.com/SearchPro"
_API_KEY_ENV = "TENCENTCLOUD_WSA_APIKEY"
_DEFAULT_MAX_RESULTS = 5
_DEFAULT_API_RESULT_COUNT = 10
_MAX_RESULTS = 50
_REQUEST_TIMEOUT_S = 30.0
_api_key_warned: set[str] = set()


def _get_tool_extras(
    tool_name: str,
    *,
    app_config: object | None = None,
) -> Mapping[str, Any]:
    config_source = get_app_config() if app_config is None else app_config
    config = config_source.get_tool_config(tool_name)
    if config is None or config.model_extra is None:
        return {}
    return config.model_extra


def _get_api_key(
    tool_name: str = "web_search",
    *,
    extras: Mapping[str, Any] | None = None,
    allow_env_fallback: bool = True,
) -> str | None:
    api_key = (extras if extras is not None else _get_tool_extras(tool_name)).get("api_key")
    if isinstance(api_key, str) and api_key.strip():
        return api_key.strip()

    if not allow_env_fallback:
        return None
    env_key = os.getenv(_API_KEY_ENV)
    if isinstance(env_key, str) and env_key.strip():
        return env_key.strip()
    return None


def _coerce_max_results(value: object, *, default: int = _DEFAULT_MAX_RESULTS) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        result = value
    elif isinstance(value, str) and value.strip().isdigit():
        result = int(value.strip())
    else:
        logger.warning("Invalid Tencent Cloud WSA max_results=%r; using default %s", value, default)
        return default
    if result <= 0:
        logger.warning("Invalid Tencent Cloud WSA max_results=%r; using default %s", value, default)
        return default
    if result > _MAX_RESULTS:
        logger.warning("Tencent Cloud WSA max_results=%s exceeds maximum %s; clamping", result, _MAX_RESULTS)
        return _MAX_RESULTS
    return result


def _get_mode(*, extras: Mapping[str, Any] | None = None) -> int | None:
    """Return an explicitly configured WSA result mode, if valid.

    Tencent Cloud defaults to natural web results when ``Mode`` is omitted.  Keep
    that default so the provider does not request VR or mixed results implicitly.
    """

    extras = extras if extras is not None else _get_tool_extras("web_search")
    if "mode" not in extras:
        return None
    mode = extras["mode"]
    if not isinstance(mode, int) or isinstance(mode, bool):
        logger.warning("Invalid Tencent Cloud WSA mode=%r; omitting Mode", extras["mode"])
        return None
    if mode not in {0, 1, 2}:
        logger.warning("Tencent Cloud WSA mode=%r is outside 0, 1, 2; omitting Mode", mode)
        return None
    return mode


def _request_count(max_results: int) -> int | None:
    """Return Tencent Cloud's supported Cnt value when one is needed.

    The API's default response size is 10. Cnt is available only on Tencent
    Cloud plans that support it, so omit it for requests that fit in the
    default response and request the smallest supported batch otherwise.
    """

    if max_results <= _DEFAULT_API_RESULT_COUNT:
        return None
    return ((max_results + _DEFAULT_API_RESULT_COUNT - 1) // _DEFAULT_API_RESULT_COUNT) * _DEFAULT_API_RESULT_COUNT


def _error(message: str, query: str, *, request_id: str | None = None) -> str:
    result: dict[str, str] = {"error": message, "query": query}
    if request_id:
        result["request_id"] = request_id
    return json.dumps(result, ensure_ascii=False)


def _request_id(response: Mapping[str, Any]) -> str | None:
    value = response.get("RequestId")
    return value if isinstance(value, str) and value else None


def _missing_key_error(query: str, tool_name: str) -> str:
    if tool_name not in _api_key_warned:
        _api_key_warned.add(tool_name)
        logger.warning(
            "Tencent Cloud WSA API key is not set for '%s'. Set %s in the environment or provide api_key in config.yaml.",
            tool_name,
            _API_KEY_ENV,
        )
    return _error(f"{_API_KEY_ENV} is not configured", query)


def _search(
    api_key: str,
    payload: dict[str, object],
    query: str,
    timeout_seconds: float = _REQUEST_TIMEOUT_S,
    max_response_bytes: int = 8 * 1024 * 1024,
    strict_response: bool = False,
) -> tuple[dict[str, Any] | None, str | None]:
    try:
        with httpx.Client(
            timeout=timeout_seconds,
            follow_redirects=False,
        ) as client:
            response = client.post(
                _SEARCH_ENDPOINT,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json; charset=utf-8",
                },
                json=payload,
            )
        response.raise_for_status()
        validate_response_body_size(
            response,
            max_response_bytes=max_response_bytes,
        )
        validate_json_content_type(response)
        data = response.json()
    except httpx.TimeoutException:
        logger.error("Tencent Cloud WSA request timed out")
        if strict_response:
            raise RetrievalProviderError("timeout") from None
        return None, _error("Tencent Cloud WSA request failed", query)
    except httpx.HTTPStatusError as exc:
        logger.error("Tencent Cloud WSA API returned HTTP %s", exc.response.status_code)
        return None, _error(f"Tencent Cloud WSA API error: HTTP {exc.response.status_code}", query)
    except httpx.HTTPError as exc:
        logger.error("Tencent Cloud WSA request failed (%s)", type(exc).__name__)
        return None, _error("Tencent Cloud WSA request failed", query)
    except RetrievalProviderError:
        logger.error("Tencent Cloud WSA returned an unsafe or oversized response")
        if strict_response:
            raise
        return None, _error("Tencent Cloud WSA returned an unexpected response format", query)
    except (TypeError, ValueError):
        logger.error("Tencent Cloud WSA returned an invalid JSON response")
        if strict_response:
            raise RetrievalProviderError("unsafe_response") from None
        return None, _error("Tencent Cloud WSA returned an invalid JSON response", query)

    if not isinstance(data, dict):
        logger.error("Tencent Cloud WSA returned an unexpected payload type: %s", type(data).__name__)
        if strict_response:
            raise RetrievalProviderError("unsafe_response")
        return None, _error("Tencent Cloud WSA returned an unexpected response format", query)
    return data, None


def _get_response(data: dict[str, Any], query: str) -> tuple[dict[str, Any] | None, str | None, str | None]:
    response = data.get("Response")
    if not isinstance(response, dict):
        logger.error("Tencent Cloud WSA response did not contain a Response object")
        return None, None, _error("Tencent Cloud WSA returned an unexpected response format", query)

    request_id = _request_id(response)
    api_error = response.get("Error")
    if isinstance(api_error, dict):
        code = api_error.get("Code")
        code = code if isinstance(code, str) and code else "UnknownError"
        logger.error("Tencent Cloud WSA API returned a provider error response")
        return None, request_id, _error(f"Tencent Cloud WSA API error: {code}", query, request_id=request_id)
    return response, request_id, None


def _parse_results(response: dict[str, Any], *, max_results: int) -> list[dict[str, Any]] | None:
    pages = response.get("Pages")
    if pages is None:
        return []
    if not isinstance(pages, list):
        logger.error("Tencent Cloud WSA returned non-list Pages value")
        return None

    results: list[dict[str, Any]] = []
    for page in pages:
        if isinstance(page, str):
            try:
                page_data = json.loads(page)
            except json.JSONDecodeError:
                logger.warning("Skipping malformed Tencent Cloud WSA page entry")
                continue
        elif isinstance(page, dict):
            # The documented schema is a JSON-string array. Accepting an object
            # too keeps the provider forward-compatible with a harmless API
            # representation change.
            page_data = page
        else:
            logger.warning("Skipping Tencent Cloud WSA page entry of type %s", type(page).__name__)
            continue

        if not isinstance(page_data, dict):
            logger.warning("Skipping Tencent Cloud WSA page entry that is not an object")
            continue

        title = page_data.get("title")
        url = page_data.get("url")
        content = page_data.get("content") or page_data.get("passage") or ""
        result = {
            "title": title if isinstance(title, str) else "",
            "url": url if isinstance(url, str) else "",
            "snippet": content if isinstance(content, str) else "",
        }
        for field in ("date", "site", "score"):
            value = page_data.get(field)
            if isinstance(value, (str, int, float)) and not isinstance(value, bool):
                result[field] = value
        results.append(result)
        if len(results) >= max_results:
            break
    return results


@tool("web_search", parse_docstring=True)
def web_search_tool(query: str, max_results: int = _DEFAULT_MAX_RESULTS) -> str:
    """Search the web using Tencent Cloud Web Search API.

    Args:
        query: Search keywords describing what you want to find.
        max_results: Maximum number of search results to return. Default is 5.
    """

    extras = _get_tool_extras("web_search")
    if "max_results" in extras:
        max_results = extras["max_results"]
    max_results = _coerce_max_results(max_results)
    query = query.strip()
    if not query:
        return _error("Search query must not be empty", query)

    api_key = _get_api_key("web_search", extras=extras)
    if not api_key:
        return _missing_key_error(query, "web_search")

    payload: dict[str, object] = {"Query": query}
    mode = _get_mode(extras=extras)
    if mode is not None:
        payload["Mode"] = mode
    request_count = _request_count(max_results)
    if request_count is not None:
        payload["Cnt"] = request_count

    data, error_json = _search(api_key, payload, query)
    if error_json is not None:
        return error_json
    assert data is not None

    response, request_id, error_json = _get_response(data, query)
    if error_json is not None:
        return error_json
    assert response is not None

    results = _parse_results(response, max_results=max_results)
    if results is None:
        return _error("Tencent Cloud WSA returned an unexpected response format", query, request_id=request_id)
    if not results:
        return _error("No results found", query, request_id=request_id)

    output: dict[str, object] = {
        "query": query,
        "total_results": len(results),
        "results": results,
    }
    if request_id:
        output["request_id"] = request_id
    return json.dumps(output, indent=2, ensure_ascii=False)


class _TencentWsaRetrievalProvider:
    def __init__(self, *, extras: Mapping[str, Any]) -> None:
        self._extras = extras

    async def search(self, request) -> ProviderRetrievalResponse:
        if not request.credential.available or not isinstance(
            request.credential.secret,
            str,
        ):
            raise RetrievalProviderError("configuration_error")
        payload: dict[str, object] = {"Query": request.query}
        mode = _get_mode(extras=self._extras)
        if mode is not None:
            payload["Mode"] = mode
        request_count = _request_count(request.constraints.max_results)
        if request_count is not None:
            payload["Cnt"] = request_count
        data, error_json = await run_blocking_provider_call(
            _search,
            request.credential.secret,
            payload,
            request.query,
            request.constraints.timeout_ms / 1_000,
            request.constraints.max_aggregate_bytes,
            True,
        )
        if error_json is not None:
            try:
                error = str(json.loads(error_json).get("error", ""))
            except (AttributeError, TypeError, ValueError):
                raise RetrievalProviderError("unsafe_response") from None
            if "HTTP 429" in error or "RequestLimitExceeded" in error:
                raise RetrievalProviderError("rate_limited")
            if "HTTP 401" in error or "HTTP 403" in error:
                raise RetrievalProviderError("authentication_failed")
            raise RetrievalProviderError("provider_unavailable")
        if not isinstance(data, dict):
            raise RetrievalProviderError("unsafe_response")
        response, request_id, response_error = _get_response(data, request.query)
        if response_error is not None:
            try:
                error = str(json.loads(response_error).get("error", ""))
            except (AttributeError, TypeError, ValueError):
                raise RetrievalProviderError("unsafe_response") from None
            if "RequestLimitExceeded" in error:
                raise RetrievalProviderError("rate_limited")
            if any(marker in error for marker in ("Auth", "Unauthorized", "Forbidden")):
                raise RetrievalProviderError("authentication_failed")
            if "unexpected response format" in error:
                raise RetrievalProviderError("unsafe_response")
            raise RetrievalProviderError("provider_unavailable")
        if response is None:
            raise RetrievalProviderError("unsafe_response")
        results = _parse_results(
            response,
            max_results=request.constraints.max_results,
        )
        if results is None:
            raise RetrievalProviderError("unsafe_response")
        if results:
            output: dict[str, object] = {
                "total_results": len(results),
                "results": results,
            }
            candidate = json.dumps(output, indent=2, ensure_ascii=False)
        else:
            candidate = json.dumps({"error": "No results found"}, ensure_ascii=False)
        items = tuple(
            ProviderRetrievalItem(
                source_locator=result["url"],
                content=result.get("snippet", ""),
            )
            for result in results
            if isinstance(result.get("url"), str) and result["url"]
        )
        return ProviderRetrievalResponse(
            candidate_result=candidate,
            items=items,
            result_count=len(results),
            # Provider request IDs are not documented as independent of query
            # or tenant data, so the portable observation omits them.
            safe_request_ref=None,
        )


async def _web_search_with_evidence(
    query: str,
    max_results: int = _DEFAULT_MAX_RESULTS,
) -> str:
    if get_active_retrieval_handoff() is None:
        return await asyncio.to_thread(
            web_search_tool.func,
            query,
            max_results,
        )
    app_config = accepted_retrieval_app_config_from_active()
    extras = dict(_get_tool_extras("web_search", app_config=app_config))
    server_max_results = configured_int(
        extras,
        "max_results",
        default=_DEFAULT_MAX_RESULTS,
        maximum=_MAX_RESULTS,
    )
    requested_max_results = _coerce_max_results(max_results)
    query = query.strip()
    api_key = _get_api_key(
        "web_search",
        extras=extras,
        allow_env_fallback=False,
    )
    timeout_ms = configured_timeout_ms(
        extras,
        default_seconds=_REQUEST_TIMEOUT_S,
    )
    policy = RetrievalPolicyV1(
        allowed_providers=("tencent_wsa",),
        allowed_endpoint_origins=(_SEARCH_ENDPOINT,),
        web_domain_allowlist=configured_domains(extras, "allowed_domains"),
        web_domain_denylist=configured_domains(extras, "denied_domains"),
        max_results=server_max_results,
        max_item_bytes=configured_int(
            extras,
            "max_item_bytes",
            default=16 * 1024,
            maximum=1024 * 1024,
        ),
        max_aggregate_bytes=configured_int(
            extras,
            "max_total_bytes",
            default=64 * 1024,
            maximum=8 * 1024 * 1024,
        ),
        timeout_ms=timeout_ms,
        allow_redirects=False,
        source_schemes=("http", "https"),
    )
    accepted = accepted_retrieval_request_from_active(
        query=query,
        credential=ResolvedRetrievalCredentialV1(
            provider_id="tencent_wsa",
            selector_ref="tencent-wsa-web-search",
            secret=api_key,
        ),
        policy=policy,
        requested_constraints=RetrievalRequestConstraintsV1(
            provider_id="tencent_wsa",
            endpoint=_SEARCH_ENDPOINT,
            max_results=requested_max_results,
            timeout_ms=timeout_ms,
        ),
    )
    candidate = await EvidenceBearingRetrievalService().retrieve(
        accepted,
        _TencentWsaRetrievalProvider(extras=extras),
    )
    if not isinstance(candidate.result, str):
        raise RetrievalProviderError("unsafe_response")
    return candidate.result


web_search_tool.coroutine = _web_search_with_evidence
web_search_tool.metadata = {
    **(web_search_tool.metadata or {}),
    RETRIEVAL_TOOL_METADATA_KEY: RetrievalToolDeclarationV1(
        provider_id="tencent_wsa",
        tool_kind="web_search",
        adapter_capability_version="tencent-wsa-http-v1",
    ).to_metadata(),
}
