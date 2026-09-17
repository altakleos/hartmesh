import asyncio
from typing import Annotated

from langchain.tools import InjectedToolCallId, tool
from langgraph.types import Command

from deerflow.community.jina_ai.jina_client import PROVIDER_REFUSAL_STATUSES, JinaClient
from deerflow.community.web_fetch_outcome import FetchRefusal, describe_refusal, refusal_meta, stamped_result
from deerflow.config import get_app_config
from deerflow.utils.readability import ReadabilityExtractor

readability_extractor = ReadabilityExtractor()


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


def _coerce_timeout(value: object, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _coerce_proxy(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    proxy = value.strip()
    return proxy or None


def _provider_refusal(status: int) -> FetchRefusal:
    if status == 429:
        return FetchRefusal("provider", "rate_limited", "the fetch provider is rate-limiting this deployment", status)
    if status == 402:
        return FetchRefusal("provider", "config", "the fetch provider's quota for this deployment is spent", status)
    return FetchRefusal("provider", "auth", "the fetch provider refuses this deployment's requests without a valid key", status)


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
    jina_client = JinaClient()
    timeout = 10
    proxy = None
    trust_env = True
    config = get_app_config().get_tool_config("web_fetch")
    if config is not None:
        timeout = _coerce_timeout(config.model_extra.get("timeout"), timeout)
        proxy = _coerce_proxy(config.model_extra.get("proxy"))
        trust_env = _coerce_bool(config.model_extra.get("trust_env"), trust_env)
    html_content = await jina_client.crawl(url, return_format="html", timeout=timeout, proxy=proxy, trust_env=trust_env)
    if jina_client.last_status in PROVIDER_REFUSAL_STATUSES:
        # The provider refused the caller, not the page: typed as such so the
        # run withdraws the tool instead of trying twelve more addresses.
        refusal = _provider_refusal(jina_client.last_status)
        return stamped_result(describe_refusal(url, refusal), refusal_meta(refusal), tool_call_id)
    if isinstance(html_content, str) and html_content.startswith("Error:"):
        return html_content
    article = await asyncio.to_thread(readability_extractor.extract_article, html_content)
    return article.to_markdown()[:4096]
