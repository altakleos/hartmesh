"""``web_fetch`` without a key: the Gateway reads the page itself.

The tool is the profile's keyless default (deploy/compose/config.yaml). It
resolves, checks and pins every address it touches (see ``client.py``),
extracts the article and returns it as Markdown, and stamps its own typed
outcome on the result: ``deerflow_tool_meta`` with ``error_scope`` set from
the transport it saw, never inferred from the words of the result. A refusal
is one bounded sentence that names the address and says whether to try
another source.
"""

from __future__ import annotations

import asyncio
import weakref
from typing import Annotated, Any

from langchain.tools import InjectedToolCallId, tool
from langgraph.types import Command

from deerflow.community.direct_fetch.client import DEFAULT_TIMEOUT_SECONDS, DirectFetchClient, FetchedPage
from deerflow.community.direct_fetch.extraction import InProcessExtractor
from deerflow.community.web_fetch_outcome import FetchRefusal, describe_refusal, refusal_meta, stamped_result, success_meta
from deerflow.config import get_app_config

__all__ = ["PROVIDER_ID", "MAX_RESULT_CHARS", "CONCURRENT_FETCHES", "web_fetch_tool"]

PROVIDER_ID = "direct_http"
#: How many pages one Gateway process reads at once. Unlike the hosted reader
#: this replaced, the work now lands on the Gateway itself: up to 2 MiB of
#: body buffered per fetch and an article extraction that spawns a Node
#: subprocess, inside the tenant profile's own memory, CPU and pid budget. A
#: model that issues several fetch calls in one step -- or two tenants' turns
#: doing so at once -- would otherwise have no ceiling at all. The same bound
#: the sibling ``web_search`` already applies, for the same reason.
CONCURRENT_FETCHES = 4
_slots_by_loop: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore] = weakref.WeakKeyDictionary()


def _fetch_slots() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    slots = _slots_by_loop.get(loop)
    if slots is None:
        slots = asyncio.Semaphore(CONCURRENT_FETCHES)
        _slots_by_loop[loop] = slots
    return slots


#: What the model reads of a page; the same bound the hosted reader had.
MAX_RESULT_CHARS = 4096
_readability = InProcessExtractor()


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
    async with _fetch_slots():
        client = _client_from_config(get_app_config())
        outcome = await client.fetch(url)
        if isinstance(outcome, FetchRefusal):
            return stamped_result(describe_refusal(url, outcome), refusal_meta(outcome), tool_call_id)
        return stamped_result(await _extract(outcome), success_meta(), tool_call_id)
