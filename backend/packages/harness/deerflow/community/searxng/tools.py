import asyncio
import json
import logging
import weakref
from typing import Any

from langchain.tools import tool

from deerflow.community.search_time_range import SearchTimeRange
from deerflow.config import get_app_config

from .searxng_client import SearxngClient

logger = logging.getLogger(__name__)

# Returned in place of results when the search service cannot be reached or
# answers with an error. Written for the model to act on: answer from what it
# already knows, and tell the person plainly that the web was not checked.
SEARCH_UNAVAILABLE_MESSAGE = "Web search is unavailable right now: the search service did not answer. Do not retry it this turn. Answer from what you already know, and tell the person clearly that you could not check the web for this."

# A result page is text somebody else wrote. These bound what of it reaches
# the model: only web addresses, and no field long enough to carry a page.
MAX_TITLE_CHARS = 256
MAX_URL_CHARS = 2048
MAX_SNIPPET_CHARS = 2048
_WEB_SCHEMES = ("http://", "https://")

# How many searches one Gateway process has in flight at once. A research
# fan-out that asks dozens of questions in a minute is what gets a tenant's
# address gated by the engines; the bound keeps the burst to what the
# instance and the engines were measured under.
CONCURRENT_SEARCHES = 4
_slots_by_loop: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore]" = weakref.WeakKeyDictionary()


def _search_slots() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    slots = _slots_by_loop.get(loop)
    if slots is None:
        slots = asyncio.Semaphore(CONCURRENT_SEARCHES)
        _slots_by_loop[loop] = slots
    return slots


def _get_tool_config(tool_name: str) -> dict | None:
    """Get tool config extras safely, returning None if not configured."""
    config = get_app_config().get_tool_config(tool_name)
    if config is None:
        return None
    extras = config.model_extra
    return extras if extras is not None else {}


def _get_searxng_client(tool_name: str = "web_search") -> SearxngClient:
    cfg = _get_tool_config(tool_name)
    base_url = "http://localhost:8088"
    if cfg is not None:
        base_url = cfg.get("base_url", base_url)
    return SearxngClient(base_url=base_url)


def _configured_max_results(tool_name: str, default: int = 5) -> int:
    cfg = _get_tool_config(tool_name)
    if cfg is None:
        return default
    raw = cfg.get("max_results", default)
    return int(raw) if not isinstance(raw, int) else raw


def normalize_results(results: list[dict[str, Any]], max_results: int) -> list[dict[str, str]]:
    """Keep web results only, each field bounded, at most *max_results* of them."""

    normalized: list[dict[str, str]] = []
    for result in results:
        url = str(result.get("url") or "")
        if not url.lower().startswith(_WEB_SCHEMES):
            continue
        normalized.append(
            {
                "title": str(result.get("title") or "")[:MAX_TITLE_CHARS],
                "url": url[:MAX_URL_CHARS],
                "snippet": str(result.get("content") or "")[:MAX_SNIPPET_CHARS],
            }
        )
        if len(normalized) >= max_results:
            break
    return normalized


@tool("web_search", parse_docstring=True)
async def web_search_tool(query: str, time_range: SearchTimeRange | None = None) -> str:
    """Search the web using SearXNG.

    Args:
        query: The query to search for.
        time_range: Optional relative publication/update window. Use only when the request requires recent results.
    """
    # Configuration errors are ours and raise; only the service's failure
    # becomes the sentence below.
    max_results = _configured_max_results("web_search")
    client = _get_searxng_client("web_search")
    search_kwargs: dict[str, object] = {"max_results": max_results}
    if time_range is not None:
        search_kwargs["time_range"] = time_range

    try:
        async with _search_slots():
            results = await client.search(query, **search_kwargs)
    except Exception as e:
        # The person asked a question and is waiting. A raw exception here
        # reads to the model as something to retry or apologize for, and it
        # would echo the query back into the transcript. Say what happened
        # in one sentence and what to do instead, so the turn ends with an
        # answer that is honest about what it could not check.
        logger.error("web_search (SearXNG) failed: %s", type(e).__name__)
        return SEARCH_UNAVAILABLE_MESSAGE

    return json.dumps(normalize_results(results, max_results), indent=2, ensure_ascii=False)


# Returned in place of image results for the same reason as the sentence
# above, worded for a tool whose caller is about to generate an image.
IMAGE_SEARCH_UNAVAILABLE_MESSAGE = "Image search is unavailable right now: the search service did not answer. Do not retry it this turn. Continue without reference images, and tell the person you could not look any up."


def normalize_image_results(results: list[dict[str, Any]], max_results: int) -> list[dict[str, str]]:
    """Keep results carrying a usable image address, each field bounded."""

    normalized: list[dict[str, str]] = []
    for result in results:
        image_url = str(result.get("img_src") or "")
        if not image_url.lower().startswith(_WEB_SCHEMES):
            # A result with no direct image address is no use to a caller
            # that is about to hand one to an image generator.
            continue
        thumbnail = str(result.get("thumbnail_src") or result.get("thumbnail") or "")
        normalized.append(
            {
                "title": str(result.get("title") or "")[:MAX_TITLE_CHARS],
                "image_url": image_url[:MAX_URL_CHARS],
                "thumbnail_url": thumbnail[:MAX_URL_CHARS] if thumbnail.lower().startswith(_WEB_SCHEMES) else "",
                "source_url": str(result.get("url") or "")[:MAX_URL_CHARS],
            }
        )
        if len(normalized) >= max_results:
            break
    return normalized


@tool("image_search", parse_docstring=True)
async def image_search_tool(query: str, max_results: int | None = None) -> str:
    """Search for images online. Use this tool BEFORE image generation to find reference images for characters, portraits, objects, scenes, or any content requiring visual accuracy.

    **When to use:**
    - Before generating character/portrait images: search for similar poses, expressions, styles
    - Before generating specific objects/products: search for accurate visual references
    - Before generating scenes/locations: search for architectural or environmental references
    - Before generating fashion/clothing: search for style and detail references

    The returned image URLs can be used as reference images in image generation to significantly improve quality.

    Args:
        query: Search keywords describing the images you want to find. Be specific for better results (e.g., "Japanese woman street photography 1990s" instead of just "woman").
        max_results: Maximum number of images to return. Defaults to the deployment's configured limit.
    """
    configured = _configured_max_results("image_search")
    limit = configured if max_results is None else max(1, min(int(max_results), configured))
    client = _get_searxng_client("image_search")

    try:
        async with _search_slots():
            results = await client.search(query, max_results=limit, categories=["images"])
    except Exception as e:
        logger.error("image_search (SearXNG) failed: %s", type(e).__name__)
        return IMAGE_SEARCH_UNAVAILABLE_MESSAGE

    normalized = normalize_image_results(results, limit)
    if not normalized:
        # Distinct from the failure above: the service answered and had
        # nothing usable, which a different query might fix.
        return json.dumps({"results": [], "note": "No images with a usable address were found for this query."}, ensure_ascii=False)

    return json.dumps(
        {
            "results": normalized,
            "usage_hint": "Use the 'image_url' values as reference images in image generation. Download them first if needed.",
        },
        indent=2,
        ensure_ascii=False,
    )
