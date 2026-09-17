import logging
from typing import Any

import httpx

from deerflow.community.search_time_range import SearchTimeRange

logger = logging.getLogger(__name__)


class SearxngClient:
    """Client for SearXNG meta search engine API."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    async def search(
        self,
        query: str,
        max_results: int = 5,
        categories: list[str] | None = None,
        time_range: SearchTimeRange | None = None,
    ) -> list[dict[str, Any]]:
        """Search the web using SearXNG.

        Args:
            query: The search query.
            max_results: Maximum number of results to return.
            categories: Search categories to use.
            time_range: Optional relative publication/update window.

        Returns:
            List of search result dictionaries.
        """
        params: dict[str, Any] = {
            "q": query,
            "format": "json",
            "language": "auto",
            "pageno": 1,
        }
        if max_results:
            params["limit"] = max_results
        if categories:
            params["categories"] = ",".join(categories)
        if time_range is not None:
            params["time_range"] = time_range

        # The query is a person's question; it stays out of the log at every level.
        logger.debug("Searching SearXNG at %s", self.base_url)
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                # POST, not GET: SearXNG accepts either, and a GET would put
                # the person's question in a URL that httpx logs at INFO and
                # that any proxy or access log would keep. In the body it
                # stays between the Gateway and the search service.
                resp = await client.post(
                    f"{self.base_url}/search",
                    data=params,
                    headers={
                        "User-Agent": "Mozilla/5.0 (compatible; DeerFlow/1.0)",
                        "Accept": "application/json",
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                results = data.get("results", [])
                return results[:max_results] if max_results else results
        except httpx.HTTPStatusError as e:
            # The exception text carries the request URL, and the URL carries
            # the query. Log the status alone.
            logger.error("SearXNG search returned error status %s", e.response.status_code)
            raise
        except httpx.RequestError as e:
            logger.error("SearXNG search request failed: %s", type(e).__name__)
            raise
        except Exception as e:
            logger.error("An unexpected error occurred during SearXNG search: %s", type(e).__name__)
            raise
