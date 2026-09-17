"""Tests for SearXNG community tools."""

import json
from unittest.mock import MagicMock, patch

import pytest

from deerflow.community.searxng import tools
from deerflow.community.searxng.searxng_client import SearxngClient


class AsyncMock(MagicMock):
    """Mock that supports async call."""

    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


@pytest.mark.asyncio
class TestSearxngClient:
    """Tests for the SearxngClient class."""

    async def test_search_success(self):
        """Search returns normalized results."""
        results_data = {
            "results": [
                {"title": "Page 1", "url": "https://example.com/1", "content": "Snippet 1"},
                {"title": "Page 2", "url": "https://example.com/2", "content": "Snippet 2"},
            ]
        }

        with patch("deerflow.community.searxng.searxng_client.httpx.AsyncClient") as mock_cls:
            mock_ctx = MagicMock()
            mock_cls.return_value.__aenter__.return_value = mock_ctx

            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = results_data
            mock_resp.raise_for_status.return_value = None
            mock_ctx.post = AsyncMock(return_value=mock_resp)

            client = SearxngClient(base_url="http://searxng:8080")
            result = await client.search("test query", max_results=5)

            assert len(result) == 2
            assert result[0]["title"] == "Page 1"
            assert result[1]["url"] == "https://example.com/2"

    async def test_search_empty_results(self):
        """Search returns empty list when no results."""
        with patch("deerflow.community.searxng.searxng_client.httpx.AsyncClient") as mock_cls:
            mock_ctx = MagicMock()
            mock_cls.return_value.__aenter__.return_value = mock_ctx

            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = {"results": []}
            mock_resp.raise_for_status.return_value = None
            mock_ctx.post = AsyncMock(return_value=mock_resp)

            client = SearxngClient(base_url="http://searxng:8080")
            result = await client.search("empty query")
            assert result == []

    async def test_search_http_error(self):
        """Search raises on HTTP error."""
        with patch("deerflow.community.searxng.searxng_client.httpx.AsyncClient") as mock_cls:
            mock_ctx = MagicMock()
            mock_cls.return_value.__aenter__.return_value = mock_ctx

            import httpx

            mock_resp = MagicMock()
            mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError("403 Forbidden", request=MagicMock(), response=MagicMock())
            mock_ctx.post = AsyncMock(return_value=mock_resp)

            client = SearxngClient(base_url="http://searxng:8080")
            with pytest.raises(httpx.HTTPStatusError):
                await client.search("blocked query")

    async def test_search_request_error(self):
        """Search raises on request error."""
        with patch("deerflow.community.searxng.searxng_client.httpx.AsyncClient") as mock_cls:
            mock_ctx = MagicMock()
            mock_cls.return_value.__aenter__.return_value = mock_ctx

            import httpx

            mock_ctx.post = AsyncMock(side_effect=httpx.RequestError("Connection refused"))

            client = SearxngClient(base_url="http://searxng:8080")
            with pytest.raises(httpx.RequestError):
                await client.search("unreachable query")

    async def test_search_with_categories(self):
        """Search passes categories parameter."""
        with patch("deerflow.community.searxng.searxng_client.httpx.AsyncClient") as mock_cls:
            mock_ctx = MagicMock()
            mock_cls.return_value.__aenter__.return_value = mock_ctx

            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = {"results": []}
            mock_resp.raise_for_status.return_value = None
            mock_ctx.post = AsyncMock(return_value=mock_resp)

            client = SearxngClient(base_url="http://searxng:8080")
            await client.search("test", categories=["news", "science"])

            call_kwargs = mock_ctx.post.call_args.kwargs
            assert call_kwargs["data"]["categories"] == "news,science"

    async def test_search_with_time_range(self):
        """Search passes a native relative time range."""
        with patch("deerflow.community.searxng.searxng_client.httpx.AsyncClient") as mock_cls:
            mock_ctx = MagicMock()
            mock_cls.return_value.__aenter__.return_value = mock_ctx

            mock_resp = MagicMock()
            mock_resp.json.return_value = {"results": []}
            mock_resp.raise_for_status.return_value = None
            mock_ctx.post = AsyncMock(return_value=mock_resp)

            client = SearxngClient(base_url="http://searxng:8080")
            await client.search("latest release", time_range="month")

            params = mock_ctx.post.call_args.kwargs["data"]
            assert params["time_range"] == "month"

    async def test_search_without_time_range_omits_parameter(self):
        """The default request shape remains unchanged."""
        with patch("deerflow.community.searxng.searxng_client.httpx.AsyncClient") as mock_cls:
            mock_ctx = MagicMock()
            mock_cls.return_value.__aenter__.return_value = mock_ctx

            mock_resp = MagicMock()
            mock_resp.json.return_value = {"results": []}
            mock_resp.raise_for_status.return_value = None
            mock_ctx.post = AsyncMock(return_value=mock_resp)

            client = SearxngClient(base_url="http://searxng:8080")
            await client.search("stable documentation")

            params = mock_ctx.post.call_args.kwargs["data"]
            assert "time_range" not in params


@pytest.mark.asyncio
class TestSearxngTools:
    """Tests for the SearXNG tool functions."""

    @patch("deerflow.community.searxng.tools._get_searxng_client")
    async def test_web_search_tool_success(self, mock_get_client):
        """web_search_tool returns JSON results."""
        mock_client = MagicMock()
        mock_client.search = AsyncMock(
            return_value=[
                {"title": "Result 1", "url": "https://example.com/1", "content": "Desc 1"},
            ]
        )
        mock_get_client.return_value = mock_client

        with patch("deerflow.community.searxng.tools._get_tool_config", return_value=None):
            result = await tools.web_search_tool.ainvoke("test query")

        data = json.loads(result)
        assert len(data) == 1
        assert data[0]["title"] == "Result 1"

    @patch("deerflow.community.searxng.tools._get_searxng_client")
    async def test_web_search_tool_error(self, mock_get_client):
        """A failure comes back as the fixed sentence, never as the exception or the query."""
        mock_client = MagicMock()
        mock_client.search = AsyncMock(side_effect=Exception("API error"))
        mock_get_client.return_value = mock_client

        with patch("deerflow.community.searxng.tools._get_tool_config", return_value=None):
            result = await tools.web_search_tool.ainvoke("test query")

        assert result == tools.SEARCH_UNAVAILABLE_MESSAGE
        assert "API error" not in result and "test query" not in result

    @patch("deerflow.community.searxng.tools._get_searxng_client")
    async def test_web_search_tool_with_max_results(self, mock_get_client):
        """web_search_tool respects max_results config."""
        mock_client = MagicMock()
        # Return 10 results; the tool should slice to max_results=3
        mock_client.search = AsyncMock(return_value=[{"title": f"Result {i}", "url": f"https://example.com/{i}", "content": f"Desc {i}"} for i in range(10)])
        mock_get_client.return_value = mock_client

        with patch("deerflow.community.searxng.tools._get_tool_config", return_value={"max_results": "3"}):
            await tools.web_search_tool.ainvoke("test query")

        # Verify that search was called with max_results=3 (coerced from string)
        mock_client.search.assert_called_once()
        call_kwargs = mock_client.search.call_args.kwargs
        assert call_kwargs["max_results"] == 3

    @patch("deerflow.community.searxng.tools._get_searxng_client")
    async def test_web_search_tool_forwards_time_range(self, mock_get_client):
        """web_search_tool forwards the requested relative time range."""
        mock_client = MagicMock()
        mock_client.search = AsyncMock(return_value=[])
        mock_get_client.return_value = mock_client

        with patch("deerflow.community.searxng.tools._get_tool_config", return_value=None):
            await tools.web_search_tool.ainvoke({"query": "latest release", "time_range": "week"})

        mock_client.search.assert_called_once_with("latest release", max_results=5, time_range="week")


@pytest.mark.asyncio
async def test_a_search_that_cannot_be_served_tells_the_model_what_to_do() -> None:
    """The person is waiting on an answer, not on a retry loop.

    A connection failure used to come back as a JSON error carrying the
    query, which the model read as something to retry or apologize for. The
    result is now one fixed sentence: stop retrying this turn, answer from
    what is known, and say plainly that the web was not checked.
    """

    with patch("deerflow.community.searxng.tools._get_tool_config", return_value={}), patch("deerflow.community.searxng.tools._get_searxng_client") as client_factory:
        client = MagicMock()
        client.search = AsyncMock(side_effect=ConnectionError("refused"))
        client_factory.return_value = client

        result = await tools.web_search_tool.ainvoke({"query": "VAT rate Germany 2026"})

    assert result == tools.SEARCH_UNAVAILABLE_MESSAGE
    assert "VAT rate" not in result, "the query never comes back through the failure path"
    assert "do not retry" in result.lower()
    assert "could not check the web" in result.lower()


@pytest.mark.asyncio
async def test_a_served_search_still_returns_its_results() -> None:
    with patch("deerflow.community.searxng.tools._get_tool_config", return_value={}), patch("deerflow.community.searxng.tools._get_searxng_client") as client_factory:
        client = MagicMock()
        client.search = AsyncMock(return_value=[{"title": "Paris", "url": "https://example.org/paris", "content": "Capital of France"}])
        client_factory.return_value = client

        result = await tools.web_search_tool.ainvoke({"query": "capital of France"})

    assert json.loads(result) == [{"title": "Paris", "url": "https://example.org/paris", "snippet": "Capital of France"}]


# ── What reaches the log, and what reaches the model ─────────────────────


def _mock_transport(status: int, body: bytes = b"", content_type: str = "application/json"):
    import httpx

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=body, headers={"Content-Type": content_type}, request=request)

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_an_error_status_is_logged_without_the_query(caplog: pytest.LogCaptureFixture) -> None:
    """An httpx status error's text is the request URL, and the URL carries ``q=``.

    A SearXNG that answers 503 under load must not leave every question a
    tenant asked in the Gateway's error log.
    """

    import httpx

    real_async_client = httpx.AsyncClient

    def client_with_transport(**kwargs):
        return real_async_client(transport=_mock_transport(503), **kwargs)

    with patch("deerflow.community.searxng.searxng_client.httpx.AsyncClient", side_effect=client_with_transport):
        client = SearxngClient(base_url="http://searxng:8080")
        with caplog.at_level("DEBUG", logger="deerflow.community.searxng"):
            with pytest.raises(httpx.HTTPStatusError):
                await client.search("VAT rate Germany 2026 secret client name")

    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "VAT rate" not in logged and "secret+client" not in logged and "q=" not in logged, logged
    assert "503" in logged, "the operator still learns what the service answered"


@pytest.mark.asyncio
async def test_a_connection_failure_is_logged_by_type_only(caplog: pytest.LogCaptureFixture) -> None:
    import httpx

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("All connection attempts failed", request=request)

    real_async_client = httpx.AsyncClient

    def client_with_transport(**kwargs):
        return real_async_client(transport=httpx.MockTransport(refuse), **kwargs)

    with patch("deerflow.community.searxng.searxng_client.httpx.AsyncClient", side_effect=client_with_transport):
        client = SearxngClient(base_url="http://searxng:8080")
        with caplog.at_level("DEBUG", logger="deerflow.community.searxng"):
            with pytest.raises(httpx.RequestError):
                await client.search("who is my competitor")

    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "competitor" not in logged, logged
    assert "ConnectError" in logged, logged


def test_results_reaching_the_model_are_web_addresses_with_bounded_fields() -> None:
    """A result page is text somebody else wrote; the tool bounds what of it the model sees."""

    results = [
        {"title": "javascript scheme", "url": "javascript:alert(1)", "content": "dropped"},
        {"title": "no url", "content": "dropped"},
        {"title": "T" * 1000, "url": "https://example.org/" + "p" * 5000, "content": "S" * 10000},
        {"title": "ftp", "url": "ftp://example.org/file", "content": "dropped"},
        {"title": "second", "url": "http://example.org/2", "content": "kept"},
        {"title": "third", "url": "https://example.org/3", "content": "beyond max_results"},
    ]

    normalized = tools.normalize_results(results, max_results=2)

    assert [item["title"][:6] for item in normalized] == ["TTTTTT", "second"]
    assert len(normalized[0]["title"]) == tools.MAX_TITLE_CHARS
    assert len(normalized[0]["url"]) == tools.MAX_URL_CHARS
    assert len(normalized[0]["snippet"]) == tools.MAX_SNIPPET_CHARS
    assert normalized[1] == {"title": "second", "url": "http://example.org/2", "snippet": "kept"}


@pytest.mark.asyncio
async def test_a_configuration_error_is_ours_and_raises() -> None:
    """Only the service's failure becomes the unavailable sentence.

    A malformed ``max_results`` would otherwise tell the model the service
    did not answer when it was never asked.
    """

    with patch("deerflow.community.searxng.tools._get_tool_config", return_value={"max_results": "five"}), patch("deerflow.community.searxng.tools._get_searxng_client") as client_factory:
        client_factory.return_value = MagicMock()
        with pytest.raises(ValueError):
            await tools.web_search_tool.ainvoke({"query": "anything"})


@pytest.mark.asyncio
async def test_searches_in_flight_per_process_are_bounded() -> None:
    """A research fan-out must not hit the engines with everything at once."""

    import asyncio

    in_flight = 0
    peak = 0
    started = asyncio.Event()

    async def slow_search(query: str, **kwargs):
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        started.set()
        await asyncio.sleep(0.02)
        in_flight -= 1
        return []

    with patch("deerflow.community.searxng.tools._get_tool_config", return_value={}), patch("deerflow.community.searxng.tools._get_searxng_client") as client_factory:
        client = MagicMock()
        client.search = slow_search
        client_factory.return_value = client
        await asyncio.gather(*(tools.web_search_tool.ainvoke({"query": f"q{i}"}) for i in range(3 * tools.CONCURRENT_SEARCHES)))

    assert started.is_set()
    assert peak == tools.CONCURRENT_SEARCHES, peak


@pytest.mark.asyncio
async def test_the_question_never_travels_in_a_url() -> None:
    """httpx logs every request line at INFO, and a GET would carry ``q=``."""

    import httpx

    seen: dict[str, object] = {}

    def capture(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["url"] = str(request.url)
        seen["body"] = request.content.decode()
        return httpx.Response(200, json={"results": []}, request=request)

    real_async_client = httpx.AsyncClient

    def client_with_transport(**kwargs):
        return real_async_client(transport=httpx.MockTransport(capture), **kwargs)

    with patch("deerflow.community.searxng.searxng_client.httpx.AsyncClient", side_effect=client_with_transport):
        await SearxngClient(base_url="http://searxng:8080").search("who are my competitors in Leeds")

    assert seen["method"] == "POST"
    assert "competitors" not in str(seen["url"]), seen["url"]
    assert "competitors" in str(seen["body"])


# ── image_search ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_image_search_asks_the_images_category_and_returns_usable_addresses() -> None:
    """The caller is about to hand these to an image generator."""

    with patch("deerflow.community.searxng.tools._get_tool_config", return_value={"max_results": 3}), patch("deerflow.community.searxng.tools._get_searxng_client") as client_factory:
        client = MagicMock()
        client.search = AsyncMock(
            return_value=[
                {"title": "A", "img_src": "https://example.org/a.jpg", "thumbnail_src": "https://example.org/a-t.jpg", "url": "https://example.org/a"},
                # No direct image address: useless to the caller, dropped.
                {"title": "B", "url": "https://example.org/b"},
                {"title": "C", "img_src": "https://example.org/c.jpg", "url": "https://example.org/c"},
            ]
        )
        client_factory.return_value = client

        result = await tools.image_search_tool.ainvoke({"query": "espresso machine stainless steel"})

    client.search.assert_called_once_with("espresso machine stainless steel", max_results=3, categories=["images"])
    payload = json.loads(result)
    assert [item["image_url"] for item in payload["results"]] == ["https://example.org/a.jpg", "https://example.org/c.jpg"]
    assert payload["results"][1]["thumbnail_url"] == "", "a thumbnail is optional; a missing one is not a broken address"
    assert "usage_hint" in payload


@pytest.mark.asyncio
async def test_image_search_never_exceeds_the_deployment_s_limit() -> None:
    """A model asking for fifty references must not make fifty a tenant's problem."""

    with patch("deerflow.community.searxng.tools._get_tool_config", return_value={"max_results": 5}), patch("deerflow.community.searxng.tools._get_searxng_client") as client_factory:
        client = MagicMock()
        client.search = AsyncMock(return_value=[])
        client_factory.return_value = client

        await tools.image_search_tool.ainvoke({"query": "anything", "max_results": 50})

    assert client.search.call_args.kwargs["max_results"] == 5


@pytest.mark.asyncio
async def test_a_down_service_tells_the_image_caller_to_continue_without_references() -> None:
    with patch("deerflow.community.searxng.tools._get_tool_config", return_value={}), patch("deerflow.community.searxng.tools._get_searxng_client") as client_factory:
        client = MagicMock()
        client.search = AsyncMock(side_effect=ConnectionError("refused"))
        client_factory.return_value = client

        result = await tools.image_search_tool.ainvoke({"query": "portrait reference 1990s"})

    assert result == tools.IMAGE_SEARCH_UNAVAILABLE_MESSAGE
    assert "portrait reference" not in result
    assert "continue without reference images" in result.lower()


@pytest.mark.asyncio
async def test_an_answered_image_search_with_nothing_usable_is_not_an_outage() -> None:
    """A service that answered and had nothing is a different thing to say."""

    with patch("deerflow.community.searxng.tools._get_tool_config", return_value={}), patch("deerflow.community.searxng.tools._get_searxng_client") as client_factory:
        client = MagicMock()
        client.search = AsyncMock(return_value=[{"title": "no address", "url": "https://example.org/x"}])
        client_factory.return_value = client

        result = await tools.image_search_tool.ainvoke({"query": "obscure subject"})

    assert result != tools.IMAGE_SEARCH_UNAVAILABLE_MESSAGE
    payload = json.loads(result)
    assert payload["results"] == []
    assert "No images" in payload["note"]
