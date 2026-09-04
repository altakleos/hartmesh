"""Unit tests for the DDGS community web search tool."""

import json
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from deerflow.community.ddg_search import tools


def test_resolve_ddgs_region_maps_worldwide_chinese_query_for_wikipedia() -> None:
    assert tools._resolve_ddgs_region("\u4e16\u754c\u676f\u65b0\u95fb 2026", "wt-wt", "auto") == "cn-zh"


def test_resolve_ddgs_region_uses_english_fallback_for_worldwide_query() -> None:
    assert tools._resolve_ddgs_region("latest world cup news", "wt-wt", "auto") == "us-en"


def test_resolve_ddgs_region_preserves_worldwide_for_non_wikipedia_backend() -> None:
    assert tools._resolve_ddgs_region("latest world cup news", "wt-wt", "duckduckgo") == "wt-wt"


def test_resolve_ddgs_region_maps_common_ddg_locale_aliases() -> None:
    assert tools._resolve_ddgs_region("\u65e5\u672c \u30cb\u30e5\u30fc\u30b9", "jp-jp", "auto") == "jp-ja"
    assert tools._resolve_ddgs_region("\ud55c\uad6d \ub274\uc2a4", "kr-kr", "auto") == "kr-ko"
    assert tools._resolve_ddgs_region("\u53f0\u7063\u65b0\u805e", "tw-tzh", "auto") == "tw-zh"


def test_search_text_passes_wikipedia_safe_region_to_ddgs(monkeypatch) -> None:
    calls = {}

    class FakeDDGS:
        def __init__(self, timeout: int) -> None:
            calls["timeout"] = timeout

        def text(self, query: str, **kwargs):
            calls["query"] = query
            calls.update(kwargs)
            return [{"title": "Result", "href": "https://example.com", "body": "Snippet"}]

    monkeypatch.setitem(sys.modules, "ddgs", SimpleNamespace(DDGS=FakeDDGS))

    results = tools._search_text("\u4e16\u754c\u676f\u65b0\u95fb 2026", backend="auto")

    assert results == [{"title": "Result", "href": "https://example.com", "body": "Snippet"}]
    assert calls["timeout"] == 30
    assert calls["region"] == "cn-zh"
    assert calls["backend"] == "auto"
    assert "timelimit" not in calls


@pytest.mark.parametrize(
    ("time_range", "expected_timelimit"),
    [
        ("day", "d"),
        ("week", "w"),
        ("month", "m"),
        ("year", "y"),
    ],
)
def test_search_text_maps_time_range_to_ddgs_timelimit(monkeypatch, time_range: str, expected_timelimit: str) -> None:
    calls = {}

    class FakeDDGS:
        def __init__(self, timeout: int) -> None:
            calls["timeout"] = timeout

        def text(self, query: str, **kwargs):
            calls["query"] = query
            calls.update(kwargs)
            return [{"title": "Result", "href": "https://example.com", "body": "Snippet"}]

    monkeypatch.setitem(sys.modules, "ddgs", SimpleNamespace(DDGS=FakeDDGS))

    results = tools._search_text("latest release", backend="duckduckgo", time_range=time_range)

    assert results == [{"title": "Result", "href": "https://example.com", "body": "Snippet"}]
    assert calls["timelimit"] == expected_timelimit


def test_search_text_time_range_replaces_auto_with_filter_capable_backends(monkeypatch) -> None:
    calls = {}

    class FakeDDGS:
        def __init__(self, timeout: int) -> None:
            calls["timeout"] = timeout

        def text(self, query: str, **kwargs):
            calls.update(kwargs)
            return []

    monkeypatch.setitem(sys.modules, "ddgs", SimpleNamespace(DDGS=FakeDDGS))

    tools._search_text("latest release", backend="auto", time_range="week")

    assert calls["backend"] == "brave,duckduckgo,yahoo"
    assert calls["region"] == "wt-wt"
    assert calls["timelimit"] == "w"


@pytest.mark.parametrize(
    ("configured_backend", "expected_backend"),
    [
        ("wikipedia,duckduckgo,yandex", "duckduckgo"),
        ("wikipedia", "brave,duckduckgo,yahoo"),
        ("all", "brave,duckduckgo,yahoo"),
    ],
)
def test_search_text_time_range_excludes_explicit_filter_agnostic_backends(
    monkeypatch,
    configured_backend: str,
    expected_backend: str,
) -> None:
    calls = {}

    class FakeDDGS:
        def __init__(self, timeout: int) -> None:
            calls["timeout"] = timeout

        def text(self, query: str, **kwargs):
            calls.update(kwargs)
            return []

    monkeypatch.setitem(sys.modules, "ddgs", SimpleNamespace(DDGS=FakeDDGS))

    tools._search_text("latest release", backend=configured_backend, time_range="day")

    assert calls["backend"] == expected_backend


def test_web_search_tool_reads_ddgs_options_from_config() -> None:
    with patch("deerflow.community.ddg_search.tools.get_app_config") as mock_config:
        tool_config = MagicMock()
        tool_config.model_extra = {
            "max_results": 3,
            "region": "us-en",
            "safesearch": "off",
            "backend": "auto",
        }
        mock_config.return_value.get_tool_config.return_value = tool_config

        with patch("deerflow.community.ddg_search.tools._search_text") as mock_search:
            mock_search.return_value = [{"title": "Result", "href": "https://example.com", "body": "Snippet"}]

            result = tools.web_search_tool.invoke({"query": "latest news", "max_results": 8, "time_range": "week"})
            parsed = json.loads(result)

    assert parsed["total_results"] == 1
    mock_search.assert_called_once_with(
        query="latest news",
        max_results=3,
        region="us-en",
        safesearch="off",
        backend="auto",
        time_range="week",
    )


def test_evidence_search_owns_fixed_destination_and_disables_redirects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    class FakeEngine:
        search_url = "https://html.duckduckgo.com/html/"
        search_method = "POST"

        def __init__(self, *, timeout: float) -> None:
            calls["engine_timeout"] = timeout
            self.http_client = SimpleNamespace(client=object())

        def search(self, query: str, **kwargs):
            calls["query"] = query
            calls["search"] = kwargs
            self.http_client.request(
                "POST",
                self.search_url,
                data={"q": query},
            )
            return [
                SimpleNamespace(
                    title="Result",
                    href="https://example.com/private/path?q=secret",
                    body="Snippet",
                )
            ]

    class FakeClient:
        def __init__(self, **kwargs) -> None:
            calls["client"] = kwargs

        def request(self, method: str, url: str, **kwargs):
            calls["request"] = {"method": method, "url": url, **kwargs}
            return SimpleNamespace(
                status_code=200,
                content=b"<html></html>",
                text="<html></html>",
                headers={"content-type": "text/html; charset=utf-8"},
            )

    monkeypatch.setattr("ddgs.engines.duckduckgo.Duckduckgo", FakeEngine)
    monkeypatch.setattr("primp.Client", FakeClient)

    results = tools._search_duckduckgo_evidence(
        "private query",
        max_results=2,
        region="us-en",
        safesearch="moderate",
        time_range="week",
        timeout_seconds=3,
    )

    assert results[0]["href"] == "https://example.com/private/path?q=secret"
    assert calls["client"] == {
        "timeout": 3,
        "follow_redirects": False,
        "https_only": True,
        "verify": True,
        "impersonate": "random",
        "impersonate_os": "random",
    }
    assert calls["search"] == {
        "region": "us-en",
        "safesearch": "moderate",
        "timelimit": "w",
    }
    assert calls["request"] == {
        "method": "POST",
        "url": "https://html.duckduckgo.com/html/",
        "data": {"q": "private query"},
        "follow_redirects": False,
    }


def test_evidence_search_fails_closed_if_sdk_destination_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ChangedEngine:
        search_url = "https://redirector.invalid/search"
        search_method = "POST"

        def __init__(self, *, timeout: float) -> None:
            del timeout

    monkeypatch.setattr("ddgs.engines.duckduckgo.Duckduckgo", ChangedEngine)

    with pytest.raises(tools.RetrievalProviderError) as caught:
        tools._search_duckduckgo_evidence(
            "query",
            max_results=1,
            region="us-en",
            safesearch="moderate",
            time_range=None,
            timeout_seconds=3,
        )

    assert caught.value.status == "configuration_error"


def test_evidence_search_classifies_sdk_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    from ddgs.exceptions import TimeoutException

    class TimeoutEngine:
        search_url = "https://html.duckduckgo.com/html/"
        search_method = "POST"

        def __init__(self, *, timeout: float) -> None:
            del timeout
            self.http_client = SimpleNamespace(client=object())

        def search(self, _query: str, **_kwargs):
            raise TimeoutException("private timeout detail")

    monkeypatch.setattr("ddgs.engines.duckduckgo.Duckduckgo", TimeoutEngine)
    monkeypatch.setattr("primp.Client", lambda **_kwargs: object())

    with pytest.raises(tools.RetrievalProviderError) as caught:
        tools._search_duckduckgo_evidence(
            "private query",
            max_results=1,
            region="us-en",
            safesearch="moderate",
            time_range=None,
            timeout_seconds=3,
        )

    assert caught.value.status == "timeout"


def test_evidence_search_rejects_an_unexpected_runtime_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DivertedEngine:
        search_url = "https://html.duckduckgo.com/html/"
        search_method = "POST"

        def __init__(self, *, timeout: float) -> None:
            del timeout
            self.http_client = SimpleNamespace(client=object())

        def search(self, _query: str, **_kwargs):
            self.http_client.request("POST", "https://attacker.invalid/search")
            return []

    network_client = MagicMock()
    monkeypatch.setattr("ddgs.engines.duckduckgo.Duckduckgo", DivertedEngine)
    monkeypatch.setattr("primp.Client", lambda **_kwargs: network_client)

    with pytest.raises(tools.RetrievalProviderError) as caught:
        tools._search_duckduckgo_evidence(
            "private query",
            max_results=1,
            region="us-en",
            safesearch="moderate",
            time_range=None,
            timeout_seconds=3,
        )

    assert caught.value.status == "configuration_error"
    network_client.request.assert_not_called()


def test_evidence_search_bounds_raw_html_before_parsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeEngine:
        search_url = "https://html.duckduckgo.com/html/"
        search_method = "POST"

        def __init__(self, *, timeout: float) -> None:
            del timeout
            self.http_client = SimpleNamespace(client=object())

        def search(self, _query: str, **_kwargs):
            self.http_client.request("POST", self.search_url)
            raise AssertionError("oversized response must fail before parsing")

    response = SimpleNamespace(
        status_code=200,
        content=b"x" * 65,
        text="x" * 65,
        headers={"content-type": "text/html"},
    )
    network_client = MagicMock()
    network_client.request.return_value = response
    monkeypatch.setattr("ddgs.engines.duckduckgo.Duckduckgo", FakeEngine)
    monkeypatch.setattr("primp.Client", lambda **_kwargs: network_client)

    with pytest.raises(tools.RetrievalProviderError) as caught:
        tools._search_duckduckgo_evidence(
            "private query",
            max_results=1,
            region="us-en",
            safesearch="moderate",
            time_range=None,
            timeout_seconds=3,
            max_response_bytes=64,
        )

    assert caught.value.status == "oversized_response"
