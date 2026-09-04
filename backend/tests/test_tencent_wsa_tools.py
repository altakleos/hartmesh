"""Unit tests for the Tencent Cloud Web Search API community provider."""

import json
from unittest.mock import MagicMock, patch

import httpx
import pytest


@pytest.fixture(autouse=True)
def reset_api_key_warned():
    import deerflow.community.tencent_wsa.tools as wsa

    wsa._api_key_warned = set()
    yield
    wsa._api_key_warned = set()


def _tool_config(extras: dict | None) -> MagicMock:
    config = MagicMock()
    config.model_extra = extras
    return config


def _response(pages: list[object] | None = None, **extra: object) -> dict:
    response: dict[str, object] = {"RequestId": "request-123"}
    if pages is not None:
        response["Pages"] = pages
    response.update(extra)
    return {"Response": response}


def _mock_http_client(response: MagicMock):
    if not isinstance(response.content, (bytes, bytearray)):
        try:
            response.content = json.dumps(response.json.return_value).encode("utf-8")
        except TypeError:
            response.content = b""
    if not isinstance(response.headers, dict):
        response.headers = {"content-type": "application/json"}
    client = MagicMock()
    client.post.return_value = response
    context_manager = MagicMock()
    context_manager.__enter__.return_value = client
    context_manager.__exit__.return_value = False
    return client, context_manager


class TestTencentWsaApiKey:
    def test_config_key_takes_precedence_over_environment(self, monkeypatch):
        monkeypatch.setenv("TENCENTCLOUD_WSA_APIKEY", "environment-key")
        with patch("deerflow.community.tencent_wsa.tools.get_app_config") as get_config:
            get_config.return_value.get_tool_config.return_value = _tool_config({"api_key": "config-key"})

            from deerflow.community.tencent_wsa.tools import _get_api_key

            assert _get_api_key() == "config-key"

    def test_environment_key_is_used_as_fallback(self, monkeypatch):
        monkeypatch.setenv("TENCENTCLOUD_WSA_APIKEY", "environment-key")
        with patch("deerflow.community.tencent_wsa.tools.get_app_config") as get_config:
            get_config.return_value.get_tool_config.return_value = _tool_config({"api_key": " "})

            from deerflow.community.tencent_wsa.tools import _get_api_key

            assert _get_api_key() == "environment-key"

    def test_environment_fallback_can_be_disabled_for_accepted_runs(
        self,
        monkeypatch,
    ):
        monkeypatch.setenv("TENCENTCLOUD_WSA_APIKEY", "late-environment-key")

        from deerflow.community.tencent_wsa.tools import _get_api_key

        assert _get_api_key(extras={}, allow_env_fallback=False) is None

    def test_missing_key_returns_a_structured_error(self, monkeypatch):
        monkeypatch.delenv("TENCENTCLOUD_WSA_APIKEY", raising=False)
        with patch("deerflow.community.tencent_wsa.tools.get_app_config") as get_config:
            get_config.return_value.get_tool_config.return_value = _tool_config({})

            from deerflow.community.tencent_wsa.tools import web_search_tool

            result = json.loads(web_search_tool.run({"query": "腾讯云"}))

        assert result == {
            "error": "TENCENTCLOUD_WSA_APIKEY is not configured",
            "query": "腾讯云",
        }


class TestTencentWsaSearch:
    def test_search_normalizes_documented_pages_and_honors_config(self):
        page_one = json.dumps(
            {
                "title": "第一条",
                "url": "https://example.com/one",
                "passage": "摘要一",
                "date": "2026-08-27",
                "site": "示例站点",
                "score": 0.9,
            }
        )
        page_two = json.dumps(
            {
                "title": "第二条",
                "url": "https://example.com/two",
                "content": "动态摘要二",
            }
        )
        http_response = MagicMock()
        http_response.json.return_value = _response([page_one, page_two])
        client, context_manager = _mock_http_client(http_response)

        with (
            patch("deerflow.community.tencent_wsa.tools.get_app_config") as get_config,
            patch("deerflow.community.tencent_wsa.tools.httpx.Client", return_value=context_manager),
        ):
            get_config.return_value.get_tool_config.return_value = _tool_config({"api_key": "test-key", "max_results": 1, "mode": 2})

            from deerflow.community.tencent_wsa.tools import web_search_tool

            result = json.loads(web_search_tool.run({"query": "  腾讯云搜索  ", "max_results": 99}))

        client.post.assert_called_once_with(
            "https://api.wsa.cloud.tencent.com/SearchPro",
            headers={
                "Authorization": "Bearer test-key",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={"Query": "腾讯云搜索", "Mode": 2},
        )
        assert get_config.call_count == 1
        assert result == {
            "query": "腾讯云搜索",
            "total_results": 1,
            "request_id": "request-123",
            "results": [
                {
                    "title": "第一条",
                    "url": "https://example.com/one",
                    "snippet": "摘要一",
                    "date": "2026-08-27",
                    "site": "示例站点",
                    "score": 0.9,
                }
            ],
        }

    def test_search_omits_mode_when_not_configured(self):
        http_response = MagicMock()
        http_response.json.return_value = _response([json.dumps({"title": "结果", "passage": "摘要"})])
        client, context_manager = _mock_http_client(http_response)

        with (
            patch("deerflow.community.tencent_wsa.tools.get_app_config") as get_config,
            patch("deerflow.community.tencent_wsa.tools.httpx.Client", return_value=context_manager),
        ):
            get_config.return_value.get_tool_config.return_value = _tool_config({"api_key": "test-key"})

            from deerflow.community.tencent_wsa.tools import web_search_tool

            web_search_tool.run({"query": "腾讯云"})

        assert client.post.call_args.kwargs["json"] == {"Query": "腾讯云"}

    def test_search_requests_supported_cnt_for_more_than_default_results(self):
        pages = [json.dumps({"title": f"结果 {index}", "passage": "摘要"}) for index in range(20)]
        http_response = MagicMock()
        http_response.json.return_value = _response(pages)
        client, context_manager = _mock_http_client(http_response)

        with (
            patch("deerflow.community.tencent_wsa.tools.get_app_config") as get_config,
            patch("deerflow.community.tencent_wsa.tools.httpx.Client", return_value=context_manager),
        ):
            get_config.return_value.get_tool_config.return_value = _tool_config({"api_key": "test-key"})

            from deerflow.community.tencent_wsa.tools import web_search_tool

            result = json.loads(web_search_tool.run({"query": "腾讯云", "max_results": 20}))

        assert client.post.call_args.kwargs["json"] == {"Query": "腾讯云", "Cnt": 20}
        assert result["total_results"] == 20

    def test_empty_query_does_not_call_paid_api(self):
        with (
            patch("deerflow.community.tencent_wsa.tools.get_app_config") as get_config,
            patch("deerflow.community.tencent_wsa.tools.httpx.Client") as client,
        ):
            get_config.return_value.get_tool_config.return_value = _tool_config({"api_key": "test-key"})

            from deerflow.community.tencent_wsa.tools import web_search_tool

            result = json.loads(web_search_tool.run({"query": "  "}))

        assert result == {"error": "Search query must not be empty", "query": ""}
        client.assert_not_called()

    def test_search_skips_malformed_pages_without_losing_valid_results(self):
        http_response = MagicMock()
        http_response.json.return_value = _response(["not-json", 42, json.dumps({"title": "有效结果", "url": "https://example.com", "passage": "摘要"})])
        _, context_manager = _mock_http_client(http_response)

        with (
            patch("deerflow.community.tencent_wsa.tools.get_app_config") as get_config,
            patch("deerflow.community.tencent_wsa.tools.httpx.Client", return_value=context_manager),
        ):
            get_config.return_value.get_tool_config.return_value = _tool_config({"api_key": "test-key"})

            from deerflow.community.tencent_wsa.tools import web_search_tool

            result = json.loads(web_search_tool.run({"query": "腾讯云"}))

        assert result["total_results"] == 1
        assert result["results"][0]["title"] == "有效结果"

    def test_response_error_is_reported_even_with_http_200(self):
        http_response = MagicMock()
        http_response.json.return_value = _response(Error={"Code": "RequestLimitExceeded", "Message": "do not expose this"})
        _, context_manager = _mock_http_client(http_response)

        with (
            patch("deerflow.community.tencent_wsa.tools.get_app_config") as get_config,
            patch("deerflow.community.tencent_wsa.tools.httpx.Client", return_value=context_manager),
        ):
            get_config.return_value.get_tool_config.return_value = _tool_config({"api_key": "test-key"})

            from deerflow.community.tencent_wsa.tools import web_search_tool

            result = json.loads(web_search_tool.run({"query": "腾讯云"}))

        assert result == {
            "error": "Tencent Cloud WSA API error: RequestLimitExceeded",
            "query": "腾讯云",
            "request_id": "request-123",
        }

    def test_http_error_does_not_expose_upstream_body(self):
        response = MagicMock()
        response.status_code = 503
        response.text = "sensitive upstream diagnostic"
        http_response = MagicMock()
        http_response.raise_for_status.side_effect = httpx.HTTPStatusError("unavailable", request=MagicMock(), response=response)
        client, context_manager = _mock_http_client(http_response)

        with (
            patch("deerflow.community.tencent_wsa.tools.get_app_config") as get_config,
            patch("deerflow.community.tencent_wsa.tools.httpx.Client", return_value=context_manager),
        ):
            get_config.return_value.get_tool_config.return_value = _tool_config({"api_key": "test-key"})

            from deerflow.community.tencent_wsa.tools import web_search_tool

            result = json.loads(web_search_tool.run({"query": "腾讯云"}))

        assert result == {"error": "Tencent Cloud WSA API error: HTTP 503", "query": "腾讯云"}
        assert "sensitive" not in json.dumps(result)
        client.post.assert_called_once()

    def test_non_list_pages_returns_unexpected_format_error(self):
        http_response = MagicMock()
        http_response.json.return_value = _response("not-a-list")
        _, context_manager = _mock_http_client(http_response)

        with (
            patch("deerflow.community.tencent_wsa.tools.get_app_config") as get_config,
            patch("deerflow.community.tencent_wsa.tools.httpx.Client", return_value=context_manager),
        ):
            get_config.return_value.get_tool_config.return_value = _tool_config({"api_key": "test-key"})

            from deerflow.community.tencent_wsa.tools import web_search_tool

            result = json.loads(web_search_tool.run({"query": "腾讯云"}))

        assert result == {
            "error": "Tencent Cloud WSA returned an unexpected response format",
            "query": "腾讯云",
            "request_id": "request-123",
        }

    def test_non_json_content_type_is_rejected(self):
        http_response = MagicMock()
        http_response.json.return_value = _response([])
        http_response.headers = {"content-type": "text/html"}
        _, context_manager = _mock_http_client(http_response)

        with (
            patch("deerflow.community.tencent_wsa.tools.get_app_config") as get_config,
            patch(
                "deerflow.community.tencent_wsa.tools.httpx.Client",
                return_value=context_manager,
            ),
        ):
            get_config.return_value.get_tool_config.return_value = _tool_config({"api_key": "test-key"})

            from deerflow.community.tencent_wsa.tools import web_search_tool

            result = json.loads(web_search_tool.run({"query": "腾讯云"}))

        assert result == {
            "error": "Tencent Cloud WSA returned an unexpected response format",
            "query": "腾讯云",
        }

    def test_strict_adapter_rejects_oversized_raw_response(self):
        from deerflow.community.tencent_wsa.tools import _search
        from deerflow.retrieval import RetrievalProviderError

        http_response = MagicMock()
        http_response.json.return_value = _response([])
        http_response.content = b"x" * 65
        _, context_manager = _mock_http_client(http_response)

        with patch(
            "deerflow.community.tencent_wsa.tools.httpx.Client",
            return_value=context_manager,
        ):
            with pytest.raises(RetrievalProviderError) as caught:
                _search(
                    "secret",
                    {"Query": "query"},
                    "query",
                    max_response_bytes=64,
                    strict_response=True,
                )

        assert caught.value.status == "oversized_response"

    def test_strict_adapter_classifies_invalid_json_as_unsafe(self):
        from deerflow.community.tencent_wsa.tools import _search
        from deerflow.retrieval import RetrievalProviderError

        http_response = MagicMock()
        http_response.content = b"not-json"
        http_response.headers = {"content-type": "application/json"}
        http_response.json.side_effect = ValueError("private provider body")
        _, context_manager = _mock_http_client(http_response)

        with patch(
            "deerflow.community.tencent_wsa.tools.httpx.Client",
            return_value=context_manager,
        ):
            with pytest.raises(RetrievalProviderError) as caught:
                _search(
                    "secret",
                    {"Query": "query"},
                    "query",
                    strict_response=True,
                )

        assert caught.value.status == "unsafe_response"

    def test_strict_adapter_classifies_client_timeout(self):
        from deerflow.community.tencent_wsa.tools import _search
        from deerflow.retrieval import RetrievalProviderError

        with patch("deerflow.community.tencent_wsa.tools.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.post.side_effect = httpx.ReadTimeout("private timeout detail")
            with pytest.raises(RetrievalProviderError) as caught:
                _search(
                    "secret",
                    {"Query": "query"},
                    "query",
                    strict_response=True,
                )

        assert caught.value.status == "timeout"


class TestTencentWsaConfiguration:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [(3, 3), ("7", 7), (True, 5), (2.9, 5), (0, 5), (-1, 5), (500, 50), ("bad", 5)],
    )
    def test_coerce_max_results(self, value, expected):
        from deerflow.community.tencent_wsa.tools import _coerce_max_results

        assert _coerce_max_results(value) == expected

    @pytest.mark.parametrize("value", (None, "2", True, 2.0, "bad", -1, 3))
    def test_invalid_mode_is_omitted(self, value):
        with patch("deerflow.community.tencent_wsa.tools.get_app_config") as get_config:
            extras = {} if value is None else {"mode": value}
            get_config.return_value.get_tool_config.return_value = _tool_config(extras)

            from deerflow.community.tencent_wsa.tools import _get_mode

            assert _get_mode() is None
