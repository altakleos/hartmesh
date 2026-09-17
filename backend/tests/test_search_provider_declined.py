"""A search provider that will not serve us says so once, and stops.

The released profile ships a keyless DuckDuckGo `web_search`, and that
endpoint answers an automated request from a server address with a
human-verification challenge rather than results. It is an access control:
nothing here tries to solve or route around it. What the deployment owes the
person instead is a truthful failure -- one that names the missing
configuration rather than reading as a transient outage the model should
retry.
"""

from __future__ import annotations

import pytest

from deerflow.retrieval.contracts import RetrievalProviderError


def test_a_declined_search_carries_what_to_do_about_it() -> None:
    from deerflow.community.ddg_search.tools import _DDG_DECLINED_GUIDANCE

    error = RetrievalProviderError("provider_unavailable", guidance=_DDG_DECLINED_GUIDANCE)

    assert error.status == "provider_unavailable", "the persisted evidence category is unchanged"
    assert "retrying will not change" in error.guidance.lower()
    assert "search provider" in error.guidance.lower()


def test_guidance_cannot_smuggle_a_payload() -> None:
    """It is a fixed sentence, not a channel for provider text or a query."""

    with pytest.raises(ValueError, match="one short line"):
        RetrievalProviderError("provider_unavailable", guidance="x" * 241)
    with pytest.raises(ValueError, match="one short line"):
        RetrievalProviderError("provider_unavailable", guidance="first line\nsecond line")


def test_a_provider_error_without_guidance_is_unchanged() -> None:
    error = RetrievalProviderError("timeout")

    assert error.guidance is None
    assert str(error) == "retrieval_timeout"


def test_the_model_is_told_to_stop_rather_than_retry() -> None:
    from langchain.agents.middleware.types import ToolCallRequest

    from deerflow.agents.middlewares.tool_error_handling_middleware import (
        _RECOVERY_HINT,
        ToolErrorHandlingMiddleware,
    )
    from deerflow.community.ddg_search.tools import _DDG_DECLINED_GUIDANCE

    middleware = ToolErrorHandlingMiddleware()
    request = ToolCallRequest(
        tool_call={"name": "web_search", "id": "call-1", "args": {"query": "anything"}},
        tool=None,
        state={},
        runtime=None,
    )

    declined = middleware._build_error_message(
        request,
        RetrievalProviderError("provider_unavailable", guidance=_DDG_DECLINED_GUIDANCE),
    )
    assert _DDG_DECLINED_GUIDANCE in declined.content
    assert _RECOVERY_HINT not in declined.content, "generic retry advice would contradict the guidance"
    assert "anything" not in declined.content, "the query never reaches the model's error text"

    ordinary = middleware._build_error_message(request, RetrievalProviderError("timeout"))
    assert _RECOVERY_HINT in ordinary.content, "a failure that may pass keeps the ordinary advice"


def test_the_keyless_endpoint_declining_is_not_treated_as_an_outage(monkeypatch) -> None:
    """Exercise the adapter's own transport decision on a 202 challenge."""

    from deerflow.community.ddg_search import tools

    class _Response:
        status_code = 202
        content = b"<html>challenge</html>"
        headers = {"content-type": "text/html"}
        text = "<html>challenge</html>"

    class _Client:
        def __init__(self, **_kwargs: object) -> None: ...

        def headers_update(self, *_args: object, **_kwargs: object) -> None: ...

        def request(self, *_args: object, **_kwargs: object) -> _Response:
            return _Response()

    primp = pytest.importorskip("primp")
    monkeypatch.setattr(primp, "Client", _Client)

    with pytest.raises(RetrievalProviderError) as raised:
        tools._search_duckduckgo_evidence(
            "capital of France",
            max_results=3,
            region="wt-wt",
            safesearch="moderate",
            time_range=None,
            timeout_seconds=5,
            max_response_bytes=65536,
        )

    assert raised.value.status == "provider_unavailable"
    assert raised.value.guidance == tools._DDG_DECLINED_GUIDANCE
