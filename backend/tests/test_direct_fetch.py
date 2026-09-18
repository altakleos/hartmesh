"""The keyless ``web_fetch`` reads pages itself, pinned to the addresses it checked.

Every control in ``direct_fetch/client.py`` is exercised here against a
transport that records exactly what would have gone on the wire: which
address the connection was made to, which name the certificate would be
checked against, which ``Host`` the origin would see. A resolver is injected
so a name can answer with whatever address the test needs -- a public one, a
private one, or a different one the second time, which is the rebinding case.
"""

from __future__ import annotations

import asyncio
import ipaddress
from typing import Any

import httpx
import pytest
from langgraph.types import Command

from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY, ToolResultMeta, normalize_tool_message
from deerflow.community.direct_fetch import tools as fetch_tools
from deerflow.community.direct_fetch.client import (
    MAX_REDIRECTS,
    USER_AGENT,
    DirectFetchClient,
    FetchedPage,
)
from deerflow.community.web_fetch_outcome import FetchRefusal, describe_refusal, refusal_meta
from deerflow.retrieval import RETRIEVAL_TOOL_METADATA_KEY, retrieval_tool_declaration

PUBLIC = ipaddress.ip_address("93.184.216.34")
PUBLIC_V6 = ipaddress.ip_address("2606:2800:220:1:248:1893:25c8:1946")
PRIVATE = ipaddress.ip_address("10.0.0.7")
METADATA = ipaddress.ip_address("169.254.169.254")
CARRIER_NAT = ipaddress.ip_address("100.64.1.1")

HTML = "<html><head><title>Great Lakes</title></head><body><article><p>Five lakes, one basin, twenty percent of the surface fresh water on Earth.</p></article></body></html>"


class _Wire:
    """Records every request the client would have sent and answers by script."""

    def __init__(self, script: list[tuple[int, dict[str, str], bytes]] | None = None) -> None:
        self.requests: list[httpx.Request] = []
        self.script = list(script or [(200, {"content-type": "text/html; charset=utf-8"}, HTML.encode())])

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        status, headers, body = self.script[min(len(self.requests) - 1, len(self.script) - 1)]
        return httpx.Response(status, headers=headers, content=body, request=request)

    @property
    def transport(self) -> httpx.AsyncBaseTransport:
        return httpx.MockTransport(self.handler)


def _resolver(*answers: list[ipaddress._BaseAddress]):
    calls: list[str] = []
    queue = list(answers)

    async def resolve(hostname: str) -> list[ipaddress._BaseAddress]:
        calls.append(hostname)
        return list(queue.pop(0) if len(queue) > 1 else queue[0])

    resolve.calls = calls  # type: ignore[attr-defined]
    return resolve


def _fetch(url: str, wire: _Wire, resolver: Any = None, **kwargs: Any) -> FetchedPage | FetchRefusal:
    client = DirectFetchClient(resolver=resolver or _resolver([PUBLIC]), transport=wire.transport, **kwargs)
    return asyncio.run(client.fetch(url))


# ── Pinning: the connection goes where the check went ───────────────────────


def test_the_request_is_sent_to_the_checked_address_with_the_name_on_host_and_sni() -> None:
    wire = _Wire()
    page = _fetch("https://example.org/great-lakes?x=1", wire)
    assert isinstance(page, FetchedPage)
    [request] = wire.requests
    assert request.url.host == str(PUBLIC), "connected to the address the validator saw, not to a second lookup"
    assert request.url.path == "/great-lakes" and request.url.query == b"x=1"
    assert request.headers["host"] == "example.org"
    assert request.extensions["sni_hostname"] == "example.org", "the certificate is verified against the name"
    assert request.headers["user-agent"] == USER_AGENT
    assert "cookie" not in request.headers and "authorization" not in request.headers


def test_a_name_that_resolves_to_a_public_and_a_private_address_is_refused_before_any_request() -> None:
    wire = _Wire()
    refusal = _fetch("https://example.org/", wire, resolver=_resolver([PUBLIC, PRIVATE]))
    assert isinstance(refusal, FetchRefusal)
    assert refusal.scope == "origin" and refusal.error_type == "permission"
    assert wire.requests == []


@pytest.mark.parametrize("address", [PRIVATE, METADATA, CARRIER_NAT, ipaddress.ip_address("127.0.0.1"), ipaddress.ip_address("::1"), ipaddress.ip_address("fc00::1")])
def test_every_never_allowed_address_class_is_refused(address: ipaddress._BaseAddress) -> None:
    wire = _Wire()
    refusal = _fetch("http://example.org/", wire, resolver=_resolver([address]))
    assert isinstance(refusal, FetchRefusal) and refusal.error_type == "permission"
    assert wire.requests == []


def test_an_ip_literal_is_checked_without_a_lookup_and_a_private_one_refused() -> None:
    wire = _Wire()
    resolver = _resolver([PUBLIC])
    refusal = _fetch("http://10.0.0.7/admin", wire, resolver=resolver)
    assert isinstance(refusal, FetchRefusal) and refusal.error_type == "permission"
    assert resolver.calls == [] and wire.requests == []


def test_an_ipv6_address_is_pinned_in_brackets() -> None:
    wire = _Wire()
    page = _fetch("https://example.org/", wire, resolver=_resolver([PUBLIC_V6]))
    assert isinstance(page, FetchedPage)
    assert wire.requests[0].url.host == str(PUBLIC_V6)
    assert wire.requests[0].headers["host"] == "example.org"


def test_a_name_that_could_not_be_resolved_is_refused_as_such() -> None:
    wire = _Wire()
    refusal = _fetch("https://nowhere.invalid/", wire, resolver=_resolver([]))
    assert isinstance(refusal, FetchRefusal)
    assert "resolved" in refusal.reason and wire.requests == []


@pytest.mark.parametrize("url", ["ftp://example.org/x", "file:///etc/passwd", "example.org/no-scheme", "https:///nohost"])
def test_only_http_and_https_with_a_host_are_fetched(url: str) -> None:
    wire = _Wire()
    refusal = _fetch(url, wire)
    assert isinstance(refusal, FetchRefusal) and wire.requests == []


# ── Redirects: every hop is checked and pinned again ─────────────────────────


def test_each_redirect_hop_is_resolved_checked_and_pinned_again() -> None:
    wire = _Wire(
        [
            (301, {"location": "https://www.example.org/moved"}, b""),
            (200, {"content-type": "text/html"}, HTML.encode()),
        ]
    )
    other = ipaddress.ip_address("93.184.216.35")
    page = _fetch("http://example.org/old", wire, resolver=_resolver([PUBLIC], [other]))
    assert isinstance(page, FetchedPage)
    assert page.url == "https://www.example.org/moved"
    first, second = wire.requests
    assert first.url.host == str(PUBLIC) and first.headers["host"] == "example.org" and "sni_hostname" not in first.extensions
    assert second.url.host == str(other) and second.headers["host"] == "www.example.org" and second.extensions["sni_hostname"] == "www.example.org"


def test_a_redirect_to_a_private_address_is_refused_at_that_hop() -> None:
    wire = _Wire([(302, {"location": "http://internal.example.org/"}, b""), (200, {"content-type": "text/html"}, HTML.encode())])
    refusal = _fetch("http://example.org/", wire, resolver=_resolver([PUBLIC], [PRIVATE]))
    assert isinstance(refusal, FetchRefusal) and refusal.error_type == "permission"
    assert len(wire.requests) == 1, "the private hop was never connected to"


def test_a_relative_location_is_resolved_against_the_current_url() -> None:
    wire = _Wire([(303, {"location": "/en/index"}, b""), (200, {"content-type": "text/html"}, HTML.encode())])
    page = _fetch("https://example.org/start", wire)
    assert isinstance(page, FetchedPage) and page.url == "https://example.org/en/index"


def test_a_redirect_chain_is_bounded() -> None:
    wire = _Wire([(302, {"location": "https://example.org/again"}, b"")])
    refusal = _fetch("https://example.org/", wire)
    assert isinstance(refusal, FetchRefusal) and refusal.error_type == "transient"
    assert len(wire.requests) == MAX_REDIRECTS + 1
    assert str(MAX_REDIRECTS) in refusal.reason


# ── The body: type and size ──────────────────────────────────────────────────


@pytest.mark.parametrize("content_type", ["application/pdf", "image/png", "application/octet-stream", ""])
def test_a_body_that_is_not_a_readable_page_is_refused_by_type(content_type: str) -> None:
    wire = _Wire([(200, {"content-type": content_type} if content_type else {}, b"%PDF-1.7 ...")])
    refusal = _fetch("https://example.org/report.pdf", wire)
    assert isinstance(refusal, FetchRefusal)
    assert refusal.error_type == "unsupported" and refusal.scope == "origin"


def test_a_page_past_the_size_bound_is_refused_not_truncated() -> None:
    wire = _Wire([(200, {"content-type": "text/html"}, b"<p>" + b"x" * 5000 + b"</p>")])
    refusal = _fetch("https://example.org/", wire, max_body_bytes=4096)
    assert isinstance(refusal, FetchRefusal) and refusal.error_type == "unsupported"
    assert "larger" in refusal.reason


def test_plain_text_is_read_as_is() -> None:
    wire = _Wire([(200, {"content-type": "text/plain; charset=utf-8"}, b"just words\n")])
    page = _fetch("https://example.org/robots.txt", wire)
    assert isinstance(page, FetchedPage) and page.text == "just words\n" and page.content_type == "text/plain"


# ── The origin's refusals stay the origin's ─────────────────────────────────


@pytest.mark.parametrize(
    ("status", "error_type"),
    [(401, "auth"), (403, "permission"), (404, "not_found"), (410, "not_found"), (429, "rate_limited"), (500, "transient"), (503, "transient"), (418, "unknown")],
)
def test_an_origin_status_is_a_typed_origin_refusal(status: int, error_type: str) -> None:
    wire = _Wire([(status, {"content-type": "text/html"}, b"<h1>no</h1>")])
    refusal = _fetch("https://example.org/", wire)
    assert isinstance(refusal, FetchRefusal)
    assert refusal.scope == "origin", "one page saying no says nothing about the next address"
    assert refusal.error_type == error_type and refusal.status_code == status


def test_a_transport_failure_is_a_transient_origin_refusal() -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    client = DirectFetchClient(resolver=_resolver([PUBLIC]), transport=httpx.MockTransport(boom))
    refusal = asyncio.run(client.fetch("https://example.org/"))
    assert isinstance(refusal, FetchRefusal) and refusal.error_type == "transient"


def test_the_whole_chain_shares_one_time_budget() -> None:
    async def slow(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.5)
        return httpx.Response(200, headers={"content-type": "text/html"}, content=HTML.encode(), request=request)

    client = DirectFetchClient(resolver=_resolver([PUBLIC]), transport=httpx.MockTransport(slow), timeout_seconds=0.1)
    refusal = asyncio.run(client.fetch("https://example.org/"))
    assert isinstance(refusal, FetchRefusal) and refusal.error_type == "transient" and "in time" in refusal.reason


def test_resolving_a_name_leaves_the_event_loop_free(monkeypatch: pytest.MonkeyPatch) -> None:
    """The Gateway is one asyncio process serving every tenant's stream.

    ``socket.getaddrinfo`` is a blocking syscall whose timeout belongs to the
    platform resolver, not to the fetch budget; called straight from the async
    path it holds the loop for the length of one slow lookup, and
    ``asyncio.timeout`` cannot preempt it because nothing awaits. So the
    property under test is not "a lookup happened" but "other work ran while
    it did". Blockbuster does not instrument ``getaddrinfo``, so the strict
    gate cannot see this; this measures it directly.
    """
    import socket

    def slow_getaddrinfo(*_args: Any, **_kwargs: Any) -> list[Any]:
        import time

        time.sleep(0.2)
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (str(PUBLIC), 80))]

    monkeypatch.setattr(socket, "getaddrinfo", slow_getaddrinfo)
    wire = _Wire()

    async def race() -> tuple[Any, int]:
        ticks = 0

        async def tick() -> None:
            nonlocal ticks
            while True:
                await asyncio.sleep(0.01)
                ticks += 1

        ticker = asyncio.create_task(tick())
        try:
            outcome = await DirectFetchClient(transport=wire.transport).fetch("http://example.org/")
        finally:
            ticker.cancel()
        return outcome, ticks

    page, ticks = asyncio.run(race())
    assert isinstance(page, FetchedPage), "the default resolver answered and the fetch completed"
    assert ticks >= 5, f"the loop was stalled through the lookup; only {ticks} tick(s) ran"


def test_only_a_bounded_number_of_pages_are_read_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each fetch buffers up to 2 MiB and extracts in a subprocess, inside the
    tenant profile's own memory, CPU and pid budget. A model can issue several
    fetch calls in one step; the ceiling is the tool's, the way it is for
    ``web_search``."""
    live = 0
    peak = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        try:
            await asyncio.sleep(0.05)
            return httpx.Response(200, headers={"content-type": "text/plain"}, content=b"hello", request=request)
        finally:
            live -= 1

    monkeypatch.setattr(fetch_tools, "get_active_retrieval_handoff", lambda: None)
    monkeypatch.setattr(fetch_tools, "get_app_config", lambda: object())
    monkeypatch.setattr(
        fetch_tools,
        "_client_from_config",
        lambda _cfg: DirectFetchClient(resolver=_resolver([PUBLIC]), transport=httpx.MockTransport(handler)),
    )

    async def burst() -> None:
        await asyncio.gather(*(fetch_tools.web_fetch_tool.coroutine(f"http://example.org/{index}", tool_call_id=f"c{index}") for index in range(12)))

    asyncio.run(burst())
    # The number is written out rather than read from the module: an assertion
    # against the constant it is policing passes however far the bound is
    # loosened, which is no assertion at all.
    assert peak <= 4, f"{peak} fetches were in flight at once"
    assert peak > 1, "the bound must not serialize the tool"
    assert fetch_tools.CONCURRENT_FETCHES == 4, "the same ceiling web_search applies, for the same budget"


# ── The tool: what the model reads, and what the runtime reads ──────────────


def _tool_result(url: str, wire: _Wire, monkeypatch: pytest.MonkeyPatch, resolver: Any = None) -> Command:
    monkeypatch.setattr(fetch_tools, "get_active_retrieval_handoff", lambda: None)
    monkeypatch.setattr(fetch_tools, "_client_from_config", lambda _cfg: DirectFetchClient(resolver=resolver or _resolver([PUBLIC]), transport=wire.transport))
    monkeypatch.setattr(fetch_tools, "get_app_config", lambda: object())
    result = asyncio.run(fetch_tools.web_fetch_tool.coroutine(url, tool_call_id="call-1"))
    assert isinstance(result, Command)
    return result


def _message(result: Command):
    [message] = result.update["messages"]
    return message


def test_a_fetched_page_reaches_the_model_as_markdown_stamped_success(monkeypatch: pytest.MonkeyPatch) -> None:
    message = _message(_tool_result("https://example.org/great-lakes", _Wire(), monkeypatch))
    assert message.tool_call_id == "call-1" and message.name == "web_fetch" and message.status == "success"
    assert "Great Lakes" in message.content and "twenty percent" in message.content
    meta = ToolResultMeta(**message.additional_kwargs[TOOL_META_KEY])
    assert meta.status == "success" and meta.source == "tool_return"


def test_an_origin_refusal_names_the_address_says_try_another_source_and_is_stamped_origin(monkeypatch: pytest.MonkeyPatch) -> None:
    wire = _Wire([(403, {"content-type": "text/html"}, b"<h1>Forbidden</h1>")])
    message = _message(_tool_result("https://www.example.org/paywalled\x07", wire, monkeypatch))
    assert message.status == "error"
    assert message.content.startswith("Error: could not fetch https://www.example.org/paywalled?")
    assert "another source" in message.content and "\x07" not in message.content
    meta = ToolResultMeta(**message.additional_kwargs[TOOL_META_KEY])
    assert meta.error_scope == "origin" and meta.recoverable_by_model is True and meta.recommended_next_action == "try_alternative"
    assert meta.error_type == "permission"


def test_the_tools_own_stamp_is_what_normalization_keeps(monkeypatch: pytest.MonkeyPatch) -> None:
    # A 401 page would read as auth/stop to the keyword rules; the tool saw
    # the transport and says origin/try_alternative, and that is what stays.
    wire = _Wire([(401, {"content-type": "text/html"}, b"<h1>401 Unauthorized</h1>")])
    message = _message(_tool_result("https://example.org/members", wire, monkeypatch))
    normalized = normalize_tool_message(message)
    meta = ToolResultMeta(**normalized.additional_kwargs[TOOL_META_KEY])
    assert (meta.error_type, meta.error_scope, meta.recommended_next_action) == ("auth", "origin", "try_alternative")


def test_a_provider_refusal_reads_as_unavailable_for_the_turn_and_is_stamped_provider() -> None:
    refusal = FetchRefusal(scope="provider", error_type="auth", reason="the fetch provider refused this deployment's address")
    text = describe_refusal("https://example.org/", refusal)
    assert "unavailable for the rest of this turn" in text and "Do not call it again" in text
    meta = ToolResultMeta(**refusal_meta(refusal))
    assert meta.error_scope == "provider" and meta.recoverable_by_model is False and meta.recommended_next_action == "stop"


def test_the_tool_declares_itself_as_evidence_bearing_retrieval() -> None:
    declaration = retrieval_tool_declaration(fetch_tools.web_fetch_tool)
    assert declaration is not None
    assert declaration.provider_id == fetch_tools.PROVIDER_ID and declaration.tool_kind == "web_fetch"
    assert declaration.protected_argument_fields == ("url",)
    assert RETRIEVAL_TOOL_METADATA_KEY in fetch_tools.web_fetch_tool.metadata


def test_the_url_is_the_models_only_visible_argument() -> None:
    schema = fetch_tools.web_fetch_tool.tool_call_schema.model_json_schema()
    assert set(schema["properties"]) == {"url"}
