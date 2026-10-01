"""The tenant profile's keyless ``web_fetch`` reads a page, and a dead provider is withdrawn.

Since 2026-09-17 the released Compose profile's ``web_fetch`` without a
fetch-provider key is ``deerflow.community.direct_fetch.tools:web_fetch_tool``. Three things a person depends on and no unit test
can show:

* a turn in which the model reaches for a page gets that page back into the
  conversation through the same Gateway, route, admission, worker, receipt
  middleware and tool dispatch a tenant runs, with the tool's own typed stamp
  on the result;
* a page that refuses (a 403) ends the turn with words and leaves the tool
  available -- one address saying no says nothing about the next; and
* a provider that refuses is withdrawn: the next model request is bound
  without ``web_fetch``, so the thirteen-call loop the tenant class recorded
  cannot recur.

What is synthetic: the model (the turn-phase probe, which issues exactly one
``web_fetch`` call and then answers) and the wire under the fetch client (a
transport that answers by script and records what would have been sent).
The tool, its client, the address checks, the stamp, the withdrawal
middleware and everything between the route and the tool are the real ones,
configured the way the profile configures them.
"""

from __future__ import annotations

import ipaddress
import json
from collections.abc import Iterator
from typing import Any

import _turn_phase_probe_model as probe
import httpx
import pytest
import test_turn_phase_gateway_stream_e2e as e2e

from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY
from deerflow.community.direct_fetch import tools as fetch_tools
from deerflow.community.direct_fetch.client import DirectFetchClient
from deerflow.community.web_fetch_outcome import FetchRefusal

_PAGE_URL = "https://example.org/great-lakes"
_PAGE_HTML = "<html><head><title>Great Lakes</title></head><body><article><p>Five lakes hold twenty percent of the surface fresh water on Earth.</p></article></body></html>"
_PUBLIC = ipaddress.ip_address("93.184.216.34")


def _profile_config() -> str:
    # The tool-plane and tool shape the profile renders for a tenant with no
    # fetch-provider key (deploy/compose/config.yaml).
    return (
        e2e._MINIMAL_CONFIG_YAML
        + """\
deployment:
  profile: local_development
tool_plane:
  enabled: true
  policy_version: deerflow-default-v1
  validation_requires_skill_review: true
tool_groups:
  - name: web
tools:
  - name: web_fetch
    group: web
    use: deerflow.community.direct_fetch.tools:web_fetch_tool
    timeout: 10
"""
    )


class _Wire:
    def __init__(self, status: int, body: bytes, content_type: str = "text/html; charset=utf-8") -> None:
        self.requests: list[httpx.Request] = []
        self._status, self._body, self._content_type = status, body, content_type

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self._status, headers={"content-type": self._content_type}, content=self._body, request=request)

    def client(self) -> DirectFetchClient:
        # Resolution is awaited, never called: the client resolves off the
        # event loop so a slow lookup cannot stall the Gateway.
        async def resolve(_hostname: str) -> list[ipaddress._BaseAddress]:
            return [_PUBLIC]

        return DirectFetchClient(resolver=resolve, transport=httpx.MockTransport(self.handler))


class _RefusingProvider:
    """A fetch path that refuses the caller: what a keyed provider with a bad key does."""

    def __init__(self) -> None:
        self.calls = 0

    async def fetch(self, url: str) -> FetchRefusal:
        self.calls += 1
        return FetchRefusal("provider", "auth", "the fetch provider refuses this deployment's requests without a valid key", 401)


@pytest.fixture(scope="module")
def fetch_gateway(tmp_path_factory: pytest.TempPathFactory) -> Iterator[e2e._Gateway]:
    home = tmp_path_factory.mktemp("web-fetch-default-e2e")
    with e2e.serve_gateway(home, config_yaml=_profile_config()) as served:
        yield served


def _fetch_turn(gateway: e2e._Gateway, url: str) -> tuple[e2e._StreamObservation, dict[str, Any]]:
    base = gateway.loopback_url
    probe.BOUND_TOOL_NAMES.clear()
    with httpx.Client() as client:
        csrf, thread_id = e2e._register_and_create_thread(client, base)
        observed = e2e._observe_stream(client, base, thread_id, csrf, f"probe:fetch {url}", timeout=120.0, recursion_limit=100)
        history = client.post(f"{base}/api/threads/{thread_id}/history", json={"limit": 20}, headers={"X-CSRF-Token": csrf})
        assert history.status_code == 200, history.text
    return observed, {"history": history.json(), "bound": [list(names) for names in probe.BOUND_TOOL_NAMES]}


def _tool_messages(history: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if node.get("type") == "tool" and node.get("name") == "web_fetch":
                found.append(node)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(history)
    return found


def test_the_page_reaches_the_conversation_with_the_tools_own_stamp(fetch_gateway: e2e._Gateway, monkeypatch: pytest.MonkeyPatch) -> None:
    wire = _Wire(200, _PAGE_HTML.encode())
    monkeypatch.setattr(fetch_tools, "_client_from_config", lambda _config: wire.client())

    observed, after = _fetch_turn(fetch_gateway, _PAGE_URL)

    assert "error" not in observed.events, observed.events
    assert observed.events[-1] == "end", observed.events
    assert fetch_gateway.journals.wait_for(observed.run_id)["outcome"] == "success"
    # One request, to the checked address, with the name where the origin and
    # the certificate verifier read it: the client sent what the profile runs.
    [request] = wire.requests
    assert request.url.host == str(_PUBLIC) and request.headers["host"] == "example.org" and request.extensions["sni_hostname"] == "example.org"
    assert observed.text_frames >= 1, "the model answered after the tool result"
    [tool_message] = _tool_messages(after["history"])
    assert "twenty percent" in tool_message["content"], "the page never reached the conversation"
    meta = tool_message["additional_kwargs"][TOOL_META_KEY]
    assert (meta["status"], meta["source"], meta["error_scope"]) == ("success", "tool_return", "origin")
    assert all("web_fetch" in names for names in after["bound"]), after["bound"]


def test_a_page_that_refuses_ends_the_turn_with_words_and_keeps_the_tool(fetch_gateway: e2e._Gateway, monkeypatch: pytest.MonkeyPatch) -> None:
    wire = _Wire(403, b"<h1>Forbidden</h1>")
    monkeypatch.setattr(fetch_tools, "_client_from_config", lambda _config: wire.client())

    observed, after = _fetch_turn(fetch_gateway, "https://example.org/paywalled")

    assert "error" not in observed.events, observed.events
    assert fetch_gateway.journals.wait_for(observed.run_id)["outcome"] == "success"
    assert observed.text_frames >= 1
    [tool_message] = _tool_messages(after["history"])
    assert tool_message["content"].startswith("Error: could not fetch https://example.org/paywalled:")
    assert "another source" in tool_message["content"]
    meta = tool_message["additional_kwargs"][TOOL_META_KEY]
    assert (meta["error_type"], meta["error_scope"], meta["recommended_next_action"]) == ("permission", "origin", "try_alternative")
    assert len(after["bound"]) >= 2 and all("web_fetch" in names for names in after["bound"]), "one address saying no says nothing about the next"


def test_a_provider_that_refuses_is_withdrawn_before_the_next_model_request(fetch_gateway: e2e._Gateway, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _RefusingProvider()
    monkeypatch.setattr(fetch_tools, "_client_from_config", lambda _config: provider)

    observed, after = _fetch_turn(fetch_gateway, _PAGE_URL)

    assert "error" not in observed.events, observed.events
    assert fetch_gateway.journals.wait_for(observed.run_id)["outcome"] == "success"
    assert observed.text_frames >= 1, "the turn still ends with an answer"
    assert provider.calls == 1, "one call found the provider out; nothing tried it again"
    [tool_message] = _tool_messages(after["history"])
    assert "unavailable for the rest of this turn" in tool_message["content"]
    assert tool_message["additional_kwargs"][TOOL_META_KEY]["error_scope"] == "provider"
    bound = after["bound"]
    assert len(bound) >= 2, bound
    assert "web_fetch" in bound[0], "the first request could call it"
    assert all("web_fetch" not in names for names in bound[1:]), f"withdrawn after the refusal: {bound}"
    assert "unavailable for the rest of this turn" in json.dumps(after["history"]), "the model was told once, in the conversation it reads"
