"""The tenant profile's keyless ``web_search`` answers, and degrades in words.

Since 2026-09-17 the released Compose profile's ``web_search`` without a
search-provider key is ``deerflow.community.searxng.tools:web_search_tool``
against the profile's own SearXNG service. Two things a person depends on and
no unit test can show:

* a turn in which the model reaches for search gets the search service's
  results back into the conversation, through the same Gateway, route,
  admission, worker, receipt middleware and tool dispatch a tenant runs; and
* when the search service is down, the turn still ends with an answer that
  says the web was not checked -- not an error frame, not a traceback in the
  transcript, and not a retry loop.

What is synthetic: the model (the turn-phase probe, which issues exactly one
``web_search`` call and then answers) and SearXNG itself, replaced by a
loopback HTTP server that speaks its JSON search API and records what it was
asked. The tool, its client, and everything between the route and the tool
are the real ones, configured the way the profile configures them.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
import test_turn_phase_gateway_stream_e2e as e2e

from deerflow.community.searxng.tools import SEARCH_UNAVAILABLE_MESSAGE

_RESULT_URL = "https://example.org/paris"
_SEARXNG_RESULTS = {
    "query": "what is the capital of france",
    "results": [
        {
            "title": "Paris",
            "url": _RESULT_URL,
            "content": "Paris is the capital and most populous city of France.",
            "engine": "bing",
        }
    ],
}


class _FakeSearxng:
    """A loopback stand-in for the profile's SearXNG: JSON search API only."""

    def __init__(self) -> None:
        self.requests: list[dict[str, list[str]]] = []
        self.urls: list[str] = []
        self.available = True
        self._lock = threading.Lock()
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args: Any) -> None:  # quiet
                return

            def do_POST(self) -> None:  # noqa: N802 - http.server API
                parts = urlsplit(self.path)
                if parts.path != "/search":
                    self.send_response(404)
                    self.end_headers()
                    return
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length).decode("utf-8")
                with fake._lock:
                    # The query travels in the body, never in the URL.
                    fake.requests.append(parse_qs(body))
                    fake.urls.append(self.path)
                    available = fake.available
                if not available:
                    # What a stopped or overloaded instance looks like from
                    # the Gateway's side.
                    self.send_response(503)
                    self.end_headers()
                    return
                body = json.dumps(_SEARXNG_RESULTS).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, name="fake-searxng", daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def reset(self, *, available: bool) -> None:
        with self._lock:
            self.requests.clear()
            self.urls.clear()
            self.available = available


def _profile_config(base_url: str) -> str:
    # The tool-plane and tool shape the profile renders for a tenant with no
    # search-provider key (deploy/compose/config.yaml), with the service's
    # address pointed at the stand-in.
    return (
        e2e._MINIMAL_CONFIG_YAML
        + f"""\
deployment:
  profile: local_development
tool_plane:
  enabled: true
  policy_version: deerflow-default-v1
  validation_requires_skill_review: true
tool_groups:
  - name: web
tools:
  - name: web_search
    group: web
    use: deerflow.community.searxng.tools:web_search_tool
    base_url: {base_url}
    max_results: 5
"""
    )


@pytest.fixture(scope="module")
def searxng_gateway(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[e2e._Gateway, _FakeSearxng]]:
    fake = _FakeSearxng()
    fake.start()
    home = tmp_path_factory.mktemp("searxng-default-e2e")
    try:
        with e2e.serve_gateway(home, config_yaml=_profile_config(fake.base_url)) as served:
            yield served, fake
    finally:
        fake.stop()


def _search_turn(gateway: e2e._Gateway) -> tuple[e2e._StreamObservation, dict[str, Any]]:
    base = gateway.loopback_url
    with httpx.Client() as client:
        csrf, thread_id = e2e._register_and_create_thread(client, base)
        observed = e2e._observe_stream(
            client,
            base,
            thread_id,
            csrf,
            "probe:search what is the capital of france",
            timeout=120.0,
            # The Gateway's own default, and what the web client sends; a
            # tool-using turn does not fit in the 25 the text-only cases use.
            recursion_limit=100,
        )
        history = client.post(
            f"{base}/api/threads/{thread_id}/history",
            json={"limit": 20},
            headers={"X-CSRF-Token": csrf},
        )
        assert history.status_code == 200, history.text
        events = client.get(
            f"{base}/api/threads/{thread_id}/runs/{observed.run_id}/events",
            headers={"X-CSRF-Token": csrf},
        )
        assert events.status_code == 200, events.text
    body = events.json()
    event_list = body.get("events") if isinstance(body, dict) else body
    return observed, {"history": history.json(), "events": event_list}


def test_the_search_service_s_results_reach_the_conversation(
    searxng_gateway: tuple[e2e._Gateway, _FakeSearxng],
) -> None:
    gateway, fake = searxng_gateway
    fake.reset(available=True)

    observed, after = _search_turn(gateway)

    assert "error" not in observed.events, observed.events
    assert observed.events[-1] == "end", observed.events
    assert gateway.journals.wait_for(observed.run_id)["outcome"] == "success"
    # One request, the model's query, asking for JSON, capped at the
    # profile's five results: the client sent what the profile configured.
    assert len(fake.requests) == 1, fake.requests
    request = fake.requests[0]
    assert request["q"] == ["what is the capital of france"], request
    assert request["format"] == ["json"], request
    assert request["limit"] == ["5"], request
    assert fake.urls == ["/search"], "the question stays out of the request line"
    assert observed.text_frames >= 1, "the model answered after the tool result"
    assert _RESULT_URL in json.dumps(after["history"]), "the search result never reached the conversation"


def test_a_down_search_service_ends_the_turn_with_words_not_an_error(
    searxng_gateway: tuple[e2e._Gateway, _FakeSearxng],
    caplog: pytest.LogCaptureFixture,
) -> None:
    gateway, fake = searxng_gateway
    fake.reset(available=False)

    with caplog.at_level(logging.DEBUG, logger="deerflow.community.searxng"):
        observed, after = _search_turn(gateway)

    assert "error" not in observed.events, observed.events
    assert observed.events[-1] == "end", observed.events
    assert gateway.journals.wait_for(observed.run_id)["outcome"] == "success"
    # The tool asked once and did not retry on its own.
    assert len(fake.requests) == 1, fake.requests
    assert observed.text_frames >= 1, "the model still answered"
    serialized = json.dumps(after["history"])
    assert SEARCH_UNAVAILABLE_MESSAGE in serialized, "the model was not told, in words, that search was unavailable"
    assert "Traceback" not in serialized and "HTTPStatusError" not in serialized, "an exception leaked into the transcript"
    assert _RESULT_URL not in serialized
    # A 503 is the case whose exception text carries the request URL, and
    # the URL carries the query; the log must not.
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "capital of france" not in logged.lower(), logged
    assert "HTTPStatusError" in logged or "503" in logged, logged


def test_the_keyless_default_makes_no_retrieval_evidence_claim(
    searxng_gateway: tuple[e2e._Gateway, _FakeSearxng],
) -> None:
    """The SearXNG tool has no durable evidence adapter, and says so by silence.

    A ``retrieval.observation.v1`` row here would be an evidence claim about a
    provider nothing verified; the profile README and the retrieval guide both
    record that the keyless default makes none.
    """

    gateway, fake = searxng_gateway
    fake.reset(available=True)

    observed, after = _search_turn(gateway)

    assert observed.events[-1] == "end", observed.events
    assert gateway.journals.wait_for(observed.run_id)["outcome"] == "success"
    # The tool did run, and its receipt pair is on the record: absence of an
    # observation is meaningful only next to a dispatch that happened.
    assert len(fake.requests) == 1, fake.requests
    types = [event.get("event_type") for event in after["events"]]
    assert types.count("tool_receipt.started.v1") == 1, types
    assert types.count("tool_receipt.outcome.v1") == 1, types
    assert "retrieval.observation.v1" not in types, types
