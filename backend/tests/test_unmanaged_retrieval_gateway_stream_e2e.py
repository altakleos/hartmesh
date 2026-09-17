"""The default tenant profile can run its configured search tool.

The released Compose profile selects ``deployment.profile: local_development``
and enables the governed tool plane; through ``2.1.0+hartmesh.20`` its keyless
``web_search`` was the declared DuckDuckGo retrieval tool, and a keyed tenant
still runs a declared provider today. A fresh tenant has a tool-plane scope but
no promoted base revision, which the governed-tool-plane contract calls a
supported state: a non-durable deployment keeps running on its own
configuration, and admission makes no governed claim.

Execution disagreed. Admission continued and sealed no ``tool_plane_revision``,
while every accepted run still carried tool-receipt evidence, so the receipt
middleware required an accepted tenant *and* four tool-plane digests before it
would dispatch a declared retrieval tool. The digests were deliberately absent,
so the middleware raised before reserving a receipt or calling the handler: on
the shipped default profile, any turn where the model reached ``web_search``
failed, and the public deep-research skill with it. The search provider was
never contacted.

What runs for real here: the same Gateway, route, admission, worker, receipt
middleware and tool dispatch as ``test_turn_phase_gateway_stream_e2e``, with
the rendered profile's own tool-plane and tool configuration and a SQL
tool-plane repository holding no promoted revision. Only two edges are
synthetic and neither is on the path under test -- the model, which calls the
tool, and DuckDuckGo's network call, which would otherwise cost a real
request to a third party.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
import test_turn_phase_gateway_stream_e2e as e2e

# The tool-plane and tool shape the Compose renderer emits for a tenant with
# no search-provider key. A hand-built revision in a middleware unit test is
# exactly how the contradiction survived, so this test reads the deployment's
# own configuration instead.
_PROFILE_CONFIG_YAML = (
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
  - name: web_search
    group: web
    use: deerflow.community.ddg_search.tools:web_search_tool
    max_results: 5
"""
)

_PROVIDER_RESULTS = [
    {
        "title": "Paris",
        "href": "https://example.org/paris",
        "body": "Paris is the capital and most populous city of France.",
    }
]


@pytest.fixture(scope="module")
def unmanaged_gateway(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[e2e._Gateway, list[dict[str, Any]]]]:
    calls: list[dict[str, Any]] = []
    monkeypatch = pytest.MonkeyPatch()

    def _stub_search(query: str, **kwargs: Any) -> list[dict[str, str]]:
        # Stands in for the network round trip only: the adapter, policy,
        # constraints and evidence around it are the real ones.
        calls.append({"query": query, **kwargs})
        return list(_PROVIDER_RESULTS)

    monkeypatch.setattr(
        "deerflow.community.ddg_search.tools._search_duckduckgo_evidence",
        _stub_search,
    )
    home = tmp_path_factory.mktemp("unmanaged-retrieval-e2e")
    try:
        with e2e.serve_gateway(home, config_yaml=_PROFILE_CONFIG_YAML) as served:
            yield served, calls
    finally:
        monkeypatch.undo()


def _run_events(client: httpx.Client, base: str, thread_id: str, run_id: str, csrf: str) -> list[dict[str, Any]]:
    response = client.get(
        f"{base}/api/threads/{thread_id}/runs/{run_id}/events",
        headers={"X-CSRF-Token": csrf},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    events = body.get("events") if isinstance(body, dict) else body
    assert isinstance(events, list), body
    return events


def _events_of_type(events: list[dict[str, Any]], event_type: str) -> list[dict[str, Any]]:
    return [event for event in events if event.get("event_type") == event_type]


def test_the_default_profile_runs_its_configured_search_tool(
    unmanaged_gateway: tuple[e2e._Gateway, list[dict[str, Any]]],
) -> None:
    gateway, calls = unmanaged_gateway
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
        assert "error" not in observed.events, observed.events
        assert observed.events[-1] == "end", observed.events
        wire = gateway.journals.wait_for(observed.run_id)
        assert wire["outcome"] == "success", wire
        events = _run_events(client, base, thread_id, observed.run_id, csrf)

    # The handler ran once, with the model's query, through the real adapter.
    assert len(calls) == 1, calls
    assert calls[0]["query"] == "what is the capital of france", calls

    starts = _events_of_type(events, "tool_receipt.started.v1")
    outcomes = _events_of_type(events, "tool_receipt.outcome.v1")
    observations = _events_of_type(events, "retrieval.observation.v1")
    assert len(starts) == 1, starts
    assert len(outcomes) == 1, outcomes
    assert len(observations) == 1, observations

    started = starts[0]["content"]
    outcome = outcomes[0]["content"]
    observation = observations[0]["content"]
    draft = observation["draft"]
    assert starts[0]["metadata"]["evidence_capability"]["kind"] == "retrieval", starts[0]
    assert started["tool_name"] == "web_search", started
    assert outcome["phase"] == "succeeded", outcome
    # The atomic pair the durable contract requires: one start, one terminal,
    # and an observation joined to that exact receipt.
    assert outcome["receipt_id"] == started["receipt_id"], (outcome, started)
    assert draft["receipt_id"] == started["receipt_id"], (draft, started)

    # The evidence says what it is: an observed retrieval that makes no
    # governed claim, rather than four digests standing in for a revision
    # nobody promoted.
    tool_plane = draft["tool_plane"]
    assert tool_plane["mode"] == "unmanaged", tool_plane
    assert tool_plane["base_revision_digest"] is None, tool_plane
    assert tool_plane["user_overlay_digest"] is None, tool_plane
    assert tool_plane["projection_digest"] is None, tool_plane
    assert tool_plane["effective_digest"] is None, tool_plane
    assert draft["provider_id"] == "duckduckgo", draft
    assert draft["result_count"] == len(_PROVIDER_RESULTS), draft
    assert draft["source_count"] == len(_PROVIDER_RESULTS), draft

    # The query never reaches evidence, in either mode.
    assert "capital of france" not in json.dumps(started), started
    assert "capital of france" not in json.dumps(observation), observation


def test_the_search_result_reaches_the_model(
    unmanaged_gateway: tuple[e2e._Gateway, list[dict[str, Any]]],
) -> None:
    """A dispatched tool nobody reads would pass the test above and help no one."""

    gateway, _calls = unmanaged_gateway
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
        assert "error" not in observed.events, observed.events
        history = client.post(
            f"{base}/api/threads/{thread_id}/history",
            json={"limit": 20},
            headers={"X-CSRF-Token": csrf},
        )
        assert history.status_code == 200, history.text

    assert observed.text_frames >= 1, "the model answered after the tool result"
    body = history.json()
    serialized = json.dumps(body)
    assert "example.org/paris" in serialized, "the provider's result never reached the conversation"
