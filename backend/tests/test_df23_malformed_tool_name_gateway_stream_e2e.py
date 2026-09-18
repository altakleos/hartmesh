"""The captured malformed tool call, driven through the real Gateway.

In a released-profile qualification, one of two identical "create a PDF about
X" requests ended after about a minute with the generic

    Runtime operation failed (reference: ...)

and a durable ``error`` carrying ``ToolEvidenceError``. Nothing had executed:
the model put a 129-byte shell command where the tool name belongs, the receipt
layer refused it as an identity, and that refusal ended the run.

This drives the same shape -- the captured name, id and arguments, streamed as
the provider streamed them -- through the Gateway, admission, worker, receipt
middleware, tool dispatch and SSE consumer a tenant runs, and asserts what the
person should get instead: no terminal error, one result the model can act on,
its own next call dispatched normally, and the file the turn produced delivered.

Synthetic, deliberately: the model is the turn-phase probe scripted to emit the
captured shape rather than a real one, and the corrected call is ``web_fetch``
because that is what this profile binds -- the property under test is that a
refused call leaves the run able to dispatch the next one, not which tool it
picks. A real-model repetition on a released profile is a separate, later check.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import _turn_phase_probe_model as probe
import httpx
import pytest
import test_df21_df22_composed_gateway_stream_e2e as composed
import test_turn_phase_gateway_stream_e2e as e2e

from deerflow.agents.middlewares.unbound_tool_call_middleware import REFUSED_TOOL_NAME
from deerflow.runtime.presented_files import PRESENTED_BY_KEY, PRESENTED_FILES_KEY


@pytest.fixture(scope="module")
def malformed_gateway(tmp_path_factory: pytest.TempPathFactory) -> Iterator[e2e._Gateway]:
    with e2e.serve_gateway(tmp_path_factory.mktemp("df23-malformed"), config_yaml=composed._profile_config()) as served:
        yield served


def _tool_messages(history: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    composed._walk(history, found, lambda node: node.get("type") == "tool")
    return found


def test_the_captured_malformed_call_neither_executes_nor_ends_the_run(malformed_gateway: e2e._Gateway, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = composed._RefusingProvider()
    monkeypatch.setattr("deerflow.community.direct_fetch.tools._client_from_config", lambda _config: provider)

    thread_id_box: dict[str, str] = {"id": ""}
    on_frame, _written = composed._write_artifact_mid_turn(malformed_gateway.tmp_home, thread_id_box)

    base = malformed_gateway.loopback_url
    probe.BOUND_TOOL_NAMES.clear()
    with httpx.Client() as client:
        csrf, thread_id = e2e._register_and_create_thread(client, base)
        thread_id_box["id"] = thread_id
        observed = e2e._observe_stream(
            client,
            base,
            thread_id,
            csrf,
            f"probe:malformed {composed.MUSE_URL}",
            on_frame=on_frame,
            timeout=120.0,
            recursion_limit=100,
        )
        run = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}").json()
        history = client.post(f"{base}/api/threads/{thread_id}/history", json={"limit": 40}, headers={"X-CSRF-Token": csrf}).json()
        delivery = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}/delivery").json()
        download = client.get(
            f"{base}/api/threads/{thread_id}/artifacts/{composed.ARTIFACT_PATH.lstrip('/')}",
            params={"download": "true"},
            headers={"X-CSRF-Token": csrf},
        )

    # 1. The run is not what the tenant got.
    assert run["status"] == "success", run
    assert run.get("stop_reason") in (None, "", "end_turn"), run
    blob = str(history)
    assert "ToolEvidenceError" not in blob
    assert "Runtime operation failed" not in blob, "the generic terminal error is the defect"

    # 2. One actionable result, and the hostile string is not its identity.
    refusals = [message for message in _tool_messages(history) if message.get("name") == REFUSED_TOOL_NAME]
    assert len(refusals) == 1, "exactly one synthetic result, not a retry loop"
    assert "not one of the tools available to you" in str(refusals[0].get("content"))
    assert "web_fetch" in str(refusals[0].get("content")), "it names what this run can actually call"

    # 3. Nothing executed the name, and it never became an identity anywhere a
    #    reader of this conversation would meet it.
    assert "weasyprint --version" not in str([message.get("name") for message in _tool_messages(history)])
    assert provider.urls == [composed.MUSE_URL], "the only fetch is the corrected call, not the refused one"

    # 4. The model's own next call went through normally.
    executed = [message for message in _tool_messages(history) if message.get("name") == "web_fetch"]
    assert len(executed) == 1, "the refusal did not poison the rest of the run"

    # 5. The file the turn produced still reaches the person.
    tagged = [message for message in composed._tagged(history)]
    presented = [path for message in tagged for path in message["additional_kwargs"][PRESENTED_FILES_KEY]]
    assert presented == [composed.ARTIFACT_PATH]
    assert tagged[-1]["additional_kwargs"][PRESENTED_BY_KEY] == "runtime"
    assert delivery["available"] is False or delivery.get("version") == 1, delivery
    assert download.status_code == 200 and download.content == composed.ARTIFACT_BYTES
