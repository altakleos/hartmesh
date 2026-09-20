"""Both tenant-class repairs on one turn, through the real Gateway.

The tenant class ran two turns on `v2.1.0+hartmesh.21`, and each showed both
defects at once:

* "Research the Great Lakes and give me a concise overview with sources."
  made three ``web_fetch`` calls to three addresses, each answered by the
  fetch provider's same deterministic 401, and consumed ten model calls and
  176,348 tokens before answering from search snippets.
* "Create pdf about muse agent" made thirteen such calls to thirteen
  addresses, wrote ``/mnt/user-data/outputs/Muse_Agent_Report.pdf`` through a
  ``bash`` call that omitted the typed ``present`` argument, never called
  ``present_files``, and ended durable ``error`` with stop reason
  ``artifact_delivery_incomplete`` while a valid 14,710-byte PDF sat in the
  outputs directory.

This drives both prompts through the same Gateway, route, admission, worker,
receipt middleware, tool dispatch, delivery fence and SSE consumer a tenant
runs, and asserts the whole shape a person experiences: the refusal stops
after one call, the run ends ``success``, the artifact is reported as
delivered in the turn's own messages, it downloads afterwards, and no
recovery-only notice is involved.

What is synthetic, and deliberately so: the model is the turn-phase probe
scripted to emit the captured failure shapes rather than a real one, the
fetch provider is a stand-in that refuses the way the hosted reader did, and
the PDF is written into the thread's outputs directory mid-turn (the probe
issues no ``bash`` call, and the worker's scan cannot tell that file from one
the agent wrote, which is the point). A real-model run of these exact prompts
on the released profile is the tenant class's to make; this is what can be
made deterministic and kept.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import _turn_phase_probe_model as probe
import httpx
import pytest
import test_turn_phase_gateway_stream_e2e as e2e

from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY
from deerflow.agents.middlewares.unbound_tool_call_middleware import REFUSED_TOOL_NAME
from deerflow.community.direct_fetch import tools as fetch_tools
from deerflow.community.web_fetch_outcome import FetchRefusal
from deerflow.runtime.presented_files import PRESENTED_BY_KEY, PRESENTED_FILES_KEY

# The exact addresses and artifact the tenant-class turns used.
GREAT_LAKES_URL = "https://www.epa.gov/greatlakes/great-lakes-facts-and-figures"
MUSE_URL = "https://about.fb.com/news/2026/09/introducing-muse-personal-ai-agent/"
ARTIFACT_NAME = "Muse_Agent_Report.pdf"
ARTIFACT_PATH = f"/mnt/user-data/outputs/{ARTIFACT_NAME}"
# Valid enough to be a real download: a PDF header, a body, and an EOF marker.
ARTIFACT_BYTES = b"%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>\nendobj\ntrailer\n<< /Root 1 0 R >>\n%%EOF\n"


def _profile_config() -> str:
    """The tool shape the released profile renders for a keyless tenant."""
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


class _RefusingProvider:
    """The hosted reader's behaviour: every address, the same refusal.

    *on_fetch* stands in for the producing call that wrote a file and left it
    out of ``present``. It runs here, inside the tool call, because this is
    the only place the write is guaranteed to land inside the window
    ``RuntimeDeliveryMiddleware`` measures -- after its ``before_agent``
    snapshot and before its ``after_agent`` diff.

    Writing from the SSE consumer instead races both edges of that window,
    and the run fails whichever one it loses. Too early and the file is
    already in the middleware's snapshot, so nothing was "produced"; too late
    and the diff has already been taken. Either way the middleware presents
    nothing while the worker's fence -- whose own snapshot is taken before the
    first frame is even published -- still sees a file this turn produced, and
    the run ends ``artifact_delivery_incomplete`` with nothing presented. The
    late edge is the reachable one: the client decides when it writes, and it
    has an unbounded tree walk and the machine's load between it and the
    frame, while the run is free to finish. Reproduced at 5 s of client lag.
    """

    def __init__(self, *, on_fetch: Any = None) -> None:
        self.urls: list[str] = []
        self._on_fetch = on_fetch

    async def fetch(self, url: str) -> FetchRefusal:
        self.urls.append(url)
        if self._on_fetch is not None:
            self._on_fetch()
        return FetchRefusal("provider", "auth", "the fetch provider refuses this deployment's requests without a valid key", 401)


@pytest.fixture(scope="module")
def composed_gateway(tmp_path_factory: pytest.TempPathFactory) -> Iterator[e2e._Gateway]:
    with e2e.serve_gateway(tmp_path_factory.mktemp("df21-df22-composed"), config_yaml=_profile_config()) as served:
        yield served


def _walk(node: Any, found: list[dict[str, Any]], match: Any) -> None:
    if isinstance(node, dict):
        if match(node):
            found.append(node)
        for value in node.values():
            _walk(value, found, match)
    elif isinstance(node, list):
        for value in node:
            _walk(value, found, match)


def _fetch_results(history: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    _walk(history, found, lambda node: node.get("type") == "tool" and node.get("name") == "web_fetch")
    return found


def _tagged(history: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    _walk(history, found, lambda node: isinstance((node.get("additional_kwargs") or {}).get(PRESENTED_FILES_KEY), list))
    return found


def _turn(gateway: e2e._Gateway, prompt: str) -> dict[str, Any]:
    base = gateway.loopback_url
    probe.BOUND_TOOL_NAMES.clear()
    with httpx.Client() as client:
        csrf, thread_id = e2e._register_and_create_thread(client, base)
        observed = e2e._observe_stream(client, base, thread_id, csrf, prompt, timeout=120.0, recursion_limit=100)
        run = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}").json()
        history = client.post(f"{base}/api/threads/{thread_id}/history", json={"limit": 30}, headers={"X-CSRF-Token": csrf}).json()
        delivery = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}/delivery").json()
        # What a reloaded page does for the file: a request of its own, once
        # the stream is finished, on the route the chip links to.
        download = client.get(
            f"{base}/api/threads/{thread_id}/artifacts/{ARTIFACT_PATH.lstrip('/')}",
            params={"download": "true"},
            headers={"X-CSRF-Token": csrf},
        )
    return {
        "observed": observed,
        "run": run,
        "history": history,
        "delivery": delivery,
        "download": download,
        "thread_id": thread_id,
        "bound": [list(names) for names in probe.BOUND_TOOL_NAMES],
    }


def _artifact_writer(home: Any, thread_id_box: dict[str, str]) -> Any:
    """The producing call's write: the PDF, into this thread's outputs directory.

    The probe issues no ``bash`` call, so this stands in for the one that
    omitted ``present``. Hand it to ``_RefusingProvider(on_fetch=...)`` so it
    runs inside the tool call; that is what makes it land in the delivery
    middleware's window every time rather than most times.
    """
    written = {"done": False}

    def write() -> None:
        if written["done"]:
            return
        for candidate in home.rglob(f"threads/{thread_id_box['id']}/user-data/outputs"):
            if candidate.is_dir():
                (candidate / ARTIFACT_NAME).write_bytes(ARTIFACT_BYTES)
                written["done"] = True
                return

    return write, written


def test_the_great_lakes_turn_stops_at_one_refusal_and_still_answers(
    composed_gateway: e2e._Gateway,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _RefusingProvider()
    monkeypatch.setattr(fetch_tools, "_client_from_config", lambda _config: provider)

    result = _turn(composed_gateway, f"probe:fetch {GREAT_LAKES_URL}")
    observed = result["observed"]

    assert "error" not in observed.events, observed.events
    assert observed.events[-1] == "end", observed.events
    assert composed_gateway.journals.wait_for(observed.run_id)["outcome"] == "success"
    assert observed.text_frames >= 1, "the turn answered rather than ending on the refusal"

    # One call found the provider out. The tenant-class turn made three.
    assert provider.urls == [GREAT_LAKES_URL], provider.urls
    [refusal] = _fetch_results(result["history"])
    assert refusal["additional_kwargs"][TOOL_META_KEY]["error_scope"] == "provider"
    assert "unavailable for the rest of this turn" in refusal["content"]

    # And the model could not have made a second call: the tool is gone.
    bound = result["bound"]
    assert len(bound) >= 2 and "web_fetch" in bound[0], bound
    assert all("web_fetch" not in names for names in bound[1:]), bound
    # The turn produced nothing, so delivery has nothing to say.
    assert result["run"]["status"] == "success"
    assert result["delivery"] == {"available": False, "version": 1}


def test_the_muse_turn_refuses_once_delivers_the_pdf_and_ends_success(
    composed_gateway: e2e._Gateway,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The thread id is only known once the run has been created, so the writer
    # reads it from the box the turn fills in before the fetch can happen.
    thread_id_box: dict[str, str] = {"id": ""}
    write_artifact, written = _artifact_writer(composed_gateway.tmp_home, thread_id_box)
    provider = _RefusingProvider(on_fetch=write_artifact)
    monkeypatch.setattr(fetch_tools, "_client_from_config", lambda _config: provider)

    base = composed_gateway.loopback_url
    probe.BOUND_TOOL_NAMES.clear()
    with httpx.Client() as client:
        csrf, thread_id = e2e._register_and_create_thread(client, base)
        thread_id_box["id"] = thread_id
        observed = e2e._observe_stream(client, base, thread_id, csrf, f"probe:fetch {MUSE_URL}", timeout=120.0, recursion_limit=100)
        run = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}").json()
        history = client.post(f"{base}/api/threads/{thread_id}/history", json={"limit": 30}, headers={"X-CSRF-Token": csrf}).json()
        delivery = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}/delivery").json()
        # What a reloaded page does for the file: a request of its own, after
        # the stream is finished and closed, on the route the chip links to.
        download = client.get(
            f"{base}/api/threads/{thread_id}/artifacts/{ARTIFACT_PATH.lstrip('/')}",
            params={"download": "true"},
            headers={"X-CSRF-Token": csrf},
        )
        # The run archive is built from the receipt's presented set, not from
        # the message the chip came from. It is the reader that would 409 if
        # the runtime's handover were recorded anywhere but ``presented_files``.
        archive = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}/artifacts/archive", headers={"X-CSRF-Token": csrf})
    # And nobody else's: a different person asking for the same path is
    # refused, so making delivery automatic widened no authorization.
    with httpx.Client() as stranger:
        stranger_csrf, _ = e2e._register_and_create_thread(stranger, base)
        forbidden = stranger.get(
            f"{base}/api/threads/{thread_id}/artifacts/{ARTIFACT_PATH.lstrip('/')}",
            params={"download": "true"},
            headers={"X-CSRF-Token": stranger_csrf},
        )
    bound = [list(names) for names in probe.BOUND_TOOL_NAMES]

    assert written["done"], "the artifact was never written, so nothing was exercised"

    # DF21: one refusal, then the tool is withdrawn. The tenant made thirteen.
    assert provider.urls == [MUSE_URL], provider.urls
    assert all("web_fetch" not in names for names in bound[1:]), bound

    # DF22: the run a person asked for ends success, with the file delivered.
    assert "error" not in observed.events, observed.events
    assert observed.events[-1] == "end", observed.events
    assert run["status"] == "success", "a requested artifact that exists is not an error"
    assert run.get("stop_reason") is None, run.get("stop_reason")
    assert delivery == {"available": False, "version": 1}, "and not through a recovery notice"
    assert not [payload for name, payload in observed.frames if name == "custom" and isinstance(payload, dict) and str(payload.get("type", "")).startswith("artifact_delivery_")]

    # The receipt and what the person sees are the same file.
    tagged = _tagged(history)
    assert tagged, "the turn's messages never reported the file as delivered"
    presented = [path for message in tagged for path in message["additional_kwargs"][PRESENTED_FILES_KEY]]
    assert presented == [ARTIFACT_PATH], presented
    assert any(message["additional_kwargs"].get(PRESENTED_BY_KEY) == "runtime" for message in tagged)

    # And it downloads, from a session that did not watch the turn.
    assert download.status_code == 200, download.text
    # As does the run archive, which reads the receipt rather than the message.
    assert archive.status_code == 200, archive.text
    assert archive.json()["file_count"] == 1, archive.text
    assert download.content == ARTIFACT_BYTES, "the bytes on the wire are the bytes the turn wrote"
    assert forbidden.status_code in (403, 404), forbidden.text


def test_the_next_turn_in_the_same_chat_tries_the_fetch_once_more(
    composed_gateway: e2e._Gateway,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tenant asks again in the same chat, and the fetch is tried again.

    The scope matters in both directions. Too narrow and the thirteen-call
    loop comes back inside one turn; too wide and a key added between turns,
    or a provider that recovers, is never tried again in a conversation the
    person is still holding. What this adds over the unit tests is the whole
    second turn: the same thread, the same Gateway, the first turn's answer
    and artifact already in history, a run of its own that ends ``success``
    and neither inherits the first turn's delivery nor re-presents its file.

    What it does *not* prove, stated because it reads as though it does: that
    the withdrawal is keyed by run. The Gateway builds a fresh agent for each
    run, so the state dies with the middleware instance whatever the key is --
    keying on the thread alone passes here unchanged. The key is load-bearing
    only when one instance outlives a run, and
    ``test_provider_refusal_middleware.py`` is where that is pinned; it fails
    on exactly that mutation.
    """
    thread_id_box: dict[str, str] = {"id": ""}
    write_artifact, written = _artifact_writer(composed_gateway.tmp_home, thread_id_box)
    provider = _RefusingProvider(on_fetch=write_artifact)
    monkeypatch.setattr(fetch_tools, "_client_from_config", lambda _config: provider)
    base = composed_gateway.loopback_url

    with httpx.Client() as client:
        csrf, thread_id = e2e._register_and_create_thread(client, base)
        thread_id_box["id"] = thread_id
        probe.BOUND_TOOL_NAMES.clear()
        first = e2e._observe_stream(client, base, thread_id, csrf, f"probe:fetch {MUSE_URL}", timeout=120.0, recursion_limit=100)
        first_bound = [list(names) for names in probe.BOUND_TOOL_NAMES]

        probe.BOUND_TOOL_NAMES.clear()
        second = e2e._observe_stream(client, base, thread_id, csrf, f"probe:fetch {GREAT_LAKES_URL}", timeout=120.0, recursion_limit=100)
        second_bound = [list(names) for names in probe.BOUND_TOOL_NAMES]
        second_run = client.get(f"{base}/api/threads/{thread_id}/runs/{second.run_id}").json()
        second_delivery = client.get(f"{base}/api/threads/{thread_id}/runs/{second.run_id}/delivery").json()
        history = client.post(f"{base}/api/threads/{thread_id}/history", json={"limit": 40}, headers={"X-CSRF-Token": csrf}).json()

    assert written["done"], "the first turn wrote no artifact, so the follow-up proves nothing"
    assert first.run_id != second.run_id, "one chat, two runs"

    # Each turn tried exactly once: the withdrawal held within a run and was
    # gone by the next one.
    assert provider.urls == [MUSE_URL, GREAT_LAKES_URL], provider.urls
    assert "web_fetch" in first_bound[0] and all("web_fetch" not in names for names in first_bound[1:]), first_bound
    assert "web_fetch" in second_bound[0], "the next turn must be able to try again"
    assert all("web_fetch" not in names for names in second_bound[1:]), second_bound

    # The follow-up is a turn of its own: it ends success, and it neither
    # inherits the first turn's delivery nor re-presents its file.
    assert "error" not in second.events and second.events[-1] == "end", second.events
    assert second_run["status"] == "success" and second_run.get("stop_reason") is None
    assert second_delivery == {"available": False, "version": 1}, "this turn produced nothing to deliver"

    # The artifact is still reported exactly once, on the turn that made it.
    presented = [path for message in _tagged(history) for path in message["additional_kwargs"][PRESENTED_FILES_KEY]]
    assert presented == [ARTIFACT_PATH], presented


def test_a_turn_that_needs_no_fetch_is_untouched_by_either_repair(composed_gateway: e2e._Gateway) -> None:
    """The ordinary turn, so neither repair is paid for by every other one."""
    result = _turn(composed_gateway, "probe:text please")
    observed = result["observed"]

    assert "error" not in observed.events, observed.events
    assert result["run"]["status"] == "success"
    assert result["delivery"] == {"available": False, "version": 1}
    assert not _tagged(result["history"]), "nothing was produced, so nothing is reported as delivered"
    assert all("web_fetch" in names for names in result["bound"]), "the tool stays available to a turn that never used it"
    assert json.dumps(result["history"]).count("unavailable for the rest of this turn") == 0


def _tool_messages(history: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    _walk(history, found, lambda node: node.get("type") == "tool")
    return found


def test_the_captured_malformed_tool_name_neither_executes_nor_ends_the_run(
    composed_gateway: e2e._Gateway,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The third shape a released-profile qualification captured, on this same Gateway.

    One of two identical "create a PDF about X" requests ended after about a
    minute with a generic ``Runtime operation failed (reference: ...)`` and a
    durable ``error`` carrying ``ToolEvidenceError``. Nothing had executed: the
    model put a 129-byte shell command where the tool name belongs, the receipt
    layer refused it as an identity, and that refusal ended the run.

    The probe replays that call verbatim -- name, id and arguments, streamed as
    the provider streamed them -- and then, once the runtime has answered it,
    recovers with a call the runtime can honour. It shares this module's
    Gateway because it needs the same profile: a second one would double a
    minute of boot on one CI shard to prove nothing extra.
    """
    thread_id_box: dict[str, str] = {"id": ""}
    write_artifact, _written = _artifact_writer(composed_gateway.tmp_home, thread_id_box)
    provider = _RefusingProvider(on_fetch=write_artifact)
    monkeypatch.setattr(fetch_tools, "_client_from_config", lambda _config: provider)

    base = composed_gateway.loopback_url
    probe.BOUND_TOOL_NAMES.clear()
    with httpx.Client() as client:
        csrf, thread_id = e2e._register_and_create_thread(client, base)
        thread_id_box["id"] = thread_id
        observed = e2e._observe_stream(client, base, thread_id, csrf, f"probe:malformed {MUSE_URL}", timeout=120.0, recursion_limit=100)
        run = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}").json()
        history = client.post(f"{base}/api/threads/{thread_id}/history", json={"limit": 40}, headers={"X-CSRF-Token": csrf}).json()
        download = client.get(f"{base}/api/threads/{thread_id}/artifacts/{ARTIFACT_PATH.lstrip('/')}", params={"download": "true"}, headers={"X-CSRF-Token": csrf})

    # Not what the deployment got.
    assert run["status"] == "success", run
    blob = str(history)
    assert "ToolEvidenceError" not in blob
    assert "Runtime operation failed" not in blob, "the generic terminal error is the defect"

    # One actionable result, and the hostile string is not its identity.
    refusals = [message for message in _tool_messages(history) if message.get("name") == REFUSED_TOOL_NAME]
    assert len(refusals) == 1, "exactly one synthetic result, not a retry loop"
    assert "not one of the tools available to you" in str(refusals[0].get("content"))
    assert "web_fetch" in str(refusals[0].get("content")), "it names what this run can actually call"
    assert "weasyprint --version" not in str([message.get("name") for message in _tool_messages(history)])

    # Nothing executed the name, and the model's own next call went through.
    assert provider.urls == [MUSE_URL], "the only fetch is the corrected call, not the refused one"
    assert len([message for message in _tool_messages(history) if message.get("name") == "web_fetch"]) == 1

    # And the file the turn produced still reaches the person.
    tagged = _tagged(history)
    assert [path for message in tagged for path in message["additional_kwargs"][PRESENTED_FILES_KEY]] == [ARTIFACT_PATH]
    assert tagged[-1]["additional_kwargs"][PRESENTED_BY_KEY] == "runtime"
    assert download.status_code == 200 and download.content == ARTIFACT_BYTES
