"""Whole mixed-source protocol proofs; no provider or generated authority."""

from copy import deepcopy
from dataclasses import replace

import httpx
import pytest
from _telegram_human_input_test_support import record, request, source_basis
from _telegram_repository_test_support import db as db  # noqa: F401
from test_telegram_human_input_plans import parts
from test_telegram_publication_authority import attempt_environment as attempt_environment  # noqa: F401
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment  # noqa: F401
from test_telegram_publication_observation import ACK, run_row

from app.telegram_bots.human_input_source import HumanInputSource, validate_request_basis
from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence


def mixed_basis(original=None, *, paths=("/mnt/user-data/outputs/first.txt", "/mnt/user-data/outputs/second.txt"), payload=None):
    basis = source_basis(original, payload=payload)
    basis["version"] = 2
    document = basis["projection"]["inventory"]["document"]
    document["version"] = 2
    document.pop("scan")
    document["output_view"] = {"kind": "no_admitted_output_view", "scope": {"version": 1, "kind": "managed_output_routing", "virtual_root": "/mnt/user-data/outputs"}}
    files = [{"kind": "file", "path": path, "surfaces": ["presentation_reference"], "witnesses": [{"kind": "root_snapshot", "root_epoch": document["root_epoch"], "message_id": "file-message"}]} for path in paths]
    document["items"] = [files[0], *document["items"], *files[1:]] if files else document["items"]
    refresh(basis)
    return basis


def refresh(basis):
    projection = basis["projection"]
    projection["inventory"]["evidence"] = build_event_inventory_evidence(projection["output"]["output_evidence"], status="success", inventory_event_seq=9, document=projection["inventory"]["document"]).model_dump(mode="json")


def test_whole_mixed_document_qualifies_and_preserves_order_without_mutation():
    from app.telegram_bots.human_input_source import mixed_file_paths

    basis = mixed_basis()
    original = deepcopy(basis)
    assert validate_request_basis(record(), basis) == request()
    assert mixed_file_paths(record(), basis) == ("/mnt/user-data/outputs/first.txt", "/mnt/user-data/outputs/second.txt")
    assert basis == original


@pytest.mark.parametrize("change", ["old_version", "no_files", "two_structured", "unknown_structured", "unpaired", "foreign_run", "changed_digest", "unsupported_request", "bool_version"])
def test_mixed_source_never_hides_unqualified_inventory_or_message(change):
    basis = mixed_basis()
    items = basis["projection"]["inventory"]["document"]["items"]
    if change == "old_version":
        basis["version"] = 1
    elif change == "no_files":
        items[:] = [items[1]]
    elif change == "two_structured":
        extra = deepcopy(items[1])
        extra["value_sha256"] = "a" * 64
        items.append(extra)
    elif change == "unknown_structured":
        items[1]["carrier"] = "terminal_field"
    elif change == "unpaired":
        basis["messages"][0]["content"]["tool_calls"][0]["id"] = "different"
    elif change == "foreign_run":
        basis["messages"][-1]["run_id"] = "other"
    elif change == "changed_digest":
        items[1]["value_sha256"] = "a" * 64
    elif change == "unsupported_request":
        basis = mixed_basis(payload=request(source="unrecognized"))
    else:
        basis["version"] = True
    refresh(basis)
    with pytest.raises(ValueError):
        validate_request_basis(record(), basis)


@pytest.mark.parametrize("path", ["/mnt/user-data/private.txt", "/mnt/user-data/outputs/../private.txt", "/mnt/user-data/outputs/a//b", "/mnt/user-data/outputs/a\\b", "/mnt/user-data/outputs/a\n", "/mnt/user-data/outputs/first.txt"])
def test_mixed_files_are_confined_canonical_and_unique(path):
    with pytest.raises(ValueError):
        basis = mixed_basis()
        basis["projection"]["inventory"]["document"]["items"][-1]["path"] = path
        validate_request_basis(record(), basis)


def test_attested_malformed_presentation_retains_source_but_old_single_request_stays_exact():
    from app.telegram_bots.human_input_source import mixed_file_paths

    payload = request(source="sandbox_network", input_mode="single_choice", details={"host": "example.test", "port": True, "ttl_seconds": 300})
    assert validate_request_basis(record(), mixed_basis(payload=payload)) == payload
    assert validate_request_basis(record(), source_basis()) == request()
    with pytest.raises(ValueError):
        mixed_file_paths(record(), source_basis())


def test_ui_freezing_is_identical_to_old_v5_and_only_final_part_has_controls():
    from app.telegram_bots.human_input_source import freeze_human_input_parts

    body = [*parts(record(), "First"), *parts(record())]
    keyboard = [[{"label": "label.choice", "label_values": {"number": 1}, "kind": "clarify_option", "target_kind": "thread", "target_id": "thread", "payload": {"request_id": "clarification:call", "option_id": "option-1"}}]]
    frozen, controls = freeze_human_input_parts(record(), request(), parts=body, keyboard=keyboard)
    assert controls == [[{**keyboard[0][0], "expires_at": None}]]
    assert "stored_keyboard" not in frozen[0] and frozen[1]["stored_keyboard"] is True
    assert [part["source_ref"] for part in frozen] == [{"key": "human_input.presentation", "values": {"request_id": "clarification:call", "part": index}} for index in range(2)]
    assert body[0].get("source_ref") is None


@pytest.mark.asyncio
async def test_actual_guarded_reader_issues_mixed_capability_and_old_v5_initial_refuses(coordinator_environment):
    from app.telegram_bots.human_input_source import check_qualified_request, prepare_human_input_reply

    e = coordinator_environment
    saved = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    basis = mixed_basis(replace(saved, run_id="coordinator-run"))

    async def respond(incoming):
        if incoming.url.path.endswith("/output"):
            return httpx.Response(200, headers=ACK, json=basis["projection"])
        if incoming.url.path.endswith("/messages"):
            return httpx.Response(200, headers=ACK, json={"data": basis["messages"], "has_more": False})
        return httpx.Response(200, headers=ACK, json={**run_row(saved), "metadata": {"channel_turn_id": saved.publication_id}, "status": "success"})

    e.responder = respond
    reader = HumanInputSource(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture)
    qualified = await reader.read_request(saved, e.link)
    actual = qualified.record
    assert qualified.basis == basis
    assert check_qualified_request(qualified, actual) == basis
    async with e.db.sessions.begin() as writer:
        inputs = await e.inbox.snapshot_publication_inputs(actual, session=writer)
    with pytest.raises(ValueError):
        prepare_human_input_reply(actual, basis, parts=parts(actual), keyboard=[], inputs=inputs)
    step, _ = prepare_human_input_reply(actual, basis, parts=parts(actual), keyboard=[], inputs=inputs, step_id="next")
    assert step["version"] == 5 and step["outcome"]["request_basis"] == basis
    copy = qualified.basis
    copy["messages"].clear()
    assert qualified.basis == basis


def test_mixed_structured_callback_witness_cannot_impersonate_actual_tool_message():
    basis = mixed_basis()
    basis["projection"]["inventory"]["document"]["items"][1]["witness"] = {"kind": "callback", "callback_run_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}
    refresh(basis)
    with pytest.raises(ValueError):
        validate_request_basis(record(), basis)


@pytest.mark.asyncio
@pytest.mark.parametrize("retire", [False, True], ids=["current", "expired-after-first-read"])
async def test_mixed_original_guard_is_preserved_without_new_metadata_leaves(coordinator_environment, retire):
    e = coordinator_environment
    saved = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    basis = mixed_basis(replace(saved, run_id="coordinator-run"))
    # Bind the run through the existing coordinator before the guarded exact-run read.
    actual = await e.inbox.bind_publication_run(saved, run_id="coordinator-run", guard=(await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)).read_guard)
    active, calls = [True], []
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)

    async def respond(incoming):
        calls.append(incoming.url.path)
        if incoming.url.path.endswith("/output"):
            if retire:
                active[0] = False
            return httpx.Response(200, headers=ACK, json=basis["projection"])
        return httpx.Response(200, headers=ACK, json={"data": basis["messages"], "has_more": False})

    class Guard:
        captured_link = e.link

        def current(self):
            return active[0]

        async def prepare(self):
            if not self.current():
                raise ValueError("expired")
            await attempt.read_guard.prepare()

        async def check(self, writer):
            return await attempt.read_guard.check(writer)

    e.responder = respond
    reader = HumanInputSource(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture)
    if retire:
        from app.channels.execution import ChannelExecutionProtocolError

        with pytest.raises(ChannelExecutionProtocolError):
            await reader.read_request(actual, e.link, guard=Guard())
        assert len(calls) == 1
    else:
        qualified = await reader.read_request(actual, e.link, guard=Guard())
        assert qualified.basis == basis
        assert len(calls) == 3
        active[0] = False
        from app.telegram_bots.human_input_source import check_qualified_request

        with pytest.raises(ValueError):
            check_qualified_request(qualified, actual)


@pytest.mark.asyncio
async def test_missing_inventory_remains_a_controlled_source_refusal(coordinator_environment):
    e = coordinator_environment
    saved = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    basis = mixed_basis(replace(saved, run_id="coordinator-run"))
    inventory = basis["projection"]["inventory"]
    inventory.update(state="unavailable", document=None, reason="source_missing")

    async def respond(incoming):
        if incoming.url.path.endswith("/output"):
            return httpx.Response(200, headers=ACK, json=basis["projection"])
        if incoming.url.path.endswith("/messages"):
            return httpx.Response(200, headers=ACK, json={"data": basis["messages"], "has_more": False})
        return httpx.Response(200, headers=ACK, json={**run_row(saved), "metadata": {"channel_turn_id": saved.publication_id}, "status": "success"})

    e.responder = respond
    reader = HumanInputSource(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture)
    with pytest.raises(ValueError, match="human-input-source-conflict"):
        await reader.read_request(saved, e.link)
