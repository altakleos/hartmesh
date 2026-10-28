"""Actual SQL custody and recorded HTTP source; protocol proofs are seeds."""

from copy import deepcopy
from dataclasses import replace

import httpx
import pytest
from _telegram_human_input_test_support import source_basis
from _telegram_repository_test_support import db as db
from sqlalchemy import delete
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_publication_observation import ACK, run_row

from app.telegram_bots.human_input_source import HumanInputSource
from app.telegram_bots.model_source import ModelSource
from app.telegram_bots.producer import DeliveryClaimRetired, SendingPart
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow


async def source(e):
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    basis = source_basis(replace(record, run_id="coordinator-run"))

    async def respond(request):
        if request.url.path.endswith("/output"):
            return httpx.Response(200, headers=ACK, json=basis["projection"])
        if request.url.path.endswith("/messages"):
            return httpx.Response(200, headers=ACK, json={"data": basis["messages"], "has_more": False})
        return httpx.Response(200, headers=ACK, json={**run_row(record), "metadata": {"channel_turn_id": record.publication_id}, "status": "success"})

    e.responder = respond
    reader = HumanInputSource(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture)
    qualified = await reader.read_request(record, e.link)
    return qualified, reader, basis


def parts(record, text="Which option?", *, force_reply=False):
    return [{"kind": "text", "html": text, "visible_text": text, "reply_to_message_id": record.origins[0].message_id, "force_reply": force_reply}]


@pytest.mark.asyncio
async def test_qualified_request_primary_and_aux_step_keep_source_read_for_each_part(coordinator_environment):
    e = coordinator_environment
    qualified, reader, basis = await source(e)
    record = qualified.record
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    identity = await e.outbox.plan_human_input_reply(record, qualified, parts=parts(record), keyboard=[], callbacks=None, guard=attempt.read_guard)
    item = await e.outbox.get(e.link, identity)
    assert item.payload["reply_basis"]["version"] == 5 and item.payload["reply_basis"]["finish_route"] is True
    model = ModelSource(read_output=e.coordinator.read_output, human_input_source=reader)
    captured = SendingPart(e.link, item, item.parts[0], "process", "c" * 32, e.link.token_revision)
    prepared = await model.read_part(captured)
    assert prepared.human_input and prepared.html == "Which option?"
    step = await e.outbox.plan_human_input_step(record, qualified, step_id="next-field:1", parts=parts(record, "Next field", force_reply=True), keyboard=[], callbacks=None, guard=attempt.read_guard)
    assert step != identity
    assert (await e.outbox.get(e.link, step)).payload["reply_basis"]["finish_route"] is False
    basis["messages"][-1]["content"]["artifact"]["human_input"]["question"] = "Changed after plan"
    with pytest.raises(DeliveryClaimRetired):
        await model.read_part(captured)


@pytest.mark.asyncio
async def test_capability_is_not_reconstructed_from_mutable_basis_or_bus_metadata(coordinator_environment):
    e = coordinator_environment
    qualified, reader, _basis = await source(e)
    raw = qualified.basis
    raw["messages"][-1]["content"]["artifact"]["human_input"]["question"] = "Caller mutation"
    assert qualified.request["question"] == "Which option?"
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    with pytest.raises(ValueError):
        await e.outbox.plan_human_input_reply(qualified.record, deepcopy(raw), parts=parts(qualified.record), keyboard=[], callbacks=None, guard=attempt.read_guard)


@pytest.mark.asyncio
async def test_controls_only_last_part_and_replay_steps_survive_original_input_gc(coordinator_environment):
    from app.telegram_bots.callbacks import CallbackRepository

    e = coordinator_environment
    qualified, reader, _basis = await source(e)
    record = qualified.record
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    callbacks = CallbackRepository(e.db.sessions)
    body = [*parts(record, "Complete first part"), *parts(record, "Which option?")]
    keyboard = [[{"label": "label.choice", "label_values": {"number": 1}, "kind": "clarify_option", "target_kind": "thread", "target_id": e.thread_id, "payload": {"request_id": "clarification:call", "option_id": "option-1"}}]]
    identity = await e.outbox.plan_human_input_reply(record, qualified, parts=body, keyboard=keyboard, callbacks=callbacks, guard=attempt.read_guard)
    item = await e.outbox.get(e.link, identity)
    assert item.parts[0].get("callback_group_id") is None and item.parts[0].get("stored_keyboard") is None
    assert item.parts[-1]["callback_group_id"] and item.parts[-1]["stored_keyboard"] is True
    assert [saved.id for saved in await e.outbox.list_human_inputs(e.link, topic=None)] == [identity]
    assert await e.outbox.plan_human_input_reply(record, qualified, parts=body, keyboard=keyboard, callbacks=callbacks, guard=attempt.read_guard) == identity
    async with e.db.sessions.begin() as writer:
        await writer.execute(delete(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id))
    qualified = await reader.read_request(record, e.link)
    assert qualified is not None, "Fresh exact source reads outlive original inbox tombstone retention"
    assert await e.outbox.plan_human_input_reply(record, qualified, parts=body, keyboard=keyboard, callbacks=callbacks, guard=attempt.read_guard) == identity
    step = await e.outbox.plan_human_input_step(record, qualified, step_id="after-original-gc", parts=parts(record, "Type an answer", force_reply=True), keyboard=[], callbacks=callbacks, guard=attempt.read_guard)
    assert step != identity
    with pytest.raises(ValueError):
        await e.outbox.plan_human_input_step(record, qualified, step_id="invalid-two-markups", parts=parts(record, force_reply=True), keyboard=keyboard, callbacks=callbacks, guard=attempt.read_guard)


@pytest.mark.asyncio
@pytest.mark.parametrize("aux", [False, True], ids=["primary", "auxiliary"])
@pytest.mark.parametrize("retire", [False, True], ids=["current", "retired-during-source-read"])
async def test_original_ui_fence_is_last_after_source_writer_read(coordinator_environment, monkeypatch, aux, retire):
    from sqlalchemy import func, select

    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = coordinator_environment
    qualified, _reader, _basis = await source(e)
    record = qualified.record
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    if aux:
        await e.outbox.plan_human_input_reply(record, qualified, parts=parts(record), keyboard=[], callbacks=None, guard=attempt.read_guard)
    active, reads = [True], [0]
    actual_check = type(qualified._guard).check

    async def source_check(self, writer):
        result = await actual_check(self, writer)
        if self is qualified._guard:
            reads[0] += 1
            if reads[0] == 2 and retire:
                active[0] = False
        return result

    monkeypatch.setattr(type(qualified._guard), "check", source_check)

    class GestureGuard:
        captured_link = e.link

        async def prepare(self):
            await attempt.read_guard.prepare()

        async def check(self, writer):
            result = await attempt.read_guard.check(writer)
            if not active[0]:
                raise ValueError("retired-original-gesture")
            return result

    async with e.db.sessions() as session:
        before = await session.scalar(select(func.count()).select_from(TelegramOutboxRow))
    planner = e.outbox.plan_human_input_step if aux else e.outbox.plan_human_input_reply
    kwargs = {"step_id": "guard-order"} if aux else {}
    if retire:
        with pytest.raises(ValueError, match="retired-original-gesture"):
            await planner(record, qualified, parts=parts(record), keyboard=[], callbacks=None, guard=GestureGuard(), **kwargs)
    else:
        await planner(record, qualified, parts=parts(record), keyboard=[], callbacks=None, guard=GestureGuard(), **kwargs)
    assert reads[0] == 2
    async with e.db.sessions() as session:
        after = await session.scalar(select(func.count()).select_from(TelegramOutboxRow))
    assert after == before + (0 if retire else 1)


@pytest.mark.asyncio
async def test_keyboard_cleanup_is_only_a_closed_source_qualified_edit(coordinator_environment):
    from app.telegram_bots.human_input_source import prepare_human_input_reply

    e = coordinator_environment
    qualified, _reader, _basis = await source(e)
    record = qualified.record
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    identity = await e.outbox.plan_human_input_reply(record, qualified, parts=parts(record), keyboard=[], callbacks=None, guard=attempt.read_guard)
    item = await e.outbox.get(e.link, identity)
    part = {**parts(record)[0], "kind": "edit", "target_message_id": 701, "remove_keyboard": True}
    saved, _prepared = prepare_human_input_reply(record, qualified.basis, parts=[part], keyboard=[], inputs=item.payload["reply_basis"]["inputs"], step_id="cleanup")
    assert saved["parts"][0]["remove_keyboard"] is True
    for invalid in [{**part, "kind": "text"}, {**part, "remove_keyboard": 1}, {**part, "force_reply": True}]:
        with pytest.raises(ValueError):
            prepare_human_input_reply(record, qualified.basis, parts=[invalid], keyboard=[], inputs=item.payload["reply_basis"]["inputs"], step_id="bad-cleanup")


@pytest.mark.asyncio
@pytest.mark.parametrize("retire", [False, True], ids=["current", "expired"])
async def test_source_reader_never_starts_another_metadata_leaf_after_original_control_expires(coordinator_environment, retire):
    e = coordinator_environment
    qualified, reader, _basis = await source(e)
    record = qualified.record
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    active, calls = [True], []
    original = e.responder

    async def expire_on_output(request):
        calls.append(request.url.path)
        response = await original(request)
        if request.url.path.endswith("/output") and retire:
            active[0] = False
        return response

    e.responder = expire_on_output

    class OriginalControl:
        captured_link = e.link

        def current(self):
            return active[0]

        async def prepare(self):
            if not self.current():
                raise ValueError("original-control-expired")
            await attempt.read_guard.prepare()
            if not self.current():
                raise ValueError("original-control-expired")

        async def check(self, writer):
            result = await attempt.read_guard.check(writer)
            if not self.current():
                raise ValueError("original-control-expired")
            return result

    from app.channels.execution import ChannelExecutionProtocolError

    if retire:
        with pytest.raises(ChannelExecutionProtocolError):
            await reader.read_request(record, e.link, guard=OriginalControl())
        assert len(calls) == 1 and calls[0].endswith("/output")
    else:
        actual = await reader.read_request(record, e.link, guard=OriginalControl())
        assert actual.basis == qualified.basis
        assert len(calls) == 3 and calls[1].endswith("/messages")
