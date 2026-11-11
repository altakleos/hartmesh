"""Captured Work text admits one exact command; stale notes stay unsubmitted."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_control_admission import admissions
from test_telegram_work_input_capture import accept, capture, prompt
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.agent_instances.human_input_contract import Respond
from deerflow.persistence.telegram_bots.model import TelegramActRow


def controller(e, inbox):
    from app.telegram_bots.work_input_admission import WorkInputAdmissions
    from app.telegram_bots.work_input_capture import WorkInputCaptures

    return WorkInputAdmissions(admissions(e), WorkInputCaptures(inbox, e.channel._services.callbacks))


async def stage_input(e, host, mark):
    return await host.stage(e.link, mark, guard=await guard_for(e))


@pytest.mark.asyncio
async def test_unchanged_changes_note_admits_once_and_worker_applies_original_cas(work_delivery):
    from app.telegram_bots.work_actions import ActsWorker

    e = work_delivery
    request, prompt_id, seen = await prompt(e)
    inbox, mark = await accept(e)
    saved = await capture(e, inbox, mark, prompt_id)
    host = controller(e, inbox)
    act = await stage_input(e, host, mark)
    assert act.id == saved.operation_ids[0] and act.kind == "changes_requested" and act.attempts == 0
    assert act.body["note"] == saved.route_basis["work_input"]["text"]
    assert act.body["expected_revision"] == seen["work_revision"]
    assert (await e.human.get(actor=e.actor, request_id=request["id"]))["state"] == "pending"
    assert (await stage_input(e, host, mark)).id == act.id
    e.clock.advance(60)
    await ActsWorker(host.controls.acts).tick()
    assert (await host.controls.acts.check(act.id)).state == "done"
    assert (await e.human.get(actor=e.actor, request_id=request["id"]))["closed_reason"] == "superseded"
    assert (await stage_input(e, controller(e, inbox), mark)).id == act.id
    async with e.db.sessions() as reader:
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1


@pytest.mark.asyncio
async def test_changed_review_keeps_input_without_admitting_an_action(work_delivery):
    from deerflow.agent_instances.contract import AgentConflict

    e = work_delivery
    request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    saved = await capture(e, inbox, mark, prompt_id)
    await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="The review has new content"))
    with pytest.raises(AgentConflict):
        await stage_input(e, controller(e, inbox), mark)
    replay = await capture(e, inbox, mark, prompt_id)
    assert replay.route_basis == saved.route_basis and replay.operation_ids == saved.operation_ids
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_review_change_after_admission_cannot_apply_note_to_new_basis(work_delivery):
    e = work_delivery
    request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    await capture(e, inbox, mark, prompt_id)
    host = controller(e, inbox)
    act = await stage_input(e, host, mark)
    await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="A later comment"))
    result = await host.controls.acts.check(act.id)
    assert result.state == "conflict" and result.body == act.body
    assert (await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=request["work_id"]))["status"] == "submitted"


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["answer", "cannot_provide", "wrong_recipient"])
async def test_captured_answer_or_reason_is_applied_once_while_actions_paused(work_delivery, purpose):
    from datetime import UTC, datetime

    from test_telegram_inbox import incoming
    from test_telegram_work_interaction_delivery import open_view, selected
    from test_telegram_work_preview_recovery import dispatch, install
    from test_telegram_work_typing_prompts import clear_old

    from app.telegram_bots.inbox import InboxRepository
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramOutboxRow

    e = work_delivery
    install(e)
    await dispatch(e, 801)
    if purpose != "answer":
        e.identity = await open_view(e, await selected(e, "hi_cant_open"))
        await clear_old(e)
        await dispatch(e, 802)
    kind = {"answer": "hi_answer", "cannot_provide": "hi_cannot", "wrong_recipient": "hi_wrong"}[purpose]
    e.identity = await open_view(e, await selected(e, kind))
    await clear_old(e)
    await dispatch(e, 803)
    async with e.db.sessions.begin() as writer:
        prompt_id = (await writer.get(TelegramOutboxRow, e.identity)).payload["work_request_basis"]["prompt_id"]
        (await writer.get(TelegramBotLinkRow, e.link.id)).acts_paused_since = datetime.now(UTC)
    inbox = InboxRepository(e.db.sessions)
    mark, _ = await inbox.accept(e.link, incoming(message=900, update_id=1900, account=e.link.chat_id, reply=803), sanitized_input={"text": "The reference is in another guide."})
    saved = await capture(e, inbox, mark, prompt_id)
    host = controller(e, inbox)
    act = await stage_input(e, host, mark)
    assert act.id == saved.operation_ids[0] and act.body["disposition"] == ("supplied" if purpose == "answer" else purpose)
    assert (await host.controls.acts.check(act.id)).state == "done"
    assert (await stage_input(e, host, mark)).id == act.id
    responses = (await e.human.responses(actor=e.actor, request_id=e.request["id"]))["responses"]
    assert len(responses) == 1 and responses[0]["text"] == mark.sanitized_input["text"]
    assert responses[0]["actor_id"] == e.link.owner_user_id
    assert (await e.human.get(actor=e.actor, request_id=e.request["id"]))["state"] == ("answered" if purpose == "answer" else "pending")


@pytest.mark.asyncio
async def test_late_permission_change_rolls_back_only_action_admission(work_delivery, monkeypatch):
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    _request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    saved = await capture(e, inbox, mark, prompt_id)
    host = controller(e, inbox)
    original = host.controls.acts._stage_work_act
    changed = []

    async def withdraw(*args, **kwargs):
        result = await original(*args, **kwargs)
        writer = kwargs["session"]
        (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 3
        await writer.flush()
        changed.append(True)
        return result

    monkeypatch.setattr(host.controls.acts, "_stage_work_act", withdraw)
    with pytest.raises(AgentDenied):
        await stage_input(e, host, mark)
    assert changed and (await capture(e, inbox, mark, prompt_id)).route_basis == saved.route_basis
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_decision_text_requires_a_separate_confirmation(work_delivery):
    from test_telegram_inbox import incoming
    from test_telegram_work_actions import management_request
    from test_telegram_work_interaction_delivery import open_view, selected
    from test_telegram_work_preview_recovery import dispatch, install
    from test_telegram_work_typing_prompts import clear_old

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.inbox import InboxRepository
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    install(e)
    _record, request, _seen = await management_request(e, "decision")
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="input-decision")
    await dispatch(e, 801)
    e.identity = await open_view(e, await selected(e, "decide_own"))
    await clear_old(e)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        prompt_id = (await reader.get(TelegramOutboxRow, e.identity)).payload["work_request_basis"]["prompt_id"]
    inbox = InboxRepository(e.db.sessions)
    mark, _ = await inbox.accept(e.link, incoming(message=900, update_id=1900, account=e.link.chat_id, reply=803), sanitized_input={"text": "Use the first guide"})
    await capture(e, inbox, mark, prompt_id)
    with pytest.raises(CallbackRefused):
        await stage_input(e, controller(e, inbox), mark)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_disappearing_input_action_cannot_be_recreated_on_replay(work_delivery, monkeypatch):
    from contextlib import asynccontextmanager

    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    _request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    await capture(e, inbox, mark, prompt_id)
    host = controller(e, inbox)
    act = await stage_input(e, host, mark)
    assert (await host.controls.acts.check(act.id)).state == "done"
    original = host.controls.callbacks.writer
    entries = []

    @asynccontextmanager
    async def delete_at_action_writer(*args, **kwargs):
        entries.append(True)
        # First writer verifies the already-owned input; the second is action
        # admission after the discovery read of the old journal.
        if len(entries) == 2:
            async with e.db.sessions.begin() as writer:
                await writer.delete(await writer.get(TelegramActRow, act.id))
        async with original(*args, **kwargs) as writer:
            yield writer

    monkeypatch.setattr(host.controls.callbacks, "writer", delete_at_action_writer)
    with pytest.raises(CallbackRefused):
        await stage_input(e, host, mark)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, act.id) is None


@pytest.mark.asyncio
async def test_missing_marked_input_journal_cannot_be_admitted_again(work_delivery):
    from app.telegram_bots.callbacks import CallbackRefused
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    _request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    captured = await capture(e, inbox, mark, prompt_id)
    host = controller(e, inbox)
    action = await stage_input(e, host, mark)
    async with e.db.sessions.begin() as writer:
        saved = await writer.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))
        assert saved.route_basis["work_action_id"] == action.id
        assert saved.route_basis["work_input"] == captured.route_basis["work_input"]
        await writer.delete(await writer.get(TelegramActRow, action.id))
    with pytest.raises(CallbackRefused):
        await stage_input(e, controller(e, inbox), mark)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, action.id) is None
