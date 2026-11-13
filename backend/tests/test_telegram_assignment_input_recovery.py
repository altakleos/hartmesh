"""Retired prompt delivery and completed-input replay retain original custody."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_assignment_editing import editing
from test_telegram_assignment_prompt_delivery import prepare_words
from test_telegram_assignment_prompt_inputs import accept_reply
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.assignment_controls import AssignmentAdmissions
from app.telegram_bots.assignment_delivery import AssignmentPlans
from app.telegram_bots.assignment_drafts import draft_identity
from app.telegram_bots.assignment_inputs import AssignmentInputs
from app.telegram_bots.assignment_navigation import AssignmentDraftNavigation
from app.telegram_bots.assignment_sources import AssignmentReader
from app.telegram_bots.authority import AuthorityUnavailable
from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["before_dispatch", "during_settings"])
async def test_closed_assignment_prompt_never_sends_a_force_reply(work_delivery, monkeypatch, boundary):
    e = work_delivery
    plans, _ = await prepare_words(e)
    closed = []

    async def close():
        await plans.prompts.close(e.link, e.link.chat_id, None, reason="cancelled", guard=await guard_for(e))
        closed.append(True)

    if boundary == "before_dispatch":
        await close()
    else:
        sources = e.channel._services.assignment_prompt_sources
        get = sources.settings.get

        async def get_after_close(*args, **kwargs):
            result = await get(*args, **kwargs)
            if not closed:
                await close()
            return result

        monkeypatch.setattr(sources.settings, "get", get_after_close)
    await dispatch(e, 5401)
    assert closed
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["route_basis", "delivery_membership"])
async def test_prompt_final_sdk_keeps_its_original_command_publication_custody(work_delivery, monkeypatch, damage):
    e = work_delivery
    plans, inbox = await prepare_words(e)
    sources = e.channel._services.assignment_prompt_sources
    get = sources.settings.get
    changed = []

    async def withdraw(*args, **kwargs):
        result = await get(*args, **kwargs)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                mark = await writer.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id))
                if damage == "route_basis":
                    mark.route_basis = {"prompt_id": "different-source-owner"}
                else:
                    mark.delivery_ids = []
        return result

    monkeypatch.setattr(sources.settings, "get", withdraw)
    await dispatch(e, 5411)
    assert changed
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
@pytest.mark.parametrize("disposition", ["save", "discard"])
async def test_owned_answer_replay_observes_its_card_after_disposition(work_delivery, monkeypatch, disposition):
    e = work_delivery
    prompts, _ = await prepare_words(e)
    await dispatch(e, 5421)
    calls = []

    async def proposal(words, **kwargs):
        calls.append(words)
        return DraftAssignment(objective="Review the guide", success_criteria="List the verified changes")

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", proposal)
    incoming = await accept_reply(e, 5422, "Review the guide", 5421)
    cards = AssignmentPlans(reader=AssignmentReader(e.acts), outbox=e.sender.outbox, callbacks=prompts.callbacks)
    inputs = AssignmentInputs(prompts, cards)
    card = await inputs.answer(e.link, incoming, guard=await guard_for(e))
    e.identity = card
    await dispatch(e, 5423)
    button = await selected(e, "assign_save" if disposition == "save" else "assign_discard")
    scope = dict(chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=button.message_thread_id, guard=await guard_for(e))
    if disposition == "save":
        admitted = await AssignmentAdmissions(cards, e.acts).stage(e.link, button.id, **scope)
        assert (await e.acts.check(admitted.id)).state == "done"
    else:
        await AssignmentDraftNavigation(cards).discard(e.link, button.id, **scope)
    assert await inputs.answer(e.link, incoming, guard=await guard_for(e)) == card
    assert calls == ["Review the guide"]


@pytest.mark.asyncio
@pytest.mark.parametrize("proposal_kind", ["valid", "none"])
async def test_retired_input_owner_preserves_extracting_for_new_owner_recovery(work_delivery, monkeypatch, proposal_kind):
    e = work_delivery
    prompts, _ = await prepare_words(e)
    await dispatch(e, 5501)
    cards = AssignmentPlans(reader=AssignmentReader(e.acts), outbox=e.sender.outbox, callbacks=prompts.callbacks)
    inputs = AssignmentInputs(prompts, cards)
    incoming = await accept_reply(e, 5502, "Review the guide", 5501)
    active = [True]
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: active[0])
    calls = []

    async def retire_after_model(words, **kwargs):
        await kwargs["before_dispatch"]()
        calls.append(words)
        active[0] = False
        return DraftAssignment(objective="Review the guide", success_criteria="List verified changes") if proposal_kind == "valid" else None

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", retire_after_model)
    try:
        result = await inputs.answer(e.link, incoming, guard=guard)
        assert result.phase == "extracting"
    except (ValueError, ProductSourceRetired, AuthorityUnavailable):
        pass
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, draft_identity(e.link, incoming))
        assert saved.body["version"] == 2 and saved.body["request"]["phase"] == "extracting"
    recovered = await inputs.requests.recover(e.link)
    assert len(recovered) == 1 and recovered[0].phase == "awaiting_done"
    assert calls == ["Review the guide"]


@pytest.mark.asyncio
async def test_discard_after_edit_draft_preparation_rolls_back_replacement(work_delivery, monkeypatch):
    e = work_delivery
    cards, prompts, inputs, draft, old_card, button, scope = await editing(e)
    e.identity = await prompts.edit(e.link, button.id, **scope)
    await dispatch(e, 5503)

    async def proposal(words, **kwargs):
        return DraftAssignment(objective="Unadopted edit", success_criteria="Keep the original disposition")

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", proposal)
    prepare = inputs.requests.drafts.prepare_create
    closed = []

    async def discard_before_adoption(*args, **kwargs):
        result = await prepare(*args, **kwargs)
        if not closed:
            closed.append(True)
            old = e.identity
            e.identity = old_card
            selected_discard = await selected(e, "assign_discard")
            await AssignmentDraftNavigation(cards).discard(e.link, selected_discard.id, chat_id=e.link.chat_id, message_id=selected_discard.message_id, message_thread_id=selected_discard.message_thread_id, guard=await guard_for(e))
            e.identity = old
        return result

    monkeypatch.setattr(inputs.requests.drafts, "prepare_create", discard_before_adoption)
    incoming = await accept_reply(e, 5504, "Include the extra chapter", 5503)
    result = await inputs.answer(e.link, incoming, guard=await guard_for(e))
    assert closed and result.phase == "extracting"
    async with e.db.sessions() as reader:
        original = await reader.get(TelegramActRow, draft.id)
        correction = await reader.get(TelegramActRow, result.id)
        assert original.state == "refused" and original.reason == "discarded"
        assert correction.body["version"] == 2
