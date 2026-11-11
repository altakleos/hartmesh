"""Accepted text retains the acknowledged Work prompt before any service action."""

from dataclasses import replace
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_inbox import incoming
from test_telegram_work_actions import management_request
from test_telegram_work_interaction_delivery import guard_for, open_view, selected
from test_telegram_work_preview_recovery import dispatch, install
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typing_prompts import clear_old
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.inbox import InboxRepository
from deerflow.agent_instances.human_input_contract import Respond
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


async def prompt(e, *, shown=True):
    install(e)
    _record, request, _seen = await management_request(e, "review")
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="capture-review")
    await dispatch(e, 801)
    e.identity = await open_view(e, await selected(e, "changes_open"))
    if shown:
        await clear_old(e)
        await dispatch(e, 802)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        return request, row.payload["work_request_basis"]["prompt_id"], row.payload["work_request_basis"]["source"]["seen"]


async def accept(e, *, message_id=900):
    inbox = InboxRepository(e.db.sessions)
    mark, _ = await inbox.accept(e.link, incoming(message=message_id, update_id=message_id + 1000, account=e.link.chat_id, reply=802), sanitized_input={"text": "Keep this note for the review I saw."})
    return inbox, mark


async def capture(e, inbox, mark, prompt_id, *, guard=None):
    from app.telegram_bots.work_input_capture import WorkInputCaptures

    return await WorkInputCaptures(inbox, e.channel._services.callbacks).capture(e.link, mark, prompt_id=prompt_id, guard=guard or await guard_for(e))


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["comment", "closed"])
async def test_changed_review_retains_accepted_text_and_original_seen_without_an_act(work_delivery, change):
    e = work_delivery
    request, prompt_id, seen = await prompt(e)
    if change == "comment":
        await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="An intervening comment"))
    else:
        from deerflow.agent_instances.work_contract import WorkCommand

        await e.human.work.command(
            actor=e.actor,
            instance_id=e.link.agent_ref,
            work_id=request["work_id"],
            request=WorkCommand(operation_id=uuid4().hex, action="cancel", expected_revision=seen["work_revision"], expected_assignment_revision=seen["assignment_revision"]),
            can_stop=False,
        )
    inbox, mark = await accept(e)
    result = await capture(e, inbox, mark, prompt_id)
    assert result.route_kind == "work_input" and result.route_target_id == request["id"]
    assert result.route_basis["work_input"]["seen"] == seen
    assert result.route_basis["work_input"]["text"] == mark.sanitized_input["text"]
    assert len(result.operation_ids) == 1 and not result.route_complete
    # A restart or a caller-supplied input copy cannot rewrite the accepted text.
    replay = await capture(e, InboxRepository(e.db.sessions), replace(mark, sanitized_input={"text": "A forged replacement"}), prompt_id)
    assert replay.route_basis == result.route_basis and replay.operation_ids == result.operation_ids
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        old = await reader.get(TelegramCallbackRow, prompt_id)
        assert old.consumed_at is not None and old.closed_reason == "answered"
        assert e.identity in result.delivery_ids


@pytest.mark.asyncio
async def test_unacknowledged_prompt_cannot_capture_text(work_delivery):
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    _request, prompt_id, _seen = await prompt(e, shown=False)
    inbox, mark = await accept(e)
    with pytest.raises(CallbackRefused):
        await capture(e, inbox, mark, prompt_id)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))).route_kind is None


@pytest.mark.asyncio
async def test_one_prompt_cannot_capture_two_different_messages(work_delivery):
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    _request, prompt_id, _seen = await prompt(e)
    inbox, first = await accept(e)
    await capture(e, inbox, first, prompt_id)
    _inbox, second = await accept(e, message_id=901)
    with pytest.raises(CallbackRefused):
        await capture(e, inbox, second, prompt_id)


@pytest.mark.asyncio
async def test_late_lifetime_withdrawal_rolls_back_route_and_prompt_close(work_delivery, monkeypatch):
    e = work_delivery
    _request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    active = [True]
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: active[0])
    original = inbox.route

    async def withdraw(*args, **kwargs):
        result = await original(*args, **kwargs)
        active[0] = False
        return result

    monkeypatch.setattr(inbox, "route", withdraw)
    with pytest.raises(ValueError, match="publication-stale"):
        await capture(e, inbox, mark, prompt_id, guard=guard)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))).route_kind is None
        assert (await reader.get(TelegramCallbackRow, prompt_id)).consumed_at is None


@pytest.mark.asyncio
async def test_pending_input_retains_its_prompt_proof_through_sweep(work_delivery):
    from datetime import timedelta

    e = work_delivery
    _request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    saved = await capture(e, inbox, mark, prompt_id)
    later = e.clock.now + timedelta(days=e.channel._services.callbacks.config.callbacks.max_age_days + 1)
    await e.channel._services.callbacks.sweep(later)
    replay = await capture(e, inbox, mark, prompt_id)
    assert replay.route_basis == saved.route_basis and replay.operation_ids == saved.operation_ids
    # Once the owning input is settled, normal callback expiry can collect it.
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))
        row.state, row.route_complete = "done", True
    await e.channel._services.callbacks.sweep(later)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, prompt_id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["part", "message", "version", "expiry"])
async def test_capture_requires_prompt_visibility_to_match_its_actual_ack(work_delivery, damage):
    from datetime import timedelta

    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    _request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramCallbackRow, prompt_id)
        if damage == "part":
            row.payload = {**row.payload, "prompt_visibility": {**row.payload["prompt_visibility"], "part_id": uuid4().hex}}
        elif damage == "message":
            row.message_id = 999
            # The accepted input must not make this false anchor authoritative.
            incoming_row = await writer.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))
            incoming_row.sanitized_input = {**incoming_row.sanitized_input, "reply_to_message_id": 999}
        elif damage == "version":
            row.payload = {**row.payload, "presentation_version": 999}
        else:
            row.expires_at += timedelta(days=1)
    with pytest.raises(CallbackRefused):
        await capture(e, inbox, mark, prompt_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted", [False, True])
async def test_input_accepted_before_prompt_ack_requires_an_exact_quote(work_delivery, quoted):
    from datetime import datetime, timedelta
    from unittest.mock import patch

    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    _request, prompt_id, _seen = await prompt(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, e.identity)
        acknowledged = datetime.fromisoformat(row.parts[0]["sent_at"])
        row.parts = [{**part, "dispatched_at": (acknowledged - timedelta(seconds=2)).isoformat()} for part in row.parts]
    inbox = InboxRepository(e.db.sessions)
    with patch("app.telegram_bots.inbox._now", lambda: acknowledged - timedelta(seconds=1)):
        mark, _ = await inbox.accept(e.link, incoming(message=900, update_id=1900, account=e.link.chat_id, reply=802 if quoted else None), sanitized_input={"text": "An earlier accepted message"})
    if quoted:
        assert (await capture(e, inbox, mark, prompt_id)).route_kind == "work_input"
    else:
        with pytest.raises(CallbackRefused):
            await capture(e, inbox, mark, prompt_id)
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))).route_kind is None


@pytest.mark.asyncio
@pytest.mark.parametrize("message_id", [700, 900])
async def test_unquoted_input_needs_both_current_ack_and_later_chat_message(work_delivery, monkeypatch, message_id):
    from datetime import timedelta

    from app.telegram_bots import inbox as module
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    _request, prompt_id, _seen = await prompt(e)
    monkeypatch.setattr(module, "_now", lambda: e.clock.now + timedelta(seconds=1))
    inbox = InboxRepository(e.db.sessions)
    mark, _ = await inbox.accept(e.link, incoming(message=message_id, update_id=1900, account=e.link.chat_id), sanitized_input={"text": "A message after the question"})
    if message_id > 802:
        assert (await capture(e, inbox, mark, prompt_id)).route_kind == "work_input"
    else:
        with pytest.raises(CallbackRefused):
            await capture(e, inbox, mark, prompt_id)
