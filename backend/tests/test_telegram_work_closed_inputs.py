"""Closed Work questions retain text for an explicit human choice."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_inbox import incoming
from test_telegram_work_input_capture import prompt
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typed_input import install_inputs
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_reply_to_closed_work_prompt_offers_explicit_choices(work_delivery):
    e = work_delivery
    _request, identity, _seen = await prompt(e)
    install_inputs(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramCallbackRow, identity)
        row.consumed_at, row.closed_reason = e.clock(), "command"
    assert await e.channel.handle_update(incoming(message=900, update_id=2900, account=e.link.chat_id, reply=802))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and not mark.route_complete
        assert not list(await reader.scalars(select(TelegramActRow)))
        notices = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id)))
        closed = [row for row in notices if any(part.get("source_ref", {}).get("key") == "prompt.closed" for part in row.parts)]
        assert len(closed) == 1
        keyboard = closed[0].payload["product_keyboard_basis"]
        assert [[button["kind"] for button in line] for line in keyboard] == [["prompt_reopen"], ["prompt_send_new"], ["prompt_discard"]]
        assert closed[0].id in mark.delivery_ids
        e.identity = closed[0].id


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["prompt_send_new", "prompt_discard"])
async def test_closed_work_input_requires_the_saved_explicit_choice(work_delivery, choice):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    e = work_delivery
    await test_reply_to_closed_work_prompt_offers_explicit_choices(e)
    e.channel._services.turns = SimpleNamespace(accept_staged=AsyncMock(), retire=AsyncMock())
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == choice))
    await press_work(e, callback, message_id=803)
    if choice == "prompt_send_new":
        stage = e.channel._services.turns.accept_staged.await_args.args[1]
        assert stage.text == "synthetic accepted text"
        assert stage.human_message_kwargs == {} and stage.callback_id == callback.id
        await press_work(e, callback, message_id=803, update_id=2902)
        replay = e.channel._services.turns.accept_staged.await_args.args[1]
        assert replay == stage
    else:
        assert e.channel._services.turns.accept_staged.await_count == 0
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_complete
        selected = await reader.get(TelegramCallbackRow, callback.id)
        if choice == "prompt_send_new":
            assert "publication_owner" in selected.payload
            assert mark.route_kind == "turn"
        else:
            assert selected.consumed_at is not None and mark.route_kind == "work_card_hold"


@pytest.mark.asyncio
async def test_reopen_work_question_requires_a_fresh_answer(work_delivery):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_input_refresh import deliver_refreshed
    from test_telegram_work_preview_recovery import dispatch

    e = work_delivery
    await test_reply_to_closed_work_prompt_offers_explicit_choices(e)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_reopen"))
    await press_work(e, callback, message_id=803)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        reopened = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["origin"]["callback_id"].as_string() == callback.id))
        assert reopened is not None
        assert reopened.payload["work_request_basis"]["interaction"] == {"view": "prompt", "purpose": "changes"}
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_complete and reopened.id in mark.delivery_ids
        e.identity = reopened.id
    await deliver_refreshed(e, 804)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent", e.dispatch_errors
        fresh = await reader.get(TelegramCallbackRow, row.payload["work_request_basis"]["prompt_id"])
        assert fresh.consumed_at is None and fresh.message_id == 804
    # Reopen did not use the retained answer; only this new human message acts.
    assert await e.channel.handle_update(incoming(message=901, update_id=2902, account=e.link.chat_id, reply=804))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        actions = list(await reader.scalars(select(TelegramActRow)))
        assert len(actions) == 1 and actions[0].state == "done" and actions[0].kind == "changes_requested"
        original = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        fresh_input = await reader.get(TelegramInboundMarkRow, (e.link.id, 901, e.link.chat_id))
        assert original.operation_ids == [] and fresh_input.operation_ids == [actions[0].id]


@pytest.mark.asyncio
async def test_reopen_after_target_closes_offers_only_new_message_or_discard(work_delivery):
    from uuid import uuid4

    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    from deerflow.agent_instances.work_contract import WorkCommand

    e = work_delivery
    await test_reply_to_closed_work_prompt_offers_explicit_choices(e)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_reopen"))
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        work_id = mark.route_basis["work_closed"]["work_id"]
    record = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=work_id)
    await e.human.work.command(
        actor=e.actor,
        instance_id=e.link.agent_ref,
        work_id=work_id,
        request=WorkCommand(operation_id=uuid4().hex, action="cancel", expected_revision=record["revision"], expected_assignment_revision=record["assignment_revision"]),
        can_stop=False,
    )
    await press_work(e, callback, message_id=803)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        replacement = await reader.get(TelegramOutboxRow, mark.route_basis["work_closed"]["notice_id"])
        assert replacement.id != parent.id
        assert replacement.parts[0]["source_ref"]["key"] == "prompt.unavailable"
        assert [[button["kind"] for button in line] for line in replacement.payload["product_keyboard_basis"]] == [["prompt_send_new"], ["prompt_discard"]]
        assert not mark.route_complete and not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_untied_text_after_work_prompt_expiry_is_held(work_delivery):
    from datetime import timedelta

    from app.telegram_bots.callbacks import _aware

    e = work_delivery
    _request, identity, _seen = await prompt(e)
    install_inputs(e)
    async with e.db.sessions() as reader:
        expires = _aware((await reader.get(TelegramCallbackRow, identity)).expires_at)
    e.clock.now = expires + timedelta(seconds=1)
    e.channel._services.callbacks.clock = e.clock
    assert await e.channel.handle_update(incoming(message=900, update_id=2900, account=e.link.chat_id))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark is not None and mark.route_kind == "work_card_hold" and not mark.route_complete
        assert mark.route_basis["work_closed"]["prompt_id"] == identity
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_closed_input_keeps_its_prompt_context_until_settled(work_delivery):
    from datetime import timedelta

    e = work_delivery
    await test_reply_to_closed_work_prompt_offers_explicit_choices(e)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        prompt_id = mark.route_basis["work_closed"]["prompt_id"]
    later = e.clock.now + timedelta(days=e.channel._services.callbacks.config.callbacks.max_age_days + 1)
    await e.channel._services.callbacks.sweep(later)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, prompt_id) is not None
    async with e.db.sessions.begin() as writer:
        mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        mark.state, mark.route_complete = "done", True
    await e.channel._services.callbacks.sweep(later)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, prompt_id) is None


@pytest.mark.asyncio
async def test_command_clears_expired_implicit_guard_but_old_reply_stays_owned(work_delivery):
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.callbacks import Prompts

    e = work_delivery
    await test_untied_text_after_work_prompt_expiry_is_held(e)
    await Prompts(e.channel._services.callbacks).close(e.link, e.link.chat_id, None, reason="command", guard=await guard_for(e))
    inputs = e.channel._services.work_text_inputs
    assert not await inputs.handle_text(e.channel, incoming(message=901, update_id=2901, account=e.link.chat_id), captured_link=e.link)
    assert await inputs.handle_text(e.channel, incoming(message=902, update_id=2902, account=e.link.chat_id, reply=802), captured_link=e.link)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramInboundMarkRow, (e.link.id, 901, e.link.chat_id)) is None
        old_reply = await reader.get(TelegramInboundMarkRow, (e.link.id, 902, e.link.chat_id))
        assert old_reply.route_kind == "work_card_hold" and "work_closed" in old_reply.route_basis


@pytest.mark.asyncio
async def test_spent_review_card_reopens_comment_without_submitting_changes(work_delivery):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_input_refresh import deliver_refreshed
    from test_telegram_work_interaction_delivery import guard_for
    from test_telegram_work_preview_recovery import dispatch

    from app.telegram_bots.callbacks import Prompts

    e = work_delivery
    _request, active_prompt, _seen = await prompt(e)
    install_inputs(e)
    assert await e.channel.handle_update(incoming(message=900, update_id=2900, account=e.link.chat_id, reply=801))
    await e.channel._drain_ingress()
    # The spent card holds only this text, without replacing the live prompt.
    current = await Prompts(e.channel._services.callbacks).current(e.link, e.link.chat_id, None, guard=await guard_for(e))
    assert current.id == active_prompt and current.consumed_at is None
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "work_card_hold"
        assert mark.route_basis["work_closed"]["prompt_id"] != active_prompt
        e.identity = mark.route_basis["work_closed"]["notice_id"]
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_reopen"))
    await press_work(e, callback, message_id=803)
    async with e.db.sessions() as reader:
        reopened = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["origin"]["callback_id"].as_string() == callback.id))
        assert reopened.payload["work_request_basis"]["interaction"] == {"view": "prompt", "purpose": "comment"}
        assert not list(await reader.scalars(select(TelegramActRow)))
        e.identity = reopened.id
    await deliver_refreshed(e, 804)
    assert await e.channel.handle_update(incoming(message=901, update_id=2902, account=e.link.chat_id, reply=804))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        actions = list(await reader.scalars(select(TelegramActRow)))
        assert len(actions) == 1 and actions[0].kind == "respond" and actions[0].state == "done"
        assert actions[0].body["disposition"] == "supplied"


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed", [None, [], {"prompt_id": {}}])
async def test_malformed_closed_history_does_not_abort_other_callback_cleanup(work_delivery, malformed):
    from datetime import timedelta

    e = work_delivery
    await test_reply_to_closed_work_prompt_offers_explicit_choices(e)
    async with e.db.sessions.begin() as writer:
        mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        original_prompt = mark.route_basis["work_closed"]["prompt_id"]
        mark.route_basis = {**mark.route_basis, "work_closed": malformed}
    await e.channel._services.callbacks.sweep(e.clock.now + timedelta(days=181))
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, original_prompt) is None


@pytest.mark.asyncio
async def test_reopen_downgraded_to_read_only_leaves_original_choice_and_text(work_delivery, monkeypatch):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    import app.gateway.authz as authz
    from app.telegram_bots.work_request_recovery import WorkRequestRecovery

    e = work_delivery
    await test_reply_to_closed_work_prompt_offers_explicit_choices(e)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_reopen"))
    read_source, permissions = WorkRequestRecovery._source, authz.resolve_route_permissions
    withdrawn, reached = [], []

    async def source(*args, **kwargs):
        result = await read_source(*args, **kwargs)
        withdrawn.append(True)
        return result

    async def downgrade(*args, **kwargs):
        result = await permissions(*args, **kwargs)
        if withdrawn:
            reached.append(True)
            return set(result) - {"agents:write"}
        return result

    monkeypatch.setattr(WorkRequestRecovery, "_source", source)
    monkeypatch.setattr(authz, "resolve_route_permissions", downgrade)
    await press_work(e, selected, message_id=803)
    assert reached
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert not mark.route_complete
        assert (await reader.get(TelegramCallbackRow, selected.id)).consumed_at is None
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["origin"]["callback_id"].as_string() == selected.id)))


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["input", "choice", "ack"])
async def test_reopened_prompt_rechecks_original_choice_and_input_before_sdk(work_delivery, monkeypatch, damage):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_input_refresh import deliver_refreshed
    from test_telegram_work_preview_recovery import dispatch

    import app.telegram_bots.work_request_source as module

    e = work_delivery
    await test_reply_to_closed_work_prompt_offers_explicit_choices(e)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_reopen"))
    await press_work(e, selected, message_id=803)
    async with e.db.sessions() as reader:
        reopened = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["origin"]["callback_id"].as_string() == selected.id))
        assert reopened is not None
        e.identity = reopened.id
    original, reached = module.current_attention, []

    async def withdraw(*args, **kwargs):
        async with e.db.sessions.begin() as writer:
            if damage == "input":
                mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
                mark.route_complete = False
            elif damage == "choice":
                row = await writer.get(TelegramCallbackRow, selected.id)
                row.closed_reason = "discarded"
            else:
                row = await writer.get(TelegramOutboxRow, parent.id)
                row.parts = [{**part, "message_ids": [999]} for part in row.parts]
        reached.append(True)
        return await original(*args, **kwargs)

    monkeypatch.setattr(module, "current_attention", withdraw)
    await deliver_refreshed(e, 804)
    assert reached
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state != "sent" and all(not part["message_ids"] for part in row.parts)
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
@pytest.mark.parametrize("shown", [False, True])
async def test_discard_old_text_preserves_another_request_prompt(work_delivery, shown):
    from test_telegram_work_actions import management_request
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_interaction_delivery import open_view
    from test_telegram_work_interaction_delivery import selected as select_control
    from test_telegram_work_preview_recovery import dispatch
    from test_telegram_work_typing_prompts import clear_old

    e = work_delivery
    await test_reply_to_closed_work_prompt_offers_explicit_choices(e)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, e.identity)
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_discard"))
    _record, request, _seen = await management_request(e, "review")
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="another-review")
    await dispatch(e, 805)
    e.identity = await open_view(e, await select_control(e, "changes_open"))
    await clear_old(e)
    if shown:
        await dispatch(e, 806)
    async with e.db.sessions() as reader:
        later_prompt = (await reader.get(TelegramOutboxRow, e.identity)).payload["work_request_basis"]["prompt_id"]
    await press_work(e, selected, message_id=803)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))).route_complete
        assert (await reader.get(TelegramCallbackRow, later_prompt)).consumed_at is None


@pytest.mark.asyncio
async def test_concurrent_source_read_does_not_retire_reopen(work_delivery, monkeypatch):
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    from app.telegram_bots.work_request_recovery import WorkRequestRecovery
    from deerflow.agent_instances.contract import AgentConflict

    e = work_delivery
    await test_reply_to_closed_work_prompt_offers_explicit_choices(e)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, e.identity)
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_reopen"))

    async def conflict(*args, **kwargs):
        raise AgentConflict("The source changed during the read")

    monkeypatch.setattr(WorkRequestRecovery, "_source", conflict)
    await press_work(e, selected, message_id=803)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert not mark.route_complete and mark.route_basis["work_closed"]["notice_id"] == notice.id
        assert (await reader.get(TelegramCallbackRow, selected.id)).consumed_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["topic", "accepted_at", "state", "purpose", "parent", "sibling"])
async def test_reopen_final_provider_await_cannot_change_its_input_or_choice(work_delivery, monkeypatch, damage):
    from datetime import timedelta

    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_preview_recovery import dispatch

    import app.gateway.authz as authz
    import app.telegram_bots.work_reopen as module

    e = work_delivery
    await test_reply_to_closed_work_prompt_offers_explicit_choices(e)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, e.identity)
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "prompt_reopen"))
    prepare, permissions = module.prepare_attention, authz.resolve_route_permissions
    active, reached = {}, []

    async def prepare_check(*args, **kwargs):
        check = await prepare(*args, **kwargs)

        async def guarded(**kw):
            active.update(kw)
            try:
                return await check(**kw)
            finally:
                active.clear()

        return guarded

    async def change(*args, **kwargs):
        held = await permissions(*args, **kwargs)
        projection = active.get("projection")
        if projection is not None and projection.guard.completed and not reached:
            writer = active["session"]
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            if damage == "topic":
                mark.message_thread_id = 77
            elif damage == "accepted_at":
                mark.accepted_at += timedelta(seconds=1)
            elif damage == "state":
                mark.state = "done"
            elif damage == "purpose":
                row = await writer.get(TelegramCallbackRow, mark.route_basis["work_closed"]["prompt_id"])
                row.payload = {**row.payload, "purpose": "work_comment"}
            elif damage == "parent":
                row = await writer.get(TelegramOutboxRow, notice.id)
                row.parts = [{**part, "message_ids": [999]} for part in row.parts]
            else:
                row = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == selected.group_id, TelegramCallbackRow.kind == "prompt_discard"))
                row.closed_reason = "discarded"
            await writer.flush()
            reached.append(True)
        return held

    monkeypatch.setattr(module, "prepare_attention", prepare_check)
    monkeypatch.setattr(authz, "resolve_route_permissions", change)
    await press_work(e, selected, message_id=803)
    assert reached
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert not mark.route_complete
        assert (await reader.get(TelegramCallbackRow, selected.id)).consumed_at is None
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["origin"]["callback_id"].as_string() == selected.id)))


@pytest.mark.asyncio
async def test_interrupted_closed_notice_retries_source_read_without_new_input(work_delivery, monkeypatch):
    from app.telegram_bots.inbox import InboxRow
    from app.telegram_bots.work_request_recovery import WorkRequestRecovery
    from deerflow.agent_instances.contract import AgentConflict

    e = work_delivery
    await test_untied_text_after_work_prompt_expiry_is_held(e)
    inputs = e.channel._services.work_text_inputs
    original = WorkRequestRecovery._source

    async def conflict(*args, **kwargs):
        raise AgentConflict("The source changed during the read")

    monkeypatch.setattr(WorkRequestRecovery, "_source", conflict)
    with pytest.raises(AgentConflict):
        await inputs.handle_text(e.channel, incoming(message=901, update_id=2901, account=e.link.chat_id, reply=802), captured_link=e.link)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 901, e.link.chat_id))
        assert mark.route_kind == "work_card_hold" and "work_closed" not in mark.route_basis
        origin = InboxRow.from_row(mark)
    monkeypatch.setattr(WorkRequestRecovery, "_source", original)
    assert await inputs.resume(e.channel, origin)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 901, e.link.chat_id))
        notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_closed"]["notice_id"])
        assert notice.parts[0]["source_ref"]["key"] == "prompt.closed"
        assert not mark.route_complete and not list(await reader.scalars(select(TelegramActRow)))
