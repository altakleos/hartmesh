"""Closed assignment prompts keep their actual input until an explicit choice."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from _telegram_bot_test_support import make_update, update_payload
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from telegram import Update
from test_telegram_assignment_entrypoints import installed_prompt
from test_telegram_assignment_typing_entrypoints import reply
from test_telegram_chat_turns import controlled_execution, until
from test_telegram_work_callback_dispatch import press_work
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@asynccontextmanager
async def closed_input(e, monkeypatch, *, missing_prompt=False, management=False, voice=False, legacy_echo=False, ambiguous=False, missing_selection=False, purpose=None, large=False):
    owner, calls = await installed_management_prompt(e, monkeypatch) if management else await installed_prompt(e, monkeypatch)
    turns = None
    try:
        e.services, e.request = owner.host, e.channel.bot.request
        turns, state, reached = await controlled_execution(e, monkeypatch)

        async def callback_thread(link, *, callback_id):
            # Only the remote thread service is controlled. Reserve and commit the
            # real callback preparation before the real queue can reserve a run.
            live = await e.services.prepare_live_owner(link.owner_user_id)
            guard = e.services.links.publication_guard(link, check_live_owner=live, check_attempt_current=lambda: True)
            stage = await e.services.outbox.inbox.read_callback_input(link, callback_id, guard=guard)
            endpoint = e.services.thread_client.endpoint_id
            preparation = await e.services.links.reserve_thread_preparation(link, stage.origins[0], purpose="ensure", endpoint_id=endpoint, check_live_owner=live, check_attempt_current=lambda: True, owner_callback_id=callback_id)
            identity = preparation.selected_thread_id or preparation.expected_thread_id or preparation.creation_id
            assert await e.services.links.commit_thread_mapping(link, preparation, thread_id=identity, endpoint_id=endpoint, check_live_owner=live, check_attempt_current=lambda: True)
            return identity

        e.services.threads.ensure_callback_thread = callback_thread
        if purpose is not None:
            await prepare_assignment_variant(e, monkeypatch, owner, calls, purpose, large=large)
        baseline_calls = tuple(calls)
        quote = 6010 if management else 6132
        if ambiguous:
            assert await e.channel.handle_update(make_update(6140, account=e.link.chat_id, text="/assign"))
            await e.channel._drain_ingress()
            # A deliberately duplicated SDK receipt makes two real source
            # publications ambiguous; neither may become an answer target.
            await dispatch(e, quote)
        if voice:
            from test_telegram_voice_notes import Transcribing, voice_on

            acts = e.services.acts
            await voice_on(e, e.db, monkeypatch, agreed=True, transcriber=Transcribing(text="Review the handbook"))
            e.services.acts = acts
            if missing_selection:
                from app.telegram_bots.inbox import InboxRow

                controller = e.services.voice_inputs.owners["management" if management else "assignment"]
                actual_capture = controller.capture

                async def legacy_capture(*args, **kwargs):
                    captured = await actual_capture(*args, **kwargs)
                    if captured is None:
                        return None
                    # Keep the real V3 recording, omitting the newer companion
                    # exactly as an interrupted older capture would have done.
                    async with e.db.sessions.begin() as writer:
                        mark = await writer.get(TelegramInboundMarkRow, (captured.link_id, captured.message_id, captured.chat_id))
                        mark.route_basis = {key: value for key, value in mark.route_basis.items() if key != "work_voice_selection"}
                        await writer.flush()
                        return InboxRow.from_row(mark)

                monkeypatch.setattr(controller, "capture", legacy_capture)
            if legacy_echo:
                from app.telegram_bots.closed_voice_echo import echo_recipe

                original_offer, echoed = owner.chat.closed_prompts.offer, []

                async def old_echo(link, origin, **kwargs):
                    if not echoed:
                        echoed.append(True)
                        await e.services.outbox.plan_product(link, source_id=f"voice-heard:{origin.chat_id}:{origin.message_id}", parts=[echo_recipe(origin)], inbox_rows=[origin], finish_route=False)
                        await dispatch(e, 6499)
                    return await original_offer(link, origin, **kwargs)

                monkeypatch.setattr(owner.chat.closed_prompts, "offer", old_echo)
        if missing_prompt:
            async with e.db.sessions.begin() as writer:
                parent = await writer.get(TelegramOutboxRow, e.identity)
                await writer.delete(await writer.get(TelegramCallbackRow, parent.payload["assignment_prompt_intent"]["prompt_id"]))
        e.clock.advance(601)
        if voice:
            from test_telegram_voice_notes import arrive, note, script_file

            script_file(e, 6151)
            update = note(e, 6151, quoted=quote if ambiguous else None)
            await arrive(e, 6151, quoted=quote if ambiguous else None)
        else:
            update = reply(e, quoted=missing_prompt or ambiguous)
            if management and ambiguous:
                payload = update.to_dict()
                payload["message"]["reply_to_message"]["message_id"] = quote
                update = Update.de_json(payload, e.channel.bot)
            assert await e.channel.handle_update(update)
            await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            notice = await reader.get(TelegramOutboxRow, mark.route_basis["work_closed"]["notice_id"])
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"])))
        for index in range(len(notice.parts)):
            await dispatch(e, 6501 + index)
        assert not e.dispatch_errors, e.dispatch_errors
        assert tuple(calls) == baseline_calls and not turns._turns
        yield SimpleNamespace(
            owner=owner, calls=calls, baseline_calls=baseline_calls, turns=turns, state=state, reached=reached, update=update, notice=notice, notice_message=6500 + len(notice.parts), controls={row.kind: row for row in controls}
        )
    finally:
        if voice:
            await e.channel._drain_ingress()
        if turns is not None:
            await turns.stop()
        await owner.stop()


async def prepare_assignment_variant(e, monkeypatch, owner, calls, purpose, *, large):
    from test_telegram_work_interaction_delivery import selected

    from app.telegram_bots.assign import DraftAssignment

    async def proposal(words, **kwargs):
        calls.append(words)
        if purpose == "assign_done":
            return None
        return DraftAssignment(objective="o" * 3000 if large and len(calls) == 1 else words, success_criteria="c" * 3000 if large and len(calls) == 1 else "Record verified corrections")

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", proposal)
    assert await e.channel.handle_update(make_update(6133, account=e.link.chat_id, text="Original assignment"))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        if purpose == "assign_done":
            parent = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_prompt_intent"]["purpose"].as_string() == purpose))
        else:
            incoming = await reader.get(TelegramInboundMarkRow, (e.link.id, 6133, e.link.chat_id))
            parent = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == incoming.route_target_id))
        assert parent is not None
        e.identity = parent.id
    for index in range(len(parent.parts)):
        await dispatch(e, 6134 + index)
    if purpose == "assign_edit":
        edit = await selected(e, "assign_edit")
        await press_work(e, edit, message_id=6133 + len(parent.parts), update_id=6140)
        async with e.db.sessions() as reader:
            parent = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_prompt_intent"]["purpose"].as_string() == purpose))
            assert parent is not None
            e.identity = parent.id
        await dispatch(e, 6141)
    assert not e.dispatch_errors, e.dispatch_errors


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose,large", [("assign_done", False), ("assign_edit", False), ("assign_edit", True)])
async def test_reopen_preserves_original_assignment_purpose_and_full_preview_ack(work_delivery, monkeypatch, purpose, large):
    e = work_delivery
    async with closed_input(e, monkeypatch, purpose=purpose, large=large) as held:
        selected = held.controls["prompt_reopen"]
        await press_work(e, selected, message_id=held.notice_message, update_id=6502)
        identity = await dispatch_reopened(e, 6504, preview=False)
        async with e.db.sessions() as reader:
            parent = await reader.get(TelegramOutboxRow, identity)
            intent = parent.payload["assignment_prompt_intent"]
            assert intent["purpose"] == purpose
            prompt = await reader.get(TelegramCallbackRow, intent["prompt_id"])
            assert prompt.message_id is None and prompt.expires_at is None
            preview = "".join(part["visible_text"] for part in parent.parts)
            if purpose == "assign_done":
                assert "Original assignment" in preview
            elif large:
                assert "o" * 3000 in preview and "c" * 3000 in preview and len(parent.parts) >= 2
        for index in range(len(parent.parts)):
            await dispatch(e, 6504 + index)
            async with e.db.sessions() as reader:
                current = await reader.get(TelegramCallbackRow, prompt.id)
                if index < len(parent.parts) - 1:
                    assert current.message_id is None and current.expires_at is None
                else:
                    assert current.message_id == 6504 + index and current.expires_at is not None
        assert tuple(held.calls) == held.baseline_calls and not held.turns._turns
        raw = update_payload(6550, account=e.link.chat_id, text="Publish verified corrections")
        raw["message"]["reply_to_message"] = {
            "message_id": 6503 + len(parent.parts),
            "date": 1_893_456_000,
            "chat": raw["message"]["chat"],
            "from": {"id": e.link.bot_id, "is_bot": True, "first_name": "Fixture bot"},
            "text": "Fresh complete prompt",
        }
        assert await e.channel.handle_update(Update.de_json(raw, e.channel.bot))
        await e.channel._drain_ingress()
        assert len(held.calls) == len(held.baseline_calls) + (purpose == "assign_edit")
        async with e.db.sessions() as reader:
            incoming = await reader.get(TelegramInboundMarkRow, (e.link.id, 6550, e.link.chat_id))
            root = await reader.get(TelegramActRow, incoming.route_target_id)
            assert root.body["draft"]["success_criteria"] == "Publish verified corrections" if purpose == "assign_done" else root.body["draft"]["objective"] == "Publish verified corrections"
            original = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            assert not original.operation_ids and original.thread_id is None and original.turn_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("management,voice", [(False, False), (True, False), (False, True), (True, True)])
async def test_ambiguous_actual_prompt_receipts_hold_input_without_picking_a_target(work_delivery, monkeypatch, management, voice):
    e = work_delivery
    async with closed_input(e, monkeypatch, management=management, voice=voice, ambiguous=True) as held:
        assert set(held.controls) == {"prompt_send_new", "prompt_discard"}
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            assert mark.route_basis["closed_typing"] == {"version": 1, "prompt_id": None, "source_outbox_id": None, "ambiguous": True}
        selected = held.controls["prompt_send_new"]
        await press_work(e, selected, message_id=held.notice_message, update_id=6503)
        await until(lambda: held.state.posts == 1)
        assert held.state.record.owner_callback_id == selected.id and not held.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("management", [False, True])
async def test_legacy_voice_without_uniqueness_receipt_cannot_reopen_a_target(work_delivery, monkeypatch, management):
    e = work_delivery
    async with closed_input(e, monkeypatch, management=management, voice=True, missing_selection=True) as held:
        assert set(held.controls) == {"prompt_send_new", "prompt_discard"}
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            assert mark.route_basis["work_voice"]["version"] == 3 and "work_voice_selection" not in mark.route_basis
            assert mark.route_basis["closed_typing"]["ambiguous"] is True
        await press_work(e, held.controls["prompt_send_new"], message_id=held.notice_message, update_id=6503)
        await until(lambda: held.state.posts == 1)
        assert len(e.transcriber.calls) == 1 and not held.calls


@pytest.mark.asyncio
async def test_retention_ignores_another_notices_callback_group_locator(work_delivery, monkeypatch):
    from app.telegram_bots.assignment_reopen_retention import ReopenDependencies
    from app.telegram_bots.closed_prompt_history import pin_owned_history

    e = work_delivery
    async with closed_input(e, monkeypatch) as held:
        selected = held.controls["prompt_reopen"]
        await press_work(e, selected, message_id=6501, update_id=6502)
        await dispatch_reopened(e, 6504)
        raw = update_payload(7001, account=e.link.chat_id, text="An independent late reply")
        raw["message"]["reply_to_message"] = {"message_id": 6132, "date": 1_893_456_000, "chat": raw["message"]["chat"], "from": {"id": e.link.bot_id, "is_bot": True, "first_name": "Fixture bot"}, "text": "Original assignment prompt"}
        assert await e.channel.handle_update(Update.de_json(raw, e.channel.bot))
        await e.channel._drain_ingress()
        await dispatch(e, 7002)
        async with e.db.sessions.begin() as writer:
            other = await writer.get(TelegramInboundMarkRow, (e.link.id, 7001, e.link.chat_id))
            foreign = await writer.get(TelegramOutboxRow, other.route_basis["work_closed"]["notice_id"])
            group = foreign.parts[-1]["callback_group_id"]
            unrelated = {row.id for row in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group))}
            notice = await writer.get(TelegramOutboxRow, held.notice.id)
            notice.parts = [{**part, "callback_group_id": group} for part in notice.parts]
        dependencies = ReopenDependencies()
        async with e.db.sessions() as reader:
            await pin_owned_history(reader, e.clock.now(), dependencies)
        assert unrelated and unrelated.isdisjoint(dependencies.callbacks)
        assert selected.id in dependencies.callbacks


@pytest.mark.asyncio
@pytest.mark.parametrize("management", [False, True])
@pytest.mark.parametrize("choice", ["prompt_send_new", "prompt_discard", "prompt_reopen"])
async def test_late_assignment_speech_stays_held_until_actual_send_new_choice(work_delivery, monkeypatch, management, choice):
    e = work_delivery
    async with closed_input(e, monkeypatch, management=management, voice=True) as held:
        assert len(e.transcriber.calls) == 1
        assert set(held.controls) == {"prompt_reopen", "prompt_send_new", "prompt_discard"}
        selected = held.controls[choice]
        await press_work(e, selected, message_id=held.notice_message, update_id=6503)
        if choice == "prompt_send_new":
            await until(lambda: held.state.posts == 1)
            assert held.state.record.owner_callback_id == selected.id
            assert [row.message_id for row in held.state.record.origins] == [6151]
        elif choice == "prompt_reopen":
            await dispatch_reopened(e, 6505)
        await press_work(e, selected, message_id=held.notice_message, update_id=6504)
        assert held.state.posts == (1 if choice == "prompt_send_new" else 0) and len(e.transcriber.calls) == 1 and not held.calls
        async with e.db.sessions() as reader:
            recording = await reader.scalar(select(TelegramActRow).where(TelegramActRow.link_id == e.link.id, TelegramActRow.message_id == 6151, TelegramActRow.kind == "voice_note"))
            assert recording.body["used_as"] == ("message" if choice == "prompt_send_new" else "discarded")


@pytest.mark.asyncio
@pytest.mark.parametrize("management", [False, True])
@pytest.mark.parametrize("choice", ["prompt_send_new", "prompt_reopen"])
async def test_legacy_acknowledged_transcript_is_not_echoed_again_before_choices(work_delivery, monkeypatch, management, choice):
    e = work_delivery
    async with closed_input(e, monkeypatch, management=management, voice=True, legacy_echo=True) as held:
        assert len(held.notice.parts) == 1
        assert len(e.transcriber.calls) == 1
        spoken = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage" and "Review the handbook" in call.parameters.get("text", "")]
        assert len(spoken) == 1
        selected = held.controls[choice]
        await press_work(e, selected, message_id=held.notice_message, update_id=6503)
        if choice == "prompt_send_new":
            await until(lambda: held.state.posts == 1)
            assert held.state.record.owner_callback_id == selected.id
        else:
            await dispatch_reopened(e, 6505)
        assert held.state.posts == (1 if choice == "prompt_send_new" else 0) and len(e.transcriber.calls) == 1 and not held.calls


async def installed_management_prompt(e, monkeypatch):
    from test_telegram_work_chat_runtime import installed_work_runtime

    owner = await installed_work_runtime(e)
    try:
        return await prepare_management_prompt(e, monkeypatch, owner)
    except BaseException:
        await owner.stop()
        raise


async def prepare_management_prompt(e, monkeypatch, owner):
    from test_telegram_work_chat_management_runtime import open_detail
    from test_telegram_work_interaction_delivery import selected
    from test_telegram_work_start import prepare_startable

    from app.telegram_bots.assign import DraftAssignment
    from app.telegram_bots.producer import DeliveryRetry

    await prepare_startable(e, monkeypatch)
    e.clock.advance(1)  # Include the actual newly created Work in the list horizon.
    calls = []

    async def proposal(words, **kwargs):
        calls.append(words)
        return DraftAssignment(objective=words, success_criteria="Record verified corrections")

    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", proposal)
    await open_detail(e, first=6001)
    change = await selected(e, "work_change")
    await press_work(e, change, message_id=6004, update_id=6005)
    async with e.db.sessions() as reader:
        parent = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_management_intent"]["kind"].as_string() == "edit"))
        e.identity = parent.id
    await dispatch(e, 6006)
    edit = await selected(e, "assign_edit")
    await press_work(e, edit, message_id=6006, update_id=6007)
    async with e.db.sessions() as reader:
        prompt = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["management_prompt_intent"]["purpose"].as_string() == "work_edit"))
        cleanup_id, e.identity = prompt.payload["management_prompt_cleanup_id"], prompt.id
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    e.clock.advance(10)
    for _ in range(8):
        assert await e.sender.tick() in {0, 1}
        await e.sender.drain()
        async with e.db.sessions() as reader:
            if (await reader.get(TelegramOutboxRow, cleanup_id)).state == "sent":
                break
        e.clock.advance(1)
    else:
        pytest.fail("The actual editor cleanup was not acknowledged")
    assert all(isinstance(error, DeliveryRetry) for error in e.dispatch_errors), e.dispatch_errors
    e.dispatch_errors.clear()
    await dispatch(e, 6010)
    assert not e.dispatch_errors, e.dispatch_errors
    return owner, calls


async def dispatch_reopened(e, message_id, *, preview=True):
    from app.telegram_bots.notifications import aware
    from app.telegram_bots.producer import DeliveryRetry

    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
        parent = await reader.get(TelegramOutboxRow, mark.outbox_id)
        cleanup_id = parent.payload["reopen_cleanup_id"]
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    for _ in range(8):
        e.clock.advance(1)
        assert await e.sender.tick() in {0, 1}
        await e.sender.drain()
        async with e.db.sessions() as reader:
            if (await reader.get(TelegramOutboxRow, cleanup_id)).state == "sent":
                break
    else:
        pytest.fail("The actual Reopen keyboard cleanup was not acknowledged")
    assert all(isinstance(error, DeliveryRetry) for error in e.dispatch_errors), e.dispatch_errors
    e.dispatch_errors.clear()
    async with e.db.sessions() as reader:
        pending = await reader.get(TelegramOutboxRow, parent.id)
        e.clock.advance(max(0, (aware(pending.not_before) - e.clock.now()).total_seconds()))
    if preview:
        await dispatch(e, message_id)
    return parent.id


@pytest.mark.asyncio
@pytest.mark.parametrize("management", [False, True])
@pytest.mark.parametrize("damage", ["input", "choice", "original_ack", "source_intent", "cleanup", "primary", "write_route"])
async def test_reopened_prompt_final_sdk_refuses_lost_original_proof(work_delivery, monkeypatch, management, damage):
    from app.gateway import authz

    e = work_delivery
    async with closed_input(e, monkeypatch, management=management) as held:
        selected = held.controls["prompt_reopen"]
        await press_work(e, selected, message_id=6501, update_id=6502)
        identity = await dispatch_reopened(e, 6504, preview=False)
        sources = e.services.work_management_prompt_sources if management else e.services.assignment_prompt_sources
        original_get, original_permissions = sources.settings.get, authz.resolve_route_permissions
        changed = []

        async def permissions(*args, **kwargs):
            current = await original_permissions(*args, **kwargs)
            return frozenset(current) - {"agents:write"} if changed and damage == "write_route" else current

        async def move(*args, **kwargs):
            result = await original_get(*args, **kwargs)
            if not changed:
                changed.append(True)
                async with e.db.sessions.begin() as writer:
                    mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
                    parent = await writer.get(TelegramOutboxRow, identity)
                    original = await writer.get(TelegramOutboxRow, mark.route_basis["closed_typing"]["source_outbox_id"])
                    if damage == "input":
                        mark.sanitized_input = {**mark.sanitized_input, "text": "Different retained words"}
                    elif damage == "choice":
                        await writer.delete(await writer.get(TelegramCallbackRow, selected.id))
                    elif damage == "original_ack":
                        original.parts = [{**part, "message_ids": []} for part in original.parts]
                    elif damage == "source_intent":
                        family = "management_prompt_intent" if management else "assignment_prompt_intent"
                        original.payload = {**original.payload, family: {**original.payload[family], "prompt_id": "f" * 32}}
                    elif damage == "cleanup":
                        await writer.delete(await writer.get(TelegramOutboxRow, parent.payload["reopen_cleanup_id"]))
                    elif damage == "primary":
                        mark.outbox_id = held.notice.id
            return result

        monkeypatch.setattr(authz, "resolve_route_permissions", permissions)
        monkeypatch.setattr(sources.settings, "get", move)
        before = len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"])
        e.clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert changed
        assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == before
        assert not held.calls and not held.turns._turns


@pytest.mark.asyncio
@pytest.mark.parametrize("management", [False, True])
async def test_damaged_reopen_intent_and_rendering_cannot_become_a_legacy_prompt(work_delivery, monkeypatch, management):
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    async with closed_input(e, monkeypatch, management=management) as held:
        selected = held.controls["prompt_reopen"]
        await press_work(e, selected, message_id=6501, update_id=6502)
        async with e.db.sessions.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            parent = await writer.get(TelegramOutboxRow, mark.outbox_id)
            family = "management_prompt_intent" if management else "assignment_prompt_intent"
            legacy = {key: value for key, value in parent.payload[family].items() if key != "reopen"}
            parent.payload = {**{key: value for key, value in parent.payload.items() if key not in {"work_news_basis", "product_keyboard_basis"}}, family: {**legacy, "version": 1}}
            parent.parts = []
            identity = parent.id
        plans = held.owner.chat.management_edits.prompts if management else held.owner.chat.assignment_prompts
        with pytest.raises(ProductSourceRetired):
            await plans.materialize(e.link, identity, guard=held.owner.chat.guard(e.channel, e.link))
        assert not held.calls and not held.turns._turns


@pytest.mark.asyncio
async def test_expired_management_prompt_reopens_current_full_private_copy(work_delivery, monkeypatch):
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    async with closed_input(e, monkeypatch, management=True) as held:
        assert "prompt_reopen" in held.controls
        selected = held.controls["prompt_reopen"]
        async with e.db.sessions() as reader:
            before = (await reader.get(AgentWorkRow, e.work["id"])).revision
        await press_work(e, selected, message_id=6501, update_id=6502)
        async with e.db.sessions() as reader:
            fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["management_prompt_intent"]["reopen"]["callback_id"].as_string() == selected.id))
            assert fresh is not None and fresh.parts
            source = fresh.payload["work_news_basis"]["source"]
            draft = source["work"]["root"]["body"]["draft"]
            preview = "".join(part["visible_text"] for part in fresh.parts)
            assert draft["objective"] in preview and draft["success_criteria"] in preview
            assert (await reader.get(AgentWorkRow, e.work["id"])).revision == before
        await dispatch_reopened(e, 6504)
        assert not e.dispatch_errors, e.dispatch_errors
        text = [call.parameters["text"] for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1]
        assert draft["objective"] in text and draft["success_criteria"] in text
        assert not held.calls and not held.turns._turns
        raw = update_payload(6550, account=e.link.chat_id, text="Revise the saved guide")
        raw["message"]["reply_to_message"] = {"message_id": 6504, "date": 1_893_456_000, "chat": raw["message"]["chat"], "from": {"id": e.link.bot_id, "is_bot": True, "first_name": "Fixture bot"}, "text": "Current private copy"}
        assert await e.channel.handle_update(Update.de_json(raw, e.channel.bot))
        await e.channel._drain_ingress()
        assert held.calls == ["Revise the saved guide"] and not held.turns._turns
        async with e.db.sessions() as reader:
            incoming = await reader.get(TelegramInboundMarkRow, (e.link.id, 6550, e.link.chat_id))
            assert incoming.route_kind == "management_edit"
            assert (await reader.get(AgentWorkRow, e.work["id"])).revision == before
            original = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            assert not original.operation_ids and original.thread_id is None and original.turn_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose,management", [("assign_done", False), ("assign_edit", False), (None, True)])
async def test_reopen_cannot_extend_the_original_private_draft_lifetime(work_delivery, monkeypatch, purpose, management):
    from test_telegram_bot_adapter import message

    e = work_delivery
    async with closed_input(e, monkeypatch, management=management, purpose=purpose) as held:
        baseline = tuple(held.calls)
        e.clock.advance(e.services.callbacks.config.callbacks.draft_ttl_hours * 3600)
        await press_work(e, held.controls["prompt_reopen"], message_id=held.notice_message, update_id=6502)
        async with e.db.sessions() as reader:
            incoming = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            assert not incoming.route_complete
            notice = await reader.get(TelegramOutboxRow, incoming.route_basis["work_closed"]["notice_id"])
            assert notice.id != held.notice.id
            assert {item["kind"] for line in notice.payload["product_keyboard_basis"] for item in line} == {"prompt_send_new", "prompt_discard"}
            assert notice.parts[-1]["source_ref"]["key"] == "prompt.unavailable"
        message(e.channel.bot.request, "editMessageText", identity=held.notice_message)
        e.clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert not e.dispatch_errors and tuple(held.calls) == baseline and not held.turns._turns


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["prompt_send_new", "prompt_discard"])
@pytest.mark.parametrize("missing_prompt", [False, True])
async def test_closed_assignment_choice_owns_actual_queue_without_original_prompt(work_delivery, monkeypatch, choice, missing_prompt):
    e = work_delivery
    async with closed_input(e, monkeypatch, missing_prompt=missing_prompt) as held:
        if missing_prompt:
            assert set(held.controls) == {"prompt_send_new", "prompt_discard"}
        selected = held.controls[choice]
        await press_work(e, selected, message_id=6501, update_id=6502)
        if choice == "prompt_send_new":
            await until(lambda: held.state.posts == 1)
            record = held.state.record
            assert record.owner_callback_id == selected.id
            assert [origin.message_id for origin in record.origins] == [6151]
            await press_work(e, selected, message_id=6501, update_id=6503)
            assert held.state.posts == 1 and held.state.record.event_id == record.event_id
        else:
            assert held.state.posts == 0 and not held.turns._turns
        assert not held.calls
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            assert mark.sanitized_input["text"] == "Review the handbook"
            assert mark.route_complete
            assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_reopen_assignment_prompt_opens_fresh_question_without_using_held_words(work_delivery, monkeypatch):
    e = work_delivery
    async with closed_input(e, monkeypatch) as held:
        selected = held.controls["prompt_reopen"]
        await press_work(e, selected, message_id=6501, update_id=6502)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_prompt_intent"]["reopen"]["callback_id"].as_string() == selected.id))
            assert fresh is not None
            assert mark.route_complete and fresh.id in mark.delivery_ids
            assert not mark.operation_ids and mark.turn_id is None and mark.thread_id is None
            prompt = await reader.get(TelegramCallbackRow, fresh.payload["assignment_prompt_intent"]["prompt_id"])
            assert prompt.message_id is None and prompt.expires_at is None and prompt.consumed_at is None
        assert not held.calls and not held.turns._turns
        await dispatch_reopened(e, 6504)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, fresh.id)).state == "sent"
            prompt = await reader.get(TelegramCallbackRow, prompt.id)
            assert prompt.message_id == 6504 and prompt.expires_at is not None
        await press_work(e, selected, message_id=6501, update_id=6505)
        assert not held.calls and not held.turns._turns
        raw = update_payload(6506, account=e.link.chat_id, text="Review the revised handbook")
        raw["message"]["reply_to_message"] = {"message_id": 6504, "date": 1_893_456_000, "chat": raw["message"]["chat"], "from": {"id": e.link.bot_id, "is_bot": True, "first_name": "Fixture bot"}, "text": "Fresh assignment prompt"}
        assert await e.channel.handle_update(Update.de_json(raw, e.channel.bot))
        await e.channel._drain_ingress()
        assert held.calls == ["Review the revised handbook"] and not held.turns._turns
        async with e.db.sessions() as reader:
            original = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            fresh_input = await reader.get(TelegramInboundMarkRow, (e.link.id, 6506, e.link.chat_id))
            draft = await reader.get(TelegramActRow, fresh_input.route_target_id)
            assert not original.operation_ids and original.thread_id is None and original.turn_id is None
            assert draft.body["origin"]["message_id"] == 6506 and not draft.operation_ids and draft.state == "draft"


@pytest.mark.asyncio
async def test_live_reopened_assignment_retains_aged_actual_origin_and_choice(work_delivery, monkeypatch):
    from app.telegram_bots.retention import sweep_rows

    e = work_delivery
    async with closed_input(e, monkeypatch) as held:
        selected = held.controls["prompt_reopen"]
        await press_work(e, selected, message_id=6501, update_id=6502)
        # Delay the actual fresh delivery beyond history retention. Its typing
        # lifetime starts only at the eventual real SDK acknowledgement.
        e.clock.advance((e.services.callbacks.config.callbacks.max_age_days + 1) * 86400)
        await dispatch_reopened(e, 6504)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            assert mark.state == "done"
            proof = mark.route_basis["closed_typing"]
            context = mark.route_basis["work_closed"]["prompt_id"]
        await e.services.callbacks.sweep(e.clock.now())
        await sweep_rows(e.db.sessions, e.clock.now(), callback_max_age_days=e.services.callbacks.config.callbacks.max_age_days)
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramOutboxRow, proof["source_outbox_id"]) is not None
            assert await reader.get(TelegramOutboxRow, held.notice.id) is not None
            for identity in (proof["prompt_id"], context, *(row.id for row in held.controls.values())):
                assert await reader.get(TelegramCallbackRow, identity) is not None
            for identity in (6131, 6151):
                assert await reader.get(TelegramInboundMarkRow, (e.link.id, identity, e.link.chat_id)) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("voice,damage", [(voice, damage) for voice in (False, True) for damage in ("descriptor", "choice", "foreign_locator", "notice", "notice_part")] + [(True, "echo_witness"), (True, "echo_missing_witness")])
async def test_damaged_reopen_retains_owned_history_behind_unfinished_actual_reply(work_delivery, monkeypatch, damage, voice):
    from _telegram_bot_test_support import make_update

    from app.telegram_bots.authority import AuthorityUnavailable
    from app.telegram_bots.retention import sweep_rows

    e = work_delivery
    async with closed_input(e, monkeypatch, voice=voice, legacy_echo=voice) as held:
        selected = held.controls["prompt_reopen"]
        await press_work(e, selected, message_id=6501, update_id=6502)
        identity = await dispatch_reopened(e, 6504)

        async def unavailable(*args, **kwargs):
            raise AuthorityUnavailable()

        raw = update_payload(6506, account=e.link.chat_id, text="An unfinished fresh answer")
        raw["message"]["reply_to_message"] = {"message_id": 6504, "date": 1_893_456_000, "chat": raw["message"]["chat"], "from": {"id": e.link.bot_id, "is_bot": True, "first_name": "Fixture bot"}, "text": "Fresh assignment prompt"}
        with monkeypatch.context() as fault:
            fault.setattr(held.owner.chat.assignment_prompts.reader, "prepare_clock", unavailable)
            with pytest.raises(AuthorityUnavailable):
                await e.channel.handle_update(Update.de_json(raw, e.channel.bot))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            pending = await reader.get(TelegramInboundMarkRow, (e.link.id, 6506, e.link.chat_id))
            assert pending.route_kind == "assignment_hold" and not pending.route_complete
            assert pending.route_basis["assignment_owner"]["source_outbox_id"] == identity
        assert await e.channel.handle_update(make_update(6507, account=e.link.chat_id, text="/help"))
        await e.channel._drain_ingress()
        await dispatch(e, 6508)
        async with e.db.sessions.begin() as writer:
            help_reply = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id, TelegramOutboxRow.message_id == 6508))
            assert help_reply is not None and help_reply.state == "sent"
            unrelated = help_reply.id
            parent = await writer.get(TelegramOutboxRow, identity)
            original = await writer.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            echo_id = original.route_basis.get("closed_voice_echo", {}).get("outbox_id")
            intent = parent.payload["assignment_prompt_intent"]
            if damage == "descriptor":
                parent.payload = {**parent.payload, "assignment_prompt_intent": {**intent, "reopen": None}}
            elif damage == "choice":
                await writer.delete(await writer.get(TelegramCallbackRow, selected.id))
            elif damage == "notice":
                await writer.delete(await writer.get(TelegramOutboxRow, held.notice.id))
            elif damage == "notice_part":
                (await writer.get(TelegramOutboxRow, held.notice.id)).parts = [None]
            elif damage in {"echo_witness", "echo_missing_witness"}:
                basis = {key: value for key, value in original.route_basis.items() if key != "closed_voice_echo"}
                original.route_basis = {**basis, **({"closed_voice_echo": {"version": True}} if damage == "echo_witness" else {})}
            else:
                parent.payload = {**parent.payload, "assignment_prompt_intent": {**intent, "reopen": {**intent["reopen"], "source_outbox_id": unrelated}}}
        e.clock.advance((e.services.callbacks.config.callbacks.max_age_days + 1) * 86400)
        await e.services.callbacks.sweep(e.clock.now())
        await sweep_rows(e.db.sessions, e.clock.now(), callback_max_age_days=e.services.callbacks.config.callbacks.max_age_days)
        async with e.db.sessions() as reader:
            for message in (6131, 6151, 6506):
                assert await reader.get(TelegramInboundMarkRow, (e.link.id, message, e.link.chat_id)) is not None
            assert await reader.get(TelegramOutboxRow, unrelated) is None
            if damage != "notice":
                assert await reader.get(TelegramOutboxRow, held.notice.id) is not None
            if voice:
                assert echo_id is not None and await reader.get(TelegramOutboxRow, echo_id) is not None
        assert not held.calls and not held.turns._turns


@pytest.mark.asyncio
async def test_reopen_recovers_the_same_committed_prompt_before_materialization(work_delivery, monkeypatch):
    from app.telegram_bots.inbox import InboxRow

    e = work_delivery
    async with closed_input(e, monkeypatch) as held:
        plans = held.owner.chat.assignment_prompts
        actual = plans.materialize

        async def interrupted(*args, **kwargs):
            raise TimeoutError("Interrupted after the original publication committed")

        selected = held.controls["prompt_reopen"]
        async with e.db.sessions() as reader:
            original = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id)))
        monkeypatch.setattr(plans, "materialize", interrupted)
        await press_work(e, selected, message_id=6501, update_id=6502)
        async with e.db.sessions() as reader:
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_prompt_intent"]["reopen"]["callback_id"].as_string() == selected.id))
            assert row is not None and not row.parts
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            assert mark.route_complete and mark.outbox_id == row.id
        monkeypatch.setattr(plans, "materialize", actual)
        guard = held.owner.chat.guard(e.channel, e.link)
        assert await held.owner.chat.closed_prompts.reopen(e.channel, e.link, selected, original, guard=guard) == row.id
        await dispatch_reopened(e, 6504)
        async with e.db.sessions() as reader:
            rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_prompt_intent"]["reopen"]["callback_id"].as_string() == selected.id)))
            assert len(rows) == 1 and rows[0].id == row.id and rows[0].state == "sent"
        assert not e.dispatch_errors and not held.calls and not held.turns._turns


@pytest.mark.asyncio
async def test_interrupted_closed_source_read_replays_original_input_after_a_new_prompt(work_delivery, monkeypatch):
    from app.telegram_bots.authority import AuthorityUnavailable

    e = work_delivery
    owner, calls = await installed_prompt(e, monkeypatch)
    e.services, e.request = owner.host, e.channel.bot.request
    turns, _state, _reached = await controlled_execution(e, monkeypatch)
    original_source = e.identity
    try:
        e.clock.advance(601)
        update = reply(e, quoted=False)

        async def unavailable(*args, **kwargs):
            raise AuthorityUnavailable()

        with monkeypatch.context() as fault:
            fault.setattr(owner.chat.assignment_prompts.reader, "read", unavailable)
            with pytest.raises(AuthorityUnavailable):
                await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            original = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            captured = original.route_basis["closed_typing"]
            assert original.route_kind == "work_card_hold" and not original.route_complete
            assert captured["source_outbox_id"] == original_source
            assert "work_closed" not in original.route_basis
            assert original.sanitized_input["text"] == "Review the handbook"
        assert await e.channel.handle_update(make_update(6201, account=e.link.chat_id, text="/assign"))
        await e.channel._drain_ingress()
        await dispatch(e, 6202)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            original = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            current = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt", TelegramCallbackRow.message_id == 6202))
            notice = await reader.get(TelegramOutboxRow, original.route_basis["work_closed"]["notice_id"])
            assert original.route_basis["closed_typing"] == captured
            assert notice is not None and not original.route_complete
            assert current is not None and current.consumed_at is None
            assert current.id != captured["prompt_id"]
            assert not list(await reader.scalars(select(TelegramActRow)))
        assert not calls and not turns._turns
    finally:
        await turns.stop()
        await owner.stop()


@pytest.mark.asyncio
async def test_expired_assignment_prompt_holds_actual_text_and_offers_acknowledged_choices(work_delivery, monkeypatch):
    e = work_delivery
    owner, calls = await installed_prompt(e, monkeypatch)
    e.services, e.request = owner.host, e.channel.bot.request
    turns, _state, _reached = await controlled_execution(e, monkeypatch)
    try:
        e.clock.advance(601)
        update = reply(e, quoted=False)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        assert not calls and not turns._turns
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            assert mark is not None and not mark.route_complete
            assert mark.sanitized_input["text"] == "Review the handbook"
            assert not list(await reader.scalars(select(TelegramActRow)))
            notice = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.id == mark.route_basis["work_closed"]["notice_id"]))
            assert notice is not None
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == notice.id)))
            assert {row.kind for row in controls} == {"prompt_reopen", "prompt_send_new", "prompt_discard"}
            assert all(row.message_id is None for row in controls)
        await dispatch(e, 6501)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions() as reader:
            for control in controls:
                assert (await reader.get(TelegramCallbackRow, control.id)).message_id == 6501
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            assert mark.route_basis["work_closed"]["notice_id"] == notice.id and not mark.route_complete
        assert not calls and not turns._turns
    finally:
        await turns.stop()
        await owner.stop()
