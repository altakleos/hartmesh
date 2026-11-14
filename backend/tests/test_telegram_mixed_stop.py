"""Mixed Stop uses actual ingress, original chat custody and Work journals.

Chat execution reads are explicitly controlled here; native workers qualify in CI.
"""

import asyncio

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_activation import seeded_attempt
from test_telegram_chat_turns import controlled_execution, until
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", [None, "answer", "work", "all", "preparing_all", "cold_preparing_all", "callback_preparing_all", "replacement_all"])
async def test_mixed_stop_asks_before_touching_original_answer_or_assignment(work_delivery, monkeypatch, choice, edge=None):
    e = work_delivery
    owner = await installed_work_runtime(e)
    e.services, e.request = owner.host, e.channel.bot.request
    e.acts = owner.host.acts
    attempt, _run, manager = await seeded_attempt(e)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    release, entered = asyncio.Event(), asyncio.Event()
    if choice in {"preparing_all", "cold_preparing_all", "callback_preparing_all"}:

        async def preparing(*args, **kwargs):
            entered.set()
            await release.wait()
            return "controller-original-thread"

        if edge in {"undispatched", "undispatched_drift"}:
            dispatch_record = e.services.publications.dispatch_record

            async def before_dispatch(*args, **kwargs):
                entered.set()
                await release.wait()
                return await dispatch_record(*args, **kwargs)

            e.services.publications.dispatch_record = before_dispatch
        else:
            e.services.threads.ensure_thread = preparing
    try:
        e.request.enqueue("sendChatAction", result=True)
        assert await e.channel.handle_update(make_update(6401, account=e.link.chat_id, text="Explain the current references"))
        if choice in {"preparing_all", "cold_preparing_all", "callback_preparing_all"}:
            await asyncio.wait_for(entered.wait(), 5)
        else:
            await asyncio.wait_for(reached.wait(), 5)
            await until(lambda: state.posts == 1)
        if choice == "callback_preparing_all":
            from test_telegram_callback_commands import press

            from app.telegram_bots.turns import ChatTurns

            await turns.retire(e.channel)
            turns = ChatTurns(e.services)
            e.services.turns = turns
            await turns.recover_link(e.channel)
            async with e.db.sessions() as reader:
                notice = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == f"notice:chat-restart:{e.link.chat_id}:{e.link.binding_generation}:6401"))
                assert notice is not None
                e.identity = notice.id
            await dispatch(e, 6398)
            async with e.db.sessions() as reader:
                retry = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == e.identity, TelegramCallbackRow.kind == "retry_message"))
            turns, state, reached = await controlled_execution(e, monkeypatch)
            entered.clear()
            e.services.threads.ensure_thread = preparing
            e.services.threads.ensure_callback_thread = preparing
            e.request.enqueue("answerCallbackQuery", result=True)
            assert await e.channel.handle_update(press(e, 6399, retry.id, message_id=6398))
            await asyncio.wait_for(entered.wait(), 5)
            assert next(iter(turns._turns.values())).callback_id == retry.id
        assert await e.channel.handle_update(make_update(6402, account=e.link.chat_id, text="/stop"))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6402, e.link.chat_id))
            assert mark.route_kind == "command_stop"
            basis = mark.route_basis["stop_command"]
            assert basis["answer"] is not None and basis["selection"] is None
            card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["command_stop_intent"]["message_id"].as_integer() == 6402))
            assert card is not None and card.parts
            e.identity = card.id
        assert state.stop_calls == 0
        manager.cancel.assert_not_awaited()
        if choice is None:
            return
        await dispatch(e, 6403)
        assert not e.dispatch_errors
        wanted = "attempt:" + attempt if choice == "work" else "all" if choice in {"preparing_all", "cold_preparing_all", "callback_preparing_all", "replacement_all"} else choice
        async with e.db.sessions() as reader:
            selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == e.identity, TelegramCallbackRow.payload["choice"].as_string() == wanted))
        assert selected is not None
        from test_telegram_callback_commands import press

        if choice == "replacement_all":
            await turns.retire(e.channel)
            turns, replacement, next_reached = await controlled_execution(e, monkeypatch)
            e.request.enqueue("sendChatAction", result=True)
            assert await e.channel.handle_update(make_update(6405, account=e.link.chat_id, text="A later independent answer"))
            await asyncio.wait_for(next_reached.wait(), 5)
            await until(lambda: replacement.posts == 1)
        e.request.enqueue("answerCallbackQuery", result=True)
        update = press(e, 6404, selected.id, message_id=6403)
        if edge == "undispatched_drift":
            read = owner.host.outbox.inbox.read_publication_input

            async def drift_after_read(*args, **kwargs):
                result = await read(*args, **kwargs)
                async with e.db.sessions.begin() as writer:
                    mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 6402, e.link.chat_id))
                    mark.sanitized_input = {"command": "help"}
                return result

            monkeypatch.setattr(owner.host.outbox.inbox, "read_publication_input", drift_after_read)
        if edge == "reserve_race":
            from app.telegram_bots.work_command_stop_answers import AnswerCancelAdmission

            admit = AnswerCancelAdmission.admit

            async def reserve_at_admission_commit(admission):
                await admit(admission)
                release.set()
                await until(lambda: state.posts == 1)

            monkeypatch.setattr(AnswerCancelAdmission, "admit", reserve_at_admission_commit)
        if choice in {"cold_preparing_all", "callback_preparing_all"}:
            original_answer = owner.chat.command_stops.answers.run

            async def interrupted(*args, **kwargs):
                raise RuntimeError("Interrupted before original answer admission")

            monkeypatch.setattr(owner.chat.command_stops.answers, "run", interrupted)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        if choice in {"answer", "all"} or edge in {"reserve_race", "undispatched", "undispatched_drift"}:
            assert state.stop_calls == 1
        else:
            assert state.stop_calls == 0
        assert manager.cancel.await_count == int(choice in {"work", "all", "preparing_all", "cold_preparing_all", "callback_preparing_all", "replacement_all"} and edge != "undispatched_drift")
        if edge in {"undispatched", "undispatched_drift"}:
            async with e.db.sessions() as reader:
                first = await reader.get(TelegramInboundMarkRow, (e.link.id, 6401, e.link.chat_id))
                mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6402, e.link.chat_id))
                assert first.route_complete == (edge == "undispatched")
                assert mark.route_complete == (edge == "undispatched")
            return
        if edge == "reserve_race":
            from app.telegram_bots.inbox import publication_identity

            original = next(iter(turns._turns.values()))
            async with e.db.sessions() as reader:
                mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6402, e.link.chat_id))
                assert mark.route_basis["stop_answer_admission"]["publication"] == publication_identity(original.record)
            return
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6402, e.link.chat_id))
            assert mark.route_complete == (choice == "work")
        e.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        assert manager.cancel.await_count == int(choice in {"work", "all", "preparing_all", "cold_preparing_all", "callback_preparing_all", "replacement_all"} and edge != "undispatched_drift")
        if choice == "replacement_all":
            current = next(iter(turns._turns.values()))
            assert current.origins[0].message_id == 6405
            assert not current.stop_requested and replacement.stop_calls == 0
        if choice in {"cold_preparing_all", "callback_preparing_all"}:
            from app.telegram_bots.turns import ChatTurns

            await turns.retire(e.channel)
            turns = ChatTurns(e.services)
            e.services.turns = turns
            owner.chat.command_stops.answers.live.clear()
            monkeypatch.setattr(owner.chat.command_stops.answers, "run", original_answer)
            if edge == "expired":
                e.clock.advance(3601)
            elif edge == "drift":
                async with e.db.sessions.begin() as writer:
                    first = await writer.get(TelegramInboundMarkRow, (e.link.id, 6401, e.link.chat_id))
                    first.sanitized_input = {"type": "text", "text": "Changed after selection"}
            elif edge in {"root", "missing_root"}:
                async with e.db.sessions.begin() as writer:
                    mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 6402, e.link.chat_id))
                    if edge == "root":
                        mark.route_basis = {}
                    else:
                        await writer.delete(mark)
            if edge == "callback_crash":
                complete = owner.chat.command_stops.answers.complete_receipt

                async def crash_before_receipt(*args, **kwargs):
                    raise RuntimeError("Interrupted after callback cancellation plan")

                monkeypatch.setattr(owner.chat.command_stops.answers, "complete_receipt", crash_before_receipt)
            await turns.recover_link(e.channel)
            if edge == "callback_crash":
                from datetime import timedelta

                from app.telegram_bots.inbox import CALLBACK_PUBLICATION_KEY
                from app.telegram_bots.retention import ACT_RETENTION_DAYS, sweep_rows

                async with e.db.sessions() as reader:
                    control = await reader.get(TelegramCallbackRow, retry.id)
                    data = control.payload[CALLBACK_PUBLICATION_KEY]
                    assert data["route_complete"]
                    deliveries = list(data["delivery_ids"])
                    assert deliveries
                    mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6402, e.link.chat_id))
                    assert "stop_answer_receipt" not in mark.route_basis
                await sweep_rows(e.db.sessions, e.clock.now() + timedelta(days=ACT_RETENTION_DAYS + 1), callback_max_age_days=180)
                async with e.db.sessions() as reader:
                    assert await reader.get(TelegramCallbackRow, retry.id) is not None
                    for identity in deliveries:
                        assert await reader.get(TelegramOutboxRow, identity) is not None
                monkeypatch.setattr(owner.chat.command_stops.answers, "complete_receipt", complete)
            await owner.recover_inputs(lambda: True, link_id=e.link.id)
            await owner.recover_inputs(lambda: True, link_id=e.link.id)
            async with e.db.sessions() as reader:
                mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6402, e.link.chat_id))
                if edge == "missing_root":
                    assert mark is None
                else:
                    assert mark.route_complete == (edge not in {"drift", "root"})
                first = await reader.get(TelegramInboundMarkRow, (e.link.id, 6401, e.link.chat_id))
                assert first.route_complete == (edge is None or choice == "callback_preparing_all")
                if edge not in {"drift", "root", "missing_root"}:
                    assert mark.route_basis["stop_answer_receipt"]["publication"] is None
                if edge == "expired":
                    receipt = await reader.get(TelegramOutboxRow, mark.route_basis["stop_answer_receipt"]["delivery_id"])
                    assert receipt.payload["command_stop_answer_notice"]["reason"] == "expired"
                    assert "stop_answer_admission" not in mark.route_basis
                receipts = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id)))
                assert sum(part.get("source_ref", {}).get("key") == "chat.restarted" for row in receipts for part in row.parts) == int(choice == "callback_preparing_all")
            assert manager.cancel.await_count == 1
        if choice == "preparing_all":
            turn = next(iter(turns._turns.values()))
            if edge == "retire":
                marker = owner.chat.command_stops.answers.stop_marker
                reads = []

                async def retire_after_final_read(*args, **kwargs):
                    result = await marker(*args, **kwargs)
                    reads.append(True)
                    if len(reads) == 2:
                        turn.active = False
                    return result

                monkeypatch.setattr(owner.chat.command_stops.answers, "stop_marker", retire_after_final_read)
            release.set()
            await until(turn.done.is_set)
            async with e.db.sessions() as reader:
                mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6402, e.link.chat_id))
                if edge == "retire":
                    assert len(reads) == 2
                    assert not mark.route_complete
                    first = await reader.get(TelegramInboundMarkRow, (e.link.id, 6401, e.link.chat_id))
                    assert not first.route_complete and not first.delivery_ids
                    return
                assert mark.route_complete and mark.route_basis["stop_answer_receipt"]["publication"] is None
                first = await reader.get(TelegramInboundMarkRow, (e.link.id, 6401, e.link.chat_id))
                assert first.route_complete and first.turn_id is None
                assert set(first.delivery_ids) <= set(mark.delivery_ids)
    finally:
        release.set()
        await turns.stop()
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("edge", ["expired", "drift", "root", "missing_root"])
async def test_cold_selected_prepublication_stop_keeps_original_clock_and_custody(work_delivery, monkeypatch, edge):
    await test_mixed_stop_asks_before_touching_original_answer_or_assignment(work_delivery, monkeypatch, "cold_preparing_all", edge=edge)


@pytest.mark.asyncio
async def test_prepublication_stop_final_writer_retires_without_input_closure(work_delivery, monkeypatch):
    await test_mixed_stop_asks_before_touching_original_answer_or_assignment(work_delivery, monkeypatch, "preparing_all", edge="retire")


@pytest.mark.asyncio
async def test_callback_cancellation_delivery_survives_crash_before_mixed_receipt(work_delivery, monkeypatch):
    await test_mixed_stop_asks_before_touching_original_answer_or_assignment(work_delivery, monkeypatch, "callback_preparing_all", edge="callback_crash")


@pytest.mark.asyncio
async def test_first_publication_at_stop_admission_commit_remains_original(work_delivery, monkeypatch):
    await test_mixed_stop_asks_before_touching_original_answer_or_assignment(work_delivery, monkeypatch, "preparing_all", edge="reserve_race")


@pytest.mark.asyncio
@pytest.mark.parametrize("edge", ["undispatched", "undispatched_drift"])
async def test_undispatched_original_seals_only_with_final_mixed_command_proof(work_delivery, monkeypatch, edge):
    await test_mixed_stop_asks_before_touching_original_answer_or_assignment(work_delivery, monkeypatch, "preparing_all", edge=edge)
