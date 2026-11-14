"""Cold original cancellation through real SQL/SDK/facade and recorded HTTP.

Remote run/output documents are protocol scaffolds, not native worker provenance.
"""

import asyncio

import httpx
import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_activation import seeded_attempt
from test_telegram_callback_commands import press
from test_telegram_chat_turns import controlled_execution, until
from test_telegram_publication_primary import RUN_ID
from test_telegram_streamed_primary import cancelled
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.channels.message_bus import MessageBus
from app.telegram_bots.gateway_client import GatewayClientOwner
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.publication import PublicationCoordinator
from app.telegram_bots.publication_authority import PublicationAttemptAuthority
from app.telegram_bots.turns import ChatTurns
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_response", [False, True])
async def test_cold_selected_stop_only_cancels_its_actual_original_publication(work_delivery, monkeypatch, lost_response, scenario=None, retention=False, live=False, damage=None):
    e = work_delivery
    owner = await installed_work_runtime(e)
    e.services, e.request = owner.host, e.channel.bot.request
    e.acts = owner.host.acts
    _attempt, _run, manager = await seeded_attempt(e)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    status, calls, captured = ["interrupted" if scenario == "terminal_non_cancel" else "running"], [], []
    ack = {"X-DeerFlow-Channel-Execution-Version": "1", "X-DeerFlow-Output-Inventory-Version": "2"}

    async def respond(request):
        calls.append((request.method, request.url.path))
        record = captured[0]
        if request.url.path.endswith("/controller-original-thread"):
            return httpx.Response(200, json={"thread_id": record.scope.thread_id, "owner_user_id": e.link.owner_user_id, "metadata": {"agent_instance_id": e.link.agent_ref}, "agent_instance": {"id": e.link.agent_ref}})
        if request.url.path.endswith("/output"):
            return httpx.Response(200, headers=ack, json=cancelled(record).model_dump(mode="json"))
        if request.url.path.endswith("/cancel"):
            if scenario == "refused":
                return httpx.Response(409)
            status[0] = "interrupted"
            if scenario == "late_result":
                e.clock.advance(3601)
            if lost_response:
                raise httpx.ReadTimeout("Original cancellation response lost")
            return httpx.Response(202)
        if request.url.path.endswith("/runs/" + RUN_ID):
            if scenario == "deadline":
                e.clock.advance(3601)
            elif scenario == "drift":
                async with e.db.sessions.begin() as writer:
                    mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 6502, e.link.chat_id))
                    mark.sanitized_input = {"command": "help"}
        return httpx.Response(
            200,
            headers=ack,
            json={
                "run_id": RUN_ID,
                "thread_id": record.scope.thread_id,
                "assistant_id": "lead_agent",
                "status": status[0],
                "stop_reason": "paused" if scenario in {"terminal_non_cancel", "barrier_mismatch"} else None,
                "created_at": "2026-11-14T00:00:00Z",
                "metadata": {"channel_turn_id": record.publication_id},
                "kwargs": {"channel_execution": {"version": 1, "publication_id": record.publication_id, "owner_user_id": e.link.owner_user_id, "thread_id": record.scope.thread_id, "assistant_id": "lead_agent", "agent_name": None}},
            },
        )

    http = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/original/api", transport=httpx.MockTransport(respond))
    e.services.thread_client = http
    await e.http_app.state.agent_conversations.create(actor=e.actor, instance_id=e.instance.id, thread_id="controller-original-thread")
    cold, observer = None, None
    try:
        e.request.enqueue("sendChatAction", result=True)
        assert await e.channel.handle_update(make_update(6501, account=e.link.chat_id, text="Original answer before a restart"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        captured.append(next(iter(turns._turns.values())).record)
        assert await e.channel.handle_update(make_update(6502, account=e.link.chat_id, text="/stop"))
        async with e.db.sessions() as reader:
            card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["command_stop_intent"]["message_id"].as_integer() == 6502))
            e.identity = card.id
        await dispatch(e, 6503)
        async with e.db.sessions() as reader:
            control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == e.identity, TelegramCallbackRow.payload["choice"].as_string() == "all"))
        if live:
            from app.telegram_bots.progress import RunObserver

            authority = PublicationAttemptAuthority(links=e.db.links, authority=e.live.authority, client=http, channel=lambda identity: e.channel if identity == e.link.id else None, create_allowed=lambda: True, read_allowed=lambda: True)
            e.services.publication_authority = authority
            cold = PublicationCoordinator(inbox=owner.host.outbox.inbox, outbox=owner.host.outbox, bus=MessageBus(), http_owner=http, capture_attempt=authority.capture)
            attempt = await authority.capture(e.link, thread_id=captured[0].scope.thread_id, endpoint_id=captured[0].scope.endpoint_id)
            turn = next(iter(turns._turns.values()))
            turn.record = await owner.host.outbox.inbox.bind_publication_run(turn.record, run_id=RUN_ID, guard=attempt.read_guard)
            observer = RunObserver(http_owner=http, coordinator=cold, capture_attempt=authority.capture, is_current=lambda record: turns._current(turn) and record.publication_id == turn.record.publication_id)
            turns._observer.request_stop = observer.request_stop
            turns._observer.stop_outcome = observer.stop_outcome
            if scenario == "before_observer":

                async def expire_before_observer(*args, **kwargs):
                    e.clock.advance(3601)
                    return await observer.request_stop(*args, **kwargs)

                turns._observer.request_stop = expire_before_observer
            if scenario == "retire_entry":
                from app.telegram_bots.work_command_stop_answers import AnswerCancelAdmission

                enter, marker = AnswerCancelAdmission.enter, owner.chat.command_stops.answers.stop_marker

                async def enter_then_retire(admission, run_id):
                    async def last_read(*args, **kwargs):
                        result = await marker(*args, **kwargs)
                        turn.active = False
                        return result

                    with monkeypatch.context() as boundary:
                        boundary.setattr(owner.chat.command_stops.answers, "stop_marker", last_read)
                        return await enter(admission, run_id)

                monkeypatch.setattr(AnswerCancelAdmission, "enter", enter_then_retire)
            e.request.enqueue("answerCallbackQuery", result=True)
            assert await e.channel.handle_update(press(e, 6504, control.id, message_id=6503))
            await e.channel._drain_ingress()
            if scenario == "retire_entry":
                async with e.db.sessions() as reader:
                    mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6502, e.link.chat_id))
                    assert not mark.route_complete and "stop_answer_dispatch" not in mark.route_basis
                assert not [path for method, path in calls if method == "POST"]
                assert manager.cancel.await_count == 1
                return
            assert not turn.stop_requested and turn.stop_authorization is None
            assert turns._current(turn)
            async with e.db.sessions() as reader:
                mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6502, e.link.chat_id))
                assert mark.route_complete
                assert "stop_answer_dispatch" not in mark.route_basis
                row = await reader.get(TelegramOutboxRow, mark.route_basis["stop_answer_receipt"]["delivery_id"])
                assert row.payload["command_stop_answer_notice"]["reason"] == "expired"
                first = await reader.get(TelegramInboundMarkRow, (e.link.id, 6501, e.link.chat_id))
                assert first.route_basis["publications"][-1]["publication_execution"].get("stop_id") is None
            await owner.recover_inputs(lambda: True, link_id=e.link.id)
            assert not [path for method, path in calls if method == "POST"]
            # The same original consent window also expires for unentered Work.
            manager.cancel.assert_not_awaited()
            return
        original = owner.chat.command_stops.answers.run

        async def interrupted(*args, **kwargs):
            raise RuntimeError("Interruption before answer admission")

        monkeypatch.setattr(owner.chat.command_stops.answers, "run", interrupted)
        e.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6504, control.id, message_id=6503))
        await e.channel._drain_ingress()
        assert manager.cancel.await_count == 1
        await turns.retire(e.channel)
        turns = ChatTurns(e.services)
        e.services.turns = turns
        owner.chat.command_stops.answers.live.clear()
        monkeypatch.setattr(owner.chat.command_stops.answers, "run", original)
        authority = PublicationAttemptAuthority(links=e.db.links, authority=e.live.authority, client=http, channel=lambda identity: e.channel if identity == e.link.id else None, create_allowed=lambda: True, read_allowed=lambda: True)
        e.services.publication_authority = authority
        cold = PublicationCoordinator(inbox=owner.host.outbox.inbox, outbox=owner.host.outbox, bus=MessageBus(), http_owner=http, capture_attempt=authority.capture)
        e.services.publications = cold
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6502, e.link.chat_id))
            incoming = InboxRow.from_row(mark)
        if scenario in {"barrier_restart", "barrier_expired", "barrier_terminal", "barrier_damage", "barrier_mismatch"}:

            async def crash_after_barrier(*args, **kwargs):
                raise RuntimeError("Interrupted after durable Stop barrier")

            with monkeypatch.context() as interrupted:
                interrupted.setattr(cold, "locate_run", crash_after_barrier)
                with pytest.raises(RuntimeError, match="after durable Stop barrier"):
                    await owner.chat.command_stops.recover(e.link, incoming, guard=owner.chat.guard(e.channel, e.link))
            async with e.db.sessions.begin() as writer:
                mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 6502, e.link.chat_id))
                assert mark.route_basis["stop_answer_admission"]["phase"] == "ready"
                assert "stop_answer_dispatch" not in mark.route_basis
                if scenario == "barrier_damage":
                    mark.route_basis = {key: value for key, value in mark.route_basis.items() if key != "stop_answer_admission"}
            if scenario == "barrier_expired":
                e.clock.advance(3601)
            elif scenario == "barrier_terminal":
                status[0] = "interrupted"
            elif scenario == "barrier_mismatch":
                attempt = await authority.capture(e.link, thread_id=captured[0].scope.thread_id, endpoint_id=captured[0].scope.endpoint_id)
                await owner.host.outbox.inbox.bind_publication_run(captured[0], run_id="different-original-run", guard=attempt.read_guard)
                status[0] = "interrupted"
            await owner.recover_inputs(lambda: True, link_id=e.link.id)
            if scenario == "barrier_damage":
                async with e.db.sessions() as reader:
                    mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6502, e.link.chat_id))
                    assert not mark.route_complete and "stop_answer_receipt" not in mark.route_basis
                status[0] = "interrupted"
        elif lost_response or scenario in {"deadline", "drift"}:
            from app.channels.execution import ChannelExecutionProtocolError

            with pytest.raises(ChannelExecutionProtocolError):
                await owner.chat.command_stops.recover(e.link, incoming, guard=owner.chat.guard(e.channel, e.link))
        else:
            await owner.chat.command_stops.recover(e.link, incoming, guard=owner.chat.guard(e.channel, e.link))
        if lost_response:
            e.clock.advance(3601)
        await owner.recover_inputs(lambda: True, link_id=e.link.id)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6502, e.link.chat_id))
            first = await reader.get(TelegramInboundMarkRow, (e.link.id, 6501, e.link.chat_id))
            if scenario in {"drift", "barrier_mismatch"}:
                assert not mark.route_complete and not first.route_complete
                assert "stop_answer_dispatch" not in mark.route_basis
            elif scenario in {"deadline", "refused", "barrier_expired", "terminal_non_cancel"}:
                assert mark.route_complete and not first.route_complete
                receipt = await reader.get(TelegramOutboxRow, mark.route_basis["stop_answer_receipt"]["delivery_id"])
                reason = "ended" if scenario == "terminal_non_cancel" else "refused" if scenario == "refused" else "expired"
                assert receipt.payload["command_stop_answer_notice"]["reason"] == reason
                assert ("stop_answer_dispatch" in mark.route_basis) == (scenario == "refused")
            else:
                assert mark.route_complete, mark.route_basis
                if scenario in {"barrier_terminal", "barrier_damage"}:
                    assert "stop_answer_dispatch" not in mark.route_basis
                else:
                    assert mark.route_basis["stop_answer_dispatch"]["run_id"] == RUN_ID
                assert first.route_complete
                receipt = await reader.get(TelegramOutboxRow, first.outbox_id)
                assert receipt.payload["turn_outcome_basis"]["proof"]["kind"] == "cancelled_stop"
                assert receipt.parts[0]["source_ref"] == {"key": "stop.answer", "values": {"written": False}}
        expected_posts = (
            [] if scenario in {"deadline", "drift", "barrier_expired", "barrier_terminal", "barrier_damage", "barrier_mismatch", "terminal_non_cancel"} else ["/original/api/threads/controller-original-thread/runs/" + RUN_ID + "/cancel"]
        )
        assert [path for method, path in calls if method == "POST"] == expected_posts
        assert manager.cancel.await_count == 1
        if retention:
            from datetime import timedelta

            from test_telegram_bot_adapter import message

            from app.telegram_bots.retention import ACT_RETENTION_DAYS, sweep_rows
            from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

            for index in range(8):
                async with e.db.sessions() as reader:
                    waiting = list(
                        await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id, TelegramOutboxRow.state.in_(("pending", "sending"))).order_by(TelegramOutboxRow.created_at, TelegramOutboxRow.id))
                    )
                if not waiting:
                    break
                message(e.request, "editMessageReplyMarkup", identity=6503)
                message(e.request, identity=6510 + index)
                e.clock.advance(5)  # Respect actual per-chat SDK pacing between receipts.
                assert await e.sender.tick() == 1, [(row.id, row.state, row.not_before, row.failed_reason) for row in waiting]
                await e.sender.drain()
                assert not e.dispatch_errors, e.dispatch_errors
            async with e.db.sessions() as reader:
                assert (await reader.get(TelegramInboundMarkRow, (e.link.id, 6502, e.link.chat_id))).state == "done"
            await sweep_rows(e.db.sessions, e.clock.now() + timedelta(days=8), callback_max_age_days=180)
            async with e.db.sessions() as reader:
                assert await reader.get(TelegramInboundMarkRow, (e.link.id, 6501, e.link.chat_id)) is not None
                assert await reader.get(TelegramInboundMarkRow, (e.link.id, 6502, e.link.chat_id)) is not None
            async with e.db.sessions.begin() as writer:
                original_work = await writer.get(AgentWorkAttemptRow, _attempt)
                original_work.status, original_work.settled_at = "cancelled", e.clock.now()
                if damage is not None:
                    from copy import deepcopy

                    first = await writer.get(TelegramInboundMarkRow, (e.link.id, 6501, e.link.chat_id))
                    answer_delivery = await writer.get(TelegramOutboxRow, first.outbox_id)
                    assert answer_delivery.state == "sent"
                    parts = deepcopy(answer_delivery.parts)
                    if damage == "ack":
                        parts[0]["message_ids"] = []
                    elif damage == "recipe":
                        parts[0]["source_ref"] = {"key": "stop.nothing", "values": {}}
                    elif damage == "protection":
                        parts[0]["protect_content"] = "false"
                    elif damage == "topic":
                        answer_delivery.message_thread_id = 12345
                    elif damage in {"root_topic", "root_input", "missing_root"}:
                        mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 6502, e.link.chat_id))
                        if damage == "root_topic":
                            mark.message_thread_id = 12345
                        elif damage == "root_input":
                            mark.sanitized_input = {"command": "help"}
                        else:
                            await writer.delete(mark)
                    answer_delivery.parts = parts
            await sweep_rows(e.db.sessions, e.clock.now() + timedelta(days=ACT_RETENTION_DAYS + 1), callback_max_age_days=180)
            async with e.db.sessions() as reader:
                assert (await reader.get(TelegramInboundMarkRow, (e.link.id, 6501, e.link.chat_id)) is None) == (damage is None)
                assert (await reader.get(TelegramInboundMarkRow, (e.link.id, 6502, e.link.chat_id)) is None) == (damage in {None, "missing_root"})
    finally:
        if observer is not None:
            await observer.drain()
        await turns.stop()
        await owner.stop()
        if cold is not None:
            await cold.stop()
        await http.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["deadline", "drift", "refused", "late_result"])
async def test_cold_actual_cancel_boundary_preserves_custody_and_original_clock(work_delivery, monkeypatch, scenario):
    await test_cold_selected_stop_only_cancels_its_actual_original_publication(work_delivery, monkeypatch, False, scenario)


@pytest.mark.asyncio
async def test_mixed_family_retains_original_answer_until_work_and_sdk_receipts_settle(work_delivery, monkeypatch):
    await test_cold_selected_stop_only_cancels_its_actual_original_publication(work_delivery, monkeypatch, False, retention=True)


@pytest.mark.asyncio
async def test_live_stop_expiry_before_native_entry_resumes_original_and_seals_feedback(work_delivery, monkeypatch):
    await test_cold_selected_stop_only_cancels_its_actual_original_publication(work_delivery, monkeypatch, False, scenario="deadline", live=True)


@pytest.mark.asyncio
async def test_expiry_before_observer_admission_resumes_original_with_no_call_proof(work_delivery, monkeypatch):
    await test_cold_selected_stop_only_cancels_its_actual_original_publication(work_delivery, monkeypatch, False, scenario="before_observer", live=True)


@pytest.mark.asyncio
async def test_live_retirement_during_last_entry_read_rolls_back_native_call_receipt(work_delivery, monkeypatch):
    await test_cold_selected_stop_only_cancels_its_actual_original_publication(work_delivery, monkeypatch, False, scenario="retire_entry", live=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["barrier_restart", "barrier_expired", "barrier_terminal", "barrier_damage", "barrier_mismatch", "terminal_non_cancel"])
async def test_cold_stop_recovers_durable_admission_and_observes_uncertain_terminal_only(work_delivery, monkeypatch, scenario):
    await test_cold_selected_stop_only_cancels_its_actual_original_publication(work_delivery, monkeypatch, False, scenario=scenario)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["ack", "recipe", "protection", "topic", "root_topic", "root_input", "missing_root"])
async def test_mixed_stop_retains_family_when_sent_answer_receipt_is_damaged(work_delivery, monkeypatch, damage):
    await test_cold_selected_stop_only_cancels_its_actual_original_publication(work_delivery, monkeypatch, False, retention=True, damage=damage)
