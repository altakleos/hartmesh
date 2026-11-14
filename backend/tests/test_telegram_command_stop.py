"""Typed Stop owns actual command inputs and original Work targets.

Seeded attempt/run journals qualify adapter routing; native workers are CI.
"""

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_activation import seeded_attempt
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.turns import ChatTurns
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("retention", [False, True, "generation", "owner", "topic", "instance"])
async def test_default_stop_command_targets_the_only_running_assignment(work_delivery, retention):
    e = work_delivery
    owner = await installed_work_runtime(e)
    owner.host.turns = ChatTurns(owner.host)
    e.acts = owner.host.acts
    attempt_id, run, manager = await seeded_attempt(e)
    e.clock.advance(1)
    try:
        update = make_update(6301, account=e.link.chat_id, text="/stop")
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            acts = list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "stop")))
            assert len(acts) == 1
            act = acts[0]
            assert act.state == "done" and act.body["attempt_id"] == attempt_id
            assert act.stop_dispatch["run_id"] == run.run_id and act.stop_dispatch["state"] == "requested"
            assert (await reader.get(AgentWorkAttemptRow, attempt_id)).status == "stopping"
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6301, e.link.chat_id))
            assert mark.route_kind == "command_stop"
            notice = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_execution_intent"]["act_id"].as_string() == act.id))
            assert notice is not None and notice.parts
            e.identity = notice.id
        if isinstance(retention, str):
            from app.telegram_bots.work_command_stop_retention import command_stop_dependencies

            async with e.db.sessions.begin() as writer:
                notice = await writer.get(TelegramOutboxRow, e.identity)
                field, value = {"generation": ("binding_generation", e.link.binding_generation + 1), "owner": ("owner_user_id", "other-owner"), "topic": ("message_thread_id", 9876), "instance": ("instance_id", "f" * 32)}[retention]
                setattr(notice, field, value)
            async with e.db.sessions() as reader:
                dependencies = await command_stop_dependencies(reader, e.clock.now())
                assert (e.link.id, 6301, e.link.chat_id) in dependencies.inputs
                assert e.identity not in dependencies.deliveries
            return
        await dispatch(e, 6302)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, e.identity)).state == "sent"
            assert (await reader.get(TelegramInboundMarkRow, (e.link.id, 6301, e.link.chat_id))).state == "done"
        manager.cancel.assert_awaited_once_with(run.run_id)
        if retention:
            from datetime import timedelta

            from app.telegram_bots.retention import ACT_RETENTION_DAYS, INBOUND_RETENTION_DAYS, sweep_rows

            await sweep_rows(e.db.sessions, e.clock.now() + timedelta(days=INBOUND_RETENTION_DAYS + 1), callback_max_age_days=180)
            async with e.db.sessions() as reader:
                assert await reader.get(TelegramInboundMarkRow, (e.link.id, 6301, e.link.chat_id)) is not None
            async with e.db.sessions.begin() as writer:
                original = await writer.get(AgentWorkAttemptRow, attempt_id)
                original.status, original.settled_at = "cancelled", e.clock.now()
            await sweep_rows(e.db.sessions, e.clock.now() + timedelta(days=max(181, ACT_RETENTION_DAYS + 1)), callback_max_age_days=180)
            async with e.db.sessions() as reader:
                assert await reader.get(TelegramInboundMarkRow, (e.link.id, 6301, e.link.chat_id)) is None
                assert await reader.get(TelegramActRow, act.id) is None
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        manager.cancel.assert_awaited_once_with(run.run_id)
    finally:
        await owner.host.turns.stop()
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "choice",
    [
        "first",
        "all",
        "recover_all",
        "first_source_failure",
        "first_observe_failure",
        "foreign_completion",
        "late_target",
        "retention_corrupt",
        "retention_ui_damaged",
        "retention_missing",
        "expire_queued",
        "expire_sent",
        "retire_queued",
        "retire_sent",
        "retire_malformed",
        "retire_claim",
    ],
)
async def test_multiple_work_targets_require_a_real_acknowledged_choice(work_delivery, monkeypatch, choice):
    from datetime import datetime
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from deerflow.agent_instances.work_contract import DelegateWork
    from deerflow.runtime.runs.manager import CancelOutcome

    e = work_delivery
    owner = await installed_work_runtime(e)
    owner.host.turns = ChatTurns(owner.host)
    e.acts = owner.host.acts
    first_attempt, first, _manager = await seeded_attempt(e)
    second_work = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Check the second guide", success_criteria="Record verified references"))
    second = SimpleNamespace(**{**vars(e), "work": second_work})
    _attempt, second_run, _manager = await seeded_attempt(second)
    runs = {first.run_id: first, second_run.run_id: second_run}
    manager = SimpleNamespace(get=AsyncMock(side_effect=lambda identity, **kwargs: runs.get(identity)), cancel=AsyncMock(return_value=CancelOutcome.requested))
    e.http_app.state.run_manager = manager
    # The fixed list horizon must include the committed second assignment,
    # including when strict cold imports outlast the initial fixture clock.
    e.clock.advance(max(1, (datetime.fromisoformat(second_work["created_at"]) - e.clock.now()).total_seconds() + 1))
    try:
        assert await e.channel.handle_update(make_update(6311, account=e.link.chat_id, text="/stop"))
        await e.channel._drain_ingress()
        manager.cancel.assert_not_awaited()
        async with e.db.sessions() as reader:
            assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "stop")))
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6311, e.link.chat_id))
            assert mark.route_kind == "command_stop"
            card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["command_stop_intent"]["message_id"].as_integer() == 6311))
            assert card is not None and card.state == "pending" and card.parts
            e.identity = card.id
        if choice.startswith("retire_"):
            from datetime import timedelta

            from app.telegram_bots.retention import sweep_rows
            from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

            if choice == "retire_claim":
                import asyncio

                release = asyncio.Event()
                reply = e.channel.bot.request.enqueue("sendMessage", result={"message_id": 6312, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Original choice"}, release=release)
                try:
                    assert await e.sender.tick() == 1
                    await asyncio.wait_for(reply.started.wait(), 5)
                    async with e.db.sessions.begin() as writer:
                        parent = await writer.get(TelegramOutboxRow, e.identity)
                        captured = parent.claim_owner, parent.claim_id, parent.claimed_at
                        assert all(value is not None for value in captured)
                        (await writer.get(TelegramBotLinkRow, e.link.id)).binding_generation += 1
                    e.clock.advance((owner.host.config.callbacks.max_age_days + 1) * 86400)
                    await asyncio.wait_for(sweep_rows(e.db.sessions, e.clock.now(), callback_max_age_days=180), 10)
                    async with e.db.sessions() as reader:
                        parent = await reader.get(TelegramOutboxRow, e.identity)
                        assert (parent.claim_owner, parent.claim_id, parent.claimed_at) == captured
                        assert not (await reader.get(TelegramInboundMarkRow, (e.link.id, 6311, e.link.chat_id))).route_complete
                    manager.cancel.assert_not_awaited()
                finally:
                    release.set()
                    await e.sender.drain()
                return
            if choice == "retire_sent":
                await dispatch(e, 6312)
                assert not e.dispatch_errors
            async with e.db.sessions.begin() as writer:
                (await writer.get(TelegramBotLinkRow, e.link.id)).binding_generation += 1
                if choice == "retire_malformed":
                    mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 6311, e.link.chat_id))
                    mark.route_basis = {**mark.route_basis, "stop_command": {**mark.route_basis["stop_command"], "version": 99}}
            e.clock.advance((owner.host.config.callbacks.max_age_days + 1) * 86400)
            await sweep_rows(e.db.sessions, e.clock.now(), callback_max_age_days=180)
            async with e.db.sessions() as reader:
                mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6311, e.link.chat_id))
                assert mark.route_complete == (choice != "retire_malformed")
                if choice == "retire_malformed":
                    assert await reader.get(TelegramOutboxRow, e.identity) is not None
            await sweep_rows(e.db.sessions, e.clock.now() + timedelta(days=8), callback_max_age_days=180)
            async with e.db.sessions() as reader:
                mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6311, e.link.chat_id))
                assert (mark is None) == (choice != "retire_malformed")
            manager.cancel.assert_not_awaited()
            return
        if choice == "expire_queued":
            e.clock.advance((owner.host.config.callbacks.max_age_days + 1) * 86400)
            await owner.recover_inputs(lambda: True, link_id=e.link.id)
            async with e.db.sessions() as reader:
                mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6311, e.link.chat_id))
                assert mark.route_complete
                assert (await reader.get(TelegramOutboxRow, e.identity)).state == "skipped"
            manager.cancel.assert_not_awaited()
            return
        await dispatch(e, 6312)
        assert not e.dispatch_errors, e.dispatch_errors
        if choice == "expire_sent":
            from datetime import timedelta

            from test_telegram_bot_adapter import message

            from app.telegram_bots.retention import sweep_rows

            e.clock.advance((owner.host.config.callbacks.max_age_days + 1) * 86400)
            await owner.recover_inputs(lambda: True, link_id=e.link.id)
            async with e.db.sessions() as reader:
                mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6311, e.link.chat_id))
                assert mark.route_complete
                controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == e.identity)))
                assert controls and all(row.consumed_at is not None for row in controls)
                assert mark.state != "done"
            message(e.channel.bot.request, "editMessageReplyMarkup", identity=6312)
            assert await e.sender.tick() == 1
            await e.sender.drain()
            assert not e.dispatch_errors
            assert e.channel.bot.request.calls[-1].endpoint == "editMessageReplyMarkup"
            await sweep_rows(e.db.sessions, e.clock.now() + timedelta(days=8), callback_max_age_days=180)
            async with e.db.sessions() as reader:
                assert await reader.get(TelegramInboundMarkRow, (e.link.id, 6311, e.link.chat_id)) is None
            manager.cancel.assert_not_awaited()
            return
        wanted = "attempt:" + first_attempt if choice == "first" else "all"
        async with e.db.sessions() as reader:
            button = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == e.identity, TelegramCallbackRow.payload["choice"].as_string() == wanted))
            assert button is not None and button.message_id == 6312
        from test_telegram_callback_commands import press

        if choice in {"recover_all", "first_source_failure", "foreign_completion", "late_target"}:
            original = owner.chat.command_stops.run_target
            calls = []

            async def interrupted(*args, **kwargs):
                calls.append(args[3]["attempt_id"])
                if choice == "first_source_failure" and len(calls) == 1 or choice == "recover_all" and len(calls) == 2:
                    raise RuntimeError("Synthetic interruption between original targets")
                if choice == "foreign_completion" and len(calls) == 2:
                    async with e.db.sessions() as reader:
                        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6311, e.link.chat_id))
                        assert not mark.route_complete
                if choice == "late_target" and len(calls) == 2:
                    e.clock.advance(3601)
                return await original(*args, **kwargs)

            monkeypatch.setattr(owner.chat.command_stops, "run_target", interrupted)
        if choice == "first_observe_failure":
            observe = owner.chat.executions.observe
            seen = []

            async def unavailable(*args, **kwargs):
                seen.append(args[1])
                if len(seen) == 1:
                    raise RuntimeError("Synthetic observation unavailable")
                return await observe(*args, **kwargs)

            monkeypatch.setattr(owner.chat.executions, "observe", unavailable)
        if choice == "foreign_completion":
            from app.telegram_bots.work_command_stop_origins import operation_id

            async with e.db.sessions.begin() as writer:
                mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 6311, e.link.chat_id))
                basis = mark.route_basis["stop_command"]
                for target in basis["targets"]:
                    writer.add(
                        TelegramOutboxRow(
                            id=uuid4().hex,
                            link_id=e.link.id,
                            owner_user_id=e.link.owner_user_id,
                            binding_generation=e.link.binding_generation + 1,
                            chat_id=e.link.chat_id,
                            instance_id=e.instance.id,
                            class_="reply",
                            dedupe_key="notice:unrelated:" + uuid4().hex,
                            payload={"work_execution_intent": {"act_id": operation_id(basis["input"], target)}},
                            parts=[],
                            state="sent",
                            created_at=e.clock.now(),
                            not_before=e.clock.now(),
                        )
                    )
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        update = press(e, 6313, button.id, message_id=6312)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        if choice in {"recover_all", "first_source_failure", "first_observe_failure"}:
            assert manager.cancel.await_count == (2 if choice == "first_observe_failure" else 1)
            async with e.db.sessions() as reader:
                mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6311, e.link.chat_id))
                assert not mark.route_complete
            if choice == "first_observe_failure":
                monkeypatch.setattr(owner.chat.executions, "observe", observe)
            else:
                monkeypatch.setattr(owner.chat.command_stops, "run_target", original)
            await owner.recover_inputs(lambda: True, link_id=e.link.id)
            await owner.recover_inputs(lambda: True, link_id=e.link.id)
        expected = {first.run_id} if choice == "first" else set(runs)
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6311, e.link.chat_id))
            selected = await reader.get(TelegramCallbackRow, button.id)
            assert mark.route_basis["stop_command"]["selection"] is not None, (selected.consumed_at, selected.closed_reason, [call.parameters for call in e.channel.bot.request.calls if call.endpoint == "answerCallbackQuery"])
            actions = list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "stop")))
            assert len(actions) == len(expected), [(row.state, row.reason, row.stop_dispatch) for row in actions]
            if choice == "late_target":
                expired = [row for row in actions if row.reason == "stop-consent-expired"]
                assert len(expired) == 1 and expired[0].attempts == 0 and expired[0].stop_dispatch["state"] == "ended"
                assert manager.cancel.await_count == 1
                assert mark.route_complete
                return
            assert all(row.state == "done" and row.stop_dispatch["state"] == "requested" for row in actions), ([(row.state, row.reason, row.stop_dispatch) for row in actions], manager.get.await_args_list)
        assert {call.args[0] for call in manager.cancel.await_args_list} == expected
        if choice in {"retention_corrupt", "retention_missing", "retention_ui_damaged"}:
            from app.telegram_bots.work_command_stop_retention import command_stop_dependencies

            async with e.db.sessions.begin() as writer:
                mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 6311, e.link.chat_id))
                if choice == "retention_missing":
                    await writer.delete(mark)
                elif choice == "retention_ui_damaged":
                    card = await writer.get(TelegramOutboxRow, e.identity)
                    card.payload = {}
                    for identity in mark.delivery_ids:
                        delivery = await writer.get(TelegramOutboxRow, identity)
                        if delivery is not None and "work_execution_intent" in delivery.payload:
                            delivery.payload = {}
                else:
                    mark.route_basis = {**mark.route_basis, "stop_command": {**mark.route_basis["stop_command"], "version": 99}}
            async with e.db.sessions() as reader:
                dependencies = await command_stop_dependencies(reader, e.clock.now())
                assert {row.id for row in actions} <= dependencies.acts
                assert e.identity in dependencies.deliveries
                assert button.group_id in dependencies.groups
            return
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        assert manager.cancel.await_count == len(expected)
    finally:
        await owner.host.turns.stop()
        await owner.stop()


@pytest.mark.asyncio
async def test_use_only_stop_does_not_disclose_or_cancel_work(work_delivery):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    owner = await installed_work_runtime(e)
    owner.host.turns = ChatTurns(owner.host)
    e.acts = owner.host.acts
    _attempt, _run, manager = await seeded_attempt(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
    try:
        assert await e.channel.handle_update(make_update(6321, account=e.link.chat_id, text="/stop"))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "stop")))
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6321, e.link.chat_id))
            reply = await reader.get(TelegramOutboxRow, mark.delivery_ids[0])
            assert [part["source_ref"]["key"] for part in reply.parts] == ["stop.nothing"]
        manager.cancel.assert_not_awaited()
    finally:
        await owner.host.turns.stop()
        await owner.stop()
