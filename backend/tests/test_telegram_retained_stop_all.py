"""Every selected real Stop target must own its acknowledged reply."""

import asyncio
from contextlib import suppress
from datetime import datetime
from uuid import uuid4

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_bot_test_support import update_payload
from _telegram_compose_turn_support import PERSON, _delivered, render_compose_config
from _telegram_work_worker_support import installed_worker
from sqlalchemy import select
from telegram import Update
from test_telegram_retained_management_refusals import tap
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.producer import ProductSourceRetired
from deerflow.agent_instances.contract import AgentDenied
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.agent_instances.work_execution_contract import ActivateWork
from deerflow.config.app_config import peek_loaded_app_config
from deerflow.config.authorization_config import AuthorizationConfig
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.spaces.contract import PrincipalRef


@pytest.mark.asyncio
async def test_stop_all_waits_for_each_actual_refusal_delivery(capture_environment, monkeypatch, tmp_path):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        human, actor, attempts = HumanInput(AgentWork(e.app.state.agent_instances)), PrincipalRef("human", e.link.owner_user_id), []
        for label in ("Review the guide", "Check the index"):
            work = await human.work.delegate(actor=actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective=label, success_criteria="Report verified gaps"))
            conversation = await e.app.state.agent_conversations.create(actor=actor, instance_id=e.instance.id, creation_id=uuid4().hex)
            execution = await e.app.state.agent_conversations.execution(actor=actor, thread_id=conversation["thread_id"])
            attempts.append(
                await WorkExecution(human.work).reserve(
                    execution=execution, work_id=work["id"], request=ActivateWork(operation_id=uuid4().hex, expected_revision=work["revision"], expected_assignment_revision=work["assignment_revision"], thread_id=execution.thread_id)
                )
            )
        e.clock.advance(max(1, (datetime.fromisoformat(work["created_at"]) - e.clock.now()).total_seconds() + 1))
        assert await e.channel.handle_update(Update.de_json(update_payload(7050, message_id=7050, account=PERSON, text="/stop"), e.channel.bot))
        await e.channel._drain_ingress()
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        async with e.e.sf() as reader:
            control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["choice"].as_string() == "all", TelegramCallbackRow.consumed_at.is_(None)))
            assert control is not None and control.message_id is not None
        check, recorded = e.host.acts.check, []
        policy = {"routes": {"allow": "*", "deny": ["runs:cancel"]}}
        refusal = AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": policy, "user": policy}}}})

        async def denied(identity, **kwargs):
            before = await e.host.acts._work_record(identity)
            if before.kind != "stop" or identity in [value[0] for value in recorded]:
                return await check(identity, **kwargs)
            authorization = peek_loaded_app_config().authorization
            monkeypatch.setattr(peek_loaded_app_config(), "authorization", refusal)
            try:
                result = await check(identity, **kwargs)
            finally:
                monkeypatch.setattr(peek_loaded_app_config(), "authorization", authorization)
            assert result.state == "refused" and result.reason == "ceiling" and result.attempts == 0
            recorded.append((identity, result.body))
            return result

        monkeypatch.setattr(e.host.acts, "check", denied)
        tick = e.host.sender.tick

        async def paused_sender():
            return 0

        monkeypatch.setattr(e.host.sender, "tick", paused_sender)
        await tap(e, control, 7051)
        assert len(recorded) == 2
        reconcile, hold = e.host.acts.reconcile_receipt, [True]

        async def unavailable(identity):
            if identity == recorded[1][0] and hold[0]:
                raise OSError("Recorded receipt read unavailable")
            return await reconcile(identity)

        monkeypatch.setattr(e.host.acts, "reconcile_receipt", unavailable)
        async with e.e.sf.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 7050, e.link.chat_id))
            assert mark.route_complete and mark.state == "reply_pending" and len(mark.operation_ids) == 2
            original_deliveries = list(mark.delivery_ids)
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
        plans, guard = e.host.notifications.chat.executions, e.host.notifications.chat.guard(e.channel, e.link)
        await plans.observe(e.link, recorded[0][0], guard=guard)
        with suppress(ProductSourceRetired, AgentDenied):
            await plans.observe(e.link, recorded[1][0], guard=guard)
        monkeypatch.setattr(e.host.sender, "tick", tick)
        for _ in range(4):
            e.clock.advance(5)
            await tick()
            await e.host.sender.drain()
        async with e.e.sf() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 7050, e.link.chat_id))
            assert mark.route_complete and mark.state == "reply_pending"
            assert set(original_deliveries) <= set(mark.delivery_ids)
            first = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_saved_refusal"]["act_id"].as_string() == recorded[0][0]))
            assert first is not None and first.state == "sent" and first.parts[0]["message_ids"]
            second = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_saved_refusal"]["act_id"].as_string() == recorded[1][0]))
            assert second is None
        hold[0] = False
        # The second canonical plan was claimed while its receipt was unreadable.
        # Restore current Inspect so that real owner can deliver; a fixed reply
        # must not replace its entered/ambiguous transport history.
        async with e.e.sf.begin() as writer:
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
        await plans.observe(e.link, recorded[1][0], guard=guard)
        for _ in range(4):
            e.clock.advance(5)
            await tick()
            await e.host.sender.drain()
        async with e.e.sf() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 7050, e.link.chat_id))
            assert mark.state == "done" and mark.route_complete and set(original_deliveries) <= set(mark.delivery_ids)
            canonical = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_execution_intent"]["act_id"].as_string() == recorded[1][0]))
            assert canonical.state == "sent" and canonical.parts[-1]["message_ids"]
            for identity, body in recorded:
                act = await reader.get(TelegramActRow, identity)
                assert act.state == "refused" and act.attempts == 0 and act.body == body
                assert await reader.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == identity)) is None
            for original in attempts:
                native = await reader.get(AgentWorkAttemptRow, original.attempt_id)
                assert native.status == "starting" and native.run_id is None
