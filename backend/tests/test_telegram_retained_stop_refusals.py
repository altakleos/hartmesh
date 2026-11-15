"""Actual admitted Stop denials keep the original native reservation intact."""

import asyncio
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
from test_telegram_retained_management_refusals import button, tap
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.telegram_bots.authority import get_local_provider
from app.telegram_bots.render import render
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.agent_instances.work_execution_contract import ActivateWork
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.spaces.contract import PrincipalRef

STOP_LOSSES = ["runs:cancel", "agents:write", "inspect", "paused", "manage", "restore"]
STOP_CASES = [("callback", loss) for loss in STOP_LOSSES] + [("command", loss) for loss in ("runs:cancel", "inspect", "paused", "sealed-inspect", "sealed-preview-inspect", "delayed-inspect")]


@pytest.mark.asyncio
@pytest.mark.parametrize("entry,loss", STOP_CASES)
async def test_saved_stop_reports_current_permission(capture_environment, monkeypatch, tmp_path, entry, loss):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        actor = PrincipalRef("human", e.link.owner_user_id)
        human = HumanInput(AgentWork(e.app.state.agent_instances))
        work = await human.work.delegate(actor=actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review the guide", success_criteria="Report verified gaps"))
        if loss == "manage":
            account = await SQLiteUserRepository(e.e.sf).create_user(User(email="other-requester@example.com", password_hash="unusable-fixture", system_role="user"))
            actor = PrincipalRef("human", str(account.id))
            async with e.e.sf.begin() as writer:
                writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=actor.subject_id, permissions=3))
        conversation = await e.app.state.agent_conversations.create(actor=actor, instance_id=e.instance.id, creation_id=uuid4().hex)
        execution = await e.app.state.agent_conversations.execution(actor=actor, thread_id=conversation["thread_id"])
        original = await WorkExecution(human.work).reserve(
            execution=execution, work_id=work["id"], request=ActivateWork(operation_id=uuid4().hex, expected_revision=work["revision"], expected_assignment_revision=work["assignment_revision"], thread_id=execution.thread_id)
        )
        assert await e.channel.handle_update(Update.de_json(update_payload(7020, message_id=7020, account=PERSON, text="/work"), e.channel.bot))
        await e.channel._drain_ingress()
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        await tap(e, await button(e, "work_details", target=work["id"]), 7021)
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        control = await button(e, "stop_attempt", target=original.attempt_id)
        check, captured = e.host.acts.check, []
        async with e.e.sf() as reader:
            revision = (await reader.get(AgentWorkRow, work["id"])).revision
            native = await reader.get(AgentWorkAttemptRow, original.attempt_id)
            native_before = (native.work_id, native.requester_id, native.thread_id, native.run_id, native.status, native.activation_id)

        async def current(identity, **kwargs):
            restore_authorization = None
            if (entry == "command" or identity == control.id) and not captured:
                async with e.e.sf.begin() as writer:
                    act = await writer.get(TelegramActRow, identity)
                    assert act.kind == "stop" and act.state == "pending" and act.attempts == 0 and act.body["attempt_id"] == original.attempt_id
                    captured.append((act.id, act.body))
                    if loss == "paused":
                        (await writer.get(AgentInstanceRow, e.instance.id)).status = "suspended"
                    elif loss in {"inspect", "delayed-inspect", "manage"}:
                        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1 if loss != "manage" else 3
                capability = "runs:cancel" if loss in {"sealed-inspect", "sealed-preview-inspect", "restore"} else loss
                if ":" in capability:
                    from deerflow.config.app_config import peek_loaded_app_config
                    from deerflow.config.authorization_config import AuthorizationConfig

                    policy = {"routes": {"allow": "*", "deny": [capability]}}
                    restore_authorization = peek_loaded_app_config().authorization
                    monkeypatch.setattr(
                        peek_loaded_app_config(),
                        "authorization",
                        AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": policy, "user": policy}}}}),
                    )
            result = await check(identity, **kwargs)
            if restore_authorization is not None and loss in {"restore", "sealed-preview-inspect"}:
                assert result.state == "refused" and result.reason == "ceiling" and result.attempts == 0
                monkeypatch.setattr(peek_loaded_app_config(), "authorization", restore_authorization)
            return result

        monkeypatch.setattr(e.host.acts, "check", current)
        if loss in {"sealed-inspect", "sealed-preview-inspect", "delayed-inspect"}:
            tick = e.host.sender.tick

            async def paused_sender():
                return 0

            monkeypatch.setattr(e.host.sender, "tick", paused_sender)
        before = len(e.normal.calls)
        if entry == "command":
            assert await e.channel.handle_update(Update.de_json(update_payload(7022, message_id=7022, account=PERSON, text="/stop"), e.channel.bot))
            await e.channel._drain_ingress()
        else:
            await tap(e, control, 7022)
        if loss in {"sealed-inspect", "sealed-preview-inspect"}:
            async with e.e.sf.begin() as writer:
                incoming = await writer.get(TelegramInboundMarkRow, (e.link.id, 7022, e.link.chat_id))
                assert incoming.route_complete and incoming.state == "reply_pending"
                canonical = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_execution_intent"]["act_id"].as_string() == captured[0][0]))
                assert canonical.state == "pending" and canonical.parts and canonical.id in incoming.delivery_ids
                assert all(not part.get("message_ids") and not part.get("dispatched_at") for part in canonical.parts)
                if loss == "sealed-preview-inspect":
                    assert canonical.parts[-1].get("callback_group_id") is not None
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
            await e.host.notifications.chat.executions.observe(e.link, captured[0][0], guard=e.host.notifications.chat.guard(e.channel, e.link))
            monkeypatch.setattr(e.host.sender, "tick", tick)
        if loss == "delayed-inspect":
            async with e.e.sf() as reader:
                act = await reader.get(TelegramActRow, captured[0][0])
                original_deadline = (act.created_at, act.first_attempt_at, act.attempts)
                fixed = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_saved_refusal"]["act_id"].as_string() == act.id))
                assert fixed.state == "pending" and all(not part.get("message_ids") and not part.get("dispatched_at") for part in fixed.parts)
            e.clock.advance(3601)
            monkeypatch.setattr(e.host.sender, "tick", tick)
        for _ in range(4):
            e.clock.advance(5)
            await e.host.sender.tick()
            await e.host.sender.drain()
        key = (
            "agent.paused"
            if loss == "paused"
            else "work.need_inspect"
            if loss in {"inspect", "sealed-inspect", "sealed-preview-inspect", "delayed-inspect"}
            else "stop.not_yours"
            if loss == "manage"
            else "tap.failed"
            if loss == "restore"
            else "denied.other"
        )
        supervisor = await get_local_provider().get_user(e.instance.supervisor.subject_id)
        expected = render(key, "en-US", name=e.instance.name, product=e.host.product_name, person="other-requester@example.com", supervisor=str(supervisor.email))
        sent = [call.parameters["text"] for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
        async with e.e.sf() as reader:
            assert len(captured) == 1
            identity, body = captured[0]
            act = await reader.get(TelegramActRow, identity)
            native = await reader.get(AgentWorkAttemptRow, original.attempt_id)
            assert act is not None and act.state == "refused" and act.body == body
            assert (native.work_id, native.requester_id, native.thread_id, native.run_id, native.status, native.activation_id) == native_before
            assert await reader.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == identity)) is None
            assert (await reader.get(AgentWorkRow, work["id"])).revision == revision
            if loss == "delayed-inspect":
                assert (act.created_at, act.first_attempt_at, act.attempts) == original_deadline
            if loss == "restore":
                renewed = await button(e, "stop_attempt", target=original.attempt_id)
                assert renewed.id != control.id and renewed.message_id is not None
            if entry == "command":
                incoming = await reader.get(TelegramInboundMarkRow, (e.link.id, 7022, e.link.chat_id))
                deliveries = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.id.in_(incoming.delivery_ids))))
                assert incoming.state == "done" and incoming.route_complete and incoming.delivery_ids, (incoming.route_complete, [(row.id, row.state, row.payload, row.parts) for row in deliveries], sent)
        matching = [text for text in sent if text == expected or loss == "restore" and text.startswith(expected + "\n\n")]
        assert len(matching) == 1, (sent, act.reason, act.attempts)
        if loss == "restore":
            assert "Review the guide" in matching[0] and "Report verified gaps" in matching[0]
            assert render("denied.other", "en-US", supervisor=str(supervisor.email)) not in sent
        assert render("stop.failed", "en-US") not in sent, sent
