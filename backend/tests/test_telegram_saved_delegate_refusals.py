"""A genuinely admitted, unattempted Save can explain current lifecycle loss."""

import asyncio

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_bot_test_support import update_payload
from _telegram_compose_turn_support import PERSON, _delivered, render_compose_config
from _telegram_work_worker_support import installed_worker
from sqlalchemy import select
from telegram import Update
from test_telegram_callback_commands import press
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.assignment_drafts import AssignmentDrafts
from app.telegram_bots.authority import get_local_provider
from app.telegram_bots.render import render
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow

DELEGATE_CASES = [(kind, change) for kind in ("assign_save", "assign_start") for change in ("pause", "inspect", "capability")]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,change", DELEGATE_CASES)
async def test_saved_delegate_keeps_original_owner_on_permission_loss(capture_environment, monkeypatch, tmp_path, kind, change):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        incoming = Update.de_json(update_payload(6990, message_id=6990, account=PERSON, text="/assign Review the guide"), e.channel.bot)
        origin, _created = await e.host.outbox.inbox.accept(e.link, incoming, sanitized_input={"command": "assign", "text": "Review the guide"})
        root = await AssignmentDrafts(e.host.acts).create(e.link, origin, draft=DraftAssignment(objective="Review the guide", success_criteria="Report verified gaps"), source="command")
        await e.host.notifications.chat.assignments.plan(e.link, root.id, guard=e.host.notifications.chat.guard(e.channel, e.link))
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        async with e.e.sf() as reader:
            control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == kind, TelegramCallbackRow.target_id == root.id))
            assert control is not None and control.payload["presentation_pending"] is False
        actual_check, recorded = e.host.acts.check, []

        async def check(identity, **kwargs):
            if identity == control.id and not recorded:
                async with e.e.sf.begin() as writer:
                    act = await writer.get(TelegramActRow, identity)
                    draft = await writer.get(TelegramActRow, root.id)
                    assert act.kind == "delegate" and act.state == "pending" and act.attempts == 0 and draft.operation_ids == [identity]
                    members = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == control.group_id)))
                    assert members and all(member.payload.get("work_action_id") == identity for member in members)
                    recorded.append(act.body)
                    if change == "pause":
                        (await writer.get(AgentInstanceRow, e.instance.id)).status = "suspended"
                    elif change == "inspect":
                        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
                if change == "capability":
                    from deerflow.config.app_config import peek_loaded_app_config
                    from deerflow.config.authorization_config import AuthorizationConfig

                    policy = {"routes": {"allow": "*", "deny": ["agents:write"]}}
                    monkeypatch.setattr(
                        peek_loaded_app_config(),
                        "authorization",
                        AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": policy, "user": policy}}}}),
                    )
            return await actual_check(identity, **kwargs)

        monkeypatch.setattr(e.host.acts, "check", check)
        before = len(e.normal.calls)
        assert await e.channel.handle_update(press(e, 6991, control.id, message_id=control.message_id))
        await e.channel._drain_ingress()
        for _ in range(4):
            e.clock.advance(5)
            await e.host.sender.tick()
            await e.host.sender.drain()
        supervisor = await get_local_provider().get_user(e.instance.supervisor.subject_id)
        key = "agent.paused" if change == "pause" else "assign.no_access" if change == "inspect" else "denied.other"
        expected = render(key, "en-US", name=e.instance.name, supervisor=str(supervisor.email), product=e.host.product_name)
        sent = [call.parameters["text"] for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
        async with e.e.sf() as reader:
            saved = await reader.get(TelegramActRow, control.id)
            assert saved.state == "refused" and saved.attempts == 0 and saved.body == recorded[0]
            assert (await reader.get(TelegramActRow, root.id)).operation_ids == [saved.id]
            assert await reader.scalar(select(AgentWorkRow.id).where(AgentWorkRow.instance_id == e.instance.id)) is None
            assert await reader.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == saved.id)) is None
            assert await reader.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == saved.id)) is None
            assert await reader.scalar(select(AgentConversationRow.thread_id).where(AgentConversationRow.instance_id == e.instance.id)) is None
        assert expected in sent, (saved.reason, saved.attempts, sent)
