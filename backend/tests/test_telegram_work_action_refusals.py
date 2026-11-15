"""Actual default callback owners tell current fixed refusal facts.

The prepared directory and transport adapters prove channel protocol behavior,
not native containment. No agent run is needed to refuse a paused Start.
"""

import asyncio
from uuid import uuid4

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_bot_test_support import update_payload
from _telegram_compose_turn_support import PERSON, _delivered, render_compose_config
from _telegram_work_worker_support import installed_worker
from sqlalchemy import func, select
from telegram import Update
from test_telegram_callback_commands import press
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.render import render
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.agent_instances.work_execution_contract import ActivateWork
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow
from deerflow.spaces.contract import PrincipalRef


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,permissions,key,entry",
    [
        ("suspended", 7, "agent.paused", "work_start"),
        ("archived", 7, "agent.retired", "work_start"),
        ("suspended", 2, "access.agent_off", "work_start"),
        ("owned", 7, None, "work_start"),
        ("suspended", 7, "agent.paused", "assign_save"),
        ("suspended", 7, "agent.paused", "assign_start"),
    ],
)
async def test_actual_start_press_tells_current_lifecycle_without_admission(capture_environment, monkeypatch, tmp_path, status, permissions, key, entry):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        human = HumanInput(AgentWork(e.app.state.agent_instances))
        work = await human.work.delegate(actor=PrincipalRef("human", e.link.owner_user_id), instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review the guide", success_criteria="Report verified gaps"))
        update = Update.de_json(update_payload(6700, message_id=6700, account=PERSON, text="/work"), e.channel.bot)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        async with e.e.sf() as reader:
            detail = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "work_details", TelegramCallbackRow.target_id == work["id"]))
            assert detail is not None and detail.payload["presentation_pending"] is False
        assert await e.channel.handle_update(press(e, 6701, detail.id, message_id=detail.message_id))
        await e.channel._drain_ingress()
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        async with e.e.sf() as reader:
            control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "work_start", TelegramCallbackRow.target_id == work["id"]))
            assert control is not None and control.payload["presentation_pending"] is False
            conversations = await reader.scalar(select(func.count()).select_from(AgentConversationRow))
        if entry != "work_start":
            from app.telegram_bots.assign import DraftAssignment
            from app.telegram_bots.assignment_drafts import AssignmentDrafts

            incoming = Update.de_json(update_payload(6703, message_id=6703, account=PERSON, text="/assign Review the guide"), e.channel.bot)
            origin, _created = await e.host.outbox.inbox.accept(e.link, incoming, sanitized_input={"command": "assign", "text": "Review the guide"})
            draft = await AssignmentDrafts(e.host.acts).create(e.link, origin, draft=DraftAssignment(objective="Review the guide", success_criteria="Report verified gaps"), source="command")
            await e.host.notifications.chat.assignments.plan(e.link, draft.id, guard=e.host.notifications.chat.guard(e.channel, e.link))
            await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
            async with e.e.sf() as reader:
                control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == entry, TelegramCallbackRow.target_id == draft.id))
                assert control is not None and control.payload["presentation_pending"] is False
        if status == "owned":
            conversations_api = e.app.state.agent_conversations
            actor = PrincipalRef("human", e.link.owner_user_id)
            conversation = await conversations_api.create(actor=actor, instance_id=e.instance.id, creation_id=uuid4().hex)
            execution = await conversations_api.execution(actor=actor, thread_id=conversation["thread_id"])
            promise = await WorkExecution(human.work).reserve(
                execution=execution, work_id=work["id"], request=ActivateWork(operation_id=control.id, expected_revision=work["revision"], expected_assignment_revision=work["assignment_revision"], thread_id=execution.thread_id)
            )
            async with e.e.sf.begin() as writer:
                (await writer.get(AgentInstanceRow, e.instance.id)).status = "suspended"
            from app.telegram_bots.callbacks import _row

            assert await e.host.notifications.chat._unadmitted_lifecycle(e.link, _row(control)) is False
            async with e.e.sf() as reader:
                saved = await reader.get(AgentWorkAttemptRow, promise.attempt_id)
                assert saved.activation_id == control.id and saved.run_id is None
            return
        async with e.e.sf.begin() as writer:
            (await writer.get(AgentInstanceRow, e.instance.id)).status = status
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = permissions
        before = len(e.normal.calls)
        pressed = press(e, 6702, control.id, message_id=control.message_id)
        assert await e.channel.handle_update(pressed)
        await e.channel._drain_ingress()
        sent = [call.parameters for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
        from app.telegram_bots.authority import get_local_provider

        supervisor = await get_local_provider().get_user(e.instance.supervisor.subject_id)
        assert sent and sent[-1]["text"] == render(key, "en-US", name=e.instance.name, product=e.host.product_name, supervisor=supervisor.email)
        async with e.e.sf() as reader:
            assert await reader.get(TelegramActRow, control.id) is None
            assert await reader.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == control.id)) is None
            assert await reader.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == control.id)) is None
            assert await reader.scalar(select(func.count()).select_from(AgentConversationRow)) == conversations
        assert not e.http_errors and not e.held_errors
