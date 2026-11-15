"""Actual saved delegation owners veto fixed replies across final SDK reads."""

import asyncio
from datetime import timedelta
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
from test_telegram_callback_commands import press
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.assignment_drafts import AssignmentDrafts
from app.telegram_bots.authority import get_local_provider
from app.telegram_bots.render import render
from app.telegram_bots.work_actions import request_for, thaw
from app.telegram_bots.work_continuation_state import activation_child_id
from app.telegram_bots.work_delegate_actions import no_due_id
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.agent_instances.work_execution_contract import ActivateWork
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.spaces.contract import PrincipalRef

OWNERSHIP_CASES = ["restore", "counter", "ack", "native", "family-initial", "family-final", "child-target", "child-native"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", OWNERSHIP_CASES)
async def test_saved_delegate_keeps_late_owners(capture_environment, monkeypatch, tmp_path, change):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        kind = "assign_start" if change in {"child-target", "child-native"} else "assign_save"
        incoming = Update.de_json(update_payload(6992, message_id=6992, account=PERSON, text="/assign Review the guide"), e.channel.bot)
        origin, _created = await e.host.outbox.inbox.accept(e.link, incoming, sanitized_input={"command": "assign", "text": "Review the guide"})
        due = {"due_at": e.clock.now() + timedelta(days=2), "due_text": "Within two days"} if change.startswith("family-") else {}
        root = await AssignmentDrafts(e.host.acts).create(e.link, origin, draft=DraftAssignment(objective="Review the guide", success_criteria="Report verified gaps", **due), source="command")
        await e.host.notifications.chat.assignments.plan(e.link, root.id, guard=e.host.notifications.chat.guard(e.channel, e.link))
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        async with e.e.sf() as reader:
            control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == kind, TelegramCallbackRow.target_id == root.id))
            assert control is not None and control.payload["presentation_pending"] is False
        actual_check, recorded, changed = e.host.acts.check, [], []

        async def preserve_family():
            # First create the no-due child and its canonical receipt through
            # the actual current permission checks, not seeded journal rows.
            async with e.e.sf.begin() as writer:
                (await writer.get(AgentInstanceRow, e.instance.id)).status = "active"
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
            saved = await actual_check(control.id)
            assert saved.state == "done" and saved.result["delegate_child_id"] == no_due_id(control.id)
            async with e.e.sf.begin() as writer:
                child = await writer.get(TelegramActRow, no_due_id(control.id))
                event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == child.id))
                assert child.state == "done" and event is not None
                parent = await writer.get(TelegramActRow, control.id)
                # Damage only the forward journal evidence. The actual child
                # and native receipt remain and must veto the no-effect claim.
                parent.state, parent.reason, parent.attempts = "refused", "denied", 0
                parent.operation_ids, parent.result = [], None
                (await writer.get(AgentInstanceRow, e.instance.id)).status = "suspended"

        async def check(identity, **kwargs):
            if identity == control.id and not recorded:
                async with e.e.sf.begin() as writer:
                    act = await writer.get(TelegramActRow, identity)
                    assert act.state == "pending" and act.attempts == 0
                    recorded.append(thaw(act.body))
                    (await writer.get(AgentInstanceRow, e.instance.id)).status = "suspended"
            result = await actual_check(identity, **kwargs)
            if change == "family-initial" and not changed:
                changed.append(change)
                await preserve_family()
            return result

        fixed, read = e.host.work_saved_refusals, e.host.work_saved_refusals.read_part

        async def late(captured, **kwargs):
            result = await read(captured, **kwargs)
            if not changed:
                changed.append(change)
                if change == "family-final":
                    await preserve_family()
                elif change in {"native", "child-native"}:
                    async with e.e.sf.begin() as writer:
                        (await writer.get(AgentInstanceRow, e.instance.id)).status = "active"
                    human = HumanInput(AgentWork(e.app.state.agent_instances))
                    actor = PrincipalRef("human", e.link.owner_user_id)
                    if change == "native":
                        await human.work.delegate(actor=actor, instance_id=e.instance.id, request=request_for("delegate", recorded[0], identity=control.id))
                    else:
                        work = await human.work.delegate(actor=actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review retained ownership", success_criteria="Preserve the launch promise"))
                        conversations = e.app.state.agent_conversations
                        conversation = await conversations.create(actor=actor, instance_id=e.instance.id, creation_id=uuid4().hex)
                        execution = await conversations.execution(actor=actor, thread_id=conversation["thread_id"])
                        await WorkExecution(human.work).reserve(
                            execution=execution,
                            work_id=work["id"],
                            request=ActivateWork(operation_id=activation_child_id(control.id), expected_revision=work["revision"], expected_assignment_revision=work["assignment_revision"], thread_id=execution.thread_id),
                        )
                    async with e.e.sf.begin() as writer:
                        (await writer.get(AgentInstanceRow, e.instance.id)).status = "suspended"
                else:
                    async with e.e.sf.begin() as writer:
                        act = await writer.get(TelegramActRow, control.id)
                        if change == "restore":
                            (await writer.get(AgentInstanceRow, e.instance.id)).status = "active"
                        elif change == "counter":
                            act.attempts = 1
                        elif change == "ack":
                            parent = await writer.get(TelegramOutboxRow, control.payload["anchor_outbox_id"])
                            parent.parts = [{**part, "message_ids": [control.message_id + 100]} for part in parent.parts]
                        elif change == "child-target":
                            data = thaw(act.continuation)
                            data["activation_continuation"].update(phase="ready", work_id="changed-work", assignment_revision=1)
                            act.continuation = data
            return result

        monkeypatch.setattr(e.host.acts, "check", check)
        monkeypatch.setattr(fixed, "read_part", late)
        before = len(e.normal.calls)
        assert await e.channel.handle_update(press(e, 6993, control.id, message_id=control.message_id))
        await e.channel._drain_ingress()
        for _ in range(4):
            e.clock.advance(5)
            await e.host.sender.tick()
            await e.host.sender.drain()
        assert changed == [change]
        supervisor = await get_local_provider().get_user(e.instance.supervisor.subject_id)
        expected = render("agent.paused", "en-US", name=e.instance.name, supervisor=str(supervisor.email), product=e.host.product_name)
        sent = [call.parameters["text"] for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
        assert expected not in sent, sent
        async with e.e.sf() as reader:
            draft = await reader.get(TelegramActRow, root.id)
            parent = await reader.get(TelegramActRow, control.id)
            assert draft.operation_ids == [control.id] and parent.body == recorded[0]
            operation = no_due_id(control.id) if change.startswith("family-") else activation_child_id(control.id) if change == "child-native" else control.id
            event = await reader.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == operation))
            assert (event is not None) is (change in {"native", "child-native"} or change.startswith("family-"))
            if change == "child-native":
                assert await reader.get(TelegramActRow, operation) is None
                native = await reader.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == operation))
                assert native is not None and native.status == "starting" and native.run_id is None
            if change.startswith("family-"):
                assert (await reader.get(TelegramActRow, operation)).state == "done"
