"""Retained management previews explain current authority without another command."""

import asyncio
import traceback
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
from app.telegram_bots.render import render
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.spaces.contract import PrincipalRef


async def button(e, kind, *, target=None):
    async with e.e.sf() as reader:
        query = select(TelegramCallbackRow).where(TelegramCallbackRow.kind == kind, TelegramCallbackRow.consumed_at.is_(None), TelegramCallbackRow.payload["presentation_pending"].as_boolean() == False)  # noqa: E712
        if target is not None:
            query = query.where(TelegramCallbackRow.target_id == target)
        control = await reader.scalar(query.order_by(TelegramCallbackRow.created_at.desc()))
        assert control is not None, kind
        return control


async def tap(e, control, identity):
    assert await e.channel.handle_update(press(e, identity, control.id, message_id=control.message_id))
    await e.channel._drain_ingress()


async def management_preview(e, kind, monkeypatch, *, reserve=False, prompt_first=False):
    # Inbox admission and prompt delivery must share the fixture clock. A
    # wall-clock input predating the advanced SDK dispatch is correctly held.
    monkeypatch.setattr("app.telegram_bots.inbox._now", e.clock.now)
    observation_errors, observe = [], e.host.notifications.chat.management.observe

    async def trace_observe(*args, **kwargs):
        try:
            return await observe(*args, **kwargs)
        except Exception:
            observation_errors.append(traceback.format_exc())
            raise

    monkeypatch.setattr(e.host.notifications.chat.management, "observe", trace_observe)

    async def delivered():
        try:
            await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        except TimeoutError:
            async with e.e.sf() as reader:
                pending = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.state.in_(("pending", "sending")))))
                print("EDIT_PENDING", [(row.id, row.state, row.payload, row.parts) for row in pending])
            print("EDIT_OBSERVATION_ERRORS", "\n".join(observation_errors[-3:]))
            raise

    if kind == "edit":
        request = e.normal.do_request

        async def sdk(url, method, request_data=None, **kwargs):
            if url.rsplit("/", 1)[-1] == "editMessageText" and not e.normal.replies["editMessageText"]:
                parameters = request_data.parameters
                e.normal.enqueue("editMessageText", result={"message_id": parameters["message_id"], "date": 1_893_456_000, "chat": {"id": parameters["chat_id"], "type": "private"}, "text": parameters["text"]})
            return await request(url, method, request_data, **kwargs)

        monkeypatch.setattr(e.normal, "do_request", sdk)
    human = HumanInput(AgentWork(e.app.state.agent_instances))
    work = await human.work.delegate(actor=PrincipalRef("human", e.link.owner_user_id), instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review the guide", success_criteria="Report verified gaps"))
    if reserve:
        from deerflow.agent_instances.work_execution import WorkExecution
        from deerflow.agent_instances.work_execution_contract import ActivateWork

        actor = PrincipalRef("human", e.link.owner_user_id)
        conversation = await e.app.state.agent_conversations.create(actor=actor, instance_id=e.instance.id, creation_id=uuid4().hex)
        execution = await e.app.state.agent_conversations.execution(actor=actor, thread_id=conversation["thread_id"])
        e.native_original = await WorkExecution(human.work).reserve(
            execution=execution, work_id=work["id"], request=ActivateWork(operation_id=uuid4().hex, expected_revision=work["revision"], expected_assignment_revision=work["assignment_revision"], thread_id=execution.thread_id)
        )
        work = await human.work.get(actor=actor, instance_id=e.instance.id, work_id=work["id"])
    assert await e.channel.handle_update(Update.de_json(update_payload(7010, message_id=7010, account=PERSON, text="/work"), e.channel.bot))
    await e.channel._drain_ingress()
    await delivered()
    await tap(e, await button(e, "work_details", target=work["id"]), 7011)
    await delivered()
    await tap(e, await button(e, "work_change" if kind == "edit" else "work_withdraw", target=work["id"]), 7012)
    await delivered()
    if kind == "edit":
        extracted_words = []

        async def extracted(words, **kwargs):
            assert words == "Include the additional chapter"
            extracted_words.append(words)
            return DraftAssignment(objective="Review the expanded guide", success_criteria="Report verified corrections")

        monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", extracted)
        if prompt_first:
            tick, paused = e.host.sender.tick, [True]

            async def scheduled():
                return 0 if paused[0] else await tick()

            monkeypatch.setattr(e.host.sender, "tick", scheduled)
        await tap(e, await button(e, "assign_edit"), 7013)
        if prompt_first:
            async with e.e.sf.begin() as writer:
                prompt = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["management_prompt_intent"]["purpose"].as_string() == "work_edit"))
                cleanup = await writer.get(TelegramOutboxRow, prompt.payload["management_prompt_cleanup_id"])
                prompt.created_at = cleanup.created_at - timedelta(microseconds=1)
                cleanup_id = cleanup.id
            assert await tick() == 1
            async with e.e.sf() as reader:
                sending = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id, TelegramOutboxRow.state == "sending"))
                assert sending.id == cleanup_id
            await e.host.sender.drain()
            paused[0] = False
        await delivered()
        async with e.e.sf() as reader:
            prompt = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["management_prompt_intent"]["purpose"].as_string() == "work_edit"))
            assert prompt is not None and prompt.state == "sent"
            prompt_id = prompt.parts[-1]["message_ids"][-1]
        incoming = update_payload(7014, message_id=7014, account=PERSON, text="Include the additional chapter")
        incoming["message"]["reply_to_message"] = {"message_id": prompt_id, "date": 1_893_456_000, "chat": incoming["message"]["chat"], "from": {"id": e.link.bot_id, "is_bot": True, "first_name": "Work bot"}, "text": "Describe the changes"}
        assert await e.channel.handle_update(Update.de_json(incoming, e.channel.bot))
        await e.channel._drain_ingress()
        try:
            await delivered()
        except TimeoutError:
            async with e.e.sf() as reader:
                pending = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.state.in_(("pending", "sending")))))
                print("EDIT_PENDING", [(row.id, row.state, row.payload, row.parts) for row in pending])
            print("EDIT_OBSERVATION_ERRORS", "\n".join(observation_errors[-3:]))
            raise
        async with e.e.sf() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 7014, e.link.chat_id))
            parents = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_management_intent"]["kind"].as_string() == "edit")))
            diagnostics = (extracted_words, (mark.state, mark.route_kind, mark.route_target_id, mark.route_basis) if mark is not None else None, [(row.state, row.payload) for row in parents])
        assert extracted_words == ["Include the additional chapter"], diagnostics
    return work, await button(e, "work_change_save" if kind == "edit" else "work_withdraw_confirm")


@pytest.mark.asyncio
async def test_management_prompt_dependency_precedes_delivery(capture_environment, monkeypatch, tmp_path):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        work, control = await management_preview(e, "edit", monkeypatch, prompt_first=True)
        assert control.kind == "work_change_save"
        async with e.e.sf() as reader:
            assert (await reader.get(AgentWorkRow, work["id"])).revision == work["revision"]
            assert await reader.get(TelegramActRow, control.id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["edit", "cancel"])
@pytest.mark.parametrize("loss", ["manage", "inspect", "paused", "capability"])
@pytest.mark.parametrize("admitted", [True, False])
async def test_retained_management_reports_current_denial(capture_environment, monkeypatch, tmp_path, kind, loss, admitted):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        work, control = await management_preview(e, kind, monkeypatch)
        saved, actual_check = [], e.host.acts.check
        failures, offer = [], e.host.work_saved_refusals.offer

        async def observed_offer(*args, **kwargs):
            try:
                return await offer(*args, **kwargs)
            except Exception:
                failures.append(traceback.format_exc())
                raise

        monkeypatch.setattr(e.host.work_saved_refusals, "offer", observed_offer)

        async def revoke():
            async with e.e.sf.begin() as writer:
                if loss == "paused":
                    (await writer.get(AgentInstanceRow, e.instance.id)).status = "suspended"
                else:
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3 if loss == "manage" else 1 if loss == "inspect" else 7
            if loss == "capability":
                from deerflow.config.app_config import peek_loaded_app_config
                from deerflow.config.authorization_config import AuthorizationConfig

                policy = {"routes": {"allow": "*", "deny": ["agents:write"]}}
                monkeypatch.setattr(
                    peek_loaded_app_config(),
                    "authorization",
                    AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": policy, "user": policy}}}}),
                )

        async def check(identity, **kwargs):
            if identity == control.id and not saved:
                async with e.e.sf() as reader:
                    act = await reader.get(TelegramActRow, identity)
                    assert act.kind == kind and act.state == "pending" and act.attempts == 0
                    saved.append(act.body)
                await revoke()
            return await actual_check(identity, **kwargs)

        if admitted:
            monkeypatch.setattr(e.host.acts, "check", check)
        else:
            await revoke()
        before = len(e.normal.calls)
        await tap(e, control, 7015)
        for _ in range(4):
            e.clock.advance(5)
            await e.host.sender.tick()
            await e.host.sender.drain()
        sent = [call.parameters["text"] for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
        key = "agent.paused" if loss == "paused" else "denied.change" if kind == "edit" else "denied.withdraw"
        expected = render(key, "en-US", name=e.instance.name, product=e.host.product_name, supervisor="manager@example.invalid")
        async with e.e.sf() as reader:
            current = await reader.get(AgentWorkRow, work["id"])
            act = await reader.get(TelegramActRow, control.id)
            assert current.revision == work["revision"] and current.status == work["status"]
            assert await reader.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == control.id)) is None
            if admitted:
                assert act is not None and act.state == "refused" and act.body == saved[0], (sent, failures, control.payload)
            else:
                assert act is None
        assert expected in sent, (sent, (act.state, act.reason, act.attempts) if act is not None else None, failures)
