"""A real foreign launch reservation remains owned after a denied Stop press."""

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
from test_telegram_callback_commands import press
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.telegram_bots.render import render
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.agent_instances.work_execution_contract import ActivateWork
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.contract import PrincipalRef

STOP_CASES = [None, "own-capability", "restore", "requester", "person"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", STOP_CASES)
async def test_stop_reports_foreign_requester_after_manage_loss(capture_environment, monkeypatch, tmp_path, change):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        human = HumanInput(AgentWork(e.app.state.agent_instances))
        work = await human.work.delegate(actor=PrincipalRef("human", e.link.owner_user_id), instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review the guide", success_criteria="Report verified gaps"))
        account = await SQLiteUserRepository(e.e.sf).create_user(User(email="other-requester@example.com", password_hash="unusable-fixture", system_role="user"))
        other = PrincipalRef("human", str(account.id))
        async with e.e.sf.begin() as writer:
            writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=other.subject_id, permissions=3))
        conversations = e.app.state.agent_conversations
        requester = PrincipalRef("human", e.link.owner_user_id) if change == "own-capability" else other
        conversation = await conversations.create(actor=requester, instance_id=e.instance.id, creation_id=uuid4().hex)
        execution = await conversations.execution(actor=requester, thread_id=conversation["thread_id"])
        operation = uuid4().hex
        original = await WorkExecution(human.work).reserve(
            execution=execution, work_id=work["id"], request=ActivateWork(operation_id=operation, expected_revision=work["revision"], expected_assignment_revision=work["assignment_revision"], thread_id=execution.thread_id)
        )
        if change == "own-capability":
            async with e.e.sf.begin() as writer:
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
        assert await e.channel.handle_update(Update.de_json(update_payload(6980, message_id=6980, account=PERSON, text="/work"), e.channel.bot))
        await e.channel._drain_ingress()
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        async with e.e.sf() as reader:
            detail = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "work_details", TelegramCallbackRow.target_id == work["id"]))
        assert await e.channel.handle_update(press(e, 6981, detail.id, message_id=detail.message_id))
        await e.channel._drain_ingress()
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        async with e.e.sf.begin() as writer:
            stop = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "stop_attempt", TelegramCallbackRow.target_id == original.attempt_id))
            assert stop is not None and stop.payload["presentation_pending"] is False
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
            revision = (await writer.get(AgentWorkRow, work["id"])).revision
        if change == "own-capability":
            from deerflow.config.app_config import peek_loaded_app_config
            from deerflow.config.authorization_config import AuthorizationConfig

            policy = {"routes": {"allow": "*", "deny": ["runs:cancel"]}}
            monkeypatch.setattr(
                peek_loaded_app_config(), "authorization", AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": policy, "user": policy}}}})
            )
        source, changed = e.host.work_callback_denials, []
        actual_read = source.read_part

        async def read(captured, **kwargs):
            result = await actual_read(captured, **kwargs)
            if change in {"restore", "requester", "person"} and not changed:
                changed.append(change)
                async with e.e.sf.begin() as writer:
                    if change == "restore":
                        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 7
                    elif change == "requester":
                        (await writer.get(AgentWorkAttemptRow, original.attempt_id)).requester_id = e.link.owner_user_id
                    else:
                        (await writer.get(UserRow, other.subject_id)).email = "current-requester@example.com"
            return result

        monkeypatch.setattr(source, "read_part", read)
        before = len(e.normal.calls)
        assert await e.channel.handle_update(press(e, 6982, stop.id, message_id=stop.message_id))
        await e.channel._drain_ingress()
        for _ in range(3):
            e.clock.advance(5)
            await e.host.sender.tick()
            await e.host.sender.drain()
        sent = [call.parameters["text"] for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
        if change in {"restore", "requester"}:
            assert changed == [change] and not any("can stop it" in text for text in sent)
        elif change == "own-capability":
            from app.telegram_bots.authority import get_local_provider

            supervisor = await get_local_provider().get_user(e.instance.supervisor.subject_id)
            assert render("denied.other", "en-US", name=e.instance.name, supervisor=str(supervisor.email)) in sent
        else:
            person = "current-requester@example.com" if change == "person" else str(account.email)
            assert render("stop.not_yours", "en-US", person=person, name=e.instance.name) in sent
            if change == "person":
                assert changed == [change]
        async with e.e.sf() as reader:
            assert await reader.get(TelegramActRow, stop.id) is None
            assert await reader.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == stop.id)) is None
            saved = await reader.get(AgentWorkAttemptRow, original.attempt_id)
            expected_requester = e.link.owner_user_id if change == "requester" else requester.subject_id
            assert (saved.requester_id, saved.status, saved.run_id, saved.activation_id) == (expected_requester, "starting", None, operation)
            assert (await reader.get(AgentWorkRow, work["id"])).revision == revision
