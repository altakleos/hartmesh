"""A real saved Start retains its owner when lifecycle changes before calling.

Default composition uses prepared directory and recorded transport adapters;
these cases establish protocol behavior rather than native containment.
"""

import asyncio
import traceback
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

from app.gateway.storage_spaces import ExecutionState
from app.telegram_bots.authority import get_local_provider
from app.telegram_bots.render import render
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.agent_instances.work_execution_contract import ActivateWork
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.contract import PrincipalRef

SAVED_CASES = [
    ("suspended", "agent.paused", None),
    ("archived", "agent.retired", None),
    ("unavailable", "unavailable.paused", None),
    ("restored", "tap.failed", None),
    ("inspect", "denied.start", None),
    ("suspended", "agent.paused", "native"),
    ("suspended", "agent.paused", "restore"),
    ("suspended", "agent.paused", "quiet"),
    ("suspended", "agent.paused", "receipt"),
    ("suspended", "agent.paused", "role"),
    ("suspended", "agent.paused", "topic"),
    ("suspended", "agent.paused", "force-reply"),
    ("suspended", "agent.paused", "reply-target"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("status,key,change", SAVED_CASES)
async def test_saved_start_reports_current_lifecycle(capture_environment, monkeypatch, tmp_path, status, key, change):
    topic = 19 if change == "topic" else None
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        human = HumanInput(AgentWork(e.app.state.agent_instances))
        work = await human.work.delegate(actor=PrincipalRef("human", e.link.owner_user_id), instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review the guide", success_criteria="Report verified gaps"))
        assert await e.channel.handle_update(Update.de_json(update_payload(6950, message_id=6950, account=PERSON, text="/work", topic=topic), e.channel.bot))
        await e.channel._drain_ingress()
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        async with e.e.sf() as reader:
            detail = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "work_details", TelegramCallbackRow.target_id == work["id"]))
        assert await e.channel.handle_update(press(e, 6951, detail.id, message_id=detail.message_id, topic=topic))
        await e.channel._drain_ingress()
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        async with e.e.sf() as reader:
            control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "work_start", TelegramCallbackRow.target_id == work["id"]))
            assert control is not None and control.payload["presentation_pending"] is False
        if change == "role":
            from deerflow.config.app_config import peek_loaded_app_config
            from deerflow.config.authorization_config import AuthorizationConfig

            configuration = peek_loaded_app_config()
            roles = {"admin": {"routes": {"allow": "*"}}, "user": {"routes": {"allow": "*", "deny": ["agents:read"]}}}
            monkeypatch.setattr(configuration, "authorization", AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": roles}}}))
            async with e.e.sf.begin() as writer:
                user = await writer.get(UserRow, e.link.owner_user_id)
                user.system_role, user.oauth_provider, user.oauth_id, user.oauth_issuer = "admin", "oidc", "saved-refusal-owner", "https://identity.example.invalid"
        actual_check, changed = e.host.acts.check, []

        async def check(identity, **kwargs):
            if identity == control.id and not changed:
                async with e.e.sf.begin() as writer:
                    journal = await writer.get(TelegramActRow, identity)
                    assert journal is not None and journal.state == "pending"
                    changed.append(journal.id)
                    if status == "inspect":
                        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
                    elif status != "unavailable":
                        (await writer.get(AgentInstanceRow, e.instance.id)).status = "suspended" if status == "restored" else status
                if status == "unavailable":

                    async def probe(instance_id=None):
                        assert instance_id == e.instance.id
                        return ExecutionState(True, "unavailable", "operator-hold")

                    async def readiness(app, instance_id=None):
                        return await probe(instance_id)

                    monkeypatch.setattr(e.app.state.agent_instances, "execution_probe", probe)
                    monkeypatch.setattr("app.gateway.storage_spaces.execution_state", readiness)
            result = await actual_check(identity, **kwargs)
            if status == "restored" and identity == control.id:
                assert result.state == "refused" and result.attempts == 0
                async with e.e.sf.begin() as writer:
                    (await writer.get(AgentInstanceRow, e.instance.id)).status = "active"
            return result

        monkeypatch.setattr(e.host.acts, "check", check)
        actual_observe, observation_errors = e.host.notifications.chat.executions.observe, []

        async def observe(*args, **kwargs):
            try:
                return await actual_observe(*args, **kwargs)
            except Exception:
                observation_errors.append(traceback.format_exc())
                raise

        monkeypatch.setattr(e.host.notifications.chat.executions, "observe", observe)
        fixed, modified, source_errors = e.host.work_saved_refusals, [], []
        actual_read, actual_authorize = fixed.read_part, fixed.authorize
        checking_fixed = []
        if change == "role":
            from app.gateway.auth.repositories.sqlite import SQLiteUserRepository

            users = SQLiteUserRepository(e.e.sf)
            actual_policy = e.host.authority.instance_send_policy

            async def policy(*args, **kwargs):
                proof = kwargs.get("projection")
                if checking_fixed and kwargs.get("expected_lifecycle") == "agent.paused" and getattr(proof, "owner", None) is fixed and proof.root_id == control.id and not modified:
                    before_snapshot = kwargs.get("before_snapshot")

                    async def limit_before_snapshot():
                        if before_snapshot is not None:
                            await before_snapshot()
                        if not modified:
                            await users.limit_role("https://identity.example.invalid", "saved-refusal-owner", "user")
                            assert (await users.get_user_by_id(e.link.owner_user_id)).system_role == "user"
                            modified.append(change)

                    kwargs["before_snapshot"] = limit_before_snapshot
                return await actual_policy(*args, **kwargs)

            monkeypatch.setattr(e.host.authority, "instance_send_policy", policy)

        async def read(captured, **kwargs):
            try:
                result = await actual_read(captured, **kwargs)
            except Exception as error:
                source_errors.append((captured.item.payload["work_saved_refusal"]["key"], error))
                raise
            if change in {"restore", "native"} and not modified:
                modified.append(change)
                async with e.e.sf.begin() as writer:
                    (await writer.get(AgentInstanceRow, e.instance.id)).status = "active"
                if change == "native":
                    actor = PrincipalRef("human", e.link.owner_user_id)
                    conversations = e.app.state.agent_conversations
                    conversation = await conversations.create(actor=actor, instance_id=e.instance.id, creation_id=uuid4().hex)
                    execution = await conversations.execution(actor=actor, thread_id=conversation["thread_id"])
                    await WorkExecution(human.work).reserve(
                        execution=execution, work_id=work["id"], request=ActivateWork(operation_id=control.id, expected_revision=work["revision"], expected_assignment_revision=work["assignment_revision"], thread_id=execution.thread_id)
                    )
                    async with e.e.sf.begin() as writer:
                        (await writer.get(AgentInstanceRow, e.instance.id)).status = status
            return result

        async def authorize(captured, **kwargs):
            if change == "quiet" and not modified:
                assert kwargs["silent"] is False
                modified.append(change)
                async with e.e.sf.begin() as writer:
                    preferences = await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)
                    minute = e.clock.now().hour * 60 + e.clock.now().minute
                    preferences.quiet_enabled = True
                    preferences.quiet_start_minute, preferences.quiet_end_minute = minute, (minute + 60) % 1440
            checking_fixed.append(True)
            try:
                return await actual_authorize(captured, **kwargs)
            finally:
                checking_fixed.clear()

        monkeypatch.setattr(fixed, "read_part", read)
        monkeypatch.setattr(fixed, "authorize", authorize)
        if change == "quiet":
            async with e.e.sf.begin() as writer:
                preferences = await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)
                preferences.quiet_enabled, preferences.timezone = False, "UTC"
        if change == "receipt":

            async def unreadable(*args, **kwargs):
                raise RuntimeError("receipt store temporarily unavailable")

            monkeypatch.setattr(e.host.acts, "reconcile_receipt", unreadable)
        before = len(e.normal.calls)
        assert await e.channel.handle_update(press(e, 6952, control.id, message_id=control.message_id, topic=topic))
        await e.channel._drain_ingress()
        if change in {"force-reply", "reply-target"}:
            async with e.e.sf.begin() as writer:
                pending = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_saved_refusal"]["act_id"].as_string() == control.id))
                assert pending is not None and pending.state == "pending"
                extra = {"force_reply": True} if change == "force-reply" else {"reply_to_message_id": control.message_id + 100}
                pending.parts = [{**part, **extra} for part in pending.parts]
                fixed_id = pending.id
            modified.append(change)
        if change == "topic":
            from app.telegram_bots.work_chat_runtime import WorkChatRuntime

            async with e.e.sf() as reader:
                pending = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_saved_refusal"]["act_id"].as_string() == control.id))
                assert pending is not None and pending.state == "pending" and pending.message_thread_id == topic
                fixed_id = pending.id
            current = e.host.notifications.chat
            e.host.notifications.chat = e.host.work_chat = WorkChatRuntime(current.runtime, current.views.reader.work)
        if change == "quiet":
            from datetime import timedelta

            async with e.e.sf.begin() as writer:
                (await writer.get(TelegramBotLinkRow, e.link.id)).last_update_at = e.clock.now() - timedelta(hours=1)
        for _ in range(3):
            e.clock.advance(1)
            await e.host.sender.tick()
            await e.host.sender.drain()
        sent = [call.parameters for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
        assert changed == [control.id]
        async with e.e.sf() as reader:
            journal = await reader.get(TelegramActRow, control.id)
            assert journal is not None and journal.work_id == work["id"]
            native = await reader.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == control.id))
            assert (native is not None) is (change == "native")
            if native is not None:
                assert native.run_id is None and native.status == "starting"
            event = await reader.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == control.id))
            assert (event is not None) is (change == "native")
            if event is not None:
                assert event.work_id == work["id"]
            saved = (journal.state, journal.reason, journal.attempts, journal.continuation)
        supervisor = await get_local_provider().get_user(e.instance.supervisor.subject_id)
        expected = render(key, "en-US", name=e.instance.name, product=e.host.product_name, custody=e.instance.custody, reader_is_administrator=False, supervisor=supervisor.email)
        if change in {"native", "restore", "receipt", "role", "force-reply", "reply-target"}:
            assert not any(expected in call["text"] for call in sent), (saved, observation_errors)
            if change != "receipt":
                assert modified == [change]
            if change in {"force-reply", "reply-target"}:
                async with e.e.sf() as reader:
                    invalid = await reader.get(TelegramOutboxRow, fixed_id)
                    assert invalid.state == "skipped", (invalid.state, invalid.skip_reason)
            if change == "role":
                await e.host.notifications.chat.executions.observe(e.link, control.id, guard=e.host.notifications.chat.guard(e.channel, e.link))
                for _ in range(8):
                    e.clock.advance(5)
                    await e.host.sender.tick()
                    await e.host.sender.drain()
                supervisor = await get_local_provider().get_user(e.instance.supervisor.subject_id)
                current = render("access.agent_off", "en-US", name=e.instance.name, supervisor=supervisor.email, product=e.host.product_name)
                delivered = any(call.endpoint == "sendMessage" and call.parameters["text"] == current for call in e.normal.calls[before:])
                if not delivered:
                    errors = [error for source_key, error in source_errors if source_key == "access.agent_off"]
                    if errors:
                        raise errors[-1]
                    async with e.e.sf() as reader:
                        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_saved_refusal"]["act_id"].as_string() == control.id)))
                        states = [(row.dedupe_key, row.state, row.attempts, row.skip_reason, row.failed_reason) for row in rows]
                    assert delivered, states
        else:
            assert any(expected in call["text"] for call in sent), (saved, observation_errors)
            if change == "quiet":
                assert modified == [change]
                assert all(call.get("disable_notification") is True for call in sent if expected in call["text"])
            if change == "topic":
                matching = [call for call in sent if expected in call["text"]]
                assert len(matching) == 1 and matching[0]["message_thread_id"] == topic
                async with e.e.sf() as reader:
                    restored = await reader.get(TelegramOutboxRow, fixed_id)
                    assert restored.state == "sent" and len(restored.parts[0]["message_ids"]) == 1
            if status == "inspect":
                assert all(work["objective"] not in call["text"] for call in sent)
        assert journal.attempts == (1 if status == "unavailable" else 0)
