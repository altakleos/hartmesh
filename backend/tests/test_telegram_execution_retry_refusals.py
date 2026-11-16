"""A real unknown Start/Stop retains its Native promise when new Retry is denied."""

import asyncio
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
from test_telegram_retained_management_refusals import button, tap
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.acts import ActsWorker
from app.telegram_bots.authority import get_local_provider
from app.telegram_bots.producer import DeliveryClaimRetired, ProductSourceRetired
from app.telegram_bots.render import render
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.agent_instances.work_execution_contract import ActivateWork
from deerflow.config.app_config import peek_loaded_app_config
from deerflow.config.authorization_config import AuthorizationConfig
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow
from deerflow.spaces.contract import PrincipalRef


async def deliver_owned(e, identity):
    while True:
        e.clock.advance(1)
        await e.host.sender.tick()
        await e.host.sender.drain()
        async with e.e.sf() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row is not None and row.state not in {"skipped", "failed"}, (row.state, row.skip_reason, row.failed_reason) if row else None
            current = [part for part in row.parts if part.get("presentation_version", 0) == row.payload.get("presentation_version", 0)]
            if row.state == "sent" and current and all(part["state"] == "sent" and part["message_ids"] for part in current):
                return
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["activate", "stop"])
@pytest.mark.parametrize("change", ["inspect", "capability"])
async def test_current_execution_retry_preserves_actual_native(capture_environment, monkeypatch, tmp_path, kind, change, *, context=False, foreign=False, carrier=False, known_stop=False, contend=False):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        monkeypatch.setattr("app.telegram_bots.inbox._now", e.clock.now)
        sdk = e.normal.do_request

        async def recorded_sdk(url, method, request_data=None, **kwargs):
            if url.rsplit("/", 1)[-1] == "editMessageText" and not e.normal.replies["editMessageText"]:
                values = request_data.parameters
                e.normal.enqueue("editMessageText", result={"message_id": values["message_id"], "date": 1_893_456_000, "chat": {"id": values["chat_id"], "type": "private"}, "text": values["text"]})
            return await sdk(url, method, request_data, **kwargs)

        monkeypatch.setattr(e.normal, "do_request", recorded_sdk)
        service, actor = AgentWork(e.app.state.agent_instances), PrincipalRef("human", e.link.owner_user_id)
        work = await service.delegate(actor=actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review the guide", success_criteria="Report verified gaps"))
        native_scope, requester = None, actor
        if foreign:
            from app.gateway.auth.models import User
            from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
            from deerflow.agent_instances.lifecycle import InstanceLifecycle
            from deerflow.spaces.attachments import SpaceAttachments

            # There are no attached workspaces. A provider without containment
            # methods makes any unexpected native operation fail, never attest.
            SpaceAttachments(e.app.state.agent_instances.files, object())
            peer = await SQLiteUserRepository(e.e.sf).create_user(User(email="colleague@example.com", password_hash="offline-placeholder"))
            requester = PrincipalRef("human", str(peer.id))
            granted = await InstanceLifecycle(e.app.state.agent_instances).change(actor=actor, instance_id=e.instance.id, expected_generation=e.instance.generation, operation_id=uuid4().hex, action="grant", member=requester, permissions=3)
            assert granted["complete"]
        if kind == "stop":
            conversation = await e.app.state.agent_conversations.create(actor=requester, instance_id=e.instance.id, creation_id=uuid4().hex)
            execution = await e.app.state.agent_conversations.execution(actor=requester, thread_id=conversation["thread_id"])
            native_scope = await WorkExecution(service).reserve(
                execution=execution, work_id=work["id"], request=ActivateWork(operation_id=uuid4().hex, expected_revision=work["revision"], expected_assignment_revision=work["assignment_revision"], thread_id=execution.thread_id)
            )
        # The list captures a fixed horizon; include the newly persisted Work.
        created = datetime.fromisoformat(work["created_at"])
        if created >= e.clock.now():
            e.clock.advance((created - e.clock.now()).total_seconds() + 0.000001)
        incoming = Update.de_json(update_payload(7110, message_id=7110, account=PERSON, text="/work"), e.channel.bot)
        assert await e.channel.handle_update(incoming)
        await e.channel._drain_ingress()
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        await tap(e, await button(e, "work_details", target=work["id"]), 7111)
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        control = await button(e, "work_start" if kind == "activate" else "stop_attempt")
        calls = []

        async def unknown(*args, **kwargs):
            calls.append((args, kwargs))
            raise RuntimeError("Host response unavailable")

        if kind == "activate":
            from app.gateway import services

            monkeypatch.setattr(services, "start_run", unknown)
        else:
            actual_command = e.host.acts.work.command
            monkeypatch.setattr(e.host.acts.work, "command", unknown)
        damaged = []
        if context:
            from deerflow.persistence.thread_meta.model import ThreadMetaRow

            locked = WorkExecution._locked

            async def changed(service, session, execution, work_id, **kwargs):
                if not damaged:
                    damaged.append(execution.thread_id)
                    async with e.e.sf.begin() as writer:
                        actual = await writer.get(ThreadMetaRow, execution.thread_id)
                        assert actual is not None
                        await writer.delete(actual)
                return await locked(service, session, execution, work_id, **kwargs)

            monkeypatch.setattr(WorkExecution, "_locked", changed)
        await tap(e, control, 7112)
        original = await e.host.acts._work_record(control.id)
        assert original.state in ({"refused"} if context else {"pending", "uncertain"}) and original.attempts == 1 and len(calls) == 1, (original.state, original.reason, original.operation_ids, original.continuation, len(calls), damaged)
        # Drain automatic action retries and observers before driving expiry.
        # An owner in a receipt read can legitimately make a worker tick skip.
        await e.host.notifications.worker.stop()
        async with e.host.notifications._maintenance_lock, e.host.notifications._send_lock:
            await e.host.sender.drain()
            e.clock.advance(3601)
            worker = ActsWorker(e.host.acts)
            try:
                if contend:
                    entered, release = asyncio.Event(), asyncio.Event()
                    receipt_for = e.host.acts.receipt_for

                    async def held_receipt(act, **kwargs):
                        if act.id == original.id:
                            entered.set()
                            await release.wait()
                        return await receipt_for(act, **kwargs)

                    with monkeypatch.context() as patch:
                        patch.setattr(e.host.acts, "receipt_for", held_receipt)
                        owner = asyncio.create_task(e.host.acts.reconcile_receipt(original.id))
                        try:
                            await asyncio.wait_for(entered.wait(), 30)
                            assert original.id in e.host.acts._work_active
                            await worker.tick()
                            held = await e.host.acts._work_record(original.id)
                            assert held.state == "pending" and held.body == original.body and held.attempts == 1 and len(calls) == 1
                        finally:
                            release.set()
                            await owner
                await worker.tick()
            finally:
                await worker.stop()
            original = await e.host.acts._work_record(control.id)
            assert original.state in ({"uncertain", "refused"} if context else {"uncertain"}) and original.attempts == 1
        if context and original.state == "refused":
            assert original.reason == "conversation-changed"
        effective = original
        if context:
            from app.telegram_bots.work_start_conversations import context_child_id

            assert len(damaged) == 1
            effective = await e.host.acts._work_record(context_child_id(original.id))
            assert effective.kind == "activate_context" and effective.state == "uncertain" and effective.attempts == 1
        guard = e.host.notifications.chat.guard(e.channel, e.link)

        async def observe():
            while True:
                await e.host.sender.drain()
                try:
                    return await e.host.notifications.chat.executions.observe(e.link, original.id, guard=guard)
                except (ProductSourceRetired, DeliveryClaimRetired):
                    # Another real observer may materialize/claim the same row
                    # during the read; reload the exact current owner.
                    await asyncio.sleep(0.05)

        identity = await asyncio.wait_for(observe(), 10)
        await asyncio.wait_for(deliver_owned(e, identity), 30)
        retry = await button(e, "act_check", target=original.id)
        async with e.e.sf() as reader:
            native = await reader.get(AgentWorkAttemptRow, native_scope.attempt_id) if native_scope else await reader.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == effective.id))
            assert native is not None and native.run_id is None
            native_id = native.id
        if foreign:
            async with e.e.sf.begin() as writer:
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
        if known_stop:
            from app.telegram_bots.work_actions import thaw
            from deerflow.agent_instances.work_contract import WorkCommand
            from deerflow.persistence.agent_instances.model import AgentInstanceRow

            assert kind == "stop" and change == "paused"
            async with e.host.notifications._maintenance_lock, e.host.notifications._send_lock:
                await e.host.sender.drain()
                await actual_command(actor=actor, instance_id=e.instance.id, work_id=original.work_id, request=WorkCommand.model_validate(thaw(original.body)), can_stop=True)
                async with e.e.sf.begin() as writer:
                    (await writer.get(AgentInstanceRow, e.instance.id)).status = "suspended"
                assert await e.host.acts._work_record(original.id) == original
        elif change == "paused":
            from deerflow.persistence.agent_instances.model import AgentInstanceRow

            async with e.e.sf.begin() as writer:
                (await writer.get(AgentInstanceRow, e.instance.id)).status = "suspended"
        elif change == "inspect":
            async with e.e.sf.begin() as writer:
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
        elif change != "manage":
            policy = {"routes": {"allow": "*", "deny": ["threads:write" if context else "runs:create" if kind == "activate" else "runs:cancel"]}}
            monkeypatch.setattr(
                peek_loaded_app_config(), "authorization", AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": policy, "user": policy}}}})
            )
        tick = e.host.sender.tick
        if carrier:

            async def paused_tick():
                return 0

            monkeypatch.setattr(e.host.sender, "tick", paused_tick)
        before = len(e.normal.calls)
        await tap(e, retry, 7113)
        if carrier:
            async with e.e.sf.begin() as writer:
                fixed = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_retry_denial"]["act_id"].as_string() == original.id))
                assert fixed is not None and fixed.state == "pending" and all(not part["message_ids"] for part in fixed.parts)
                fixed.payload = {key: value for key, value in fixed.payload.items() if key != "work_retry_denial"}
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 7
            monkeypatch.setattr(e.host.sender, "tick", tick)
        for _ in range(4):
            e.clock.advance(1)
            await e.host.sender.tick()
            await e.host.sender.drain()
        supervisor = await get_local_provider().get_user(e.instance.supervisor.subject_id)
        key = "agent.paused" if change == "paused" else "denied.start" if kind == "activate" else "work.need_inspect" if change == "inspect" else "stop.not_yours" if change == "manage" else "denied.other"
        expected = render(key, "en-US", name=e.instance.name, supervisor=str(supervisor.email), person="colleague@example.com" if foreign else "")
        sent = [call.parameters.get("text") for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
        assert (expected in sent) is not carrier

        current = await e.host.acts._work_record(original.id)
        if known_stop:
            assert current.state == "done" and current.body == original.body and current.attempts == original.attempts
            assert current.stop_dispatch["state"] == "ready" and current.stop_dispatch["attempts"] == 0
        else:
            assert current == original
        assert len(calls) == 1
        if context:
            assert await e.host.acts._work_record(effective.id) == effective
        async with e.e.sf() as reader:
            assert (await reader.get(AgentWorkAttemptRow, native_id)).run_id is None
            assert await reader.get(TelegramActRow, retry.id) is None


@pytest.mark.asyncio
async def test_context_retry_checks_its_actual_threads_capability(capture_environment, monkeypatch, tmp_path):
    await test_current_execution_retry_preserves_actual_native(capture_environment, monkeypatch, tmp_path, "activate", "capability", context=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["manage", "capability"])
async def test_foreign_stop_retry_uses_capability_before_manage(capture_environment, monkeypatch, tmp_path, change):
    await test_current_execution_retry_preserves_actual_native(capture_environment, monkeypatch, tmp_path, "stop", change, foreign=True)


@pytest.mark.asyncio
async def test_retry_denial_cannot_fall_back_without_its_origin(capture_environment, monkeypatch, tmp_path):
    await test_current_execution_retry_preserves_actual_native(capture_environment, monkeypatch, tmp_path, "stop", "inspect", carrier=True)
