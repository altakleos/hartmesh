"""An actual unknown Save keeps its consent when the next Retry lacks access."""

import asyncio
from contextlib import AsyncExitStack

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_bot_test_support import update_payload
from _telegram_compose_turn_support import PERSON, _delivered, render_compose_config
from _telegram_work_worker_support import installed_worker, owned_retry_observation
from sqlalchemy import select
from telegram import Update
from test_telegram_execution_retry_refusals import deliver_owned, denial_for, settle_denial
from test_telegram_retained_management_refusals import button, tap
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.acts import ActsWorker
from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.assignment_drafts import AssignmentDrafts
from app.telegram_bots.authority import get_local_provider
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.render import render
from deerflow.config.app_config import peek_loaded_app_config
from deerflow.config.authorization_config import AuthorizationConfig
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["inspect", "capability"])
async def test_current_save_retry_preserves_unknown_delegation(capture_environment, monkeypatch, tmp_path, change, *, no_due=False):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e, AsyncExitStack() as phase:
        sdk = e.normal.do_request

        async def recorded_sdk(url, method, request_data=None, **kwargs):
            if url.rsplit("/", 1)[-1] == "editMessageText" and not e.normal.replies["editMessageText"]:
                values = request_data.parameters
                e.normal.enqueue("editMessageText", result={"message_id": values["message_id"], "date": 1_893_456_000, "chat": {"id": values["chat_id"], "type": "private"}, "text": values["text"]})
            return await sdk(url, method, request_data, **kwargs)

        monkeypatch.setattr(e.normal, "do_request", recorded_sdk)
        incoming = Update.de_json(update_payload(7120, message_id=7120, account=PERSON, text="/assign Review the guide"), e.channel.bot)
        origin, _created = await e.host.outbox.inbox.accept(e.link, incoming, sanitized_input={"command": "assign", "text": "Review the guide"})
        from datetime import timedelta

        proposed = DraftAssignment(objective="Review the guide", success_criteria="Report verified gaps", due_text="By Friday" if no_due else None, due_at=e.clock.now() + timedelta(days=2) if no_due else None)
        draft = await AssignmentDrafts(e.host.acts).create(e.link, origin, draft=proposed, source="command")
        guard = e.host.notifications.chat.guard(e.channel, e.link)
        await e.host.notifications.chat.assignments.plan(e.link, draft.id, guard=guard)
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        control = await button(e, "assign_save", target=draft.id)
        calls = []
        actual_delegate = e.host.acts.work.delegate

        async def unknown(**kwargs):
            request = kwargs["request"]
            calls.append(request.model_dump(mode="json", exclude_unset=True))
            if no_due and request.due_at is not None:
                async with e.e.sf.begin() as writer:
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
                return await actual_delegate(**kwargs)
            raise ConnectionError("Save response unavailable")

        monkeypatch.setattr(e.host.acts.work, "delegate", unknown)
        await tap(e, control, 7121)
        original = await e.host.acts._work_record(control.id)
        assert original.state == "pending" and original.attempts == 1 and len(calls) == (2 if no_due else 1)
        await phase.enter_async_context(owned_retry_observation(e))
        e.clock.advance(3601)
        worker = ActsWorker(e.host.acts)
        try:
            await worker.tick()
        finally:
            await worker.stop()
        original = await e.host.acts._work_record(control.id)
        assert original.state in ({"pending", "uncertain"} if no_due else {"uncertain"}) and original.attempts == 1

        async def observe():
            while True:
                await e.host.sender.drain()
                try:
                    return await e.host.notifications.chat.assignments.observe(e.link, draft.id, guard=guard)
                except ProductSourceRetired:
                    await asyncio.sleep(0.05)

        identity = await asyncio.wait_for(observe(), 10)
        try:
            await asyncio.wait_for(deliver_owned(e, identity), 30)
        except TimeoutError:
            from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

            async with e.e.sf() as reader:
                pending = await reader.get(TelegramOutboxRow, identity)
                print(
                    "SAVED_RETRY_ACK_WAIT",
                    pending.id,
                    pending.state,
                    pending.claim_owner,
                    pending.payload.get("presentation_version"),
                    [(part.get("presentation_version"), part["state"], part.get("message_ids"), part.get("target_message_id")) for part in pending.parts],
                )
            raise
        retry = await button(e, "act_check", target=original.id)
        child = None
        if no_due:
            from app.telegram_bots.work_actions import thaw
            from app.telegram_bots.work_delegate_actions import no_due_id
            from deerflow.agent_instances.work_contract import DelegateWork
            from deerflow.persistence.agent_instances.model import AgentInstanceRow
            from deerflow.spaces.contract import PrincipalRef

            child = await e.host.acts._work_record(no_due_id(original.id))
            assert original.operation_ids == (child.id,) and child.state in {"pending", "uncertain"} and child.body.get("due_at") is None
            await e.host.sender.drain()
            await actual_delegate(actor=PrincipalRef("human", e.link.owner_user_id), instance_id=e.instance.id, request=DelegateWork.model_validate(thaw(child.body)))
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
        else:
            policy = {"routes": {"allow": "*", "deny": ["agents:write"]}}
            monkeypatch.setattr(
                peek_loaded_app_config(), "authorization", AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": policy, "user": policy}}}})
            )
        actual = e.host.notifications.chat.retry_denials.offer

        async def traced(*args, **kwargs):
            try:
                return await actual(*args, **kwargs)
            except Exception:
                import traceback

                print(traceback.format_exc())
                raise

        monkeypatch.setattr(e.host.notifications.chat.retry_denials, "offer", traced)
        before = len(e.normal.calls)
        await tap(e, retry, 7122)
        if change != "paused":
            identity = await denial_for(e, retry, original.id, "query-7122")
            await settle_denial(e, identity, retired=False)
        key = "agent.paused" if change == "paused" else "assign.no_access" if change == "inspect" else "denied.other"
        supervisor = await get_local_provider().get_user(e.instance.supervisor.subject_id)
        assert render(key, "en-US", name=e.instance.name, supervisor=str(supervisor.email)) in [call.parameters.get("text") for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
        if no_due:
            current, settled_child = await e.host.acts._work_record(original.id), await e.host.acts._work_record(child.id)
            assert current.state == settled_child.state == "done" and current.body == original.body and current.attempts == original.attempts
            assert current.result["delegate_child_id"] == child.id and settled_child.body == child.body and settled_child.attempts == child.attempts
            assert len(calls) == 2
        else:
            assert await e.host.acts._work_record(original.id) == original and len(calls) == 1
        async with e.e.sf() as reader:
            assert await reader.get(TelegramActRow, retry.id) is None
            assert await reader.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == original.id)) is None
