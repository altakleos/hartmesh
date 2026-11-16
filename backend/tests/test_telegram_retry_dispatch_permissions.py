"""Known commands retain their own physical Stop authority and saved outcome."""

import asyncio

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_compose_turn_support import render_compose_config
from _telegram_work_worker_support import installed_worker
from sqlalchemy import func, select
from test_telegram_execution_retry_refusals import deliver_owned
from test_telegram_retained_management_refusals import button, management_preview, tap
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.render import render
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow


@pytest.mark.asyncio
async def test_original_requester_can_retry_known_command_dispatch_without_manage(capture_environment, monkeypatch, tmp_path, *, changed_scope=False):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        _work, control = await management_preview(e, "cancel", monkeypatch, reserve=True)
        sdk = e.normal.do_request

        async def recorded_sdk(url, method, request_data=None, **kwargs):
            if url.rsplit("/", 1)[-1] == "editMessageText" and not e.normal.replies["editMessageText"]:
                values = request_data.parameters
                e.normal.enqueue("editMessageText", result={"message_id": values["message_id"], "date": 1_893_456_000, "chat": {"id": values["chat_id"], "type": "private"}, "text": values["text"]})
            return await sdk(url, method, request_data, **kwargs)

        monkeypatch.setattr(e.normal, "do_request", recorded_sdk)
        e.host.acts.outbox.config.acts.max_attempts = 1
        dispatched = []

        async def unavailable(*args, **kwargs):
            dispatched.append(kwargs)
            raise ConnectionError("Physical cancellation response unavailable")

        monkeypatch.setattr("app.gateway.work_execution.dispatch_work_stop", unavailable)
        await tap(e, control, 7130)
        original = await e.host.acts._work_record(control.id)
        assert original.state == "done" and original.stop_dispatch["state"] == "uncertain" and len(dispatched) == 1
        guard = e.host.notifications.chat.guard(e.channel, e.link)

        async def observe():
            while True:
                await e.host.sender.drain()
                try:
                    return await e.host.notifications.chat.management.observe(e.link, control.payload["anchor_outbox_id"], guard=guard)
                except ProductSourceRetired:
                    await asyncio.sleep(0.05)

        identity = await asyncio.wait_for(observe(), 10)
        await asyncio.wait_for(deliver_owned(e, identity), 30)
        retry = await button(e, "act_check", target=original.id)
        async with e.e.sf.begin() as writer:
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
        changed = []
        if changed_scope:
            from contextvars import ContextVar

            from deerflow.config.app_config import peek_loaded_app_config
            from deerflow.config.authorization_config import AuthorizationConfig

            policy = {"routes": {"allow": "*", "deny": ["runs:cancel"]}}
            monkeypatch.setattr(
                peek_loaded_app_config(), "authorization", AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": policy, "user": policy}}}})
            )
            finalising = ContextVar("remaining-stop-finalising", default=False)
            actual_authorize, actual_settings = e.host.work_retry_denials.authorize, e.host.settings.get

            async def authorize(captured, **kwargs):
                token = finalising.set(True)
                try:
                    return await actual_authorize(captured, **kwargs)
                finally:
                    finalising.reset(token)

            async def settings(owner):
                result = await actual_settings(owner)
                if finalising.get() and not changed:
                    changed.append(True)
                    async with e.e.sf.begin() as writer:
                        # Negative scope corruption of a real Native owner.
                        (await writer.get(AgentWorkAttemptRow, e.native_original.attempt_id)).thread_id = "substituted-thread"
                return result

            monkeypatch.setattr(e.host.work_retry_denials, "authorize", authorize)
            monkeypatch.setattr(e.host.settings, "get", settings)
        offered, actual_offer = [], e.host.notifications.chat.retry_denials.offer

        async def tracked_offer(*args, **kwargs):
            value = await actual_offer(*args, **kwargs)
            offered.append(value)
            return value

        monkeypatch.setattr(e.host.notifications.chat.retry_denials, "offer", tracked_offer)
        before = len(e.normal.calls)
        await tap(e, retry, 7131)
        for _ in range(4):
            e.clock.advance(1)
            await e.host.sender.tick()
            await e.host.sender.drain()
        assert offered == [changed_scope] and len(dispatched) == (1 if changed_scope else 2)
        assert bool(changed) is changed_scope
        sent = [call.parameters.get("text") for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
        assert render("tap.failed", "en-US") not in sent, "known saved command and uncertain physical dispatch must retain their status"
        assert render("denied.withdraw", "en-US", name=e.instance.name) not in sent
        current = await e.host.acts._work_record(original.id)
        assert (current.state, current.body, current.attempts, current.result) == ("done", original.body, original.attempts, original.result)
        assert current.stop_dispatch["attempts"] == original.stop_dispatch["attempts"] + (0 if changed_scope else 1)
        async with e.e.sf() as reader:
            native = await reader.get(AgentWorkAttemptRow, e.native_original.attempt_id)
            assert native is not None and native.run_id is None
            assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == original.id)) == 1


@pytest.mark.asyncio
async def test_remaining_stop_scope_is_rechecked_before_sdk(capture_environment, monkeypatch, tmp_path):
    await test_original_requester_can_retry_known_command_dispatch_without_manage(capture_environment, monkeypatch, tmp_path, changed_scope=True)
