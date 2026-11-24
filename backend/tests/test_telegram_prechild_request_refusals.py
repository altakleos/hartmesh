"""Actual Decision/Voice primaries keep their receipt before an unadmitted Start."""

import asyncio
from contextvars import ContextVar

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_compose_turn_support import render_compose_config
from _telegram_work_worker_support import installed_worker
from sqlalchemy import select
from test_telegram_default_composite_controls import decision_confirmation, voice_confirmation
from test_telegram_prechild_composite_refusals import damage_primary_ack
from test_telegram_retained_management_refusals import tap
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.authority import get_local_provider
from app.telegram_bots.render import render
from deerflow.config.app_config import peek_loaded_app_config
from deerflow.config.authorization_config import AuthorizationConfig
from deerflow.persistence.agent_instances.human_input import HumanInputResponseRow
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decision", "voice"])
@pytest.mark.parametrize("change", ["capability", "inspect", "paused"])
async def test_request_primary_survives_unadmitted_start(capture_environment, monkeypatch, tmp_path, kind, change, *, damage_ack=False, pending_primary=False):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        sdk_request = e.normal.do_request
        rate_limited_edit = None
        rate_limit_reply = {"ok": False, "error_code": 429, "description": "Too Many Requests", "parameters": {"retry_after": 100}}

        async def sdk(url, method, request_data=None, **kwargs):
            nonlocal rate_limited_edit
            if url.rsplit("/", 1)[-1] == "editMessageText" and not e.normal.replies["editMessageText"]:
                values = request_data.parameters
                if values["message_id"] == rate_limited_edit:
                    rate_limited_edit = None
                    e.normal.enqueue("editMessageText", status=429, body=rate_limit_reply)
                else:
                    e.normal.enqueue("editMessageText", result={"message_id": values["message_id"], "date": 1_893_456_000, "chat": {"id": values["chat_id"], "type": "private"}, "text": values["text"]})
            return await sdk_request(url, method, request_data, **kwargs)

        monkeypatch.setattr(e.normal, "do_request", sdk)
        work, request, control = await (decision_confirmation(e) if kind == "decision" else voice_confirmation(e, monkeypatch))
        resume, entered, captured = e.host.acts.resume_continuation, [], []
        tick, paused = e.host.sender.tick, [pending_primary]
        if pending_primary:

            async def scheduled_tick():
                return 0 if paused[0] else await tick()

            monkeypatch.setattr(e.host.sender, "tick", scheduled_tick)
        damaged = []
        if damage_ack:
            finalising = ContextVar("request-refusal-finalising", default=None)
            authorize, settings = e.host.work_saved_refusals.authorize, e.host.settings.get

            async def final_authorize(delivery, **kwargs):
                token = finalising.set(delivery.item.id)
                try:
                    return await authorize(delivery, **kwargs)
                finally:
                    finalising.reset(token)

            async def lose_ack(owner):
                value = await settings(owner)
                if finalising.get() is not None and not damaged:
                    damaged.append(finalising.get())
                    async with e.e.sf.begin() as writer:
                        await damage_primary_ack(writer, damaged[0])
                return value

            monkeypatch.setattr(e.host.work_saved_refusals, "authorize", final_authorize)
            monkeypatch.setattr(e.host.settings, "get", lose_ack)

        async def primary_delivered(identity):
            while True:
                e.clock.advance(1)
                await e.host.sender.tick()
                await e.host.sender.drain()
                async with e.e.sf() as reader:
                    act = await reader.get(TelegramActRow, identity)
                    primary = await reader.get(TelegramOutboxRow, act.continuation["request_activation"]["outcome_id"])
                    assert primary.state not in {"failed", "skipped"}, (primary.id, primary.state, primary.failed_reason, primary.skip_reason, primary.parts)
                    if primary.state == "sent" and primary.parts and all(part["state"] == "sent" and part["message_ids"] for part in primary.parts):
                        return
                await asyncio.sleep(0.05)

        async def deny_child(identity, **kwargs):
            nonlocal rate_limited_edit
            if identity != control.id:
                return await resume(identity, **kwargs)
            if entered and not captured:
                return await e.host.acts._work_record(identity)
            if not entered:
                entered.append(identity)
                if not pending_primary:
                    await asyncio.wait_for(primary_delivered(identity), 30)
                async with e.e.sf.begin() as writer:
                    act = await writer.get(TelegramActRow, identity)
                    state = act.continuation["activation_continuation"]
                    assert act.kind == ("decide" if kind == "decision" else "respond") and act.state == "done" and state["phase"] == "ready"
                    primary = await writer.get(TelegramOutboxRow, act.continuation["request_activation"]["outcome_id"])
                    if pending_primary:
                        assert primary.state == "pending" and all(not part["message_ids"] for part in primary.parts)
                        if kind == "decision":
                            # The decision receipt edits the confirmation card. The earlier question has its own edit.
                            rate_limited_edit = primary.payload["work_outcome_basis"]["edit"]["message_id"]
                            assert rate_limited_edit == control.message_id
                    else:
                        assert primary.state == "sent" and primary.parts[-1]["state"] == "sent"
                    assert await writer.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == identity)) is not None
                    captured.append((act.body, act.result, state["child_id"], act.attempts, state["authorized_at"]))
                    if change == "paused":
                        (await writer.get(AgentInstanceRow, e.instance.id)).status = "suspended"
                    elif change == "inspect":
                        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
                if change == "capability":
                    policy = {"routes": {"allow": "*", "deny": ["runs:create"]}}
                    monkeypatch.setattr(
                        peek_loaded_app_config(),
                        "authorization",
                        AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": policy, "user": policy}}}}),
                    )
            return await resume(identity, **kwargs)

        monkeypatch.setattr(e.host.acts, "resume_continuation", deny_child)
        before = len(e.normal.calls)
        await tap(e, control, 7070)
        await e.host.work_callbacks.outcomes.observe_start_refusal(e.link, control.id, guard=e.host.notifications.chat.guard(e.channel, e.link))
        if pending_primary:
            if kind != "decision":
                e.normal.enqueue("sendMessage", status=429, body=rate_limit_reply)
            paused[0] = False
            for _ in range(2):
                e.clock.advance(1)
                await tick()
                await e.host.sender.drain()
            async with e.e.sf() as reader:
                act = await reader.get(TelegramActRow, control.id)
                primary_id = act.continuation["request_activation"]["outcome_id"]
                primary = await reader.get(TelegramOutboxRow, primary_id)
                assert primary.state == "pending" and any(part.get("dispatched_at") for part in primary.parts)
                assert all(not part["message_ids"] for part in primary.parts)
                fixed = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_saved_refusal"]["act_id"].as_string() == control.id))
                assert fixed is not None and fixed.state == "pending" and all(not part["message_ids"] for part in fixed.parts), "an actually pending primary must defer its fixed reply"
                assert all(part.get("dispatched_at") is None for part in fixed.parts), "dependent advice cannot claim the sender before its primary"
                assert fixed.payload["after_delivery_id"] == primary_id
                fixed_id, fixed_intent = fixed.id, dict(fixed.payload["work_saved_refusal"])
            e.clock.advance(101)
            try:
                await asyncio.wait_for(primary_delivered(control.id), 30)
            except TimeoutError as error:
                async with e.e.sf() as reader:
                    actual = await reader.get(TelegramActRow, control.id)
                    rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.id.in_([fixed_id, actual.continuation["request_activation"]["outcome_id"]]))))
                    details = [(row.id, row.state, str(row.not_before), row.failed_reason, row.skip_reason, row.claim_owner, row.parts) for row in rows]
                raise AssertionError({"now": str(e.clock.now()), "actual": details}) from error
        for _ in range(4):
            e.clock.advance(5)
            await e.host.sender.tick()
            await e.host.sender.drain()
        supervisor = await get_local_provider().get_user(e.instance.supervisor.subject_id)
        expected = render("agent.paused" if change == "paused" else "denied.start", "en-US", name=e.instance.name, supervisor=str(supervisor.email))
        sent = [call.parameters["text"] for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
        if not damage_ack:
            assert expected in sent and sent.count(expected) == 1, sent
        async with e.e.sf() as reader:
            act = await reader.get(TelegramActRow, control.id)
            body, receipt, child_id, attempts, authorized_at = captured[0]
            assert act.state == "done" and act.body == body and act.result == receipt and act.attempts == attempts
            state = act.continuation["activation_continuation"]
            assert state["authorized_at"] == authorized_at and state["phase"] == "refused"
            assert state["child_body"] is None and not act.operation_ids
            assert await reader.get(TelegramActRow, child_id) is None
            assert await reader.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == child_id)) is None
            assert await reader.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == child_id)) is None
            responses = list(await reader.scalars(select(HumanInputResponseRow).where(HumanInputResponseRow.request_id == request["id"])))
            assert len(responses) == (1 if kind == "voice" else 0)
            if kind == "voice":
                assert body["text"] == responses[0].text == "Use the current account."
            if pending_primary:
                fixed = await reader.get(TelegramOutboxRow, fixed_id)
                assert fixed.state == "sent" and fixed.parts[0]["message_ids"]
                assert fixed.payload["work_saved_refusal"] == fixed_intent
            if damage_ack:
                assert len(damaged) == 1
                fixed = await reader.get(TelegramOutboxRow, damaged[0])
                assert fixed.state != "sent" and all(not part["message_ids"] for part in fixed.parts), "a missing actual primary ACK must veto fixed advice"
                assert expected not in sent
