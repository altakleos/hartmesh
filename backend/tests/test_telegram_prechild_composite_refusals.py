"""A committed Save keeps its receipt when its explicit Start cannot be admitted."""

import asyncio
from contextvars import ContextVar
from copy import deepcopy

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
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow

SAVE_CHANGES = ["capability", "inspect", "paused", "use", "read-ceiling", "work-off", "work-off-observer", "work-off-no-base", "readiness-paused", "readiness-admin", "readiness-unknown"]


async def set_work_policy(e, writer, enabled):
    from deerflow.agent_instances.contract import DefinitionSnapshot
    from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow

    instance = await writer.get(AgentInstanceRow, e.instance.id)
    old = await writer.get(AgentDefinitionRevisionRow, instance.definition_revision)
    updated = DefinitionSnapshot.capture(owner_id=old.owner_id, config={**old.config, "work_policy": {**old.config["work_policy"], "enabled": enabled}}, soul=old.soul)
    if await writer.get(AgentDefinitionRevisionRow, updated.revision) is None:
        writer.add(AgentDefinitionRevisionRow(revision=updated.revision, owner_id=updated.owner_id, config=updated.config, soul=updated.soul))
        await writer.flush()
    instance.definition_revision = updated.revision


async def damage_primary_ack(writer, fixed_id):
    fixed = await writer.get(TelegramOutboxRow, fixed_id)
    primary = await writer.get(TelegramOutboxRow, fixed.payload["work_saved_refusal"]["canonical_id"])
    parts = deepcopy(primary.parts)
    current = [part for part in parts if part["presentation_version"] == primary.payload["presentation_version"]]
    assert current and all(part["state"] == "sent" and part["message_ids"] for part in current)
    for part in current:
        part["message_ids"] = []
    primary.parts = parts


@pytest.mark.asyncio
@pytest.mark.parametrize("change", SAVE_CHANGES)
async def test_committed_save_explains_unadmitted_start(capture_environment, monkeypatch, tmp_path, change, *, race=None):
    def configure(document):
        document["telegram_bots"]["public_base_url"] = None if change == "work-off-no-base" else "https://chat.example.test"

    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"}, tweak=configure)
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        from deerflow.config.app_config import peek_loaded_app_config

        authorization = peek_loaded_app_config().authorization
        readiness = {"state": "space-unverified" if change == "readiness-admin" else "operator-hold"}
        request = e.normal.do_request

        async def sdk(url, method, request_data=None, **kwargs):
            if url.rsplit("/", 1)[-1] == "editMessageText" and not e.normal.replies["editMessageText"]:
                values = request_data.parameters
                e.normal.enqueue("editMessageText", result={"message_id": values["message_id"], "date": 1_893_456_000, "chat": {"id": values["chat_id"], "type": "private"}, "text": values["text"]})
            return await request(url, method, request_data, **kwargs)

        monkeypatch.setattr(e.normal, "do_request", sdk)
        incoming = Update.de_json(update_payload(7040, message_id=7040, account=PERSON, text="/assign Review the guide"), e.channel.bot)
        origin, _created = await e.host.outbox.inbox.accept(e.link, incoming, sanitized_input={"command": "assign", "text": "Review the guide"})
        root = await AssignmentDrafts(e.host.acts).create(e.link, origin, draft=DraftAssignment(objective="Review the guide", success_criteria="Report verified gaps"), source="command")
        await e.host.notifications.chat.assignments.plan(e.link, root.id, guard=e.host.notifications.chat.guard(e.channel, e.link))
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        async with e.e.sf() as reader:
            control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "assign_start", TelegramCallbackRow.target_id == root.id))
            assert control is not None and control.payload["presentation_pending"] is False
        resume, captured, entered = e.host.acts.resume_continuation, [], []
        finalising = ContextVar("fixed-refusal-finalising", default=None)
        policy_read = ContextVar("fixed-refusal-policy-read", default=False)
        raced = []
        if race is not None:
            authorize, settings = e.host.work_saved_refusals.authorize, e.host.settings.get
            policy, instance_view = e.host.authority.instance_send_policy, e.host.authority.instance_view

            async def final_authorize(delivery, **kwargs):
                token = finalising.set(delivery.item.id)
                try:
                    return await authorize(delivery, **kwargs)
                finally:
                    finalising.reset(token)

            async def change_after_settings(owner):
                value = await settings(owner)
                if race != "readiness-authority" and finalising.get() is not None and not raced:
                    raced.append(finalising.get())
                    if race == "base":
                        peek_loaded_app_config().model_extra["telegram_bots"]["public_base_url"] = "https://changed.example.test"
                    elif race == "readiness":
                        readiness["state"] = "ready"
                    elif race == "primary-ack":
                        async with e.e.sf.begin() as writer:
                            await damage_primary_ack(writer, finalising.get())
                    else:
                        async with e.e.sf.begin() as writer:
                            if race == "work-enabled":
                                await set_work_policy(e, writer, True)
                            else:
                                assert race == "manage"
                                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
                return value

            async def final_policy(*args, **kwargs):
                projection = kwargs.get("projection")
                delivery = getattr(projection, "captured", None)
                final = getattr(projection, "composite", False) and delivery is not None and delivery.item.id == finalising.get()
                token = policy_read.set(final)
                try:
                    return await policy(*args, **kwargs)
                finally:
                    policy_read.reset(token)

            async def change_after_authority(*args, **kwargs):
                value = await instance_view(*args, **kwargs)
                if race == "readiness-authority" and finalising.get() is not None and policy_read.get() and not raced:
                    raced.append(finalising.get())
                    readiness["state"] = "ready"
                return value

            monkeypatch.setattr(e.host.work_saved_refusals, "authorize", final_authorize)
            monkeypatch.setattr(e.host.settings, "get", change_after_settings)
            monkeypatch.setattr(e.host.authority, "instance_send_policy", final_policy)
            monkeypatch.setattr(e.host.authority, "instance_view", change_after_authority)

        async def deny_child(identity, **kwargs):
            if identity != control.id:
                return await resume(identity, **kwargs)
            if entered and not captured:
                return await e.host.acts._work_record(identity)
            if not entered:
                entered.append(identity)
                await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
                async with e.e.sf.begin() as writer:
                    act = await writer.get(TelegramActRow, identity)
                    state = act.continuation["activation_continuation"]
                    assert act.kind == "delegate" and act.state == "done" and state["phase"] == "ready"
                    primary = await writer.get(TelegramOutboxRow, control.payload["anchor_outbox_id"])
                    assert primary.state == "sent" and primary.parts[-1]["state"] == "sent"
                    assert await writer.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == act.id)) is not None
                    captured.append((act.body, act.result, state["child_id"], act.attempts, state["authorized_at"]))
                    if change == "paused":
                        (await writer.get(AgentInstanceRow, e.instance.id)).status = "suspended"
                    elif change in {"inspect", "use"}:
                        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1 if change == "inspect" else 2
                    elif change.startswith("work-off"):
                        await set_work_policy(e, writer, False)
                        if change == "work-off-observer":
                            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
                if change in {"capability", "read-ceiling"} or change.startswith("readiness-"):
                    from deerflow.config.authorization_config import AuthorizationConfig

                    policy = {"routes": {"allow": "*", "deny": ["agents:read" if change == "read-ceiling" else "runs:create"]}}
                    monkeypatch.setattr(
                        peek_loaded_app_config(),
                        "authorization",
                        AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": policy, "user": policy}}}}),
                    )
            result = await resume(identity, **kwargs)
            if change.startswith("readiness-"):
                from app.gateway.storage_spaces import ExecutionState
                from deerflow.persistence.user.model import UserRow

                monkeypatch.setattr(peek_loaded_app_config(), "authorization", authorization)

                async def probe(instance_id=None):
                    assert instance_id == e.instance.id
                    if change == "readiness-unknown":
                        raise OSError("Execution readiness could not be read")
                    return ExecutionState(True, "ready" if readiness["state"] == "ready" else "unavailable", None if readiness["state"] == "ready" else readiness["state"])

                monkeypatch.setattr(e.app.state.agent_instances, "execution_probe", probe)
                if change == "readiness-admin":
                    async with e.e.sf.begin() as writer:
                        (await writer.get(UserRow, e.link.owner_user_id)).system_role = "admin"
            return result

        monkeypatch.setattr(e.host.acts, "resume_continuation", deny_child)
        before = len(e.normal.calls)
        assert await e.channel.handle_update(press(e, 7041, control.id, message_id=control.message_id))
        await e.channel._drain_ingress()
        if change == "readiness-unknown":
            with pytest.raises(OSError, match="readiness could not be read"):
                await e.host.notifications.chat.assignments.observe(e.link, root.id, guard=e.host.notifications.chat.guard(e.channel, e.link))
        else:
            await e.host.notifications.chat.assignments.observe(e.link, root.id, guard=e.host.notifications.chat.guard(e.channel, e.link))
        for _ in range(4):
            e.clock.advance(5)
            await e.host.sender.tick()
            await e.host.sender.drain()
        supervisor = await get_local_provider().get_user(e.instance.supervisor.subject_id)
        key = "agent.paused" if change == "paused" else "access.agent_off" if change in {"use", "read-ceiling"} else "assign.no_work" if change.startswith("work-off") else "denied.start"
        if change.startswith("readiness-"):
            key = "unavailable.files_unchecked" if change == "readiness-admin" else "unavailable.paused"
        expected = render(
            key,
            "en-US",
            name=e.instance.name,
            supervisor=str(supervisor.email),
            product=e.host.product_name,
            custody=e.instance.custody,
            reader_is_administrator=change == "readiness-admin",
            code="files-unchecked" if change == "readiness-admin" else None,
        )
        sent = [call.parameters["text"] for call in e.normal.calls[before:] if call.endpoint in {"sendMessage", "editMessageText"}]
        async with e.e.sf() as reader:
            act = await reader.get(TelegramActRow, control.id)
            body, receipt, child_id, attempts, authorized_at = captured[0]
            assert act.state == "done" and act.body == body and act.result == receipt and act.attempts == attempts
            assert act.continuation["activation_continuation"]["authorized_at"] == authorized_at
            assert act.continuation["activation_continuation"]["phase"] == "refused"
            assert not act.operation_ids and await reader.get(TelegramActRow, child_id) is None
            assert await reader.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == child_id)) is None
            assert await reader.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == child_id)) is None
            if change.startswith("work-off") and race is None:
                plans = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_saved_refusal"]["act_id"].as_string() == control.id)))
                assert len(plans) == 1 and plans[0].state == "sent" and plans[0].parts[0]["message_ids"]
                url = "https://chat.example.test/workspace/instances?instance=" + e.instance.id + "&tab=settings" if change == "work-off" else None
                assert plans[0].parts[0].get("url_button") == ({"page": "web.agentSettings", "url": url} if url else None)
                assert plans[0].payload["work_saved_refusal"].get("settings_url") == url
            if change == "readiness-unknown":
                assert await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.payload["work_saved_refusal"]["act_id"].as_string() == control.id)) is None
            if race is not None:
                assert len(raced) == 1
                old = await reader.get(TelegramOutboxRow, raced[0])
                assert old.state != "sent" and all(not part["message_ids"] for part in old.parts), "current changes must veto the old fixed reply before SDK ACK"
                old_url = (old.parts[0].get("url_button") or {}).get("url")
                if old_url:
                    assert all(old_url not in str(call.parameters.get("reply_markup")) for call in e.normal.calls[before:] if call.endpoint in {"sendMessage", "editMessageText"})
                else:
                    assert expected not in sent
        if change != "readiness-unknown" and race is None:
            assert any(expected == text or expected + "\n" in text for text in sent), sent
        if change in {"capability", "inspect"}:
            monkeypatch.setattr(peek_loaded_app_config(), "authorization", authorization)
            async with e.e.sf.begin() as writer:
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 7
            result = await resume(control.id)
            assert result.continuation["activation_continuation"]["phase"] == "refused"
            async with e.e.sf() as reader:
                assert await reader.get(TelegramActRow, child_id) is None
                assert await reader.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == child_id)) is None
