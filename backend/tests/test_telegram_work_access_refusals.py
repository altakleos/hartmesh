"""An actual Start press can explain lost Inspect without reading Work."""

import asyncio
from copy import deepcopy
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

from app.telegram_bots.render import render
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.agent_instances.work_execution_contract import ActivateWork
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.contract import PrincipalRef

ACCESS_CASES = [
    ("work_start", None),
    ("assign_save", None),
    ("assign_start", None),
    ("work_start", "inspect"),
    ("work_start", "use"),
    ("work_start", "reserved"),
    ("work_start", "ack"),
    ("work_start", "age"),
    ("work_start", "supervisor"),
    ("work_start", "quiet"),
    ("work_start", "cap:agents:write"),
    ("work_start", "cap:runs:create"),
    ("work_start", "cap:threads:write"),
    ("assign_save", "cap:agents:write"),
    ("assign_start", "cap:runs:create"),
    ("assign_start", "cap:threads:write"),
    ("work_start", "replay-pending"),
    ("work_start", "replay-sent"),
    ("work_start", "closed-before"),
    ("work_start", "closed-other"),
    ("work_start", "closed-owner"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("entry,change", ACCESS_CASES)
async def test_actual_start_press_explains_lost_inspect(capture_environment, monkeypatch, tmp_path, entry, change):
    capability = change is not None and change.startswith("cap:")

    def restrict(capability):
        from deerflow.config.app_config import peek_loaded_app_config
        from deerflow.config.authorization_config import AuthorizationConfig

        configuration = peek_loaded_app_config()
        policy = {"routes": {"allow": "*", "deny": [capability]}}
        monkeypatch.setattr(configuration, "authorization", AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": policy, "user": policy}}}}))

    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        human = HumanInput(AgentWork(e.app.state.agent_instances))
        work = await human.work.delegate(
            actor=PrincipalRef("human", e.link.owner_user_id), instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review the private guide", success_criteria="Report verified gaps")
        )
        update = Update.de_json(update_payload(6800, message_id=6800, account=PERSON, text="/work"), e.channel.bot)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        async with e.e.sf() as reader:
            detail = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "work_details", TelegramCallbackRow.target_id == work["id"]))
            assert detail is not None and detail.payload["presentation_pending"] is False
        assert await e.channel.handle_update(press(e, 6801, detail.id, message_id=detail.message_id))
        await e.channel._drain_ingress()
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        async with e.e.sf() as reader:
            control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "work_start", TelegramCallbackRow.target_id == work["id"]))
            assert control is not None and control.payload["presentation_pending"] is False
        if entry != "work_start":
            from app.telegram_bots.assign import DraftAssignment
            from app.telegram_bots.assignment_drafts import AssignmentDrafts

            incoming = Update.de_json(update_payload(6803, message_id=6803, account=PERSON, text="/assign Review the private guide"), e.channel.bot)
            origin, _created = await e.host.outbox.inbox.accept(e.link, incoming, sanitized_input={"command": "assign", "text": "Review the private guide"})
            draft = await AssignmentDrafts(e.host.acts).create(e.link, origin, draft=DraftAssignment(objective="Review the private guide", success_criteria="Report verified gaps"), source="command")
            await e.host.notifications.chat.assignments.plan(e.link, draft.id, guard=e.host.notifications.chat.guard(e.channel, e.link))
            await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
            async with e.e.sf() as reader:
                control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == entry, TelegramCallbackRow.target_id == draft.id))
                assert control is not None and control.payload["presentation_pending"] is False
        async with e.e.sf.begin() as writer:
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3 if capability else 1
            if change == "quiet":
                preferences = await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)
                preferences.quiet_enabled, preferences.timezone = False, "UTC"
        if capability:
            restrict(change.removeprefix("cap:"))
        source_errors = []
        source = e.host.work_callback_denials
        actual_read = source.read_part
        actual_authorize = source.authorize
        changed = []

        async def read(captured, **kwargs):
            try:
                result = await actual_read(captured, **kwargs)
                if change is not None and change != "quiet" and not capability and not change.startswith("replay-") and not changed:
                    changed.append(change)
                    async with e.e.sf.begin() as writer:
                        if change in {"inspect", "use"}:
                            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3 if change == "inspect" else 2
                        elif change == "reserved":
                            actual = await writer.get(TelegramCallbackRow, control.id)
                            actual.payload = {**actual.payload, "work_action_id": control.id}
                        elif change == "ack":
                            actual = await writer.get(TelegramOutboxRow, control.payload["anchor_outbox_id"])
                            actual.parts = [{**part, "message_ids": [control.message_id + 1]} if part.get("callback_group_id") == control.group_id else part for part in actual.parts]
                        elif change == "supervisor":
                            (await writer.get(UserRow, e.instance.supervisor.subject_id)).email = "current-supervisor@example.com"
                        elif change.startswith("closed-"):
                            for member in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == control.group_id)):
                                member.consumed_at = captured.item.created_at - timedelta(seconds=1) if change == "closed-before" else e.clock.now()
                                member.closed_reason = "discarded" if change == "closed-other" else "superseded"
                    if change == "closed-owner":
                        actor = PrincipalRef("human", e.link.owner_user_id)
                        async with e.e.sf.begin() as writer:
                            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
                        conversations = e.app.state.agent_conversations
                        conversation = await conversations.create(actor=actor, instance_id=e.instance.id, creation_id=uuid4().hex)
                        execution = await conversations.execution(actor=actor, thread_id=conversation["thread_id"])
                        await WorkExecution(human.work).reserve(
                            execution=execution, work_id=work["id"], request=ActivateWork(operation_id=control.id, expected_revision=work["revision"], expected_assignment_revision=work["assignment_revision"], thread_id=execution.thread_id)
                        )
                        async with e.e.sf.begin() as writer:
                            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
                    if change == "age":
                        e.clock.advance(timedelta(days=e.host.config.callbacks.max_age_days).total_seconds())
                return result
            except Exception as error:
                if capability:
                    from app.telegram_bots.work_callback_denials import callback_facts

                    async with e.e.sf() as reader:
                        latest = await reader.get(TelegramCallbackRow, control.id)
                        actual = callback_facts(latest)
                    error.add_note(
                        repr(
                            {
                                "callback_differences": {
                                    key: (captured.item.payload["work_callback_denial"]["callback"].get(key), value) for key, value in actual.items() if captured.item.payload["work_callback_denial"]["callback"].get(key) != value
                                },
                                "consumed": str(latest.consumed_at),
                                "closed_reason": latest.closed_reason,
                            }
                        )
                    )
                source_errors.append(error)
                raise

        monkeypatch.setattr(source, "read_part", read)

        async def authorize(captured, **kwargs):
            if change == "quiet" and not changed:
                assert kwargs["silent"] is False
                changed.append(change)
                async with e.e.sf.begin() as writer:
                    settings = await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)
                    settings.quiet_enabled = True
                    minute = e.clock.now().hour * 60 + e.clock.now().minute
                    settings.quiet_start_minute, settings.quiet_end_minute = minute, (minute + 60) % 1440
            return await actual_authorize(captured, **kwargs)

        monkeypatch.setattr(source, "authorize", authorize)
        before = len(e.normal.calls)
        assert await e.channel.handle_update(press(e, 6802, control.id, message_id=control.message_id))
        await e.channel._drain_ingress()
        from app.telegram_bots.producer import DeliveryClaimRetired
        from app.telegram_bots.work_callback_denials import validate_intent

        async with e.e.sf() as reader:
            denial = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_callback_denial"]["callback"]["id"].as_string() == control.id))
            assert denial is not None
            intent = denial.payload["work_callback_denial"]
        assert validate_intent(intent) == intent
        if change is not None and change.startswith("replay-"):
            from app.telegram_bots.callbacks import CallbackRefused, _row

            if change == "replay-sent":
                await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
            async with e.e.sf() as reader:
                saved = await reader.get(TelegramOutboxRow, denial.id)
                fields = ("payload", "parts", "state", "claim_owner", "claim_id", "claimed_at", "message_id")
                original_delivery = deepcopy({key: getattr(saved, key) for key in fields})
            async with e.e.sf.begin() as writer:
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
            restrict("agents:write")
            with pytest.raises(CallbackRefused) as refused:
                await source.offer(e.link, _row(control), guard=e.host.notifications.chat.guard(e.channel, e.link), query_id=intent["query_id"])
            assert refused.value.key is None
            async with e.e.sf() as reader:
                saved = await reader.get(TelegramOutboxRow, denial.id)
                assert {key: getattr(saved, key) for key in fields} == original_delivery
            changed.append(change)
            before = len(e.normal.calls)
        for damage in ("query", "callback", "signature"):
            invalid = deepcopy(intent)
            if damage == "query":
                invalid["query_id"] = "q" * 257
            elif damage == "callback":
                invalid["callback"]["unexpected"] = True
            else:
                invalid["signature"] = "g" * 64
            with pytest.raises(DeliveryClaimRetired):
                validate_intent(invalid)
        if change == "quiet":
            async with e.e.sf.begin() as writer:
                (await writer.get(TelegramBotLinkRow, e.link.id)).last_update_at = e.clock.now() - timedelta(seconds=e.host.config.outbox.recent_act_seconds + 1)
        withheld = change in {"inspect", "use", "reserved", "ack", "age", "replay-pending", "replay-sent", "closed-before", "closed-other", "closed-owner"}
        if withheld:
            for _ in range(3):
                e.clock.advance(5)
                await e.host.sender.tick()
                await e.host.sender.drain()
            assert changed == [change]
        else:
            try:
                await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
            except TimeoutError:
                if source_errors:
                    raise source_errors[0]
                raise
        from app.telegram_bots.authority import get_local_provider

        supervisor = await get_local_provider().get_user(e.instance.supervisor.subject_id)
        sent = [call.parameters for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
        key = "denied.start" if entry == "work_start" else "denied.other" if capability else "assign.no_access"
        if withheld:
            assert not any("Permission to start" in call["text"] or "To give" in call["text"] for call in sent)
        else:
            assert sent and sent[-1]["text"] == render(key, "en-US", name=e.instance.name, supervisor=supervisor.email)
            if change is not None and not capability:
                assert changed == [change]
            if change == "quiet":
                assert sent[-1]["disable_notification"] is True
        assert all(work["objective"] not in call["text"] for call in sent)
        async with e.e.sf() as reader:
            if change == "inspect":
                retired = await reader.get(TelegramOutboxRow, denial.id)
                assert retired.state == "skipped", (retired.state, retired.skip_reason)
            assert await reader.get(TelegramActRow, control.id) is None
            promise = await reader.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == control.id))
            assert (promise is not None) is (change == "closed-owner")
            if promise is not None:
                assert promise.run_id is None and promise.work_id == work["id"]
            event = await reader.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == control.id))
            assert (event is not None) is (change == "closed-owner")
            if event is not None:
                assert event.work_id == work["id"]
            actual = await reader.get(TelegramCallbackRow, control.id)
            if actual is None:
                assert change == "age"
            else:
                assert actual.consumed_at is None or capability and actual.closed_reason == "superseded" or change in {"closed-before", "closed-other", "closed-owner"}
                assert ("work_action_id" in actual.payload) is (change == "reserved")
        assert not e.http_errors and not e.held_errors
