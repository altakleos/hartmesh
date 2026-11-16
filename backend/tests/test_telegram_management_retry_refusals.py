"""Current Retry refusals preserve an actual uncertain command and Native owner."""

import asyncio

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_compose_turn_support import _delivered, render_compose_config
from _telegram_work_worker_support import installed_worker
from sqlalchemy import select
from test_telegram_retained_management_refusals import button, management_preview, tap
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.acts import ActsWorker
from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired
from app.telegram_bots.render import render
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["edit", "cancel"])
async def test_current_management_retry_denial_preserves_unknown_owner(
    capture_environment, monkeypatch, tmp_path, kind, *, change="manage", race=None, invalid=None, known=False, lifecycle_invalid=None, restore_notice=False, inactive_rejection=None
):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        work, control = await management_preview(e, kind, monkeypatch, reserve=True)
        sdk = e.normal.do_request

        async def recorded_sdk(url, method, request_data=None, **kwargs):
            if url.rsplit("/", 1)[-1] == "editMessageText" and not e.normal.replies["editMessageText"]:
                values = request_data.parameters
                e.normal.enqueue("editMessageText", result={"message_id": values["message_id"], "date": 1_893_456_000, "chat": {"id": values["chat_id"], "type": "private"}, "text": values["text"]})
            return await sdk(url, method, request_data, **kwargs)

        monkeypatch.setattr(e.normal, "do_request", recorded_sdk)
        calls = []
        actual_command = e.host.acts.work.command

        async def unknown_command(**kwargs):
            calls.append(kwargs["request"].model_dump(mode="json", exclude_unset=True))
            raise ConnectionError("Command response unavailable")

        monkeypatch.setattr(e.host.acts.work, "command", unknown_command)
        await tap(e, control, 7080)
        original = await e.host.acts._work_record(control.id)
        assert original.state == "pending" and original.attempts == 1 and len(calls) == 1
        e.clock.advance(3601)
        worker = ActsWorker(e.host.acts)
        try:
            await worker.tick()
        finally:
            await worker.stop()
        original = await e.host.acts._work_record(control.id)
        assert original.state == "uncertain" and original.attempts == 1
        guard = e.host.notifications.chat.guard(e.channel, e.link)
        identity = control.payload["anchor_outbox_id"]

        async def observe_current():
            while True:
                await e.host.sender.drain()
                try:
                    return await e.host.notifications.chat.management.observe(e.link, identity, guard=guard)
                except ProductSourceRetired:
                    # An actual sender/observer may have refreshed the row
                    # during the previous read. Re-read its current ownership.
                    await asyncio.sleep(0.05)

        await asyncio.wait_for(observe_current(), 10)
        await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
        retry = await button(e, "act_check", target=original.id)
        assert retry.message_id is not None and retry.payload["presentation_pending"] is False

        async def change_authority():
            if change in {"use", "paused", "retired"}:
                from deerflow.persistence.agent_instances.model import AgentInstanceRow

                async with e.e.sf.begin() as writer:
                    if change == "use":
                        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 2
                    else:
                        (await writer.get(AgentInstanceRow, e.instance.id)).status = "suspended" if change == "paused" else "archived"
            elif change in {"manage", "inspect"}:
                async with e.e.sf.begin() as writer:
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1 if change == "inspect" else 3
            else:
                from deerflow.config.app_config import peek_loaded_app_config
                from deerflow.config.authorization_config import AuthorizationConfig

                policy = {"routes": {"allow": "*", "deny": ["agents:read" if change == "read-capability" else "runs:cancel" if change == "cancel-capability" else "agents:write"]}}
                authorization = AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": policy, "user": policy}}}})
                monkeypatch.setattr(peek_loaded_app_config(), "authorization", authorization)

        if known:
            from app.telegram_bots.work_actions import thaw
            from deerflow.agent_instances.work_contract import WorkCommand
            from deerflow.spaces.contract import PrincipalRef

            async with e.host.notifications._maintenance_lock, e.host.notifications._send_lock:
                await e.host.sender.drain()
                await actual_command(actor=PrincipalRef("human", e.link.owner_user_id), instance_id=e.instance.id, work_id=original.work_id, request=WorkCommand.model_validate(thaw(original.body)), can_stop=True)
                await change_authority()
                assert await e.host.acts._work_record(original.id) == original
        else:
            await change_authority()
        if inactive_rejection is not None:
            from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
            from app.telegram_bots.authority import AccessOff, CeilingRefused
            from deerflow.agent_instances.contract import AgentDenied
            from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
            from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

            actual_service = e.app.state.agent_instances
            try:
                if inactive_rejection == "binding":
                    async with e.e.sf.begin() as writer:
                        (await writer.get(TelegramBotLinkRow, e.link.id)).binding_generation += 1
                elif inactive_rejection == "account":
                    await SQLiteUserRepository(e.e.sf).disable_identity(LOCAL_ACCOUNT_ISSUER, e.link.owner_user_id)
                else:
                    assert inactive_rejection == "service"
                    e.app.state.agent_instances = object()
                with pytest.raises((AgentDenied, AccessOff, CeilingRefused)):
                    await e.host.acts.receipt_for(original, allow_inactive=True)
                assert await e.host.acts._work_record(original.id) == original and len(calls) == 1
            finally:
                e.app.state.agent_instances = actual_service
            return
        if lifecycle_invalid is not None:
            from contextvars import ContextVar

            from app.telegram_bots.callbacks import CallbackRefused
            from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

            async with e.e.sf.begin() as writer:
                if lifecycle_invalid == "consumed":
                    (await writer.get(TelegramCallbackRow, retry.id)).consumed_at = e.clock.now()
                else:
                    assert lifecycle_invalid == "ack"
                    parent = await writer.get(TelegramOutboxRow, retry.payload["anchor_outbox_id"])
                    parent.parts = [{**part, "message_ids": []} for part in parent.parts]
            active = ContextVar("new-lifecycle-gesture", default=False)
            reached = []
            actual_receipt, actual_notice = e.host.acts.receipt_for, e.host.notice_instance_unavailable

            async def read_receipt(*args, **kwargs):
                if active.get():
                    reached.append("receipt")
                return await actual_receipt(*args, **kwargs)

            async def notice(*args, **kwargs):
                if active.get():
                    reached.append("notice")
                return await actual_notice(*args, **kwargs)

            monkeypatch.setattr(e.host.acts, "receipt_for", read_receipt)
            monkeypatch.setattr(e.host, "notice_instance_unavailable", notice)
            token = active.set(True)
            try:
                with pytest.raises((CallbackRefused, ProductSourceRetired, PermanentDeliveryError)):
                    await e.host.notifications.chat.retry_denials.offer(e.link, retry, guard=guard, query_id="invalid-lifecycle-retry")
            finally:
                active.reset(token)
            assert not reached and await e.host.acts._work_record(original.id) == original and len(calls) == 1
            return
        restored = []
        if restore_notice:
            from contextvars import ContextVar

            from deerflow.persistence.agent_instances.model import AgentInstanceRow

            protecting = ContextVar("new-retry-status-protection", default=False)
            actual_protect, actual_policy = e.host.authority.protect_notice, e.host.authority.instance_send_policy

            async def protect(context):
                token = protecting.set(True)
                try:
                    return await actual_protect(context)
                finally:
                    protecting.reset(token)

            async def policy_check(link, **kwargs):
                if protecting.get() and kwargs.get("expected_lifecycle") == "agent.paused" and not restored:

                    async def restore():
                        if not restored:
                            restored.append(True)
                            async with e.e.sf.begin() as writer:
                                (await writer.get(AgentInstanceRow, e.instance.id)).status = "active"

                    kwargs["before_snapshot"] = restore
                return await actual_policy(link, **kwargs)

            monkeypatch.setattr(e.host.authority, "protect_notice", protect)
            monkeypatch.setattr(e.host.authority, "instance_send_policy", policy_check)
        if invalid is not None:
            from app.telegram_bots.callbacks import CallbackRefused
            from app.telegram_bots.work_actions import thaw
            from deerflow.agent_instances.work_contract import WorkCommand
            from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
            from deerflow.spaces.contract import PrincipalRef

            selected_original = original
            if invalid == "retarget":
                async with e.e.sf.begin() as writer:
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 7
                selected_original = await second_unknown_withdrawal(e)
            # Settle the actual Native command while retaining the unknown journal.
            # Hold the real maintenance lock so no observer reconciles it first.
            async with e.host.notifications._maintenance_lock, e.host.notifications._send_lock:
                await e.host.sender.drain()
                async with e.e.sf.begin() as writer:
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 7
                await actual_command(actor=PrincipalRef("human", e.link.owner_user_id), instance_id=e.instance.id, work_id=selected_original.work_id, request=WorkCommand.model_validate(thaw(selected_original.body)), can_stop=True)
                async with e.e.sf.begin() as writer:
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
                    control_row = await writer.get(TelegramCallbackRow, retry.id)
                    if invalid == "retarget":
                        control_row.target_id = selected_original.id
                        control_row.payload = {**control_row.payload, "act_id": selected_original.id}
                    elif invalid == "consumed":
                        control_row.consumed_at = e.clock.now()
                    elif invalid == "expired":
                        from datetime import timedelta

                        control_row.expires_at = e.clock.now() - timedelta(seconds=1)
                    elif invalid == "target":
                        from uuid import uuid4

                        control_row.target_id = uuid4().hex
                    else:
                        assert invalid == "ack"
                        parent = await writer.get(TelegramOutboxRow, retry.payload["anchor_outbox_id"])
                        parent.parts = [{**part, "message_ids": []} for part in parent.parts]
                assert await e.host.acts._work_record(original.id) == original
                assert await e.host.acts._work_record(selected_original.id) == selected_original
                if invalid == "retarget":
                    async with e.e.sf() as reader:
                        retry = await reader.get(TelegramCallbackRow, retry.id)
                reached = []

                async def forbidden_receipt_lookup(*args, **kwargs):
                    reached.append(True)
                    raise AssertionError("invalid Retry reached receipt settlement")

                def native_facts(rows):
                    return [tuple(getattr(row, key) for key in ("id", "work_id", "activation_id", "requester_id", "status", "thread_id", "run_id")) for row in rows]

                async with e.e.sf() as reader:
                    before_acts = set(await reader.scalars(select(TelegramActRow.id)))
                    before_native = native_facts(await reader.scalars(select(AgentWorkAttemptRow).order_by(AgentWorkAttemptRow.id)))
                    before_events = set(await reader.scalars(select(AgentWorkEventRow.id)))
                cancelled = []

                async def forbidden_cancel(*args, **kwargs):
                    cancelled.append(True)
                    raise AssertionError("invalid Retry dispatched cancellation")

                monkeypatch.setattr("app.gateway.work_execution.dispatch_work_stop", forbidden_cancel)
                monkeypatch.setattr(e.host.acts, "receipt_for", forbidden_receipt_lookup)
                monkeypatch.setattr(e.host.acts, "reconcile_receipt", forbidden_receipt_lookup)
                with pytest.raises((CallbackRefused, ProductSourceRetired, PermanentDeliveryError)):
                    await e.host.notifications.chat.retry_denials.offer(e.link, retry, guard=guard, query_id="invalid-current-retry")
                assert not reached and await e.host.acts._work_record(original.id) == original
                async with e.e.sf() as reader:
                    assert await reader.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == selected_original.id)) is not None
                    assert await reader.get(TelegramActRow, retry.id) is None
                    assert set(await reader.scalars(select(TelegramActRow.id))) == before_acts
                    assert set(await reader.scalars(select(AgentWorkEventRow.id))) == before_events
                    assert native_facts(await reader.scalars(select(AgentWorkAttemptRow).order_by(AgentWorkAttemptRow.id))) == before_native
                assert not cancelled
                assert await e.host.acts._work_record(selected_original.id) == selected_original
            return
        raced = []
        if race in {"restore", "receipt"}:
            from contextvars import ContextVar

            finalising = ContextVar("retry-denial-finalising", default=False)
            actual_authorize, actual_settings = e.host.work_retry_denials.authorize, e.host.settings.get

            async def final_authorize(captured, **kwargs):
                token = finalising.set(True)
                try:
                    return await actual_authorize(captured, **kwargs)
                finally:
                    finalising.reset(token)

            async def change_after_settings(owner):
                result = await actual_settings(owner)
                if finalising.get() and not raced:
                    raced.append(race)
                    async with e.e.sf.begin() as writer:
                        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 7
                    if race == "receipt":
                        from app.telegram_bots.work_actions import thaw
                        from deerflow.agent_instances.work_contract import WorkCommand
                        from deerflow.spaces.contract import PrincipalRef

                        await actual_command(actor=PrincipalRef("human", e.link.owner_user_id), instance_id=e.instance.id, work_id=original.work_id, request=WorkCommand.model_validate(thaw(original.body)), can_stop=True)
                        async with e.e.sf.begin() as writer:
                            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
                return result

            monkeypatch.setattr(e.host.work_retry_denials, "authorize", final_authorize)
            monkeypatch.setattr(e.host.settings, "get", change_after_settings)
        if race == "age":

            async def paused_tick():
                return 0

            monkeypatch.setattr(e.host.sender, "tick", paused_tick)
        before = len(e.normal.calls)
        await tap(e, retry, 7081)
        for _ in range(4):
            e.clock.advance(1)
            await e.host.sender.tick()
            await e.host.sender.drain()
        key = "access.agent_off" if change in {"use", "read-capability"} else "agent.paused" if change == "paused" else "agent.retired" if change == "retired" else "denied.change" if kind == "edit" else "denied.withdraw"
        from app.telegram_bots.authority import get_local_provider

        supervisor = await get_local_provider().get_user(e.instance.supervisor.subject_id)
        expected = render(key, "en-US", name=e.instance.name, supervisor=str(supervisor.email), product=e.host.product_name)
        sent = [call.parameters.get("text") for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
        if race == "age":
            from app.telegram_bots.retention import sweep_rows
            from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

            e.clock.advance(31 * 86400)
            later = e.clock.now()
            await e.host.callbacks.sweep(later)
            await sweep_rows(e.e.sf, later, callback_max_age_days=e.host.callbacks.config.callbacks.max_age_days)
            async with e.e.sf() as reader:
                fixed = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_retry_denial"]["act_id"].as_string() == original.id))
                assert fixed is not None and fixed.state == "pending" and all(not part["message_ids"] for part in fixed.parts)
                assert await reader.get(TelegramCallbackRow, retry.id) is not None
                assert await reader.get(TelegramOutboxRow, retry.payload["anchor_outbox_id"]) is not None
            assert not sent
        elif race is None:
            if restore_notice:
                assert restored == [True] and expected not in sent
                assert render("tap.failed", "en-US") not in sent
            else:
                assert expected in sent
        else:
            assert raced == [race] and expected not in sent
        if known and change in {"paused", "retired"}:
            current = await e.host.acts._work_record(original.id)
            assert current.state == "done" and current.body == original.body and current.attempts == original.attempts
            assert current.stop_dispatch["attempts"] == 0
        elif race == "receipt":
            current = await e.host.acts.reconcile_receipt(original.id)
            assert current.state == "done" and current.attempts == original.attempts and current.body == original.body
        else:
            current = await e.host.acts._work_record(original.id)
            if race == "age":
                assert (current.state, current.attempts, current.body, current.stop_dispatch, current.cancel_requested_at) == (original.state, original.attempts, original.body, original.stop_dispatch, original.cancel_requested_at)
            else:
                assert current == original
        assert len(calls) == 1
        async with e.e.sf() as reader:
            native = await reader.get(AgentWorkAttemptRow, e.native_original.attempt_id)
            assert native is not None and native.run_id is None
            event = await reader.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == original.id))
            assert (event is not None) is (race == "receipt" or known)
            assert await reader.get(TelegramActRow, retry.id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["inspect", "capability", "cancel-capability"])
async def test_management_retry_checks_current_grants_and_capabilities(capture_environment, monkeypatch, tmp_path, change):
    await test_current_management_retry_denial_preserves_unknown_owner(capture_environment, monkeypatch, tmp_path, "cancel", change=change)


@pytest.mark.asyncio
@pytest.mark.parametrize("race", ["restore", "receipt", "age"])
async def test_retry_denial_keeps_final_authority_receipt_and_retention(capture_environment, monkeypatch, tmp_path, race):
    await test_current_management_retry_denial_preserves_unknown_owner(capture_environment, monkeypatch, tmp_path, "cancel", race=race)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["consumed", "expired", "target", "ack"])
async def test_invalid_retry_cannot_reconcile_a_known_native_receipt(capture_environment, monkeypatch, tmp_path, invalid):
    await test_current_management_retry_denial_preserves_unknown_owner(capture_environment, monkeypatch, tmp_path, "cancel", invalid=invalid)


async def second_unknown_withdrawal(e):
    """A second genuine callback-owned command, never a seeded journal."""
    from uuid import uuid4

    from _telegram_bot_test_support import update_payload
    from _telegram_compose_turn_support import PERSON
    from telegram import Update

    from deerflow.agent_instances.work_contract import DelegateWork
    from deerflow.agent_instances.work_execution import WorkExecution
    from deerflow.agent_instances.work_execution_contract import ActivateWork
    from deerflow.spaces.contract import PrincipalRef

    actor = PrincipalRef("human", e.link.owner_user_id)
    service = e.host.acts.work
    work = await service.delegate(actor=actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review a second guide", success_criteria="List verified corrections"))
    conversation = await e.app.state.agent_conversations.create(actor=actor, instance_id=e.instance.id, creation_id=uuid4().hex)
    execution = await e.app.state.agent_conversations.execution(actor=actor, thread_id=conversation["thread_id"])
    promise = await WorkExecution(service).reserve(
        execution=execution, work_id=work["id"], request=ActivateWork(operation_id=uuid4().hex, expected_revision=work["revision"], expected_assignment_revision=work["assignment_revision"], thread_id=execution.thread_id)
    )
    assert promise.run_id is None
    # Complete B's actual preview before freezing its later receipt boundary.
    assert await e.channel.handle_update(Update.de_json(update_payload(7180, message_id=7180, account=PERSON, text="/work"), e.channel.bot))
    await e.channel._drain_ingress()
    await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
    await tap(e, await button(e, "work_details", target=work["id"]), 7181)
    await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
    await tap(e, await button(e, "work_withdraw", target=work["id"]), 7182)
    await asyncio.wait_for(_delivered(e.e, e.host, e.clock), 30)
    control = await button(e, "work_withdraw_confirm", target=work["id"])
    await tap(e, control, 7183)
    entered = await e.host.acts._work_record(control.id)
    assert entered.kind == "cancel" and entered.state == "pending" and entered.attempts == 1
    e.clock.advance(3601)
    worker = ActsWorker(e.host.acts)
    try:
        await worker.tick()
    finally:
        await worker.stop()
    entered = await e.host.acts._work_record(control.id)
    assert entered.state == "uncertain" and entered.attempts == 1
    return entered


@pytest.mark.asyncio
async def test_retry_cannot_substitute_another_unknown_journal_with_known_receipt(capture_environment, monkeypatch, tmp_path):
    await test_current_management_retry_denial_preserves_unknown_owner(capture_environment, monkeypatch, tmp_path, "cancel", invalid="retarget")
