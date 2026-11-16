"""Real saved request receipts own fresh full previews after Start ends."""

import asyncio
from copy import deepcopy

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_compose_turn_support import render_compose_config
from _telegram_work_worker_support import installed_worker
from sqlalchemy import delete, select
from test_telegram_default_composite_controls import decision_confirmation, voice_confirmation
from test_telegram_retained_management_refusals import button, tap
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.render import render
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decision", "voice"])
@pytest.mark.parametrize("ending", ["refused", "expired"])
async def test_saved_request_opens_fresh_resume_without_replaying_primary(capture_environment, monkeypatch, tmp_path, kind, ending, *, mode="stage"):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        sdk = e.normal.do_request

        async def acknowledged_edit(url, method, request_data=None, **kwargs):
            if url.rsplit("/", 1)[-1] == "editMessageText" and not e.normal.replies["editMessageText"]:
                values = request_data.parameters
                e.normal.enqueue("editMessageText", result={"message_id": values["message_id"], "date": 1_893_456_000, "chat": {"id": values["chat_id"], "type": "private"}, "text": values["text"]})
            return await sdk(url, method, request_data, **kwargs)

        monkeypatch.setattr(e.normal, "do_request", acknowledged_edit)
        work, _request, control = await (decision_confirmation(e) if kind == "decision" else voice_confirmation(e, monkeypatch))
        views = e.host.work_callbacks.outcomes.continuation_views
        if mode in {"changed", "marker", "retention", "retention-corrupt", "long"}:
            e.host.work_callbacks.outcomes.continuation_views = None
        actual_resume, captured = e.host.acts.resume_continuation, []

        async def primary_delivered(identity):
            while True:
                e.clock.advance(1)
                await e.host.sender.tick()
                await e.host.sender.drain()
                async with e.e.sf() as reader:
                    parent = await reader.get(TelegramActRow, identity)
                    primary = await reader.get(TelegramOutboxRow, parent.continuation["request_activation"]["outcome_id"])
                    assert primary.state not in {"failed", "skipped"}
                    if primary.state == "sent" and primary.parts and all(part["state"] == "sent" and part["message_ids"] for part in primary.parts):
                        return
                await asyncio.sleep(0.05)

        async def end_child(identity, **kwargs):
            if identity == control.id and not captured:
                await asyncio.wait_for(primary_delivered(identity), 30)
                async with e.e.sf() as reader:
                    parent = await reader.get(TelegramActRow, identity)
                    grant = await reader.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))
                    captured.append((deepcopy(parent.body), deepcopy(parent.result), parent.attempts, deepcopy(parent.continuation["activation_continuation"]), grant.permissions))
                if ending == "refused":
                    async with e.e.sf.begin() as writer:
                        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
                else:
                    e.clock.advance(3601)
            return await actual_resume(identity, **kwargs)

        monkeypatch.setattr(e.host.acts, "resume_continuation", end_child)
        await tap(e, control, 7080)
        assert len(captured) == 1
        async with e.e.sf() as reader:
            parent = await reader.get(TelegramActRow, control.id)
            assert parent.state == "done" and parent.continuation["activation_continuation"]["phase"] == ending
        if ending == "refused":
            async with e.e.sf.begin() as writer:
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = captured[0][4]
        await asyncio.wait_for(primary_delivered(control.id), 30)
        if mode in {"changed", "long"}:
            from uuid import uuid4

            from deerflow.agent_instances.work import AgentWork
            from deerflow.agent_instances.work_contract import WorkCommand
            from deerflow.config.app_config import peek_loaded_app_config
            from deerflow.config.authorization_config import AuthorizationConfig
            from deerflow.spaces.contract import PrincipalRef

            service, actor = AgentWork(e.app.state.agent_instances), PrincipalRef("human", e.link.owner_user_id)
            current = await service.get(actor=actor, instance_id=e.instance.id, work_id=work["id"])
            work = await service.command(
                actor=actor,
                instance_id=e.instance.id,
                work_id=work["id"],
                request=WorkCommand(
                    operation_id=uuid4().hex,
                    expected_revision=current["revision"],
                    expected_assignment_revision=current["assignment_revision"],
                    action="edit",
                    objective=("Review the updated guide " * 155).strip() if mode == "long" else "Review the updated guide",
                    **({"success_criteria": ("Report verified findings " * 150).strip()} if mode == "long" else {}),
                ),
            )
            policy = {"routes": {"allow": "*", "deny": ["runs:create"] if mode == "changed" else []}}
            monkeypatch.setattr(
                peek_loaded_app_config(), "authorization", AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": policy, "user": policy}}}})
            )
        if mode == "marker":
            async with e.e.sf.begin() as writer:
                parent = await writer.get(TelegramActRow, control.id)
                marker = deepcopy(parent.continuation["work_outcome"])
                marker["outbox_id"] = "f" * 32
                parent.continuation = {**parent.continuation, "work_outcome": marker}
        e.host.work_callbacks.outcomes.continuation_views = views
        guard = e.host.notifications.chat.guard(e.channel, e.link)
        if mode == "marker":
            from app.telegram_bots.producer import ProductSourceRetired

            with pytest.raises(ProductSourceRetired):
                await e.host.work_callbacks.outcomes.observe_start_refusal(e.link, control.id, guard=guard)
            return
        if mode in {"retention", "retention-corrupt"}:
            from datetime import timedelta

            from app.telegram_bots.retention import sweep_rows

            tick = e.host.sender.tick

            async def paused_tick():
                return 0

            monkeypatch.setattr(e.host.sender, "tick", paused_tick)
            await e.host.work_callbacks.outcomes.observe_start_refusal(e.link, control.id, guard=guard)
            async with e.e.sf.begin() as writer:
                preview = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_view_intent"]["origin"]["kind"].as_string() == "continuation"))
                assert preview is not None and preview.state == "pending"
                primary_id = preview.payload["work_view_intent"]["origin"]["anchor_outbox_id"]
                damaged = deepcopy(preview.payload)
                if mode == "retention":
                    damaged["work_view_intent"].pop("origin")
                    damaged["work_news_basis"]["source"]["work"].pop("origin")
                else:
                    damaged["work_view_intent"]["origin"]["signature"] = "f" * 64
                    damaged["work_news_basis"]["source"]["work"]["origin"]["signature"] = "f" * 64
                preview.payload = damaged
                preview_id = preview.id
            later = e.clock.now() + timedelta(days=401)
            await e.host.callbacks.sweep(later)
            await sweep_rows(e.e.sf, later, callback_max_age_days=e.host.callbacks.config.callbacks.max_age_days)
            async with e.e.sf() as reader:
                assert await reader.get(TelegramOutboxRow, preview_id) is not None
                assert await reader.get(TelegramActRow, control.id) is not None, "a damaged pending preview must retain its own primary history"
                assert await reader.get(TelegramOutboxRow, primary_id) is not None
            monkeypatch.setattr(e.host.sender, "tick", tick)
            return

        async def observe_current():
            from app.telegram_bots.producer import ProductSourceRetired

            while True:
                await e.host.sender.drain()
                try:
                    return await e.host.work_callbacks.outcomes.observe_start_refusal(e.link, control.id, guard=guard)
                except ProductSourceRetired:
                    await asyncio.sleep(0.05)

        await asyncio.wait_for(observe_current(), 10)

        async def preview_delivered():
            while True:
                e.clock.advance(1)
                await e.host.sender.tick()
                await e.host.sender.drain()
                async with e.e.sf() as reader:
                    current = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_view_intent"]["origin"]["kind"].as_string() == "continuation"))
                    if current is not None:
                        assert current.state not in {"failed", "skipped"}, (current.failed_reason, current.skip_reason)
                        if current.state == "sent" and current.parts and all(part["state"] == "sent" and part["message_ids"] for part in current.parts):
                            return
                await asyncio.sleep(0.05)

        await asyncio.wait_for(preview_delivered(), 30)
        async with e.e.sf() as reader:
            preview = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_view_intent"]["origin"]["kind"].as_string() == "continuation"))
            assert preview is not None, "a settled saved primary needs an explicit current Resume preview"
            assert preview.state == "sent" and all(part["state"] == "sent" and part["message_ids"] for part in preview.parts)
            if mode == "long":
                assert len(preview.parts) >= 2
            assert preview.work_id == work["id"] and preview.payload["work_view_intent"]["origin"]["act_id"] == control.id
            texts = "".join(part.get("html", "") for part in preview.parts)
            assert work["objective"] in texts and work["success_criteria"] in texts
            if ending == "expired":
                assert render("start.saved_expired", "en-US") in texts
            parent = await reader.get(TelegramActRow, control.id)
            body, result, counter, state, _permissions = captured[0]
            assert (parent.body, parent.result, parent.attempts) == (body, result, counter)
            assert parent.continuation["activation_continuation"]["authorized_at"] == state["authorized_at"]
            assert parent.continuation["activation_continuation"]["child_body"] is None and not parent.operation_ids
            assert await reader.get(TelegramActRow, state["child_id"]) is None
            assert await reader.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == state["child_id"])) is None
        if mode == "changed":
            assert not any(button.get("kind") == "work_start" for line in preview.payload["product_keyboard_basis"] for button in line)
            return
        fresh = await button(e, "work_start", target=work["id"])
        assert fresh.id not in {control.id, state["child_id"]}
        assert fresh.payload["anchor_outbox_id"] == preview.id
        if mode in {"primary-ack", "missing-origin"}:
            from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired

            async with e.e.sf.begin() as writer:
                if mode == "primary-ack":
                    source = await writer.get(TelegramOutboxRow, preview.payload["work_view_intent"]["origin"]["anchor_outbox_id"])
                    parts = deepcopy(source.parts)
                    parts[-1]["message_ids"] = []
                    source.parts = parts
                else:
                    source = await writer.get(TelegramOutboxRow, preview.id)
                    payload = deepcopy(source.payload)
                    payload["work_view_intent"].pop("origin")
                    payload["work_news_basis"]["source"]["work"].pop("origin")
                    source.payload = payload
            with pytest.raises((ProductSourceRetired, PermanentDeliveryError)):
                await e.host.notifications.chat.starts.stage(e.link, fresh.id, chat_id=e.link.chat_id, message_id=fresh.message_id, message_thread_id=fresh.message_thread_id, guard=guard)
            async with e.e.sf() as reader:
                assert await reader.get(TelegramActRow, fresh.id) is None
            return
        staged = await e.host.notifications.chat.starts.stage(e.link, fresh.id, chat_id=e.link.chat_id, message_id=fresh.message_id, message_thread_id=fresh.message_thread_id, guard=guard)
        assert staged.id == fresh.id and staged.attempts == 0 and staged.body["expected_assignment_revision"] == fresh.payload["assignment_revision"]
        async with e.e.sf() as reader:
            journal = await reader.get(TelegramActRow, staged.id)
            assert journal.continuation["news_origin"]["anchor_outbox_id"] == preview.id
        async with e.e.sf() as reader:
            assert await reader.get(TelegramActRow, state["child_id"]) is None
            assert (await reader.get(TelegramActRow, control.id)).body == body
            witness = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "work_activation_origin", TelegramCallbackRow.payload["work_action_id"].as_string() == fresh.id))
            assert witness is not None
        if mode in {"native", "missing-marker", "missing-witness"}:
            from unittest.mock import AsyncMock

            from app.gateway import services

            # The real Gateway/WorkExecution path reserves a Native promise;
            # only the subsequent host launch fails. No model call is scripted.
            launch = AsyncMock(side_effect=RuntimeError("Host admission unavailable"))
            monkeypatch.setattr(services, "start_run", launch)
            if mode in {"missing-marker", "missing-witness"}:
                from app.telegram_bots.work_news_carriers import news_witness_id

                async with e.e.sf.begin() as writer:
                    if mode == "missing-marker":
                        journal = await writer.get(TelegramActRow, staged.id)
                        journal.continuation = {key: value for key, value in journal.continuation.items() if key != "news_origin"}
                    else:
                        await writer.execute(delete(TelegramCallbackRow).where(TelegramCallbackRow.id == news_witness_id(staged.id)))
            try:
                await e.host.acts.check(staged.id)
            except ValueError:
                if mode != "missing-marker":
                    raise
            if mode in {"missing-marker", "missing-witness"}:
                async with e.e.sf() as reader:
                    assert await reader.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == staged.id)) is None
                    assert (await reader.get(TelegramActRow, control.id)).body == body
                assert launch.await_count == 0
                return
            async with e.e.sf() as reader:
                native = await reader.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == staged.id))
                assert native is not None and native.requester_id == e.link.owner_user_id and native.run_id is None
                assert native.request == dict(staged.body)
                assert await reader.get(TelegramActRow, state["child_id"]) is None
                primary = await reader.get(TelegramActRow, control.id)
                assert (primary.body, primary.result, primary.attempts) == (body, result, counter)
            assert launch.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decision", "voice"])
async def test_fresh_resume_reaches_native_with_retained_primary(capture_environment, monkeypatch, tmp_path, kind):
    await test_saved_request_opens_fresh_resume_without_replaying_primary(capture_environment, monkeypatch, tmp_path, kind, "expired", mode="native")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["changed", "marker", "retention", "retention-corrupt"])
async def test_continuation_preview_preserves_independent_receipt_proof(capture_environment, monkeypatch, tmp_path, mode):
    await test_saved_request_opens_fresh_resume_without_replaying_primary(capture_environment, monkeypatch, tmp_path, "decision", "expired", mode=mode)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["primary-ack", "missing-origin", "missing-marker", "missing-witness"])
async def test_fresh_resume_rejects_missing_original_proof(capture_environment, monkeypatch, tmp_path, mode):
    await test_saved_request_opens_fresh_resume_without_replaying_primary(capture_environment, monkeypatch, tmp_path, "decision", "expired", mode=mode)


@pytest.mark.asyncio
async def test_updated_assignment_has_complete_multipart_resume(capture_environment, monkeypatch, tmp_path):
    await test_saved_request_opens_fresh_resume_without_replaying_primary(capture_environment, monkeypatch, tmp_path, "decision", "expired", mode="long")
