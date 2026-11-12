"""Receipt observation cannot publish an obsolete outcome after a fresh preview."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("recovery", ["known_refusal", "conflict", "retained_note", "routing", "direct_note", "direct_closed", "direct_replaced", "direct_closed_failed"])
async def test_observer_preserves_newer_complete_preview(work_delivery, monkeypatch, recovery):
    from test_telegram_work_callback_recovery import (
        test_actual_changed_confirmation_shows_complete_current_review,
        test_actual_known_refusal_refreshes_changed_review_without_another_command,
    )
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.work_outcome_recovery import WorkOutcomeRecovery

    e = work_delivery
    if recovery == "known_refusal":
        await test_actual_known_refusal_refreshes_changed_review_without_another_command(e, monkeypatch)
    elif recovery == "conflict":
        await test_actual_changed_confirmation_shows_complete_current_review(e, monkeypatch, True)
    elif recovery == "routing":
        from test_telegram_work_callback_recovery import test_actual_general_routing_refreshes_a_withdrawn_recipient

        await test_actual_general_routing_refreshes_a_withdrawn_recipient(e, monkeypatch, True, False)
    elif recovery in {"direct_note", "direct_closed", "direct_replaced", "direct_closed_failed"}:
        from test_telegram_inbox import incoming
        from test_telegram_work_input_capture import prompt
        from test_telegram_work_input_refresh import deliver_refreshed
        from test_telegram_work_refusal_recovery import comment
        from test_telegram_work_typed_input import install_inputs

        from deerflow.persistence.telegram_bots.model import TelegramActRow

        request, _prompt_id, _seen = await prompt(e)
        plans, _inputs = install_inputs(e)
        original = plans.acts.work.command
        calls = []

        async def change_before_commit(*args, **kwargs):
            calls.append(kwargs["request"].operation_id)
            assert len(calls) == 1
            if recovery == "direct_note":
                await comment(e, request)
            else:
                from test_telegram_work_retired_reviews import close_review

                with monkeypatch.context() as domain:
                    domain.setattr(plans.acts.work, "command", original)
                    await close_review(e, request, replacement=recovery == "direct_replaced")
            return await original(*args, **kwargs)

        monkeypatch.setattr(plans.acts.work, "command", change_before_commit)
        if recovery == "direct_closed_failed":
            from test_telegram_work_outcome_delivery import clear_outcome_cleanup

            preview = _inputs.previews.open

            async def failed_old_outcome(link, captured, **kwargs):
                act = kwargs["conclusive_act"]
                e.outcome_identity = await plans.plan(link, act.id, guard=await guard_for(e))
                await clear_outcome_cleanup(e)
                e.channel.bot.request.enqueue("sendMessage", body={"ok": False, "error_code": 400, "description": "Bad Request: message is too long"}, status=400)
                e.clock.advance(10)
                assert await e.sender.tick() == 1
                await e.sender.drain()
                async with e.db.sessions() as reader:
                    assert (await reader.get(TelegramOutboxRow, e.outcome_identity)).state == "failed"
                return await preview(link, captured, **kwargs)

            monkeypatch.setattr(_inputs.previews, "open", failed_old_outcome)
        assert await e.channel.handle_update(incoming(message=900, update_id=1900, account=e.link.chat_id, reply=802))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            action = await reader.get(TelegramActRow, calls[0])
            assert action.state == "conflict"
            if recovery in {"direct_closed", "direct_closed_failed"}:
                e.identity = await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key == "notice:work-review-retired:" + action.id))
            else:
                e.identity = await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == action.id))
            assert e.identity is not None
        await deliver_refreshed(e, 804)
        if recovery == "direct_closed_failed":
            from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

            async with e.db.sessions() as reader:
                old = await reader.get(TelegramOutboxRow, e.outcome_identity)
                mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
                assert old.state == "skipped" and old.skip_reason == "superseded"
                assert mark.route_complete and mark.state == "done"
    else:
        from test_telegram_work_callback_dispatch import install_dispatch
        from test_telegram_work_callback_recovery import typed_refused
        from test_telegram_work_input_refresh import deliver_refreshed
        from test_telegram_work_refusal_recovery import comment

        from app.telegram_bots.work_input_refresh import WorkInputRefresh
        from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans

        controls, request, callback, _captured, _first = await typed_refused(e, monkeypatch)
        await comment(e, request)
        e.identity = await WorkInputRefresh(controls).refresh(e.link, callback.id, chat_id=e.link.chat_id, message_id=803, message_thread_id=None, guard=await guard_for(e))
        await deliver_refreshed(e, 804)
        install_dispatch(e, WorkOutcomePlans(e.plans, controls.acts))
    plans = e.channel._services.work_callbacks.outcomes
    async with e.db.sessions() as reader:
        before = set(await reader.scalars(select(TelegramOutboxRow.id)))
        preview = await reader.get(TelegramOutboxRow, e.identity)
        assert preview.state == "sent"
        presentation = preview.payload
    observer = WorkOutcomeRecovery(plans)
    try:
        await observer.tick(e.link, guard=await guard_for(e))
        await observer.tick(e.link, guard=await guard_for(e))
    finally:
        await observer.stop()
    async with e.db.sessions() as reader:
        assert set(await reader.scalars(select(TelegramOutboxRow.id))) == before
        fresh = await reader.get(TelegramOutboxRow, e.identity)
        assert fresh.state == "sent" and fresh.payload == presentation

    from datetime import timedelta

    from app.telegram_bots.retention import _sweep_acts, sweep_rows
    from app.telegram_bots.work_outcome_delivery import delivery_receipt
    from app.telegram_bots.work_outcome_transfer import transfer_receipt
    from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow

    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramCallbackRow)):
            row.created_at = e.clock() - timedelta(days=181)
            if row.consumed_at is not None:
                row.consumed_at = e.clock() - timedelta(days=8)
        for row in await writer.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.state == "sent")):
            row.created_at = row.closed_at = e.clock() - timedelta(days=31)
        actions = list(await writer.scalars(select(TelegramActRow)))
        assert len(actions) == 1
        action_id = actions[0].id
        assert transfer_receipt(actions[0].continuation) is not None
        assert delivery_receipt(actions[0].continuation) is None
    await e.channel._services.callbacks.sweep(e.clock())
    swept = await sweep_rows(e.db.sessions, e.clock(), callback_max_age_days=180)
    assert swept["outbox"] > 0
    async with e.db.sessions() as reader:
        remaining_preview = await reader.get(TelegramOutboxRow, e.identity)
        if recovery in {"direct_note", "retained_note", "direct_replaced"}:
            # An unfinished input still pins its preview independently of the
            # expired controls. Its transfer proof is retained on the Act too.
            assert remaining_preview is not None
        else:
            assert remaining_preview is None
        if recovery == "direct_note":
            assert await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)) is not None
        retained = set(await reader.scalars(select(TelegramOutboxRow.id)))
    restarted = WorkOutcomeRecovery(plans)
    try:
        await restarted.tick(e.link, guard=await guard_for(e))
    finally:
        await restarted.stop()
    async with e.db.sessions() as reader:
        assert set(await reader.scalars(select(TelegramOutboxRow.id))) == retained
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramActRow, action_id)).created_at = e.clock() - timedelta(days=401)
    assert await _sweep_acts(e.db.sessions, e.clock()) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["before_claim", "final_authority", "failed_delivery"])
async def test_queued_conflict_yields_to_fresh_preview_before_sdk(work_delivery, monkeypatch, boundary):
    from test_telegram_work_control_admission import admissions, stage
    from test_telegram_work_interaction_delivery import guard_for
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup
    from test_telegram_work_outcome_reads import review_confirmation
    from test_telegram_work_refusal_recovery import comment

    from app.telegram_bots import work_outcome_delivery as module
    from app.telegram_bots.work_interaction_delivery import WorkInteractionPlans
    from app.telegram_bots.work_stale_recovery import WorkStaleRecovery

    e = work_delivery
    _work, request, _seen, callback = await review_confirmation(e)
    controls = admissions(e)
    operation = await stage(e, controls, callback)
    await comment(e, request)
    assert (await controls.acts.check(operation.id)).state == "conflict"
    plans = module.WorkOutcomePlans(e.plans, controls.acts)
    e.channel._services.work_outcome_sources = module.WorkOutcomeSources(plans)
    identity = await plans.plan(e.link, operation.id, guard=await guard_for(e))
    assert identity is not None
    refresh = WorkStaleRecovery(controls, WorkInteractionPlans(e.plans))
    refreshed = []

    async def transfer():
        fresh = await refresh.refresh(e.link, callback.id, chat_id=e.link.chat_id, message_id=802, message_thread_id=None, guard=await guard_for(e))
        assert fresh is not None
        refreshed.append(fresh)

    if boundary == "failed_delivery":
        e.outcome_identity = identity
        await clear_outcome_cleanup(e)
        e.channel.bot.request.enqueue("sendMessage", body={"ok": False, "error_code": 400, "description": "Bad Request: message is too long"}, status=400)
        e.clock.advance(10)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, identity)).state == "failed"
    if boundary in {"before_claim", "failed_delivery"}:
        from datetime import timedelta

        await transfer()
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramOutboxRow, refreshed[0])).not_before = e.clock() + timedelta(hours=1)
    else:
        authority = module.current_attention

        async def after_source_read(*args, **kwargs):
            if not refreshed:
                await transfer()
            return await authority(*args, **kwargs)

        monkeypatch.setattr(module, "current_attention", after_source_read)
    e.outcome_identity = identity
    await clear_outcome_cleanup(e)
    before = len(e.channel.bot.request.calls)
    e.clock.advance(10)
    assert await e.sender.tick() == (1 if boundary == "final_authority" else 0)
    await e.sender.drain()
    if boundary == "final_authority":
        # A live claim cannot be retired by the concurrent transition. Its
        # failed send releases the claim; the same explicit refresh then wins.
        assert not refreshed
        await transfer()
    assert len(refreshed) == 1
    assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint in {"sendMessage", "editMessageText"}]
    async with e.db.sessions() as reader:
        old = await reader.get(TelegramOutboxRow, identity)
        fresh = await reader.get(TelegramOutboxRow, refreshed[0])
        assert old.state == "skipped" and old.skip_reason == "superseded"
        assert fresh.state == "pending" and "revised source" in "".join(part["html"] for part in fresh.parts)


@pytest.mark.asyncio
async def test_canonical_receipt_wins_after_presentation_transfer(work_delivery, monkeypatch):
    from test_telegram_work_callback_recovery import test_actual_general_routing_refreshes_a_withdrawn_recipient
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.work_actions import request_for
    from app.telegram_bots.work_outcome_recovery import WorkOutcomeRecovery
    from deerflow.agent_instances.contract import PrincipalRef
    from deerflow.agent_instances.human_input import HumanInput
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = work_delivery
    await test_actual_general_routing_refreshes_a_withdrawn_recipient(e, monkeypatch, True, False)
    plans = e.channel._services.work_callbacks.outcomes
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 3
        saved = await writer.scalar(select(TelegramActRow))
        identity, body, request_id = saved.id, saved.body, saved.request_id
        assert saved.state == "refused"
    # Actual canonical success becomes visible independently of the old journal.
    await HumanInput(plans.acts.work).command(actor=PrincipalRef("human", e.link.owner_user_id), request_id=request_id, request=request_for("route", body, identity=identity))

    async def forbidden(*args, **kwargs):
        pytest.fail("Receipt observation must not execute a command")

    monkeypatch.setattr(HumanInput, "command", forbidden)
    observer = WorkOutcomeRecovery(plans)
    try:
        await observer.tick(e.link, guard=await guard_for(e))
    finally:
        await observer.stop()
    async with e.db.sessions() as reader:
        current = await reader.get(TelegramActRow, identity)
        assert current.state == "done" and current.body == body


@pytest.mark.asyncio
async def test_refresh_waits_for_actual_sdk_ack_before_transferring_outcome(work_delivery):
    import asyncio

    from test_telegram_work_control_admission import admissions, stage
    from test_telegram_work_interaction_delivery import guard_for
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup
    from test_telegram_work_outcome_reads import review_confirmation
    from test_telegram_work_refusal_recovery import comment

    from app.telegram_bots.producer import DeliveryRetry
    from app.telegram_bots.work_interaction_delivery import WorkInteractionPlans
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources, delivery_receipt
    from app.telegram_bots.work_outcome_transfer import transfer_receipt
    from app.telegram_bots.work_stale_recovery import WorkStaleRecovery
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = work_delivery
    _work, request, _seen, callback = await review_confirmation(e)
    controls = admissions(e)
    operation = await stage(e, controls, callback)
    await comment(e, request)
    assert (await controls.acts.check(operation.id)).state == "conflict"
    plans = WorkOutcomePlans(e.plans, controls.acts)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    e.outcome_identity = await plans.plan(e.link, operation.id, guard=await guard_for(e))
    await clear_outcome_cleanup(e)
    release = asyncio.Event()
    reply = e.channel.bot.request.enqueue("sendMessage", result={"message_id": 803, "date": 1, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Changed"}, release=release)
    refresh = WorkStaleRecovery(controls, WorkInteractionPlans(e.plans))

    async def transfer():
        return await refresh.refresh(e.link, callback.id, chat_id=e.link.chat_id, message_id=802, message_thread_id=None, guard=await guard_for(e))

    try:
        e.clock.advance(10)
        assert await e.sender.tick() == 1
        await asyncio.wait_for(reply.started.wait(), 5)
        with pytest.raises(DeliveryRetry):
            await transfer()
        async with e.db.sessions() as reader:
            act = await reader.get(TelegramActRow, operation.id)
            assert transfer_receipt(act.continuation) is None
            assert (await reader.get(TelegramOutboxRow, e.outcome_identity)).claim_id is not None
    finally:
        release.set()
        await e.sender.drain()
    async with e.db.sessions() as reader:
        assert delivery_receipt((await reader.get(TelegramActRow, operation.id)).continuation) is not None
    fresh = await transfer()
    async with e.db.sessions() as reader:
        old = await reader.get(TelegramOutboxRow, e.outcome_identity)
        assert old.state == "sent" and old.parts[-1]["message_ids"] == [803]
        assert transfer_receipt((await reader.get(TelegramActRow, operation.id)).continuation)["outbox_id"] == fresh
