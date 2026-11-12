"""A conclusively stale admitted note retains its exact failed operation."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_callback_dispatch import install_dispatch, press_work
from test_telegram_work_control_admission import admissions, stage
from test_telegram_work_input_refresh import deliver_refreshed
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_retained_drafts import draft
from test_telegram_work_retired_reviews import close_review
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", [False, True])
async def test_conclusive_old_confirmation_recovers_without_repeating_its_command(work_delivery, monkeypatch, replacement):
    e = work_delivery
    request, _inbox, origin = await draft(e, monkeypatch)
    control = await selected(e, "changes_confirm")
    controls = admissions(e)
    old = await stage(e, controls, control)
    next_request = await close_review(e, request, replacement=replacement)
    failed = await controls.acts.check(old.id)
    assert failed.state == "conflict"
    original = controls.acts.work.command

    async def unexpected(*args, **kwargs):
        pytest.fail("A conclusive old operation must not be resubmitted")

    monkeypatch.setattr(controls.acts.work, "command", unexpected)
    install_dispatch(e, WorkOutcomePlans(e.plans, controls.acts))
    previous = e.identity
    await press_work(e, control, message_id=803)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is not None
        saved = await reader.get(TelegramActRow, old.id)
        assert saved.state == "conflict" and saved.attempts == failed.attempts and same_body(saved.body, old.body)
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1
        mark = await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
        assert mark.route_complete is not replacement
        if replacement:
            fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.id != previous, TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == origin.operation_ids[0]))
            assert fresh is not None and fresh.request_id == next_request["id"]
        else:
            fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:work-review-retired:" + origin.operation_ids[0]))
        e.identity = fresh.id
    await deliver_refreshed(e, 804)
    if replacement:
        monkeypatch.setattr(controls.acts.work, "command", original)
        fresh = await stage(e, controls, await selected(e, "changes_confirm"))
        assert fresh.id != old.id and fresh.body["note"] == origin.route_basis["work_input"]["text"]
        assert (await controls.acts.check(fresh.id)).state == "done"


@pytest.mark.asyncio
async def test_known_non_effect_confirmation_checks_original_operation_after_closure(work_delivery, monkeypatch):
    from deerflow.agent_instances.contract import AgentConflict

    e = work_delivery
    request, _inbox, origin = await draft(e, monkeypatch)
    control = await selected(e, "changes_confirm")
    controls = admissions(e)
    old = await stage(e, controls, control)
    original = controls.acts.work.command

    async def refused(*args, **kwargs):
        raise AgentConflict("The attempt is not settled", reason="attempt-unresolved")

    monkeypatch.setattr(controls.acts.work, "command", refused)
    first = await controls.acts.check(old.id)
    assert first.state == "pending" and first.reason == "attempt-unresolved"
    monkeypatch.setattr(controls.acts.work, "command", original)
    await close_review(e, request, replacement=False)
    install_dispatch(e, WorkOutcomePlans(e.plans, controls.acts))
    await press_work(e, control, message_id=803)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, old.id)
        assert saved.state == "conflict" and saved.attempts == first.attempts + 1 and same_body(saved.body, old.body)
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is not None
        assert (await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))).route_complete
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", [False, True])
async def test_direct_note_closure_after_admission_recovers_original_input(work_delivery, monkeypatch, replacement):
    from test_telegram_inbox import incoming
    from test_telegram_work_input_capture import prompt
    from test_telegram_work_typed_input import install_inputs

    from app.telegram_bots import inbox

    e = work_delivery
    request, _prompt, _seen = await prompt(e)
    monkeypatch.setattr(inbox, "_now", e.clock)
    _plans, inputs = install_inputs(e)
    original, admitted = inputs.admissions.stage, []

    async def close_after_admission(*args, **kwargs):
        result = await original(*args, **kwargs)
        admitted.append(result)
        await close_review(e, request, replacement=replacement)
        return result

    monkeypatch.setattr(inputs.admissions, "stage", close_after_admission)
    update = incoming(message=900, update_id=1900, account=e.link.chat_id, reply=802)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    assert len(admitted) == 1
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_complete is not replacement
        saved = await reader.get(TelegramActRow, admitted[0].id)
        assert saved.state == "conflict" and same_body(saved.body, admitted[0].body)
        if replacement:
            assert await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == mark.operation_ids[0])) is not None
        else:
            assert await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:work-review-retired:" + mark.operation_ids[0])) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["unknown", "pending", "missing", "body", "scope"])
async def test_unproven_confirmation_never_recovers_as_conclusive(work_delivery, monkeypatch, damage):
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.work_review_recovery import WorkReviewRecovery

    e = work_delivery
    request, _inbox, origin = await draft(e, monkeypatch)
    control = await selected(e, "changes_confirm")
    controls = admissions(e)
    old = await stage(e, controls, control)
    await close_review(e, request, replacement=True)
    assert (await controls.acts.check(old.id)).state == "conflict"
    async with e.db.sessions.begin() as writer:
        saved = await writer.get(TelegramActRow, old.id)
        if damage == "missing":
            await writer.delete(saved)
        elif damage == "body":
            saved.body = {**saved.body, "note": "A different note"}
        elif damage == "scope":
            saved.binding_generation += 1
        else:
            saved.state = "uncertain" if damage == "unknown" else "pending"
    with pytest.raises(CallbackRefused):
        await WorkReviewRecovery(e.plans, controls=controls).from_callback(e.link, control.id, chat_id=e.link.chat_id, message_id=803, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is None
        assert not (await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))).route_complete


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", [False, True])
@pytest.mark.parametrize("damage", ["body", "kind"])
async def test_final_recovery_rechecks_conclusive_journal(work_delivery, monkeypatch, replacement, damage):
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_review_recovery import WorkReviewRecovery

    e = work_delivery
    request, _inbox, origin = await draft(e, monkeypatch)
    control = await selected(e, "changes_confirm")
    controls = admissions(e)
    old = await stage(e, controls, control)
    await close_review(e, request, replacement=replacement)
    assert (await controls.acts.check(old.id)).state == "conflict"
    base, changed = await guard_for(e), []

    class Drift:
        @property
        def captured_link(self):
            return base.captured_link

        def current(self):
            return base.current()

        async def check(self, writer):
            current = await base.check(writer)
            row = await writer.get(TelegramCallbackRow, control.id)
            if row.consumed_at is not None and not changed:
                changed.append(True)
                saved = await writer.get(TelegramActRow, old.id)
                if damage == "body":
                    saved.body = {**saved.body, "note": "Changed at final fence"}
                else:
                    saved.kind = "accept"
                await writer.flush()
            return current

    with pytest.raises(ProductSourceRetired):
        await WorkReviewRecovery(e.plans, controls=controls).from_callback(e.link, control.id, chat_id=e.link.chat_id, message_id=803, message_thread_id=None, guard=Drift())
    assert changed
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is None
        assert not (await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))).route_complete
        saved = await reader.get(TelegramActRow, old.id)
        assert saved.kind == old.kind and same_body(saved.body, old.body)
