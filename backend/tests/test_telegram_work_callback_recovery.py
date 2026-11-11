"""Work callback recovery retains known refusals and current routing choices."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_callback_dispatch import install_dispatch, press_work
from test_telegram_work_control_admission import admissions
from test_telegram_work_routing_recovery import send, setup
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow


@pytest.mark.asyncio
@pytest.mark.parametrize("after_admission", [False, True])
@pytest.mark.parametrize("protected", [False, True])
async def test_actual_general_routing_refreshes_a_withdrawn_recipient(work_delivery, monkeypatch, after_admission, protected):
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    routing = await setup(e, monkeypatch, protected=protected)
    await send(e)
    if protected:
        from test_telegram_work_interaction_delivery import guard_for

        async with e.db.sessions() as reader:
            parent = await reader.get(TelegramOutboxRow, e.identity)
            show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"]))
        await e.recovery.reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=971, message_thread_id=None, guard=await guard_for(e))
        await send(e, endpoint="editMessageText")
    plans = WorkOutcomePlans(e.plans, admissions(e).acts, routing=routing)
    dispatcher = install_dispatch(e, plans)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"]))

    async def withdraw():
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1

    if after_admission:
        original = dispatcher.routing.stage

        async def stage(*args, **kwargs):
            saved = await original(*args, **kwargs)
            await withdraw()
            return saved

        monkeypatch.setattr(dispatcher.routing, "stage", stage)
    else:
        await withdraw()
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).notify = "none"
    e.sender.proactive = False
    await press_work(e, callback, message_id=971)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        assert parent.state == "pending"
        assert parent.payload["work_request_basis"]["routing"]["recipients"] == []
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is not None
        saved = await reader.get(TelegramActRow, callback.id)
        if after_admission:
            assert saved.state == "refused" and saved.reason == "recipient-ineligible" and saved.attempts == 1
        else:
            assert saved is None
    await send(e, endpoint="editMessageText")
    assert not e.dispatch_errors, e.dispatch_errors
    call = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"][-1]
    assert "Nobody else can answer" in call.parameters["text"]


@pytest.mark.asyncio
async def test_actual_known_refusal_refreshes_changed_review_without_another_command(work_delivery, monkeypatch):
    from test_telegram_work_refusal_recovery import comment, refused

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans

    e = work_delivery
    controls, _record, request, _seen, callback, original = await refused(e, monkeypatch)
    await comment(e, request)
    plans = WorkOutcomePlans(e.plans, controls.acts)
    install_dispatch(e, plans)

    async def unexpected(*args, **kwargs):
        pytest.fail("Changed review must be shown before any new command")

    monkeypatch.setattr(plans.acts.work, "command", unexpected)
    await press_work(e, callback, message_id=callback.message_id)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, original.id)
        assert saved.state == "conflict" and saved.cancel_requested_at is not None
        assert saved.attempts == original.attempts
        preview = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:work-refusal-refresh:" + callback.id))
        assert preview is not None and preview.state == "pending"
        assert "revised source" in "".join(part["html"] for part in preview.parts)
        assert preview.parts[0]["html"].startswith("This changed since the message was sent.")
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is not None
    from test_telegram_work_preview_recovery import dispatch
    from test_telegram_work_typing_prompts import clear_old

    e.identity = preview.id
    await clear_old(e)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        fresh = await reader.get(TelegramOutboxRow, preview.id)
        assert fresh.state == "sent", (fresh.failed_reason, e.dispatch_errors)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == fresh.parts[-1]["callback_group_id"])))
        assert controls and all(not row.payload["presentation_pending"] for row in controls)


@pytest.mark.asyncio
@pytest.mark.parametrize("journal", ["pending", "missing"])
async def test_routing_refresh_cannot_replace_unresolved_or_collected_operation(work_delivery, monkeypatch, journal):
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import DeliveryRetry
    from app.telegram_bots.work_actions import same_body
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans

    e = work_delivery
    routing = await setup(e, monkeypatch)
    await send(e)
    plans = WorkOutcomePlans(e.plans, admissions(e).acts, routing=routing)
    dispatcher = install_dispatch(e, plans)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"]))
    scope = dict(chat_id=e.link.chat_id, message_id=971, message_thread_id=None, guard=await guard_for(e))
    action = await dispatcher.routing.stage(e.link, callback.id, **scope)
    if journal == "missing":
        async with e.db.sessions.begin() as writer:
            await writer.delete(await writer.get(TelegramActRow, action.id))
    with pytest.raises((CallbackRefused, DeliveryRetry)):
        await dispatcher.routing_recovery.refresh_choice(e.link, callback.id, **scope)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is None
        assert (await reader.get(TelegramOutboxRow, e.identity)).state == "sent"
        saved = await reader.get(TelegramActRow, action.id)
        if journal == "missing":
            assert saved is None
        else:
            assert saved.state == "pending" and same_body(saved.body, action.body)


@pytest.mark.asyncio
async def test_routing_refresh_rechecks_candidates_after_outer_guard_await(work_delivery, monkeypatch):
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.links import prepare_publication_guard
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    routing = await setup(e, monkeypatch)
    await send(e)
    plans = WorkOutcomePlans(e.plans, admissions(e).acts, routing=routing)
    dispatcher = install_dispatch(e, plans)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"]))
    base = await guard_for(e)
    reached = []

    class Withdraw:
        async def prepare(self):
            await prepare_publication_guard(base)

        def current(self):
            base.current()

        async def check(self, writer):
            current = await base.check(writer)
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
            await writer.flush()
            reached.append(True)
            return current

    with pytest.raises(ProductSourceRetired):
        await dispatcher.routing_recovery.refresh_choice(e.link, callback.id, chat_id=e.link.chat_id, message_id=971, message_thread_id=None, guard=Withdraw())
    assert reached
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is None
        assert (await reader.get(TelegramOutboxRow, e.identity)).state == "sent"
        assert (await reader.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("after_admission", [False, True])
async def test_actual_changed_confirmation_shows_complete_current_review(work_delivery, monkeypatch, after_admission):
    from test_telegram_work_outcome_reads import review_confirmation
    from test_telegram_work_preview_recovery import dispatch
    from test_telegram_work_refusal_recovery import comment
    from test_telegram_work_typing_prompts import clear_old

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans

    e = work_delivery
    _work, request, _seen, callback = await review_confirmation(e)
    plans = WorkOutcomePlans(e.plans, admissions(e).acts)
    dispatcher = install_dispatch(e, plans)
    if after_admission:
        original = dispatcher.controls.stage

        async def stage(*args, **kwargs):
            saved = await original(*args, **kwargs)
            await comment(e, request)
            return saved

        monkeypatch.setattr(dispatcher.controls, "stage", stage)
    else:
        await comment(e, request)
    await press_work(e, callback, message_id=802)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, callback.id)
        assert saved.state == "conflict" if after_admission else saved is None
        fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:work-stale-refresh:" + callback.id))
        assert fresh is not None and fresh.state == "pending"
        assert fresh.parts[0]["html"].startswith("This changed since the message was sent.")
        assert "revised source" in "".join(part["html"] for part in fresh.parts)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == fresh.parts[-1]["callback_group_id"])))
        assert all(row.payload["presentation_pending"] for row in controls)
    e.identity = fresh.id
    await clear_old(e)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, fresh.id)).state == "sent", e.dispatch_errors
        assert all([not (await reader.get(TelegramCallbackRow, row.id)).payload["presentation_pending"] for row in controls])


@pytest.mark.asyncio
@pytest.mark.parametrize("journal", ["pending", "missing"])
async def test_stale_review_recovery_never_replaces_an_unresolved_or_missing_journal(work_delivery, monkeypatch, journal):
    from test_telegram_work_interaction_delivery import guard_for
    from test_telegram_work_refusal_recovery import comment, refused

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import DeliveryRetry
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans

    e = work_delivery
    controls, _record, request, _seen, callback, original = await refused(e, monkeypatch, unknown=True)
    await comment(e, request)
    dispatcher = install_dispatch(e, WorkOutcomePlans(e.plans, controls.acts))
    if journal == "missing":
        async with e.db.sessions.begin() as writer:
            await writer.delete(await writer.get(TelegramActRow, original.id))
    with pytest.raises((CallbackRefused, DeliveryRetry)):
        await dispatcher.stale.refresh(e.link, callback.id, chat_id=e.link.chat_id, message_id=callback.message_id, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is None
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("notice:work-stale-refresh:"))))
        saved = await reader.get(TelegramActRow, original.id)
        assert saved.state == "pending" if journal == "pending" else saved is None


@pytest.mark.asyncio
async def test_known_refusal_keeps_typed_decision_and_input_lineage(work_delivery, monkeypatch):
    from test_telegram_inbox import incoming
    from test_telegram_work_actions import management_request
    from test_telegram_work_control_admission import stage
    from test_telegram_work_input_capture import capture
    from test_telegram_work_input_previews import previews
    from test_telegram_work_interaction_delivery import open_view, selected
    from test_telegram_work_preview_recovery import dispatch, install
    from test_telegram_work_refusal_recovery import comment
    from test_telegram_work_typing_prompts import clear_old

    from app.telegram_bots.inbox import InboxRepository
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.agent_instances.contract import AgentConflict
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    install(e)
    _record, request, _seen = await management_request(e, "decision")
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="retained-decision")
    await dispatch(e, 801)
    e.identity = await open_view(e, await selected(e, "decide_own"))
    await clear_old(e)
    await dispatch(e, 802)
    async with e.db.sessions() as reader:
        prompt_id = (await reader.get(TelegramOutboxRow, e.identity)).payload["work_request_basis"]["prompt_id"]
    inbox = InboxRepository(e.db.sessions)
    mark, _ = await inbox.accept(e.link, incoming(message=900, update_id=1900, account=e.link.chat_id, reply=802), sanitized_input={"text": "Keep my typed decision and its note."})
    captured = await capture(e, inbox, mark, prompt_id)
    e.identity = await previews(e, inbox, mark)
    await dispatch(e, 803)
    callback = await selected(e, "decide_confirm")
    controls = admissions(e)
    action = await stage(e, controls, callback)

    async def refuse(*args, **kwargs):
        raise AgentConflict("The attempt is still closing", reason="attempt-unresolved")

    monkeypatch.setattr(controls.acts.work, "command", refuse)
    first = await controls.acts.check(action.id)
    await comment(e, request)
    install_dispatch(e, WorkOutcomePlans(e.plans, controls.acts))
    await press_work(e, callback, message_id=803)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, action.id)
        assert saved.state == "pending" and saved.reason == "attempt-unresolved" and saved.attempts == first.attempts
        assert saved.body["note"] == captured.route_basis["work_input"]["text"]
        assert saved.cancel_requested_at is None
        retained = await reader.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))
        assert retained.route_basis["work_input"] == captured.route_basis["work_input"] and not retained.route_complete
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is None
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("notice:work-refusal-refresh:"))))


async def refreshed_choice(e, monkeypatch):
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    routing = await setup(e, monkeypatch)
    await send(e)
    dispatcher = install_dispatch(e, WorkOutcomePlans(e.plans, admissions(e).acts, routing=routing))
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, e.identity)
        callback = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"]))
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
    await press_work(e, callback, message_id=971)
    return dispatcher, callback


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["consumption", "topic", "ack", "group"])
async def test_final_sdk_rechecks_consumed_routing_choice_proof(work_delivery, monkeypatch, damage):
    from copy import deepcopy

    e = work_delivery
    _dispatcher, callback = await refreshed_choice(e, monkeypatch)
    sources = e.channel._services.work_routing_sources
    original, reached = sources.settings.get, []

    async def withdraw(*args, **kwargs):
        value = await original(*args, **kwargs)
        if not reached:
            reached.append(True)
            async with e.db.sessions.begin() as writer:
                control = await writer.get(TelegramCallbackRow, callback.id)
                if damage == "consumption":
                    control.consumed_at = None
                elif damage == "topic":
                    control.message_thread_id = 7
                elif damage == "group":
                    control.group_id = "a" * 32
                else:
                    row = await writer.get(TelegramOutboxRow, e.identity)
                    parts = deepcopy(row.parts)
                    parts[0]["message_ids"] = [True]
                    row.parts = parts
        return value

    monkeypatch.setattr(sources.settings, "get", withdraw)
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert reached and e.dispatch_errors
    assert not any(call.endpoint == "editMessageText" for call in e.channel.bot.request.calls)


@pytest.mark.asyncio
async def test_consumed_choice_survives_sweep_and_later_refresh(work_delivery, monkeypatch):
    from test_telegram_work_interaction_delivery import guard_for

    e = work_delivery
    dispatcher, callback = await refreshed_choice(e, monkeypatch)
    await send(e, endpoint="editMessageText")
    callbacks = e.channel._services.callbacks
    e.clock.advance((callbacks.config.callbacks.sweep_after_days + 1) * 86400)
    await callbacks.sweep(e.clock())
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, callback.id) is not None
    await dispatcher.routing_recovery.refresh_anchor(e.link, e.identity, expected_version=1, guard=await guard_for(e))
    await send(e, endpoint="editMessageText")
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent" and row.class_ == "reply", (row.failed_reason, e.dispatch_errors)
        assert row.payload["work_request_basis"]["choice_origin"] == callback.id
