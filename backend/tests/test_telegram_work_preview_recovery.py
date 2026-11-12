"""Durable Work previews recover without changing a business operation."""

from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_outbox_workers import Clock
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInstancePolicyRow, TelegramOutboxRow, TelegramPersonSettingsRow


def install(e):
    from app.telegram_bots.work_request_recovery import WorkRequestRecovery
    from app.telegram_bots.work_request_source import WorkRequestSources

    e.recovery = WorkRequestRecovery(e.plans)
    e.channel._services.work_request_sources = WorkRequestSources(reader=e.reader, outbox=e.channel._services.outbox, callbacks=e.channel._services.callbacks, recovery=e.recovery)
    e.clock = Clock()
    e.clock.now = datetime.now(UTC)
    e.sender.outbox.clock = e.clock


async def dispatch(e, identity):
    e.clock.advance(10)
    message(e.channel.bot.request, identity=identity)
    assert await e.sender.tick() == 1
    await e.sender.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["revision", "language", "protection"])
async def test_queued_preview_refreshes_before_any_sdk_content(work_delivery, change):
    e = work_delivery
    install(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, e.identity)
        original_group = row.parts[-1]["callback_group_id"]
        old_revision = row.payload["work_request_basis"]["source"]["seen"]["work_revision"]
        if change == "revision":
            (await writer.get(AgentWorkRow, e.work["id"])).revision += 1
        elif change == "language":
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
        else:
            writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "pending" and row.payload["presentation_version"] == 1, (row.failed_reason, e.dispatch_errors)
        assert row.parts[-1]["callback_group_id"] != original_group
        assert all(control.consumed_at for control in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == original_group)))
        basis = row.payload["work_request_basis"]
        if change == "revision":
            assert basis["source"]["seen"]["work_revision"] == old_revision + 1
        elif change == "language":
            assert basis["locale"] == "zh-CN"
        else:
            assert basis["concealed"] and "Which guide?" not in basis["parts"][0]["html"]
    await dispatch(e, 811)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent", (row.state, row.failed_reason, e.dispatch_errors)
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1
    if change == "protection":
        assert calls[0].parameters["protect_content"] and calls[0].parameters["text"] == "A question is waiting."


async def concealed(e):
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    e.identity = await e.plans.plan(e.link, request_id=e.request["id"], dedupe_key="concealed")
    await dispatch(e, 821)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        return show.id


async def reveal(e, identity):
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    return await e.recovery.reveal(e.link, identity, chat_id=e.link.chat_id, message_id=821, message_thread_id=None, guard=guard)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_protected_reveal_and_missing_anchor_preserve_current_source(work_delivery, missing):
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    install(e)
    show = await concealed(e)
    assert await reveal(e, show) == e.identity
    with pytest.raises(CallbackRefused):
        await reveal(e, show)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.payload["work_request_basis"]["reveal"] == show
        assert row.parts[-1]["kind"] == "edit" and row.parts[-1]["target_message_id"] == 821
        group = row.parts[-1]["callback_group_id"]
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group)))
        original = [(row.id, row.created_at, row.expires_at) for row in controls]
        assert all(row.message_id is None for row in controls)
    e.clock.advance(10)
    if missing:
        e.channel.bot.request.enqueue("editMessageText", body={"ok": False, "error_code": 400, "description": "Bad Request: message to edit not found"}, status=400)
    else:
        message(e.channel.bot.request, endpoint="editMessageText", identity=821)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    if missing:
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, e.identity)
            assert row.state == "pending" and row.payload["presentation_version"] == 2, (row.state, e.dispatch_errors)
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group)))
            assert all(row.message_id is None and row.payload["presentation_pending"] for row in controls)
            assert [(row.id, row.created_at, row.expires_at) for row in controls] == original
        await dispatch(e, 822)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent", (row.state, row.failed_reason, e.dispatch_errors)
        assert row.payload["work_request_basis"]["reveal"] == show
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group)))
        assert all(row.message_id == (822 if missing else 821) and not row.payload["presentation_pending"] for row in controls)
    calls = [call for call in e.channel.bot.request.calls if call.endpoint in {"sendMessage", "editMessageText"}]
    assert "Which guide?" in calls[-1].parameters["text"]
    if missing:
        assert calls[-1].parameters["protect_content"]


@pytest.mark.asyncio
async def test_refresh_preserves_an_uncertain_operation_and_its_controls(work_delivery, monkeypatch):
    from uuid import uuid4

    from app.telegram_bots.acts import Acts
    from deerflow.agent_instances.human_input import HumanInput
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = work_delivery
    install(e)
    acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)

    async def ambiguous(self, **kwargs):
        raise RuntimeError("Unknown command outcome")

    monkeypatch.setattr(HumanInput, "respond", ambiguous)
    identity = uuid4().hex
    act = await acts.respond(e.link, operation_id=identity, request_id=e.request["id"], request_revision=1, assignment_revision=1, text="Original answer")
    assert act.state == "pending"
    async with e.db.sessions.begin() as writer:
        before = (await writer.get(TelegramOutboxRow, e.identity)).payload
        (await writer.get(AgentWorkRow, e.work["id"])).revision += 1
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        journal = await reader.get(TelegramActRow, identity)
        assert row.state == "pending" and row.payload == before
        assert journal.state == "pending" and journal.body["text"] == "Original answer" and journal.attempts == 1
        assert all(control.consumed_at is None for control in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
@pytest.mark.parametrize("race", ["claim", "revision"])
async def test_refresh_rechecks_its_claim_and_source_before_replacing(work_delivery, monkeypatch, race):
    e = work_delivery
    install(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentWorkRow, e.work["id"])).revision += 1
    original = e.recovery._source

    async def change(*args, **kwargs):
        result = await original(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            if race == "claim":
                (await writer.get(TelegramOutboxRow, e.identity)).claim_id = "other-process"
            else:
                (await writer.get(AgentWorkRow, e.work["id"])).revision += 1
        return result

    monkeypatch.setattr(e.recovery, "_source", change)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.payload["presentation_version"] == 0
        if race == "claim":
            assert row.state == "sending" and row.claim_id == "other-process"
        else:
            assert row.state == "pending", (row.state, e.dispatch_errors)
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_protected_full_preview_requires_the_saved_reveal_receipt(work_delivery):
    e = work_delivery
    install(e)
    show = await concealed(e)
    await reveal(e, show)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramCallbackRow, show)).closed_reason = "superseded"
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, e.identity)).state == "skipped"


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup", ["ok", "missing", "retry", "cannot_edit"])
async def test_reveal_reads_new_comments_and_withholds_controls_until_all_chunks_ack(work_delivery, cleanup):
    from uuid import uuid4

    from test_telegram_work_actions import management_request

    from deerflow.agent_instances.human_input_contract import Respond

    e = work_delivery
    install(e)
    e.work, e.request, _seen = await management_request(e, "review")
    show = await concealed(e)
    for index in range(3):
        await e.human.respond(actor=e.actor, request_id=e.request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text=f"Comment {index}: " + "Complete evidence. " * 150))
    await reveal(e, show)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        parts = [part for part in row.parts if part["presentation_version"] == 1]
        group = parts[-1]["callback_group_id"]
        cleanup_id = row.payload["work_request_basis"]["cleanup_id"]
        assert len(parts) > 1 and len(row.payload["work_request_basis"]["source"]["responses"]) == 3
    assert all(part["kind"] == "text" for part in parts)
    e.clock.advance(10)
    if cleanup == "ok":
        message(e.channel.bot.request, endpoint="editMessageReplyMarkup", identity=821)
    else:
        e.channel.bot.request.enqueue(
            "editMessageReplyMarkup",
            status=500 if cleanup == "retry" else 400,
            body={"ok": False, "error_code": 500 if cleanup == "retry" else 400, "description": "Bad Request: message to edit not found" if cleanup == "missing" else "Retry later"},
        )
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == 1
    if cleanup == "cannot_edit":
        async with e.db.sessions() as reader:
            failed = await reader.get(TelegramOutboxRow, cleanup_id)
            assert failed.state == "failed" and "cleanup_absent" not in failed.payload
            assert (await reader.get(TelegramOutboxRow, e.identity)).state == "pending"
        return
    if cleanup == "retry":
        from app.telegram_bots.producer import OutboxSender

        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, cleanup_id)).state == "pending"
        await e.sender.stop()
        e.sender = OutboxSender(e.sender.outbox, run_id="recovery-after-restart", deliver_part=e.sender.deliver_part, eligibility=e.sender.eligibility, proactive=True)
        e.clock.advance(60)
        message(e.channel.bot.request, endpoint="editMessageReplyMarkup", identity=821)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == 1
    if cleanup == "ok":
        from datetime import timedelta

        from app.telegram_bots.retention import _sweep_outbox

        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramOutboxRow, cleanup_id)).closed_at = e.clock.now - timedelta(days=31)
        await _sweep_outbox(e.db.sessions, e.clock.now, 180)
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramOutboxRow, cleanup_id) is not None
    for index in range(len(parts)):
        e.clock.advance(10)
        message(e.channel.bot.request, identity=831 + index)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        async with e.db.sessions() as reader:
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group)))
            if index < len(parts) - 1:
                assert all(row.message_id is None for row in controls)
            else:
                assert all(row.message_id == 831 + index for row in controls), e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint in {"sendMessage", "editMessageText"}][1:]
    assert len(calls) == len(parts)
    assert all(call.endpoint == "sendMessage" and call.parameters["protect_content"] for call in calls)
    assert all(not call.parameters.get("reply_markup") for call in calls[:-1])
    assert calls[-1].parameters["reply_markup"]
    text = "\n".join(call.parameters["text"] for call in calls)
    assert "Comment 0:" in text and "Comment 2:" in text

    if cleanup == "ok":
        await _sweep_outbox(e.db.sessions, e.clock.now, 180)
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramOutboxRow, cleanup_id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("withdraw", ["grant", "policy"])
async def test_reveal_does_not_commit_consent_after_authority_withdrawal(work_delivery, monkeypatch, withdraw):
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    install(e)
    show = await concealed(e)
    original = e.recovery._source

    async def change(*args, **kwargs):
        result = await original(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            if withdraw == "grant":
                (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
            else:
                (await writer.get(TelegramInstancePolicyRow, e.link.agent_ref)).policy = "off"
        return result

    monkeypatch.setattr(e.recovery, "_source", change)
    with pytest.raises(AgentDenied):
        await reveal(e, show)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, show)).consumed_at is None
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent" and row.payload["work_request_basis"]["concealed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["instance_id", "work_id", "request_id"])
async def test_work_source_cannot_cross_the_outbox_target_columns(work_delivery, field):
    from uuid import uuid4

    e = work_delivery
    install(e)
    async with e.db.sessions.begin() as writer:
        setattr(await writer.get(TelegramOutboxRow, e.identity), field, uuid4().hex)
    await dispatch(e, 891)
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_refresh_of_a_sent_multipart_preview_clears_the_old_last_keyboard(work_delivery):
    from uuid import uuid4

    from test_telegram_work_actions import management_request

    from deerflow.agent_instances.human_input_contract import Respond

    e = work_delivery
    install(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.work, e.request, _seen = await management_request(e, "review")
    await e.human.respond(actor=e.actor, request_id=e.request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="Long complete comment. " * 170))
    e.identity = await e.plans.plan(e.link, request_id=e.request["id"], dedupe_key="multipart")
    async with e.db.sessions() as reader:
        count = len((await reader.get(TelegramOutboxRow, e.identity)).parts)
    assert count > 1
    for index in range(count):
        await dispatch(e, 910 + index)
    await e.human.respond(actor=e.actor, request_id=e.request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="New current comment"))
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    await e.recovery.refresh_anchor(e.link, e.identity, expected_version=0, guard=guard)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.payload["presentation_version"] == 1
        cleanup = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["kind"].as_string() == "keyboard_cleanup")))
        assert len(cleanup) == 1
        assert cleanup[0].payload["target_message_id"] == 910 + count - 1
        assert cleanup[0].parts[0]["remove_keyboard"]
        current = [part for part in row.parts if part["presentation_version"] == 1]
        assert current[0]["kind"] == "edit" and current[0]["target_message_id"] == 910
    # A repeated old refresh cannot replace the newly prepared presentation.
    assert await e.recovery.refresh_anchor(e.link, e.identity, expected_version=0, guard=guard) is None


@pytest.mark.asyncio
async def test_reveal_authority_borrows_the_existing_writer_connection(work_delivery, monkeypatch):
    from sqlalchemy import event

    from app.gateway.auth.mode import account_refusal
    from deerflow.spaces.contract import ResolvedPrincipal

    e = work_delivery
    install(e)
    show = await concealed(e)
    original = e.human.agents.directory.human

    class Human:
        async def __call__(self, actor):
            return await original(actor)

        async def in_session(self, actor, session):
            user = await e.live.users.get_user_by_id(actor.subject_id, session=session)
            return ResolvedPrincipal(actor) if user is not None and account_refusal(user, sign_on_only_mode=False) is None else None

    monkeypatch.setattr(e.human.agents.directory, "human", Human())
    active = 0
    maximum = 0

    def checkout(*args):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)

    def checkin(*args):
        nonlocal active
        active -= 1

    event.listen(e.db.engine.sync_engine, "checkout", checkout)
    event.listen(e.db.engine.sync_engine, "checkin", checkin)
    try:
        await reveal(e, show)
    finally:
        event.remove(e.db.engine.sync_engine, "checkout", checkout)
        event.remove(e.db.engine.sync_engine, "checkin", checkin)
    assert maximum == 1 and active == 0


@pytest.mark.asyncio
async def test_reveal_final_outer_guard_cannot_bypass_current_permission(work_delivery):
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    install(e)
    show = await concealed(e)
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    base = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    calls = []

    class Guard:
        current = base.current

        async def check(self, writer):
            result = await base.check(writer)
            calls.append(True)
            if len(calls) == 3:
                (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
                await writer.flush()
            return result

    with pytest.raises(AgentDenied):
        await e.recovery.reveal(e.link, show, chat_id=e.link.chat_id, message_id=821, message_thread_id=None, guard=Guard())
    assert len(calls) == 3
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, show)).consumed_at is None
        assert (await reader.get(TelegramOutboxRow, e.identity)).payload["work_request_basis"]["concealed"]


@pytest.mark.asyncio
async def test_reveal_captures_enabled_route_provider_before_writer(work_delivery, monkeypatch):
    from app.gateway import authz
    from deerflow.authz.provider import AuthzDecision

    e = work_delivery
    install(e)
    show = await concealed(e)
    e.live.config.authorization.enabled = True
    in_writer = [False]
    preparations = []
    checks = []

    class Provider:
        async def aauthorize(self, request):
            return AuthzDecision(allow=True)

    async def prepare(config):
        assert not in_writer[0], "provider constructed while holding the writer"
        preparations.append(True)
        return Provider()

    original = e.recovery._admit

    async def admit(*args, **kwargs):
        in_writer[0] = True
        try:
            checks.append(True)
            return await original(*args, **kwargs)
        finally:
            in_writer[0] = False

    monkeypatch.setattr(authz, "_aget_cached_route_provider", prepare)
    monkeypatch.setattr(e.recovery, "_admit", admit)
    await reveal(e, show)
    assert preparations and len(checks) >= 2


@pytest.mark.parametrize("proof", ["unrelated_skip", "wrong_part", "wrong_target"])
def test_cleanup_dependency_requires_a_real_clear_or_exact_absence_receipt(proof):
    from types import SimpleNamespace

    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_request_source import cleanup_ready

    scope = dict(link_id="link", owner_user_id="owner", binding_generation=1, chat_id=7)
    parent = SimpleNamespace(id="parent", payload={"work_request_basis": {"cleanup_id": "cleanup"}}, **scope)
    part = dict(part_id="a" * 32, presentation_version=0, kind="edit", remove_keyboard=True, target_message_id=123, state="sent", sent_at=datetime.now(UTC).isoformat(), message_ids=[123])
    cleanup = SimpleNamespace(id="cleanup", payload={"kind": "keyboard_cleanup", "work_request_parent_id": "parent", "target_message_id": 123}, parts=[part], state="sent", skip_reason=None, class_="reply", **scope)
    if proof == "unrelated_skip":
        cleanup.state, cleanup.skip_reason = "skipped", "superseded"
    elif proof == "wrong_part":
        part.update(kind="text", remove_keyboard=False)
    else:
        part["target_message_id"] = 456
    with pytest.raises(ProductSourceRetired):
        cleanup_ready(parent, cleanup)
