"""Fixed repair notices retain their original purpose across delivery retries."""

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db
from sqlalchemy import event, select
from telegram import Chat, Message, Update, User

from app.telegram_bots.inbox import InboxRepository
from app.telegram_bots.producer import Outbox
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio
NOW = datetime(2030, 1, 1, tzinfo=UTC)


def repair_part(*, navigation=True):
    part = {"kind": "text", "source_ref": {"key": "custom_agent.deleted", "values": {"agent": "Fixture AI assistant", "product": "HartMesh"}}}
    if navigation:
        part["url_button"] = {"page": "web.telegram", "url": "https://example.test/workspace?settings=telegram&view=all"}
    return part


async def setup(db):
    link = await bind_link(db, await add_link(db))
    inbox = InboxRepository(db.sessions)
    return link, inbox, Outbox(db.sessions, inbox=inbox, clock=lambda: NOW)


async def origin(inbox, link, identity=102, *, topic=None):
    update = Update(identity, message=Message(identity, NOW, Chat(link.chat_id, "private"), from_user=User(link.telegram_user_id, "Fixture", False), text="synthetic input", message_thread_id=topic))
    row, _ = await inbox.accept(link, update, sanitized_input={"text": "synthetic input"})
    return await inbox.route(row, kind="chat", target_id=link.agent_ref, basis={"agent": link.agent_ref}, operation_ids=[])


async def saved(db, identity):
    async with db.sessions() as session:
        return await session.get(TelegramOutboxRow, identity)


@pytest.mark.parametrize("navigation", [False, True])
async def test_deleted_custom_notice_is_a_fixed_status_with_optional_navigation(db, navigation):
    link, inbox, outbox = await setup(db)
    mark = await origin(inbox, link, topic=33)
    parts = [repair_part(navigation=navigation)]
    identity = await outbox.plan_status_notice(link, source_id="deleted-102", parts=parts, inbox_rows=[mark], finish_route=True)
    first = await saved(db, identity)
    assert first.payload["delivery_kind"] == "status_notice"
    assert first.payload["status_basis"]["parts"] == parts
    assert first.message_thread_id == 33
    assert first.parts[0]["source_ref"] == parts[0]["source_ref"]
    assert ("url_button" in first.parts[0]) is navigation
    assert await outbox.plan_status_notice(link, source_id="deleted-102", parts=parts, inbox_rows=[mark], finish_route=True) == identity
    assert (await saved(db, identity)).parts == first.parts


@pytest.mark.parametrize("change", ["origin", "values", "url", "finish", "empty-origin", "topic"])
async def test_status_replay_cannot_reassign_original_purpose(db, change):
    link, inbox, outbox = await setup(db)
    first = await origin(inbox, link)
    other = await origin(inbox, link, identity=103, topic=33 if change == "topic" else None)
    parts = [{"kind": "text", "source_ref": {"key": "unavailable.fix", "values": {}}}]
    if change == "url":
        parts = [repair_part()]
    origins = [] if change == "empty-origin" else [first]
    identity = await outbox.plan_status_notice(link, source_id="fixed", parts=parts, inbox_rows=origins)
    before = await saved(db, identity)
    changed = deepcopy(parts)
    finish = False
    if change == "topic":
        origins = [replace(first, message_thread_id=33)]
    elif change in {"origin", "empty-origin"}:
        origins = [other]
    elif change == "values":
        changed[0]["source_ref"]["values"]["agent"] = "Another target"
    elif change == "url":
        changed[0]["url_button"]["url"] = "https://example.test/another-purpose"
    else:
        finish = True
    with pytest.raises(ValueError, match="status-source-conflict"):
        await outbox.plan_status_notice(link, source_id="fixed", parts=changed, inbox_rows=origins, finish_route=finish)
    after = await saved(db, identity)
    assert (after.parts, after.payload) == (before.parts, before.payload)
    async with db.sessions() as session:
        untouched = await session.get(TelegramInboundMarkRow, (link.id, other.message_id, link.chat_id))
        assert untouched.delivery_ids == [] and not untouched.route_complete


async def test_status_new_plan_strips_transport_receipts_and_regenerates_server_identity(db):
    link, _inbox, outbox = await setup(db)
    supplied = repair_part()
    receipts = {"sent_at": "forged", "dispatched_at": "forged", "unavailable_reason": "forged", "invalid_token_verification_pending": {"token_revision": 100}, "url_button_fallback": True, "plain_fallback": True}
    server_fields = {"part_id": "forged", "presentation_version": 88, "ordinal": 88, "state": "sent", "message_ids": [999]}
    supplied.update(receipts | server_fields)
    identity = await outbox.plan_status_notice(link, source_id="sanitized", parts=[supplied])
    first = await saved(db, identity)
    assert first.payload["status_basis"]["parts"] == [repair_part()]
    assert all(key not in first.parts[0] for key in receipts)
    assert first.parts[0]["part_id"] != "forged"
    assert (first.parts[0]["presentation_version"], first.parts[0]["ordinal"], first.parts[0]["state"], first.parts[0]["message_ids"]) == (0, 0, "pending", [])
    assert await outbox.plan_status_notice(link, source_id="sanitized", parts=[repair_part()]) == identity


@pytest.mark.parametrize("source,page", [("custom_agent.deleted", "web.memory"), ("web.memory", "web.telegram"), ("unavailable.fix", "web.telegram")])
async def test_navigation_source_and_page_pairs_cannot_be_crossed(db, source, page):
    link, _inbox, outbox = await setup(db)
    supplied = repair_part()
    supplied["source_ref"]["key"], supplied["url_button"]["page"] = source, page
    with pytest.raises(ValueError):
        await outbox.plan_status_notice(link, source_id="crossed", parts=[supplied])
    with pytest.raises(ValueError):
        Outbox._parts([supplied], 0)


@pytest.mark.parametrize("extra", [{"label": "Caller label"}, {"callback_group_id": "forged"}, {"protect_content": False}, {"arbitrary": 1}])
async def test_fixed_status_rejects_arbitrary_fields_and_callback_authority(db, extra):
    link, _inbox, outbox = await setup(db)
    with pytest.raises(ValueError):
        await outbox.plan_status_notice(link, source_id="arbitrary", parts=[repair_part(navigation=False) | extra])


@pytest.mark.parametrize("finish", [1, "yes"])
async def test_status_completion_requires_an_explicit_boolean(db, finish):
    link, _inbox, outbox = await setup(db)
    with pytest.raises(ValueError):
        await outbox.plan_status_notice(link, source_id="finish", parts=[{"kind": "text", "source_ref": {"key": "unavailable.fix", "values": {}}}], finish_route=finish)


async def test_status_replay_uses_immutable_basis_after_url_and_plain_fallbacks(db):
    link, _inbox, outbox = await setup(db)
    identity = await outbox.plan_status_notice(link, source_id="fallback", parts=[repair_part()])
    before = await saved(db, identity)
    for claim_id, method in [("first", outbox.fallback_url_button), ("second", outbox.fallback_to_plain)]:
        async with db.sessions.begin() as session:
            row = await session.get(TelegramOutboxRow, identity)
            row.state, row.claim_owner, row.claim_id, row.claimed_at = "sending", "process", claim_id, NOW
            parts = deepcopy(row.parts)
            parts[0]["state"] = "sending"
            row.parts = parts
        captured = await outbox.capture_sending_part(link_id=link.id, outbox_id=identity, part_id=before.parts[0]["part_id"], claim_owner="process", claim_id=claim_id, token_revision=link.token_revision)
        assert await method(captured)
    mutated = await saved(db, identity)
    assert mutated.parts[0]["url_button_fallback"] and mutated.parts[0]["plain_fallback"]
    assert await outbox.plan_status_notice(link, source_id="fallback", parts=[repair_part()]) == identity
    assert (await saved(db, identity)).parts == mutated.parts


@pytest.mark.parametrize("existing", [False, True])
async def test_fixed_status_refuses_old_token_attempt_but_current_token_can_replay(db, existing):
    link, _inbox, outbox = await setup(db)
    first = await outbox.plan_status_notice(link, source_id="rotation", parts=[repair_part()]) if existing else None
    await db.links.store_token(link.id, "new-wrapped-fixture", actor_id=link.owner_user_id, expected_revision=link.token_revision, token_changed=True)
    with pytest.raises(ValueError, match="link-not-connected"):
        await outbox.plan_status_notice(link, source_id="rotation", parts=[repair_part()])
    current = await db.links.get(link.id)
    identity = await outbox.plan_status_notice(current, source_id="rotation", parts=[repair_part()])
    if existing:
        assert identity == first


@pytest.mark.parametrize("stage", ["nested-lookup", "registration"])
async def test_fixed_status_rechecks_process_attempt_after_writer_sql_before_mutation(db, monkeypatch, stage):
    link, inbox, outbox = await setup(db)
    mark = await origin(inbox, link)
    current = True
    lookups = 0

    def replace_adapter(connection, cursor, statement, parameters, context, many):
        nonlocal current, lookups
        if statement.lstrip().startswith("SELECT") and "telegram_outbox" in statement:
            lookups += 1
            if stage == "nested-lookup" and lookups == 2:
                current = False

    register = inbox.register_delivery

    async def register_then_replace(*args, **kwargs):
        nonlocal current
        await register(*args, **kwargs)
        if stage == "registration":
            current = False

    monkeypatch.setattr(inbox, "register_delivery", register_then_replace)

    event.listen(db.engine.sync_engine, "before_cursor_execute", replace_adapter)
    try:
        with pytest.raises(ValueError, match="stale-status-attempt"):
            await outbox.plan_status_notice(link, source_id="stale-adapter", parts=[repair_part()], inbox_rows=[mark], finish_route=True, check_attempt_current=lambda: current)
    finally:
        event.remove(db.engine.sync_engine, "before_cursor_execute", replace_adapter)
    async with db.sessions() as session:
        assert await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:stale-adapter")) is None
        stored = await session.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))
        assert stored.delivery_ids == [] and not stored.route_complete
    assert lookups >= 2
