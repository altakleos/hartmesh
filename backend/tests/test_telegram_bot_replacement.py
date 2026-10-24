"""Token revisions, atomic bot swaps, and immutable replacement receipts."""

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from _telegram_repository_test_support import add_link, bind_link, code_of, rows
from _telegram_repository_test_support import db as db
from sqlalchemy import event
from sqlalchemy.orm import Session

from app.telegram_bots.links import BotLinked, TooManyCodes
from deerflow.persistence.channel_connections.model import ChannelConnectionRow, ChannelConversationRow, ChannelOAuthStateRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkEventRow, TelegramCallbackRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio

OPERATION = "a" * 32
DIGEST = "b" * 64


async def replace(db, link, *, bot=None, operation=OPERATION, digest=DIGEST, **validated):
    return await db.links.replace_from_token(
        owner_user_id=link.owner_user_id,
        old_link_id=link.id,
        operation_id=operation,
        request_digest=digest,
        validated_bot={"id": bot if bot is not None else link.bot_id + 100, "username": "replacement_fixture_bot", **validated},
        encrypted_token="wrapped-replacement-fixture",
    )


async def existing_bound_state(db):
    link = await bind_link(db, await add_link(db))
    await db.connections.set_thread_id(connection_id=link.connection_id, owner_user_id=link.owner_user_id, provider="telegram", external_conversation_id=str(link.chat_id), thread_id="c" * 32)
    callback_id = uuid4().hex
    async with db.sessions.begin() as session:
        session.add(TelegramCallbackRow(id=callback_id, group_id=uuid4().hex, link_id=link.id, chat_id=link.chat_id, binding_generation=link.binding_generation, kind="fixture", created_at=datetime.now(UTC)))
    return link, await db.links.token(link.id), callback_id


async def assert_original_binding_survives(db, link, token, callback_id):
    assert await db.links.get(link.id) == link
    assert await db.links.token(link.id) == token
    assert await db.connections.get_thread_id(link.connection_id, str(link.chat_id)) == "c" * 32
    connections = await rows(db, ChannelConnectionRow)
    assert next(row for row in connections if row.id == link.connection_id).status == "connected"
    callback = next(row for row in await rows(db, TelegramCallbackRow) if row.id == callback_id)
    assert callback.consumed_at is None
    assert [row.state for row in await rows(db, TelegramOutboxRow) if row.link_id == link.id] == ["pending"]
    assert await db.links.replacement_receipt(link.owner_user_id, OPERATION, DIGEST) is None


async def test_other_bot_replacement_is_atomic_and_keeps_a_slot_above_lowered_limits(db):
    original = await bind_link(db, await add_link(db))
    await add_link(db, bot=original.bot_id + 1)
    await add_link(db, owner="owner-b", bot=original.bot_id + 2)
    await db.connections.set_thread_id(connection_id=original.connection_id, owner_user_id=original.owner_user_id, provider="telegram", external_conversation_id=str(original.chat_id), thread_id="c" * 32)
    db.config.max_bots = db.config.max_bots_per_person = 1
    receipt = await replace(db, original)
    assert receipt["old_link_id"] == original.id and receipt["new_link_id"] != original.id
    successor = await db.links.get(receipt["new_link_id"])
    assert successor.bot_id == original.bot_id + 100 and successor.state == "waiting_connect"
    assert (successor.owner_user_id, successor.agent_kind, successor.agent_ref) == (original.owner_user_id, original.agent_kind, original.agent_ref)
    assert successor.token_revision == receipt["token_revision"] == 1
    assert await db.links.get(original.id) is None
    assert len(await db.links.list_all()) == 3 and len(await db.links.list_for_owner(original.owner_user_id)) == 2
    assert await db.links.codes_waiting(original.owner_user_id) == 1
    assert (await rows(db, ChannelConnectionRow))[0].status == "revoked"
    assert not await rows(db, ChannelConversationRow)
    assert await db.links.replacement_receipt(original.owner_user_id, OPERATION, DIGEST) == receipt


async def test_other_bot_replacement_rolls_back_when_the_new_code_would_exceed_the_cap(db):
    original, token, callback = await existing_bound_state(db)
    for offset in range(1, 6):
        await db.links.mint_code(await add_link(db, bot=original.bot_id + offset))
    assert await db.links.codes_waiting(original.owner_user_id) == 5
    with pytest.raises(TooManyCodes):
        await replace(db, original)
    await assert_original_binding_survives(db, original, token, callback)
    assert await db.links.get_by_bot(original.bot_id + 100) is None
    assert await db.links.codes_waiting(original.owner_user_id) == 5


async def test_other_bot_replacement_rolls_back_on_an_already_owned_bot(db):
    original, token, callback = await existing_bound_state(db)
    other = await add_link(db, owner="owner-b", bot=original.bot_id + 100)
    with pytest.raises(BotLinked) as exc:
        await replace(db, original, bot=other.bot_id)
    assert exc.value.own is False
    await assert_original_binding_survives(db, original, token, callback)
    assert await db.links.get(other.id) == other


async def test_other_bot_replacement_rolls_back_every_effect_on_precommit_failure(db):
    original, token, callback = await existing_bound_state(db)

    def refuse_commit(session):
        if session.get_bind() is db.engine.sync_engine:
            raise RuntimeError("fixture-replacement-commit")

    event.listen(Session, "before_commit", refuse_commit)
    try:
        with pytest.raises(RuntimeError, match="fixture-replacement-commit"):
            await replace(db, original)
    finally:
        event.remove(Session, "before_commit", refuse_commit)
    await assert_original_binding_survives(db, original, token, callback)
    assert await db.links.get_by_bot(original.bot_id + 100) is None
    assert not [row for row in await rows(db, TelegramBotLinkEventRow) if row.action in {"replace_bot", "token_replace_receipt"}]


async def test_replacement_receipt_replays_after_original_link_was_deleted(db):
    original = await add_link(db)
    receipt = await replace(db, original)
    links_before = await db.links.list_all()
    events_before = len(await rows(db, TelegramBotLinkEventRow))
    states_before = len(await rows(db, ChannelOAuthStateRow))
    assert await db.links.get(original.id) is None
    replay = await replace(db, original, bot=original.bot_id + 200)
    assert replay == receipt
    assert await db.links.list_all() == links_before
    assert len(await rows(db, TelegramBotLinkEventRow)) == events_before
    assert len(await rows(db, ChannelOAuthStateRow)) == states_before
    assert await db.links.get_by_bot(original.bot_id + 200) is None


async def test_replacement_receipt_remains_after_successor_removal_without_resurrecting_it(db):
    original = await add_link(db)
    receipt = await replace(db, original)
    await db.links.remove(receipt["new_link_id"], actor_id=original.owner_user_id)
    assert await db.links.replacement_receipt(original.owner_user_id, OPERATION, DIGEST) == receipt
    assert await replace(db, original) == receipt
    assert await db.links.get(receipt["new_link_id"]) is None and not await db.links.list_all()


async def test_replacement_operation_digest_mismatch_cannot_swap_again_or_cross_owners(db):
    original = await add_link(db)
    receipt = await replace(db, original)
    with pytest.raises(ValueError, match="operation-mismatch"):
        await db.links.replacement_receipt(original.owner_user_id, OPERATION, "d" * 64)
    with pytest.raises(ValueError, match="operation-mismatch"):
        await replace(db, original, digest="d" * 64)
    assert await db.links.replacement_receipt("another-owner", OPERATION, DIGEST) is None
    assert await db.links.replacement_receipt(original.owner_user_id, OPERATION, DIGEST) == receipt
    assert len(await db.links.list_all()) == 1


async def test_replacement_receipts_and_audits_contain_no_token_or_connect_code(db):
    original = await add_link(db)
    original_code = code_of(await db.links.mint_code(original))
    receipt = await replace(db, original)
    events = await rows(db, TelegramBotLinkEventRow)
    serialized = json.dumps(receipt) + json.dumps([row.detail for row in events])
    assert original_code not in serialized
    assert "wrapped-replacement-fixture" not in serialized and "wrapped-fixture" not in serialized
    assert "https://t.me" not in serialized and "encrypted_token" not in serialized
    receipt_events = [row for row in events if row.action == "token_replace_receipt"]
    assert len(receipt_events) == 1
    assert (receipt_events[0].owner_user_id, receipt_events[0].operation_id, receipt_events[0].request_digest) == (original.owner_user_id, OPERATION, DIGEST)
    for key in ["old_link_id", "new_link_id", "token_revision"]:
        assert receipt_events[0].detail[key] == receipt[key]


@pytest.mark.parametrize("proof", [None, False, True])
async def test_same_bot_replacement_preserves_binding_and_clears_pause_only_with_current_changed_token_proof(db, proof):
    original = await bind_link(db, await add_link(db))
    pause = datetime.now(UTC)
    paused = await db.links.update(original.id, acts_paused_since=pause)
    validated = {} if proof is None else {"token_changed": proof, "expected_token_revision": paused.token_revision}
    receipt = await replace(db, paused, bot=original.bot_id, **validated)
    current = await db.links.get(original.id)
    assert receipt["old_link_id"] == receipt["new_link_id"] == original.id
    assert current.token_revision == original.token_revision + 1
    assert (current.state, current.connection_id, current.binding_generation, current.telegram_user_id, current.chat_id) == (original.state, original.connection_id, original.binding_generation, original.telegram_user_id, original.chat_id)
    assert (current.acts_paused_since is None) is (proof is True)
    if proof is not True:
        assert current.acts_paused_since == paused.acts_paused_since


async def test_changed_token_proof_requires_a_revision_and_cannot_clear_a_newer_pause(db):
    original = await add_link(db)
    paused = await db.links.update(original.id, acts_paused_since=datetime.now(UTC))
    with pytest.raises(ValueError):
        await replace(db, paused, bot=original.bot_id, token_changed=True)
    assert await db.links.get(original.id) == paused
    advanced = await db.links.store_token(original.id, "wrapped-newer-fixture", actor_id=original.owner_user_id)
    with pytest.raises(ValueError):
        await replace(db, advanced, bot=original.bot_id, token_changed=True, expected_token_revision=original.token_revision)
    assert await db.links.get(original.id) == advanced and advanced.acts_paused_since is not None


async def test_store_token_advances_revision_and_invalidates_only_this_links_old_codes(db):
    original = await add_link(db)
    other = await add_link(db, bot=original.bot_id + 1)
    old_code = code_of(await db.links.mint_code(original))
    other_code = code_of(await db.links.mint_code(other))
    updated = await db.links.store_token(original.id, "wrapped-new-logical-token", actor_id=original.owner_user_id)
    assert updated.token_revision == original.token_revision + 1 and updated.connect_expires_at is None
    assert await db.links.token(original.id) == ("wrapped-new-logical-token", updated.token_revision)
    assert await db.links.codes_waiting(original.owner_user_id) == 1
    async with db.links.capacity_writer(original.owner_user_id) as session:
        assert await db.links.consume_code(updated, old_code, session=session) == "expired"
        assert await db.links.consume_code(other, other_code, session=session) == "ok"
    actions = [row.action for row in await rows(db, TelegramBotLinkEventRow) if row.link_id == original.id]
    assert actions.count("replace_token") == 1


async def test_secret_only_rewrap_uses_compare_and_set_and_preserves_revision_and_valid_code(db):
    original = await add_link(db)
    code = code_of(await db.links.mint_code(original))
    before = await db.links.get(original.id)
    encrypted, revision = await db.links.token(original.id)
    with pytest.raises(ValueError):
        await db.links.store_token(original.id, "wrapped-rekey-fixture", actor_id=original.owner_user_id, action="rewrap")
    result = await db.links.store_token(original.id, "wrapped-rekey-fixture", actor_id=original.owner_user_id, action="rewrap", expected_revision=revision, expected_encrypted_token=encrypted)
    assert result.token_revision == revision and result.connect_expires_at == before.connect_expires_at
    assert await db.links.token(original.id) == ("wrapped-rekey-fixture", revision)
    assert await db.links.codes_waiting(original.owner_user_id) == 1
    with pytest.raises(ValueError):
        await db.links.store_token(original.id, "wrapped-stale-rekey", actor_id=original.owner_user_id, action="rewrap", expected_revision=revision, expected_encrypted_token=encrypted)
    assert await db.links.token(original.id) == ("wrapped-rekey-fixture", revision)
    async with db.links.capacity_writer(original.owner_user_id) as session:
        assert await db.links.consume_code(result, code, session=session) == "ok"
