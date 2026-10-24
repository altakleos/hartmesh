"""Public transport seams preserve saved plans and retire obsolete admissions."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest
from _telegram_repository_test_support import add_link, bind_link, rows
from _telegram_repository_test_support import db as db
from telegram import Chat, Message, Update, User

from app.telegram_bots import producer
from app.telegram_bots.inbox import InboxRepository
from app.telegram_bots.producer import Outbox
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio
NOW = datetime(2030, 1, 1, tzinfo=UTC)


def product_part(**extra):
    return {"kind": "text", "source_ref": {"key": "chat.restarted", "values": {"product": "HartMesh"}}, **extra}


async def setup(db):
    link = await bind_link(db, await add_link(db))
    inbox = InboxRepository(db.sessions)
    return link, inbox, Outbox(db.sessions, inbox=inbox, clock=lambda: NOW)


async def origin(inbox, link, *, identity=102, topic=None):
    message = Message(identity, NOW, Chat(link.chat_id, "private"), from_user=User(link.telegram_user_id, "Fixture", False), text="/privacy", message_thread_id=topic)
    mark, _ = await inbox.accept(link, Update(identity + 1000, message=message), sanitized_input={"command": "/privacy"})
    return await inbox.route(mark, kind="command", target_id=None, basis={"command": "privacy"}, operation_ids=[])


async def saved(db, identity):
    async with db.sessions() as session:
        return await session.get(TelegramOutboxRow, identity)


async def claim(db, identity, *, number=0, admission="a" * 32):
    async with db.sessions.begin() as session:
        row = await session.get(TelegramOutboxRow, identity)
        row.state, row.claim_owner, row.claim_id, row.claimed_at = "sending", "process", admission, NOW
        parts = deepcopy(row.parts)
        parts[number].update(state="sending", protect_content=True)
        row.parts = parts
    return await saved(db, identity)


async def capture(outbox, link, row, *, part_index=0, **override):
    values = dict(link_id=link.id, outbox_id=row.id, part_id=row.parts[part_index]["part_id"], claim_owner=row.claim_owner, claim_id=row.claim_id, token_revision=link.token_revision)
    return await outbox.capture_sending_part(**(values | override))


async def test_product_replay_retains_ids_and_exact_origin_registration(db):
    link, inbox, outbox = await setup(db)
    mark = await origin(inbox, link)
    identity = await outbox.plan_product(link, source_id="privacy-102", parts=[product_part()], inbox_rows=[mark], finish_route=True)
    first = await saved(db, identity)
    replay = await outbox.plan_product(link, source_id="privacy-102", parts=[product_part()], inbox_rows=[mark], finish_route=True)
    assert replay == identity and (await saved(db, identity)).parts == first.parts
    mark = next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == 102)
    assert mark.delivery_ids == [identity] and mark.route_complete is True and mark.state == "reply_pending"


@pytest.mark.parametrize("initial_origins", [False, True])
async def test_product_replay_cannot_change_empty_or_nonempty_origin_set(db, initial_origins):
    link, inbox, outbox = await setup(db)
    mark = await origin(inbox, link)
    initial = [mark] if initial_origins else []
    changed = [] if initial_origins else [mark]
    identity = await outbox.plan_product(link, source_id="fixed", parts=[product_part()], inbox_rows=initial)
    first = await saved(db, identity)
    with pytest.raises(ValueError, match="product-source-conflict"):
        await outbox.plan_product(link, source_id="fixed", parts=[product_part()], inbox_rows=changed)
    assert (await saved(db, identity)).parts == first.parts
    mark = next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == 102)
    assert mark.delivery_ids == ([identity] if initial_origins else [])


async def test_product_replay_cannot_attach_another_input_or_replace_source(db):
    link, inbox, outbox = await setup(db)
    first, other = await origin(inbox, link), await origin(inbox, link, identity=103)
    identity = await outbox.plan_product(link, source_id="fixed", parts=[product_part()], inbox_rows=[first])
    for origins, parts in [([other], [product_part()]), ([first], [product_part(source_ref={"key": "hello.privacy", "values": {}})])]:
        with pytest.raises(ValueError, match="product-source-conflict"):
            await outbox.plan_product(link, source_id="fixed", parts=parts, inbox_rows=origins)
    other = next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == 103)
    assert other.delivery_ids == [] and (await saved(db, identity)).parts[0]["source_ref"] == product_part()["source_ref"]


async def test_product_replay_preserves_authorized_topic_relocation(db):
    link, inbox, outbox = await setup(db)
    mark = await origin(inbox, link, topic=33)
    identity = await outbox.plan_product(link, source_id="fixed", parts=[product_part()], inbox_rows=[mark], message_thread_id=33)
    original = await claim(db, identity)
    await outbox.replace_presentation(link, identity, expected_version=0, claim_owner=original.claim_owner, claim_id=original.claim_id, reason="topic-missing", parts=[product_part()], message_thread_id=None)
    replaced = await saved(db, identity)
    assert await outbox.plan_product(link, source_id="fixed", parts=[product_part()], inbox_rows=[mark], message_thread_id=33) == identity
    assert (await saved(db, identity)).parts == replaced.parts and replaced.message_thread_id is None


async def test_borrowed_product_plan_and_completion_rollback_with_caller(db):
    link, inbox, outbox = await setup(db)
    mark = await origin(inbox, link)
    async with db.sessions() as session:
        await session.begin()
        identity = await outbox.plan_product(link, source_id="fixed", parts=[product_part()], inbox_rows=[mark], finish_route=True, session=session)
        await session.rollback()
    assert await saved(db, identity) is None
    current = next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == 102)
    assert current.delivery_ids == [] and current.route_complete is False


async def test_sending_capture_is_detached_exact_and_releases_its_transaction(db):
    link, inbox, outbox = await setup(db)
    identity = await outbox.plan_product(link, source_id="fixed", parts=[product_part(), product_part()])
    row = await claim(db, identity)
    async with db.sessions.begin() as session:
        stored = await session.get(TelegramOutboxRow, identity)
        stored.silent = True
    row = await saved(db, identity)
    snapshot = await capture(outbox, link, row)
    assert snapshot.link == link and snapshot.item.id == identity
    assert snapshot.part == row.parts[0] and snapshot.part["protect_content"] is True
    assert snapshot.claim_owner == "process" and snapshot.claim_id == row.claim_id and snapshot.token_revision == link.token_revision
    assert snapshot.item.attempts == row.attempts
    assert snapshot.item.silent is True
    assert not hasattr(snapshot.link, "encrypted_token")
    snapshot.part["source_ref"]["values"]["product"] = "changed locally"
    assert (await saved(db, identity)).parts[0]["source_ref"] == product_part()["source_ref"]
    assert await db.links.update_if_current(link.id, token_revision=link.token_revision, topics_enabled=True) is not None


@pytest.mark.parametrize("override", [{"claim_owner": "other"}, {"claim_id": "b" * 32}, {"token_revision": -1}, {"token_revision": True}, {"outbox_id": "missing"}, {"link_id": "missing"}, {"part_id": "missing"}])
async def test_sending_capture_retires_any_wrong_claim_identity(db, override):
    link, inbox, outbox = await setup(db)
    identity = await outbox.plan_product(link, source_id="fixed", parts=[product_part()])
    row = await claim(db, identity)
    with pytest.raises(producer.DeliveryClaimRetired):
        await capture(outbox, link, row, **override)
    assert (await saved(db, identity)).state == "sending"


async def test_capture_cannot_skip_required_parts_or_reuse_a_rotated_token(db):
    link, inbox, outbox = await setup(db)
    identity = await outbox.plan_product(link, source_id="fixed", parts=[product_part(), product_part()])
    row = await claim(db, identity, number=1)
    with pytest.raises(producer.DeliveryClaimRetired):
        await capture(outbox, link, row, part_index=1)
    row = await claim(db, identity)
    await db.links.store_token(link.id, "new-wrapped-fixture", actor_id=link.owner_user_id)
    with pytest.raises(producer.DeliveryClaimRetired):
        await capture(outbox, link, row)


async def test_plain_fallback_is_same_part_once_and_cannot_touch_new_admission(db):
    link, inbox, outbox = await setup(db)
    identity = await outbox.plan_product(link, source_id="fixed", parts=[product_part()])
    original = await claim(db, identity)
    snapshot = await capture(outbox, link, original)
    assert await outbox.fallback_to_plain(snapshot) is True
    fallback = await saved(db, identity)
    assert fallback.state == "pending" and fallback.claim_id is None and fallback.attempts == 0
    assert fallback.parts[0] == original.parts[0] | {"state": "pending", "plain_fallback": True}
    assert await outbox.fallback_to_plain(snapshot) is False
    successor = await claim(db, identity, admission="b" * 32)
    assert await outbox.fallback_to_plain(snapshot) is False
    assert await outbox.fallback_to_plain(await capture(outbox, link, successor)) is False
    assert (await saved(db, identity)).claim_id == successor.claim_id


@pytest.mark.parametrize("source_id", [None, ""])
async def test_product_source_requires_a_named_identity(db, source_id):
    link, inbox, outbox = await setup(db)
    with pytest.raises(ValueError, match="invalid-delivery-key"):
        await outbox.plan_product(link, source_id=source_id, parts=[product_part()])


@pytest.mark.parametrize("topic", [-2, 0, True, "33"])
async def test_product_destination_requires_a_real_topic_identity(db, topic):
    link, inbox, outbox = await setup(db)
    with pytest.raises(ValueError, match="invalid-delivery-target"):
        await outbox.plan_product(link, source_id="fixed", parts=[product_part()], message_thread_id=topic)


@pytest.mark.parametrize("finish", [1, "yes", {"finish": True}])
async def test_product_completion_requires_an_explicit_boolean(db, finish):
    link, inbox, outbox = await setup(db)
    mark = await origin(inbox, link)
    with pytest.raises(ValueError, match="invalid-product-completion"):
        await outbox.plan_product(link, source_id="fixed", parts=[product_part()], inbox_rows=[mark], finish_route=finish)
    current = next(row for row in await rows(db, TelegramInboundMarkRow) if row.message_id == 102)
    assert current.route_complete is False and current.delivery_ids == []


@pytest.mark.parametrize("owner,admission", [(None, None), ("", ""), ("process", None), (None, "a" * 32)])
async def test_capture_cannot_authorize_an_orphaned_sending_row(db, owner, admission):
    link, inbox, outbox = await setup(db)
    identity = await outbox.plan_product(link, source_id="fixed", parts=[product_part()])
    await claim(db, identity)
    async with db.sessions.begin() as session:
        row = await session.get(TelegramOutboxRow, identity)
        row.claim_owner, row.claim_id = owner, admission
    with pytest.raises(producer.DeliveryClaimRetired):
        await capture(outbox, link, await saved(db, identity))


async def test_token_verification_wait_is_saved_with_original_part_and_claim_fences(db):
    link, inbox, outbox = await setup(db)
    identity = await outbox.plan_product(link, source_id="fixed", parts=[product_part()])
    original = await claim(db, identity)
    snapshot = await capture(outbox, link, original)
    assert await outbox.defer_token_verification(snapshot, retry_after_seconds=80, http_status=401) is True
    pending = await saved(db, identity)
    assert pending.state == "pending" and pending.claim_owner is pending.claim_id is None
    assert pending.not_before.replace(tzinfo=UTC) == NOW + timedelta(seconds=80)
    assert pending.attempts == 0
    assert pending.parts[0] == original.parts[0] | {"state": "pending", "invalid_token_verification_pending": {"token_revision": link.token_revision, "http_status": 401, "failure_logged": True}}
    assert await outbox.defer_token_verification(snapshot, retry_after_seconds=100, http_status=404) is False
    successor = await claim(db, identity, admission="b" * 32)
    assert await outbox.defer_token_verification(snapshot, retry_after_seconds=100, http_status=404) is False
    assert (await saved(db, identity)).claim_id == successor.claim_id


async def test_observed_token_failure_can_be_journaled_during_conflict_but_not_rotation(db):
    link, inbox, outbox = await setup(db)
    identity = await outbox.plan_product(link, source_id="fixed", parts=[product_part()])
    original = await claim(db, identity)
    snapshot = await capture(outbox, link, original)
    await db.links.set_health(link.id, "conflict", holds=True)
    assert await outbox.defer_token_verification(snapshot, retry_after_seconds=80, http_status=404) is True
    await db.links.set_health(link.id, "conflict", holds=False)
    current = await claim(db, identity, admission="b" * 32)
    snapshot = await capture(outbox, link, current)
    await db.links.store_token(link.id, "new-wrapped-fixture", actor_id=link.owner_user_id)
    assert await outbox.defer_token_verification(snapshot, retry_after_seconds=100, http_status=401) is False
    assert (await saved(db, identity)).parts[0]["invalid_token_verification_pending"]["token_revision"] == link.token_revision


@pytest.mark.parametrize("delay,http", [(-1, 401), (True, 401), (float("inf"), 401), (80, 400), (80, 401.0)])
async def test_verification_continuation_rejects_unbounded_or_unclassified_waits(db, delay, http):
    link, inbox, outbox = await setup(db)
    identity = await outbox.plan_product(link, source_id="fixed", parts=[product_part()])
    current = await claim(db, identity)
    snapshot = await capture(outbox, link, current)
    with pytest.raises(ValueError):
        await outbox.defer_token_verification(snapshot, retry_after_seconds=delay, http_status=http)
    assert (await saved(db, identity)).parts == current.parts


async def test_unverified_network_failure_keeps_claim_for_existing_scheduler_backoff(db):
    link, inbox, outbox = await setup(db)
    identity = await outbox.plan_product(link, source_id="fixed", parts=[product_part()])
    original = await claim(db, identity)
    snapshot = await capture(outbox, link, original)
    assert await outbox.defer_token_verification(snapshot, retry_after_seconds=None, http_status=401) is True
    current = await saved(db, identity)
    assert (current.state, current.claim_owner, current.claim_id, current.not_before, current.attempts) == (original.state, original.claim_owner, original.claim_id, original.not_before, original.attempts)
    assert current.parts[0] == original.parts[0] | {"invalid_token_verification_pending": {"token_revision": link.token_revision, "http_status": 401, "failure_logged": True}}


async def test_new_plan_cannot_supply_a_transport_verification_receipt(db):
    link, inbox, outbox = await setup(db)
    supplied = product_part(invalid_token_verification_pending={"token_revision": link.token_revision, "http_status": 401, "failure_logged": True})
    identity = await outbox.plan_product(link, source_id="fixed", parts=[supplied])
    assert "invalid_token_verification_pending" not in (await saved(db, identity)).parts[0]
