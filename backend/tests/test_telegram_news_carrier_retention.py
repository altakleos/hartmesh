"""Source witnesses retain their actual scope without trusting saved locators."""

from copy import deepcopy
from datetime import timedelta
from uuid import uuid4

import pytest
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db
from test_telegram_work_carriers import stopped_carrier
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable
from test_telegram_work_view_delivery import prepare_view
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import Acts
from app.telegram_bots.producer import _item
from app.telegram_bots.work_news_carriers import activation_news_origin, news_witness_dependencies, news_witness_id
from app.telegram_bots.work_start_controls import WorkStartAdmissions
from app.telegram_bots.work_start_retention import reserved_starts
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def pending(e, monkeypatch):
    button = await stopped_carrier(e, "stop")
    await prepare_startable(e, monkeypatch)
    other_work = e.work
    views, _identity = await prepare_view(e)
    acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    act = await WorkStartAdmissions(views, acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))
    return act, button, other_work


@pytest.mark.asyncio
async def test_pending_news_witness_and_original_ack_survive_actual_retention(work_delivery, monkeypatch):
    from app.telegram_bots.retention import sweep_rows

    e = work_delivery
    act, button, _other = await pending(e, monkeypatch)
    future = e.sender.outbox.clock() + timedelta(days=401)
    await e.channel._services.callbacks.sweep(future)
    await sweep_rows(e.db.sessions, future, callback_max_age_days=e.channel._services.callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, act.id) is not None
        assert await reader.get(TelegramCallbackRow, news_witness_id(act.id)) is not None
        assert await reader.get(TelegramCallbackRow, button.id) is not None
        assert await reader.get(TelegramOutboxRow, button.payload["anchor_outbox_id"]) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["foreign-binding", "wrong-topic", "wrong-work", "wrong-group"])
async def test_news_witness_cannot_pin_unrelated_actual_rows(work_delivery, monkeypatch, damage):
    e = work_delivery
    act, button, other_work = await pending(e, monkeypatch)
    foreign = await add_link(e.db, owner=e.link.owner_user_id)
    await bind_link(e.db, foreign, account=e.link.chat_id + 1)
    foreign = await e.db.links.get(foreign.id)
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, button.payload["anchor_outbox_id"])
        copy = TelegramOutboxRow(**{column.name if column.name != "class" else "class_": deepcopy(getattr(parent, column.key if column.key != "class" else "class_")) for column in TelegramOutboxRow.__table__.columns})
        copy.id, copy.dedupe_key = uuid4().hex, "scope-negative:" + uuid4().hex
        if damage == "foreign-binding":
            copy.link_id, copy.binding_generation, copy.chat_id = foreign.id, foreign.binding_generation, foreign.chat_id
        elif damage == "wrong-topic":
            copy.message_thread_id = 777
        elif damage == "wrong-work":
            copy.work_id = other_work["id"]
        writer.add(copy)
        group = uuid4().hex
        writer.add(
            TelegramCallbackRow(
                id=uuid4().hex,
                group_id=group,
                link_id=copy.link_id,
                binding_generation=copy.binding_generation,
                chat_id=copy.chat_id,
                message_thread_id=copy.message_thread_id,
                message_id=123,
                kind="dismiss",
                target_kind="outbox",
                target_id=copy.id,
                payload={"anchor_outbox_id": copy.id},
                created_at=e.sender.outbox.clock(),
            )
        )
        journal = await writer.get(TelegramActRow, act.id)
        witness = await writer.get(TelegramCallbackRow, news_witness_id(act.id))
        marker = activation_news_origin(_item(copy))
        journal.continuation = {"version": 1, "news_origin": marker}
        witness.payload = {**witness.payload, "news_origin": marker, "anchor_outbox_id": copy.id, "source_group_id": group}
        await writer.flush()
        groups, anchors = await news_witness_dependencies(writer, witness, journal, e.link)
        assert not groups and not anchors
        all_groups, all_anchors = await reserved_starts(writer)
        assert witness.group_id in all_groups
        assert group not in all_groups and copy.id not in all_anchors
        assert not e.launches


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["ack", "recipe"])
async def test_malformed_original_news_holds_only_its_scope_and_cleanup_continues(work_delivery, monkeypatch, damage):
    e = work_delivery
    act, button, _other = await pending(e, monkeypatch)
    disposable = uuid4().hex
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, button.payload["anchor_outbox_id"])
        if damage == "ack":
            parent.parts = [{**part, "state": "pending", "message_ids": []} for part in parent.parts]
        else:
            basis = parent.payload["work_news_basis"]
            parent.payload = {**parent.payload, "work_news_basis": {**basis, "source": {**basis["source"], "key": "unknown.recipe"}}}
        writer.add(
            TelegramCallbackRow(
                id=disposable,
                group_id=disposable,
                link_id=e.link.id,
                binding_generation=e.link.binding_generation,
                chat_id=e.link.chat_id,
                message_thread_id=None,
                message_id=123,
                kind="back",
                target_kind="outbox",
                target_id=uuid4().hex,
                payload={},
                created_at=e.sender.outbox.clock() - timedelta(days=500),
            )
        )
    future = e.sender.outbox.clock() + timedelta(days=401)
    await e.channel._services.callbacks.sweep(future)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, disposable) is None
        assert await reader.get(TelegramCallbackRow, news_witness_id(act.id)) is not None
        assert await reader.get(TelegramActRow, act.id) is not None
    assert not e.launches
