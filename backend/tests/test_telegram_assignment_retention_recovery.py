"""Damaged custody and concurrent retries keep assignment recovery evidence."""

from copy import deepcopy
from datetime import timedelta

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from test_telegram_assignment_card_delivery import prepare_card, save_card
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots import assignment_retention as retention
from app.telegram_bots.assignment_retention import assignment_dependencies
from app.telegram_bots.retention import _writer, sweep_rows
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing_root", "missing_root_and_frozen_age"])
async def test_missing_root_keeps_uncertain_evidence_even_when_frozen_age_is_missing(work_delivery, damage):
    e = work_delivery
    plans, draft, inbox = await prepare_card(e)
    await dispatch(e, 5111)
    button, admitted = await save_card(e, plans)
    now = e.clock.now()
    async with e.db.sessions.begin() as writer:
        root = await writer.get(TelegramActRow, draft.id)
        await writer.delete(root)
        if damage == "missing_root_and_frozen_age":
            delivery = await writer.get(TelegramOutboxRow, e.identity)
            payload = deepcopy(delivery.payload)
            del payload["work_news_basis"]["source"]["work"]["root"]["created_at"]
            delivery.payload = payload
        journal = await writer.get(TelegramActRow, admitted.id)
        journal.state, journal.attempts = ("uncertain", 1)
        mark = await writer.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id))
        mark.state, mark.closed_at = ("done", now)
        (await writer.get(TelegramOutboxRow, e.identity)).closed_at = now
    later = now + timedelta(days=500)
    await plans.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, admitted.id) is not None
        assert await reader.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id)) is not None
        assert await reader.get(TelegramOutboxRow, e.identity) is not None
        assert await reader.get(TelegramCallbackRow, button.id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["callback_payload", "frozen_work"])
async def test_bad_json_shape_does_not_break_global_assignment_retention(work_delivery, damage):
    e = work_delivery
    plans, draft, inbox = await prepare_card(e)
    await dispatch(e, 5161)
    button, admitted = await save_card(e, plans)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramActRow, admitted.id)).state = "uncertain"
        if damage == "callback_payload":
            (await writer.get(TelegramCallbackRow, button.id)).payload = []
        else:
            delivery = await writer.get(TelegramOutboxRow, e.identity)
            payload = deepcopy(delivery.payload)
            payload["work_news_basis"]["source"]["work"] = []
            delivery.payload = payload
    async with e.db.sessions() as reader:
        held = await assignment_dependencies(reader, e.clock.now() + timedelta(days=500), max_age_days=180)
    assert e.identity in held.deliveries
    assert admitted.id in held.acts
    assert (inbox.link_id, inbox.message_id, inbox.chat_id) in held.inputs


@pytest.mark.asyncio
async def test_concurrent_pending_retry_cannot_be_deleted_from_old_closure(work_delivery, monkeypatch):
    e = work_delivery
    if e.db.engine.dialect.name != "postgresql":
        pytest.skip("SQLite serializes both writers through BEGIN IMMEDIATE")
    plans, draft, _ = await prepare_card(e)
    await dispatch(e, 5171)
    button, admitted = await save_card(e, plans)

    async def refuse(**kwargs):
        raise AgentDenied("Synthetic conclusive refusal", reason="denied")

    monkeypatch.setattr(e.acts.work, "delegate", refuse)
    assert (await e.acts.check(admitted.id)).state == "refused"
    real = retention.assignment_dependencies
    protected = []

    async def concurrent_retry(session, *args, **kwargs):
        dependencies = await real(session, *args, **kwargs)
        assert admitted.id in dependencies.expired_history
        try:
            async with e.db.sessions.begin() as writer:
                await writer.execute(text("SET LOCAL lock_timeout = '1s'"))
                parent = await writer.get(TelegramActRow, admitted.id, with_for_update=True)
                parent.state, parent.reason, parent.attempts = ("pending", None, parent.attempts + 1)
                await writer.flush()
        except DBAPIError as exc:
            if "lock timeout" not in str(exc).lower():
                raise
            protected.append(True)
        return dependencies

    monkeypatch.setattr(retention, "assignment_dependencies", concurrent_retry)
    async with _writer(e.db.sessions) as writer:
        await retention.expire_assignments(writer, e.clock.now() + timedelta(days=500), max_age_days=180, draft_ttl_hours=24)
    if not protected:
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramActRow, admitted.id) is not None
            assert await reader.get(TelegramActRow, draft.id) is not None
