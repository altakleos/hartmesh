"""Bounded complete prepublication reads preserve actual input ownership."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import delete
from test_telegram_callback_publication import retry_input_environment as retry_input_environment
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment

from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

pytestmark = pytest.mark.asyncio


async def test_reader_returns_complete_durable_group_once(coordinator_environment):
    e = coordinator_environment
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    grouped = await e.inbox.group_prepublication(e.origins, guard=attempt.create_guard)
    page = await e.inbox.prepublication_page(limit=32)
    assert len(page.entries) == 1 and not page.has_more
    entry = page.entries[0]
    assert entry.hold_reason is None and entry.input_owner_callback_id is None
    assert entry.origins == grouped and all(row.thread_id is None and row.turn_id is None for row in entry.origins)
    async with e.db.sessions.begin() as session:
        await session.execute(delete(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == grouped[1].message_id))
    broken = await e.inbox.prepublication_page(limit=32)
    assert len(broken.entries) == 1 and broken.entries[0].hold_reason == "incomplete_group"


async def test_reader_returns_staged_callback_without_fake_publication(retry_input_environment):
    e = retry_input_environment
    stage = await e.stage()
    page = await e.inbox.prepublication_page(limit=32)
    assert len(page.entries) == 1
    entry = page.entries[0]
    assert entry.input_owner_callback_id == e.callback_id and entry.origins == stage.origins and entry.hold_reason is None


async def test_unrouted_singleton_retains_held_profile_without_inventing_forwarded_fact(coordinator_environment):
    e = coordinator_environment
    async with e.db.sessions.begin() as session:
        for row in await session.scalars(__import__("sqlalchemy").select(TelegramInboundMarkRow)):
            row.route_kind, row.route_target_id, row.route_basis = None, None, {}
    page = await e.inbox.prepublication_page(limit=1)
    assert page.has_more and page.next_after is not None
    assert len(page.entries) == 1 and page.entries[0].hold_reason == "unsupported_input"
    assert page.entries[0].origins[0].route_kind is None
    other = await e.inbox.prepublication_page(after=page.next_after, limit=1)
    assert len(other.entries) == 1 and other.entries[0].origins[0].message_id != page.entries[0].origins[0].message_id
