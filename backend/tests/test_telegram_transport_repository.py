"""Captured transport facts cannot publish over a replacement token."""

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db
from sqlalchemy import event
from sqlalchemy.orm import Session

from app.telegram_bots.links import BotLinkRepository

pytestmark = pytest.mark.asyncio


def another_repository(db):
    return BotLinkRepository(db.sessions, db.connections, db.config)


async def test_current_status_guard_refreshes_binding_without_rewriting_on_empty_facts(db):
    link = await add_link(db)
    guarded = await db.links.update_if_current(link.id, token_revision=link.token_revision)
    assert guarded == link
    bound = await bind_link(db, link)
    current = await db.links.update_if_current(link.id, token_revision=link.token_revision)
    assert current == bound and current.binding_generation > link.binding_generation
    assert not hasattr(current, "encrypted_token")
    now = datetime(2030, 1, 2, tzinfo=UTC)
    updated = await db.links.update_if_current(link.id, token_revision=link.token_revision, last_ok_at=now, last_update_at=now, can_manage_bots=True, topics_enabled=True, last_error="NetworkError network", last_error_at=now)
    assert updated.last_ok_at == updated.last_update_at == updated.last_error_at == now
    assert updated.can_manage_bots is True and updated.topics_enabled is True
    assert updated.last_error == "NetworkError network"


async def test_old_status_facts_are_refused_after_token_rotation(db):
    link = await add_link(db)
    current = await another_repository(db).store_token(link.id, "new-wrapped-fixture", actor_id=link.owner_user_id)
    assert await db.links.update_if_current(link.id, token_revision=link.token_revision, last_ok_at=datetime(2030, 1, 2, tzinfo=UTC), can_manage_bots=True) is None
    assert await db.links.get(link.id) == current


@pytest.mark.parametrize("facts", [{"health": "token_rejected"}, {"state": "blocked"}, {"binding_generation": 50}, {"chat_id": 88}, {"encrypted_token": "opaque"}, {"notices": {}}, {"profile_version": 22}])
async def test_status_guard_cannot_write_lifecycle_or_other_service_facts(db, facts):
    link = await add_link(db)
    with pytest.raises(ValueError):
        await db.links.update_if_current(link.id, token_revision=link.token_revision, **facts)
    assert await db.links.get(link.id) == link


@pytest.mark.parametrize("facts", [{"can_manage_bots": 1}, {"topics_enabled": "true"}, {"last_ok_at": datetime(2030, 1, 2)}, {"last_update_at": "later"}, {"last_error": "NetworkError: raw description"}, {"last_error_at": False}])
async def test_status_guard_rejects_ambiguous_or_payload_bearing_facts(db, facts):
    link = await add_link(db)
    with pytest.raises(ValueError):
        await db.links.update_if_current(link.id, token_revision=link.token_revision, **facts)
    assert await db.links.get(link.id) == link


async def test_deleted_transport_cannot_publish_health_or_status(db):
    link = await add_link(db)
    await db.links.remove(link.id, actor_id=link.owner_user_id)
    assert await db.links.update_if_current(link.id, token_revision=link.token_revision) is None
    assert await db.links.set_health(link.id, "token_rejected", holds=True, expected_token_revision=link.token_revision) is None


async def test_old_health_callback_cannot_reject_or_resurrect_a_rotated_token(db):
    link = await add_link(db)
    since = datetime(2030, 1, 2, tzinfo=UTC)
    await db.links.set_health(link.id, "conflict", holds=True, transition={"conflict_since": since, "probe_until": since + timedelta(hours=24), "webhook_host": "hooks.example.com"})
    replacement = await another_repository(db).store_token(link.id, "new-wrapped-fixture", actor_id=link.owner_user_id)
    assert await db.links.set_health(link.id, "token_rejected", holds=True, expected_token_revision=link.token_revision) is None
    assert await db.links.get(link.id) == replacement
    new = await db.links.set_health(link.id, "unreachable", holds=True, expected_token_revision=replacement.token_revision)
    assert new.health == "unreachable" and new.conflict_since is None
    cleared = await db.links.set_health(link.id, "unreachable", holds=False, expected_token_revision=replacement.token_revision)
    assert cleared.health == "ok" and cleared.acts_paused_since == replacement.acts_paused_since


@pytest.mark.parametrize("operation", ["status", "health"])
async def test_token_guard_is_checked_in_writer_after_overlapping_rotation(db, monkeypatch, operation):
    link = await add_link(db)
    entered, release = asyncio.Event(), asyncio.Event()
    original = db.links._owner_writer

    @asynccontextmanager
    async def paused(owner):
        entered.set()
        await release.wait()
        async with original(owner) as session:
            yield session

    monkeypatch.setattr(db.links, "_owner_writer", paused)
    if operation == "status":
        call = db.links.update_if_current(link.id, token_revision=link.token_revision, can_manage_bots=True)
    else:
        call = db.links.set_health(link.id, "token_rejected", holds=True, expected_token_revision=link.token_revision)
    task = asyncio.create_task(call)
    try:
        await asyncio.wait_for(entered.wait(), timeout=3)
        replacement = await another_repository(db).store_token(link.id, "new-wrapped-fixture", actor_id=link.owner_user_id)
        release.set()
        assert await asyncio.wait_for(task, timeout=3) is None
        assert await db.links.get(link.id) == replacement
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_failed_new_revision_health_commit_does_not_publish_cached_causes(db):
    link = await add_link(db)
    await db.links.set_health(link.id, "conflict", holds=True)
    replacement = await another_repository(db).store_token(link.id, "new-wrapped-fixture", actor_id=link.owner_user_id)

    def refuse(session):
        if session.get_bind() is db.engine.sync_engine:
            raise RuntimeError("fixture-health-commit")

    event.listen(Session, "before_commit", refuse)
    try:
        with pytest.raises(RuntimeError, match="fixture-health-commit"):
            await db.links.set_health(link.id, "secret_unreadable", holds=True, expected_token_revision=replacement.token_revision)
    finally:
        event.remove(Session, "before_commit", refuse)
    current = await db.links.set_health(link.id, "unreachable", holds=False, expected_token_revision=replacement.token_revision)
    assert current.health == "ok" and current.conflict_since is None


@pytest.mark.parametrize("revision", [True, -1, "1"])
async def test_revision_guards_require_an_exact_nonnegative_integer(db, revision):
    link = await add_link(db)
    with pytest.raises(ValueError):
        await db.links.update_if_current(link.id, token_revision=revision)
    with pytest.raises(ValueError):
        await db.links.set_health(link.id, "unreachable", holds=True, expected_token_revision=revision)
