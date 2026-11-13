"""Owner-scoped opaque Work cursors keep the complete actionable ordering."""

from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from test_work_attention_pages import seed_records
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.attention import AttentionReader
from app.telegram_bots.authority import AccessOff
from deerflow.agent_instances.contract import AgentDenied
from deerflow.agent_instances.human_input_contract import CreateRequest
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow


def reader(e):
    return AttentionReader(work=e.human.work, authority=e.live.authority, links=e.db.links)


@pytest.mark.asyncio
async def test_work_list_keeps_all_actionable_ids_and_opaque_page_order(work_pages):
    e = work_pages
    await seed_records(e, 125)
    source = reader(e)
    cursor = await source.open_list(e.link)
    page = await source.page(e.link, cursor)
    assert page["work"][0]["id"] == e.work["id"] and len(page["work"]) == 10
    assert page["previous"] is None and page["next"] and page["pages"] == 13
    first_ids = [row["id"] for row in page["work"]]
    later = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=operation(), objective="A newly created assignment", success_criteria="Use a fresh list"))
    following = await source.page(e.link, page["next"])
    assert [row["id"] for row in (await source.page(e.link, following["previous"]))["work"]] == first_ids
    ids = first_ids[:]
    while page["next"]:
        page = await source.page(e.link, page["next"])
        ids.extend(row["id"] for row in page["work"])
    assert len(ids) == len(set(ids)) == 126 and later["id"] not in ids
    fresh = await source.page(e.link, await source.open_list(e.link))
    assert later["id"] in [row["id"] for row in fresh["work"]]
    async with e.db.sessions() as session:
        row = await session.get(TelegramCallbackRow, cursor)
        assert row.kind == "work_list_snapshot" and row.target_kind is None and row.message_id is None
        assert row.expires_at is not None


@pytest.mark.asyncio
async def test_work_list_cursor_rejects_owner_instance_generation_and_expiry(work_pages):
    e = work_pages
    source = reader(e)
    cursor = await source.open_list(e.link)
    for changed in (replace(e.link, owner_user_id="another-person"), replace(e.link, agent_ref=operation()), replace(e.link, binding_generation=e.link.binding_generation + 1)):
        with pytest.raises((AgentDenied, AccessOff, ValueError)):
            await source.page(changed, cursor)
    with pytest.raises(ValueError):
        await source.page(e.link, operation())
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramCallbackRow, cursor)
        row.expires_at = source.clock() - timedelta(seconds=1)
    with pytest.raises(ValueError):
        await source.page(e.link, cursor)


@pytest.mark.asyncio
async def test_work_page_rechecks_current_assignment_and_inspect(work_pages):
    from test_agent_work_records import command

    e = work_pages
    source = reader(e)
    cursor = await source.open_list(e.link)
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=current["id"], request=command(current, "edit", objective="Updated after opening the list"))
    assert (await source.page(e.link, cursor))["work"][0]["objective"] == "Updated after opening the list"
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 1
    with pytest.raises(AgentDenied):
        await source.page(e.link, cursor)


@pytest.mark.asyncio
async def test_snapshot_fixes_work_membership_but_reads_new_current_question(work_pages):
    e = work_pages
    work = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Original listed assignment", success_criteria="Verified answer"))
    source = reader(e)
    cursor = await source.open_list(e.link)
    request = await e.human.create(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=work["id"],
        request=CreateRequest(
            operation_id=uuid4().hex,
            expected_work_revision=work["revision"],
            expected_assignment_revision=work["assignment_revision"],
            purpose="information",
            question="A new current question",
            reason="Need current input",
            expected_response="A reference",
            recipient_id=e.actor.subject_id,
        ),
    )
    frame = await source.page(e.link, cursor)
    assert work["id"] in [row["id"] for row in frame["work"]]
    assert request["id"] in [row["id"] for row in frame["pending"]]


@pytest.mark.asyncio
async def test_snapshot_must_still_be_unexpired_after_final_authority_await(work_pages, monkeypatch):
    e = work_pages
    source = reader(e)
    cursor = await source.open_list(e.link)
    now = source.clock()
    source.clock = lambda: now
    real = source._current
    calls = []

    async def delayed(link):
        nonlocal now
        result = await real(link)
        calls.append(True)
        if len(calls) == 4:
            now += timedelta(hours=source.links.config.callbacks.draft_ttl_hours + 1)
        return result

    monkeypatch.setattr(source, "_current", delayed)
    with pytest.raises(ValueError, match="work-list-unavailable"):
        await source.page(e.link, cursor)
    assert len(calls) == 4


@pytest.mark.asyncio
async def test_snapshot_retired_during_final_authority_is_not_returned(work_pages, monkeypatch):
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_pages
    source = reader(e)
    cursor = await source.open_list(e.link)
    real = source._current
    calls = []

    async def retire(link):
        calls.append(True)
        if len(calls) == 4:
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramCallbackRow, cursor)
                row.consumed_at = source.clock()
                row.closed_reason = "retired"
        return await real(link)

    monkeypatch.setattr(source, "_current", retire)
    with pytest.raises(ValueError, match="work-list-unavailable"):
        await source.page(e.link, cursor)
    assert len(calls) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["delete", "payload", "expiry", "generation"])
async def test_actual_provider_wait_rechecks_exact_list_snapshot(work_pages, monkeypatch, damage):
    from app.gateway import authz
    from app.telegram_bots.attention_scope import _factories

    e = work_pages
    source = reader(e)
    cursor = await source.open_list(e.link)
    real = authz.resolve_route_permissions
    changed = []

    async def withdraw(*args, **kwargs):
        held = await real(*args, **kwargs)
        if "prepared_provider" in kwargs and _factories.get() and not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramCallbackRow, cursor)
                if damage == "delete":
                    await writer.delete(row)
                elif damage == "payload":
                    row.payload = {**row.payload, "work_ids": []}
                elif damage == "expiry":
                    row.expires_at = source.clock() - timedelta(seconds=1)
                else:
                    row.binding_generation += 1
        return held

    monkeypatch.setattr(authz, "resolve_route_permissions", withdraw)
    with pytest.raises(ValueError, match="work-list-unavailable"):
        await source.page(e.link, cursor)
    assert changed
