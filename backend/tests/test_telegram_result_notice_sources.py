"""Non-review result source facts come from settled Work and its recipient."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def queued(e):
    from app.telegram_bots.producer import _item
    from app.telegram_bots.result_notice_sources import ResultNoticeReader

    assert await e.event_worker.tick() >= 1
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow)))
        item = _item(next(row for row in rows if row.payload.get("work_event_intent", {}).get("key") in {"result.done", "result.waiting_review"}))
    return ResultNoticeReader(work=e.human.work, authority=e.live.authority, links=e.db.links), item


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
async def test_done_source_retains_exact_event_attempt_and_recipient_redaction(work_exports):
    from deerflow.persistence.spaces.model import SpaceGrantRow

    e = work_exports
    reader, item = await queued(e)
    source = await reader.read(e.link, item)
    assert source.work["outcome"] == e.result["outcome"]
    assert source.navigation["attempt_id"] == e.result["outcome"]["attempt_id"]
    assert source.key == "result.done" and source.person is None
    assert source.name == e.instance.name
    async with e.db.sessions.begin() as writer:
        await writer.delete(await writer.get(SpaceGrantRow, (e.home.id, "human", e.actor.subject_id)))
    redacted = await reader.read(e.link, item)
    assert redacted.work["outcome"]["sources"] == [{"kind": "unavailable", "current_contents": "not_checked"}]
    assert redacted.identity == source.identity
    assert not e.native_reads


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("change", ["event", "outcome", "requester", "attempt", "intent", "suggestion_actor", "suggestion_attempt"])
async def test_result_notice_final_provider_snapshot_refuses_changed_source(work_exports, monkeypatch, change):
    import app.gateway.authz as authz
    from app.telegram_bots.attention_authority import prepare_attention
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.result_notice_sources import ResultNoticeProjection
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow

    e = work_exports
    reader, item = await queued(e)
    source = await reader.read(e.link, item)
    original, changed, active = authz.resolve_route_permissions, [], {}

    async def provider(*args, **kwargs):
        held = await original(*args, **kwargs)
        writer = active.get("writer")
        if writer is not None and not changed:
            if change == "event":
                (await writer.get(AgentWorkEventRow, item.event_id)).actor_id = "changed-actor"
            elif change == "outcome":
                work = await writer.get(AgentWorkRow, item.work_id)
                work.outcome = {**work.outcome, "statement": "Different result"}
            elif change.startswith("suggestion_"):
                suggestion = await writer.get(AgentWorkEventRow, source.work["suggestion"]["id"])
                if change == "suggestion_actor":
                    suggestion.actor_id = "different-agent"
                else:
                    suggestion.request = {**suggestion.request, "attempt_id": "f" * 32}
            elif change in {"requester", "attempt"}:
                attempt = await writer.get(AgentWorkAttemptRow, source.navigation["attempt_id"])
                if change == "requester":
                    attempt.requester_id = "another-person"
                else:
                    attempt.thread_id = None
            else:
                row = await writer.get(TelegramOutboxRow, item.id)
                row.payload = {**row.payload, "work_event_intent": {**row.payload["work_event_intent"], "event_revision": 999}}
            await writer.flush()
            changed.append(True)
        return held

    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    admission = await prepare_attention(reader, e.link)
    with pytest.raises(ProductSourceRetired):
        async with e.db.sessions.begin() as writer:
            active["writer"] = writer
            await admission(session=writer, projection=ResultNoticeProjection(reader, e.link, item, source))
    assert changed and not e.native_reads


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["waiting_event"], indirect=True)
async def test_waiting_result_is_for_requester_and_names_current_reviewer(work_exports):
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow

    e = work_exports
    reader, item = await queued(e)
    source = await reader.read(e.link, item)
    assert source.key == "result.waiting_review" and source.person == e.reviewer.email
    assert item.silent is True
    async with e.db.sessions.begin() as writer:
        (await writer.get(HumanInputRequestRow, item.request_id)).recipient_id = e.actor.subject_id
    with pytest.raises(ProductSourceRetired):
        await reader.read(e.link, item)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
async def test_initial_notice_signature_cannot_seal_old_display_with_new_canonical_outcome(work_exports, monkeypatch):
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow

    e = work_exports
    reader, item = await queued(e)
    original, changed = reader.work.get, []

    async def changed_after_read(*args, **kwargs):
        record = await original(*args, **kwargs)
        if not changed:
            async with e.db.sessions.begin() as writer:
                work = await writer.get(AgentWorkRow, item.work_id)
                work.outcome = {**work.outcome, "statement": "New canonical statement"}
                event = await writer.get(AgentWorkEventRow, item.event_id)
                event.result = {**event.result, "outcome": work.outcome}
            changed.append(True)
        return record

    monkeypatch.setattr(reader.work, "get", changed_after_read)
    with pytest.raises(ProductSourceRetired):
        await reader.read(e.link, item)
    assert changed
