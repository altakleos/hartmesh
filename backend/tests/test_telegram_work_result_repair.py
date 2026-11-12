"""Explicit repair retains delivered result files and rechecks current source."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_bot_adapter import message
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_result_delivery import prepare_delivery
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.work_result_recovery import WorkResultRecovery
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
@pytest.mark.parametrize("refusal", [None, "claim", "claim_owner", "claimed_at", "permission", "outcome", "topic"])
async def test_explicit_repair_preserves_uploaded_file_and_replaces_only_card(work_exports, monkeypatch, refusal):
    from copy import deepcopy

    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_exports
    identity = await prepare_delivery(e)
    if refusal == "topic":
        from sqlalchemy import select

        from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramOutboxRow, identity)
            row.message_thread_id = 91
            for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])):
                control.message_thread_id = 91
    sdk = e.channel.bot.request
    message(sdk, "sendDocument", identity=1501)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    sdk.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message thread not found"})
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "failed" and row.failed_reason == "work-result-refresh-required", (row.state, row.failed_reason, e.dispatch_errors)
        assert row.claim_id is None
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    recovery = WorkResultRecovery(e.plans)
    if refusal in {"claim", "claim_owner", "claimed_at", "permission"}:
        async with e.db.sessions.begin() as writer:
            if refusal in {"claim", "claim_owner", "claimed_at"}:
                field = "claim_id" if refusal == "claim" else refusal
                setattr(await writer.get(TelegramOutboxRow, identity), field, e.delivery_clock.now() if refusal == "claimed_at" else "unreconciled-claim")
            else:
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 1
    elif refusal == "outcome":
        original = recovery._source

        async def changed(*args, **kwargs):
            result = await original(*args, **kwargs)
            async with e.db.sessions.begin() as writer:
                work = await writer.get(AgentWorkRow, e.result["id"])
                outcome = deepcopy(work.outcome)
                outcome["statement"] = "Changed after the source read"
                work.outcome = outcome
            return result

        monkeypatch.setattr(recovery, "_source", changed)
    if refusal:
        if refusal in {"claim", "claim_owner", "claimed_at", "topic"}:
            assert await recovery.repair(e.link, identity, guard=guard) is None
        else:
            with pytest.raises((ProductSourceRetired, AgentDenied)):
                await recovery.repair(e.link, identity, guard=guard)
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.state == "failed" and row.payload["presentation_version"] == 0
            assert row.parts[0]["message_ids"] == [1501]
            if refusal == "topic":
                assert row.message_thread_id == 91
                calls = [call for call in sdk.calls if call.endpoint in {"sendDocument", "sendMessage"}]
                assert len(calls) == 2 and all(call.parameters["message_thread_id"] == 91 for call in calls)
        return
    assert await recovery.repair(e.link, identity, guard=guard) == identity
    assert await recovery.repair(e.link, identity, guard=guard) is None
    message(sdk, identity=1502)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.state, row.failed_reason, e.dispatch_errors)
        assert row.parts[0]["message_ids"] == [1501] and row.parts[-1]["message_ids"] == [1502]
    assert len([call for call in sdk.calls if call.endpoint == "sendDocument"]) == 1
    assert e.native_reads == ["outputs/result.txt"]
