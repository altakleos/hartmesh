"""A saved answer makes the current assignment resumable for its reader."""

from datetime import UTC, datetime, timedelta

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_answer_news import answered
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.attention import AttentionReader
from app.telegram_bots.summary import sections
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow


@pytest.mark.asyncio
@pytest.mark.parametrize("withdraw_recipient", [False, True])
async def test_summary_includes_own_blocked_work_answered_by_colleague(work_delivery, withdraw_recipient):
    e = work_delivery
    item = await answered(e)
    if withdraw_recipient:
        req = await e.human.get(actor=e.actor, request_id=item.request_id)
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, req["recipient_id"]))).permissions = 2
    now = datetime.now(UTC) + timedelta(seconds=1)
    attention = await AttentionReader(work=e.human.work, authority=e.live.authority, links=e.db.links).collect(e.link, as_of=now)
    work = next(row for row in attention["work"] if row["id"] == item.work_id)
    req = next(row for row in attention["requests"] if row["id"] == item.request_id)
    assert work["status"] == "blocked" and work["attempt"]["settled_at"]
    assert req["state"] == "answered" and req["recipient_id"] != e.link.owner_user_id
    result = sections(attention, owner_user_id=e.link.owner_user_id, since=now - timedelta(days=1))
    assert any(row["work_id"] == work["id"] and row["kind"] == "resume" for row in result["waiting"]), (work, req, result)
