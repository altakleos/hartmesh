"""Work buttons compare displayed content, not changing execution probe metadata."""

from dataclasses import replace

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_control_admission import admissions, review_confirmation, stage
from test_telegram_work_interaction_delivery import open_view, selected
from test_telegram_work_preview_recovery import dispatch, install
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typing_prompts import clear_old
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


def changing_probe(e, monkeypatch):
    original = e.reader.read_request

    async def read(*args, **kwargs):
        source = await original(*args, **kwargs)
        return replace(source, work={**source.work, "execution_available": not source.work.get("execution_available", True), "attempt": {**(source.work.get("attempt") or {}), "workspace": "ready"}})

    monkeypatch.setattr(e.reader, "read_request", read)


@pytest.mark.asyncio
async def test_first_tap_and_sdk_origin_ignore_undisplayed_execution_probe(work_delivery, monkeypatch):
    e = work_delivery
    install(e)
    await dispatch(e, 801)
    callback = await selected(e, "hi_cant_open")
    changing_probe(e, monkeypatch)
    identity = await open_view(e, callback)
    await clear_old(e)
    await dispatch(e, 802)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.failed_reason, e.dispatch_errors)


@pytest.mark.asyncio
async def test_confirmed_action_ignores_undisplayed_probe_but_retains_seen(work_delivery, monkeypatch):
    e = work_delivery
    _record, _request, seen, callback = await review_confirmation(e)
    changing_probe(e, monkeypatch)
    controller = admissions(e)
    act = await stage(e, controller, callback)
    assert act.body["expected_revision"] == seen["work_revision"]
    assert (await controller.acts.check(act.id)).state == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["question", "seen", "protection"])
async def test_displayed_or_authorizing_changes_still_refuse_first_tap(work_delivery, monkeypatch, change):
    from app.telegram_bots.producer import DeliveryRetry
    from deerflow.agent_instances.contract import AgentConflict

    e = work_delivery
    install(e)
    await dispatch(e, 801)
    callback = await selected(e, "hi_cant_open")
    original = e.reader.read_request

    async def read(*args, **kwargs):
        source = await original(*args, **kwargs)
        if change == "question":
            return replace(source, request={**source.request, "question": "A different question"})
        if change == "seen":
            return replace(source, seen={**source.seen, "assignment_revision": source.seen["assignment_revision"] + 1})
        return replace(source, protected=not source.protected)

    monkeypatch.setattr(e.reader, "read_request", read)
    with pytest.raises(DeliveryRetry if change == "protection" else AgentConflict):
        await open_view(e, callback)
