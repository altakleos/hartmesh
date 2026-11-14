"""Request children publish through the actual acknowledged primary gesture."""

from unittest.mock import AsyncMock

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_request_continuations import ready_decision
from test_telegram_voice_continuations import pending_voice_child
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_outcome_delivery import clear_outcome_cleanup
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.gateway import work_execution
from app.telegram_bots.work_execution_delivery import WorkExecutionPlans
from app.telegram_bots.work_execution_sources import WorkExecutionReader
from app.telegram_bots.work_news_delivery import WorkNewsSources
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def request_child(e, monkeypatch, kind, *, reason=None):
    from deerflow.agent_instances.contract import Refusal

    failure = Refusal("Host capacity", reason=reason) if reason is not None else RuntimeError("Host admission unavailable")
    if kind == "voice":
        e.acts, parent, original = await pending_voice_child(e, monkeypatch, failure=failure)
    else:
        _record, _request, parent, _outcome = await ready_decision(e, monkeypatch)
        original = work_execution.activate_work
    monkeypatch.setattr(work_execution, "activate_work", AsyncMock(side_effect=failure))
    await e.acts.resume_continuation(parent.id)
    e.clock.advance(3601)
    parent = await e.acts.resume_continuation(parent.id)
    child_id = parent.continuation["activation_continuation"]["child_id"]
    e.outcome_identity = parent.continuation["request_activation"]["outcome_id"]
    await clear_outcome_cleanup(e)
    await dispatch(e, 1001)
    assert not e.dispatch_errors, e.dispatch_errors
    plans = WorkExecutionPlans(acts=e.acts, reader=WorkExecutionReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=e.clock.now), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
    e.channel._services.work_execution_sources = WorkNewsSources(plans)
    return plans, parent, child_id, original


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decision", "voice"])
async def test_request_child_unknown_notice_uses_original_full_confirmation(work_delivery, monkeypatch, kind):
    e = work_delivery
    plans, parent, child_id, _original = await request_child(e, monkeypatch, kind)
    e.identity = await plans.observe(e.link, child_id)
    assert e.identity is not None
    await dispatch(e, 1002)
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, e.identity)
        assert notice.state == "sent"
        assert notice.payload["work_execution_intent"]["origin"]["callback_id"] == parent.id
        assert await reader.get(TelegramCallbackRow, child_id) is None
    retry = await selected(e, "act_check")
    assert retry.target_id == child_id and retry.message_id == 1002
    assert not e.launches


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decision", "voice"])
@pytest.mark.parametrize("mode", ["ordinary", "retired"])
async def test_request_capacity_uses_original_consent_and_requires_a_new_full_start(work_delivery, monkeypatch, kind, mode):
    import test_telegram_composite_execution_outcomes as composite

    async def request_family(e, patches, *, reason=None):
        plans, parent, child_id, _original = await request_child(e, patches, kind, reason=reason)
        return plans, parent.id, child_id

    monkeypatch.setattr(composite, "uncertain_child", request_family)
    await composite.test_composite_capacity_offer_uses_parent_consent_and_new_full_start(work_delivery, monkeypatch, mode)
