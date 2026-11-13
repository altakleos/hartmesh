"""One drafting attempt per stored input, with explicit criteria after recovery."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_work_actions import actions as actions

from deerflow.persistence.agent_instances.model import AgentConversationRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow


async def setup(e, monkeypatch, *, answer=None):
    import app.telegram_bots.assign as extraction
    from app.telegram_bots.assignment_requests import AssignmentDraftRequests

    monkeypatch.setattr(extraction, "get_app_config", lambda: e.live.config)
    model = AsyncMock(return_value=json.dumps(answer or {"objective": "Review the guide", "success_criteria": "Explain verified gaps"}))
    monkeypatch.setattr(extraction, "run_oneshot_llm", model)
    owner = AssignmentDraftRequests(e.acts)
    words = "Review the guide and explain verified gaps"
    inbox, _ = await e.acts.outbox.inbox.accept(e.link, make_update(4701, account=e.link.chat_id, text="/assign " + words), sanitized_input={"command": "assign", "text": words})
    return owner, inbox, words, model


@pytest.mark.asyncio
async def test_input_drafts_once_and_never_opens_thread_run_or_work(actions, monkeypatch):
    e = actions
    owner, inbox, words, model = await setup(e, monkeypatch)
    result = await owner.request(e.link, inbox, words=words)
    assert result.draft.objective == "Review the guide"
    assert await owner.request(e.link, inbox, words=words) == result
    model.assert_awaited_once()
    assert model.await_args.kwargs["run_name"] == "telegram_assignment_draft"
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramActRow)) == 1
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1
        assert await reader.scalar(select(func.count()).select_from(AgentConversationRow)) == 0
        assert await reader.scalar(select(func.count()).select_from(ThreadMetaRow)) == 0


@pytest.mark.asyncio
async def test_invalid_model_answer_waits_for_actual_done_when_input(actions, monkeypatch):
    e = actions
    owner, inbox, words, model = await setup(e, monkeypatch, answer={"unexpected": "model data"})
    result = await owner.request(e.link, inbox, words=words)
    assert result.phase == "awaiting_done"
    text = "The reproducible omissions are listed with evidence"
    criteria, _ = await e.acts.outbox.inbox.accept(e.link, make_update(4702, account=e.link.chat_id, text=text), sanitized_input={"text": text})
    completed = await owner.criteria(e.link, result.id, criteria, text=text)
    assert completed.id == result.id and completed.draft.objective == words and completed.draft.success_criteria == text
    model.assert_awaited_once()
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramActRow)) == 1
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1


@pytest.mark.asyncio
async def test_long_words_make_no_model_call_or_draft(actions, monkeypatch):
    e = actions
    owner, inbox, _words, model = await setup(e, monkeypatch)
    with pytest.raises(ValueError, match="assign-too-long"):
        await owner.request(e.link, inbox, words="X" * (owner.config.assign.max_chars + 1))
    model.assert_not_awaited()
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramActRow)) == 0


@pytest.mark.asyncio
async def test_duplicate_input_coalesces_one_owned_model_call(actions, monkeypatch):
    e = actions
    owner, inbox, words, model = await setup(e, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def wait(**kwargs):
        entered.set()
        await release.wait()
        return json.dumps({"objective": "Review the guide", "success_criteria": "Explain gaps"})

    model.side_effect = wait
    first = asyncio.create_task(owner.request(e.link, inbox, words=words))
    await asyncio.wait_for(entered.wait(), 5)
    second = asyncio.create_task(owner.request(e.link, inbox, words=words))
    release.set()
    a, b = await asyncio.gather(first, second)
    assert a == b
    model.assert_awaited_once()


@pytest.mark.asyncio
async def test_recovery_never_calls_the_model_again_or_accepts_its_late_result(actions, monkeypatch):
    from app.telegram_bots.assignment_requests import AssignmentDraftRequests

    e = actions
    owner, inbox, words, model = await setup(e, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def wait(**kwargs):
        entered.set()
        await release.wait()
        return json.dumps({"objective": "Late model draft", "success_criteria": "Must not replace fallback"})

    model.side_effect = wait
    first = asyncio.create_task(owner.request(e.link, inbox, words=words))
    await asyncio.wait_for(entered.wait(), 5)
    replacement = AssignmentDraftRequests(e.acts)
    recovered = await replacement.recover(e.link)
    assert len(recovered) == 1 and recovered[0].phase == "awaiting_done"
    release.set()
    result = await first
    assert result.phase == "awaiting_done" and result.id == recovered[0].id
    model.assert_awaited_once()


@pytest.mark.asyncio
async def test_completed_fallback_wins_over_a_late_model_result(actions, monkeypatch):
    from app.telegram_bots.assignment_requests import AssignmentDraftRequests

    e = actions
    owner, inbox, words, model = await setup(e, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def wait(**kwargs):
        entered.set()
        await release.wait()
        return json.dumps({"objective": "Late model draft", "success_criteria": "Not the confirmed fallback"})

    model.side_effect = wait
    first = asyncio.create_task(owner.request(e.link, inbox, words=words))
    await asyncio.wait_for(entered.wait(), 5)
    replacement = AssignmentDraftRequests(e.acts)
    recovered = await replacement.recover(e.link)
    text = "Give the checked list with source references"
    criteria, _ = await e.acts.outbox.inbox.accept(e.link, make_update(4712, account=e.link.chat_id, text=text), sanitized_input={"text": text})
    complete = await replacement.criteria(e.link, recovered[0].id, criteria, text=text)
    release.set()
    assert await first == complete
    model.assert_awaited_once()


@pytest.mark.asyncio
async def test_closed_input_cannot_dispatch_the_drafting_model(actions, monkeypatch):
    import threading

    import app.telegram_bots.assign as extraction
    from app.telegram_bots.authority import AuthorityUnavailable
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = actions
    owner, inbox, words, model = await setup(e, monkeypatch)
    entered, release = threading.Event(), threading.Event()

    def config():
        entered.set()
        release.wait(10)
        return e.live.config

    monkeypatch.setattr(extraction, "get_app_config", config)
    task = asyncio.create_task(owner.request(e.link, inbox, words=words))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id))
            row.state, row.route_complete = "interrupted", True
        release.set()
        with pytest.raises((ProductSourceRetired, AuthorityUnavailable)):
            await task
        model.assert_not_awaited()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_manual_criteria_and_prompt_custody_share_the_draft_writer(actions, monkeypatch):
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = actions
    owner, inbox, words, model = await setup(e, monkeypatch, answer={"unexpected": "model data"})
    request = await owner.request(e.link, inbox, words=words)
    text = "Give a checked list of omissions"
    criteria, _ = await e.acts.outbox.inbox.accept(e.link, make_update(4720, account=e.link.chat_id, text=text), sanitized_input={"text": text})
    calls = []

    async def refuse_late(writer):
        calls.append(writer)
        if len(calls) == 2:
            raise ProductSourceRetired()

    with pytest.raises(ProductSourceRetired):
        await owner.criteria(e.link, request.id, criteria, text=text, admission=refuse_late)
    assert len(calls) == 2 and calls[0] is calls[1]
    async with e.db.sessions() as reader:
        draft = await reader.get(TelegramActRow, request.id)
        mark = await reader.get(TelegramInboundMarkRow, (criteria.link_id, criteria.message_id, criteria.chat_id))
        assert draft.body["version"] == 2 and draft.body["request"]["phase"] == "awaiting_done"
        assert mark.route_kind is None

    calls.clear()

    async def own_prompt(writer):
        calls.append(writer)

    complete = await owner.criteria(e.link, request.id, criteria, text=text, admission=own_prompt)
    assert complete.draft.success_criteria == text
    assert len(calls) == 2 and calls[0] is calls[1]
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (criteria.link_id, criteria.message_id, criteria.chat_id))
        assert mark.route_kind == "assignment_criteria" and mark.route_target_id == request.id
    model.assert_awaited_once()


@pytest.mark.asyncio
async def test_losing_independent_owner_cannot_replace_a_healthy_extraction(actions, monkeypatch):
    from app.telegram_bots.assignment_requests import AssignmentDraftRequests

    e = actions
    _, inbox, words, model = await setup(e, monkeypatch)
    first_owner, second_owner = AssignmentDraftRequests(e.acts), AssignmentDraftRequests(e.acts)
    second_ready, model_entered, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def model_wait(**kwargs):
        model_entered.set()
        await release.wait()
        return json.dumps({"objective": "Review the guide", "success_criteria": "Explain verified gaps"})

    model.side_effect = model_wait
    first_phase, second_phase = first_owner._phase, second_owner._phase

    async def phase_a(*args, **kwargs):
        if kwargs.get("phase") == "extracting":
            await asyncio.wait_for(second_ready.wait(), 5)
        return await first_phase(*args, **kwargs)

    async def phase_b(*args, **kwargs):
        if kwargs.get("phase") == "extracting":
            second_ready.set()
            await asyncio.wait_for(model_entered.wait(), 5)
        return await second_phase(*args, **kwargs)

    monkeypatch.setattr(first_owner, "_phase", phase_a)
    monkeypatch.setattr(second_owner, "_phase", phase_b)
    first = asyncio.create_task(first_owner.request(e.link, inbox, words=words))
    second = asyncio.create_task(second_owner.request(e.link, inbox, words=words))
    try:
        observed = await asyncio.wait_for(second, 10)
        assert observed.phase == "extracting"
    finally:
        release.set()
        results = await asyncio.gather(first, second, return_exceptions=True)
    assert not isinstance(results[0], BaseException), results[0]
    assert results[0].draft.objective == "Review the guide"
    model.assert_awaited_once()


@pytest.mark.asyncio
async def test_interrupted_original_input_cannot_be_converted_after_preparation(actions, monkeypatch):
    from app.telegram_bots.authority import AuthorityUnavailable
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = actions
    owner, inbox, words, model = await setup(e, monkeypatch)
    prepare = owner.drafts.prepare_create

    async def interrupt(*args, **kwargs):
        result = await prepare(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id))
            mark.state, mark.route_complete = "interrupted", True
        return result

    monkeypatch.setattr(owner.drafts, "prepare_create", interrupt)
    with pytest.raises((ProductSourceRetired, AuthorityUnavailable)):
        await owner.request(e.link, inbox, words=words)
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramActRow))
        assert row.body["version"] == 2 and row.body["request"]["phase"] == "extracting"
    model.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["state", "route", "body", "clock"])
async def test_criteria_custody_is_rechecked_after_prompt_admission(actions, monkeypatch, change):
    from _telegram_bot_test_support import ManualClock

    from app.telegram_bots.authority import AuthorityUnavailable
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = actions
    clock = ManualClock(e.acts.outbox.clock())
    e.acts.outbox.clock = clock.now
    owner, inbox, words, _model = await setup(e, monkeypatch, answer={"unexpected": "model data"})
    request = await owner.request(e.link, inbox, words=words)
    text = "Give the verified list"
    criteria, _ = await e.acts.outbox.inbox.accept(e.link, make_update(4721, account=e.link.chat_id, text=text), sanitized_input={"text": text})
    calls = []

    async def move_input(writer):
        calls.append(writer)
        if len(calls) == 2:
            mark = await writer.get(TelegramInboundMarkRow, (criteria.link_id, criteria.message_id, criteria.chat_id))
            if change == "state":
                mark.state = "interrupted"
            elif change == "route":
                mark.route_kind, mark.route_target_id = "chat", "different-owner"
            elif change == "body":
                root = await writer.get(TelegramActRow, request.id)
                root.body = {**root.body, "draft": {**root.body["draft"], "objective": "Unreviewed replacement words"}}
            else:
                clock.advance(owner.config.callbacks.draft_ttl_hours * 3600 + 1)
            await writer.flush()

    with pytest.raises((ProductSourceRetired, AuthorityUnavailable, CallbackRefused)):
        await owner.criteria(e.link, request.id, criteria, text=text, admission=move_input)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramActRow, request.id)
        assert row.body["version"] == 2 and row.body["request"]["phase"] == "awaiting_done"
        mark = await reader.get(TelegramInboundMarkRow, (criteria.link_id, criteria.message_id, criteria.chat_id))
        assert mark.route_kind is None and mark.state != "interrupted"
