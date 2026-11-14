import asyncio
import contextvars
import json
from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import db as db
from langchain_core.messages import AIMessage
from test_telegram_assignment_prompt_inputs import accept_reply
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_management_editing import editing
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assignment_drafts import origin_facts
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow

DRAFT = {"objective": "Review the entire manual", "success_criteria": "Return verified corrections", "due_at": None, "due_text": None}


def recording_model(monkeypatch, invoke, config):
    from app.telegram_bots import assign

    monkeypatch.setattr(assign, "get_app_config", lambda: config)
    from deerflow.utils import oneshot_llm

    monkeypatch.setattr(oneshot_llm, "create_chat_model", lambda **kwargs: SimpleNamespace(ainvoke=invoke))
    monkeypatch.setattr(oneshot_llm, "inject_langfuse_metadata", lambda *args, **kwargs: None)


async def assert_no_replacement(e, parent_id, old_id, fresh_id):
    async with e.db.sessions() as reader:
        old = await reader.get(TelegramActRow, old_id)
        assert old.state == "draft" and old.result is None
        assert await reader.get(TelegramActRow, fresh_id) is None
        parent = await reader.get(TelegramOutboxRow, parent_id)
        assert parent.payload["work_management_intent"]["draft_id"] == old_id
        assert not parent.payload.get("work_management_replacements")
        prompt = await reader.get(TelegramCallbackRow, parent.payload["work_management_editor"]["prompt_id"])
        assert prompt.payload["management_input"]["phase"] == "extracting"


@pytest.mark.asyncio
async def test_stop_drains_actual_one_shot_return_without_late_adoption(work_delivery, monkeypatch):
    e = work_delivery
    cards, edits, parent_id, control = await editing(e)
    incoming = await accept_reply(e, 6494, "Include the final appendix", 6301)
    fresh_id = digest(["management-edit", origin_facts(incoming)])[:32]
    entered, release, exited = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = []

    async def invoke(messages, *, config):
        calls.append((messages, config))
        entered.set()
        try:
            await release.wait()
            return AIMessage(content=json.dumps(DRAFT))
        finally:
            exited.set()

    recording_model(monkeypatch, invoke, e.live.config)
    answer = asyncio.create_task(edits.answer(e.link, incoming, guard=await guard_for(e)))
    stopping = None
    try:
        await asyncio.wait_for(entered.wait(), 30)
        stopping = asyncio.create_task(edits.stop())
        await asyncio.sleep(0)
        assert edits._stopping and not stopping.done() and not exited.is_set()
        release.set()
        results = await asyncio.gather(answer, stopping, return_exceptions=True)
        assert isinstance(results[0], ProductSourceRetired), results
        assert results[1] is None and exited.is_set() and len(calls) == 1
    finally:
        release.set()
        await asyncio.gather(answer, *([stopping] if stopping else []), return_exceptions=True)
    await assert_no_replacement(e, parent_id, control.target_id, fresh_id)
    assert not edits._tasks and not edits._active


@pytest.mark.asyncio
async def test_last_actual_permission_provider_retirement_rolls_back_visible_adoption(work_delivery, monkeypatch):
    from app.gateway import authz
    from app.telegram_bots import work_management_editing as module

    e = work_delivery
    cards, edits, parent_id, control = await editing(e)
    incoming = await accept_reply(e, 6495, "Include the final appendix", 6301)
    fresh_id = digest(["management-edit", origin_facts(incoming)])[:32]
    active = [True]
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: active[0])
    calls = []

    async def invoke(messages, *, config):
        calls.append((messages, config))
        return AIMessage(content=json.dumps(DRAFT))

    recording_model(monkeypatch, invoke, e.live.config)
    selected = contextvars.ContextVar("review_management_writer", default=None)
    prepare = module.prepare_attention

    async def capture(*args, **kwargs):
        current = await prepare(*args, **kwargs)

        async def checked(*args, **kwargs):
            token = selected.set(kwargs.get("session"))
            try:
                return await current(*args, **kwargs)
            finally:
                selected.reset(token)

        return checked

    monkeypatch.setattr(module, "prepare_attention", capture)
    resolve = authz.resolve_route_permissions
    entered = []

    async def withdraw(*args, **kwargs):
        result = await resolve(*args, **kwargs)
        writer = selected.get()
        if writer is not None and (fresh := await writer.get(TelegramActRow, fresh_id, populate_existing=True)) is not None:
            old = await writer.get(TelegramActRow, control.target_id, populate_existing=True)
            parent = await writer.get(TelegramOutboxRow, parent_id, populate_existing=True)
            prompt = await writer.get(TelegramCallbackRow, parent.payload["work_management_editor"]["prompt_id"], populate_existing=True)
            assert fresh.state == "draft" and old.reason == "replaced"
            assert parent.payload["work_management_intent"]["draft_id"] == fresh_id
            assert prompt.payload["management_input"]["phase"] == "completed"
            entered.append(True)
            active[0] = False
        return result

    monkeypatch.setattr(authz, "resolve_route_permissions", withdraw)
    with pytest.raises(ValueError, match="publication-stale"):
        await edits.answer(e.link, incoming, guard=guard)
    assert entered and len(calls) == 1 and not active[0]
    await assert_no_replacement(e, parent_id, control.target_id, fresh_id)
    assert not edits._tasks and not edits._active
