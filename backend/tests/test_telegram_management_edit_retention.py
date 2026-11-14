"""Typed replacement history keeps its whole private command family."""

from copy import deepcopy
from datetime import timedelta

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from sqlalchemy import delete
from test_telegram_assignment_prompt_inputs import accept_reply
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_management_editing import editing
from test_telegram_work_management_navigation import publish_edit
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.retention import sweep_rows
from app.telegram_bots.work_actions import _ACT_SCOPE
from app.telegram_bots.work_stop_retries import child_identity
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


async def prepare_family(e, monkeypatch, disposition):
    cards, edits, parent_id, edit = await editing(e)
    prompt_parent = e.identity

    async def proposal(*args, **kwargs):
        if disposition == "extracting":
            raise RuntimeError("Synthetic interrupted model admission")
        return DraftAssignment(objective="Review the expanded guide", success_criteria="Record verified changes")

    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", proposal)
    incoming = await accept_reply(e, 6401, "Include the extra chapter", 6301)
    if disposition == "extracting":
        with pytest.raises(RuntimeError):
            await edits.answer(e.link, incoming, guard=await guard_for(e))
        newest = edit.target_id
    else:
        assert await edits.answer(e.link, incoming, guard=await guard_for(e)) == parent_id
        e.identity = parent_id
        await publish_edit(e)
        if disposition == "discard":
            control = await selected(e, "assign_discard")
            newest = control.target_id
            await cards.discard(e.link, control.id, chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None, guard=await guard_for(e))
        else:
            control = await selected(e, "work_change_save")
            newest = control.target_id
            act = await cards.confirm(e.link, control.id, chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None, guard=await guard_for(e))
            if disposition == "done":
                assert (await e.acts.check(act.id)).state == "done"
        if disposition != "pending":
            await cards.observe(e.link, parent_id)
            await publish_edit(e)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, parent_id)
        prompt_id = parent.payload["work_management_editor"]["prompt_id"]
    return cards, parent_id, edit, newest, incoming, prompt_parent, prompt_id


@pytest.mark.asyncio
@pytest.mark.parametrize("disposition", ["pending", "done", "discard", "extracting"])
async def test_replacement_history_retires_only_a_complete_closed_family(work_delivery, monkeypatch, disposition):
    e = work_delivery
    cards, parent_id, edit, newest, incoming, prompt_parent, prompt_id = await prepare_family(e, monkeypatch, disposition)
    later = e.clock.now() + timedelta(days=401)
    await cards.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        if disposition in {"done", "discard"}:
            assert await reader.get(TelegramActRow, edit.target_id) is None
            assert await reader.get(TelegramActRow, newest) is None
            assert await reader.get(TelegramOutboxRow, parent_id) is None
        else:
            assert await reader.get(TelegramActRow, edit.target_id) is not None
            assert await reader.get(TelegramActRow, newest) is not None
            assert await reader.get(TelegramOutboxRow, parent_id) is not None
            assert await reader.get(TelegramOutboxRow, prompt_parent) is not None
            assert await reader.get(TelegramCallbackRow, prompt_id) is not None
            assert await reader.get(TelegramCallbackRow, edit.id) is not None
            assert await reader.get(TelegramInboundMarkRow, (incoming.link_id, incoming.message_id, incoming.chat_id)) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["witness", "receipt", "foreign", "overlap"])
async def test_damaged_or_shared_replacement_history_keeps_the_entire_custody(work_delivery, monkeypatch, damage):
    e = work_delivery
    cards, parent_id, edit, newest, incoming, prompt_parent, prompt_id = await prepare_family(e, monkeypatch, "done")
    async with e.db.sessions.begin() as writer:
        root = await writer.get(TelegramActRow, newest)
        native = await writer.get(TelegramActRow, root.operation_ids[0])
        native_id = native.id
        if damage == "witness":
            old = await writer.get(TelegramActRow, edit.target_id)
            old.result = {**old.result, "draft_id": operation()}
        elif damage == "receipt":
            await writer.execute(delete(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == native.id))
        elif damage == "foreign":
            child = child_identity(native.id, 2)
            writer.add(
                TelegramActRow(
                    id=child,
                    **{**{key: getattr(native, key) for key in _ACT_SCOPE}, "kind": "stop"},
                    body=WorkCommand(operation_id=child, expected_revision=1, expected_assignment_revision=1, action="stop", attempt_id=operation()).model_dump(mode="json", exclude_unset=True),
                    state="pending",
                    operation_ids=[],
                    attempts=0,
                    continuation={"version": 1, "stop_retry": {"parent_id": native.id, "ordinal": 2}},
                    created_at=e.clock.now(),
                    updated_at=e.clock.now(),
                )
            )
        else:
            parent = await writer.get(TelegramOutboxRow, parent_id)
            values = {prop.key: deepcopy(getattr(parent, prop.key)) for prop in parent.__mapper__.column_attrs}
            identity = operation()
            values.update(id=identity, dedupe_key="duplicate-history:" + identity)
            writer.add(TelegramOutboxRow(**values))
    later = e.clock.now() + timedelta(days=401)
    await cards.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        for identity in (edit.target_id, newest, native_id):
            assert await reader.get(TelegramActRow, identity) is not None
        assert await reader.get(TelegramOutboxRow, parent_id) is not None
        assert await reader.get(TelegramOutboxRow, prompt_parent) is not None
        assert await reader.get(TelegramCallbackRow, prompt_id) is not None
        assert await reader.get(TelegramCallbackRow, edit.id) is not None
        assert await reader.get(TelegramInboundMarkRow, (incoming.link_id, incoming.message_id, incoming.chat_id)) is not None
