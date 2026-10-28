"""Send-as-new owns an actual held human row, without answering the old request."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment

from app.telegram_bots.callbacks import CallbackRepository, Prompts
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow


@pytest.mark.asyncio
async def test_explicit_send_as_new_stages_actual_held_message_without_request_metadata(coordinator_environment):
    e = coordinator_environment
    callbacks = CallbackRepository(e.db.sessions, config=e.db.config)
    prompts = Prompts(callbacks)
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    origin = await e.add_origin(message_id=901, text="This is explicitly a new instruction")
    prompt = await prompts.open(e.link, "clarify_other", {"thread_id": "old-request-thread", "request_id": "old-request"}, chat_id=e.link.chat_id, message_thread_id=None, message_id=1801)
    await prompts.close(e.link, e.link.chat_id, None, reason="expired")
    parent = await e.outbox.plan_product_keyboard(
        e.link,
        source_id="held-prompt:901",
        parts=[{"kind": "text", "source_ref": {"key": "prompt.closed", "values": {}}}],
        keyboard=[[{"label": "label.try_again", "kind": "prompt_send_new", "target_kind": "thread", "target_id": e.thread_id, "payload": {"prompt_id": prompt, "message_id": 901}}]],
        callbacks=callbacks,
        guard=attempt.read_guard,
        inbox_rows=(origin,),
    )
    async with e.db.sessions() as reader:
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt_send_new"))
        identity, group = selected.id, selected.group_id
    assert await callbacks.adopt(e.link, group, 1902, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=parent, presentation_version=0, check_current=lambda: True)
    stage = await e.inbox.stage_prompt_message(e.link, callback_id=identity, update_id=3902, chat_id=e.link.chat_id, message_id=1902, message_thread_id=None, callbacks=callbacks, guard=attempt.read_guard)
    assert stage.text == origin.sanitized_input["text"] and stage.human_message_kwargs == {}
    assert stage.callback_id == identity and stage.thread_id is None and stage.parent_publication is None
    assert [row.message_id for row in stage.origins] == [901]
    reread = await e.inbox.read_submission(e.link, stage, guard=attempt.read_guard)
    assert reread == stage
    async with e.db.sessions() as reader:
        held = await reader.get(TelegramInboundMarkRow, (e.link.id, 901, e.link.chat_id))
        assert held.route_complete and held.turn_id is None and held.thread_id is None
