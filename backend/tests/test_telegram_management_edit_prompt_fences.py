from copy import deepcopy

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from test_telegram_management_edit_prompt_barrier import acknowledge_cleanup, open_editor
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["opening", "grant", "editor", "full_ack"])
async def test_prompt_fences_custody_after_real_cleanup_ack_and_final_read(work_delivery, monkeypatch, drift):
    e = work_delivery
    sources, parent_id, identity, cleanup_id, prompt_id, edit = await open_editor(e)
    await acknowledge_cleanup(e, cleanup_id)
    assert not e.dispatch_errors, e.dispatch_errors
    get = sources.settings.get
    changed = []

    async def move(*args, **kwargs):
        settings = await get(*args, **kwargs)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                parent = await writer.get(TelegramOutboxRow, parent_id)
                if drift == "opening":
                    gesture = parent.payload["work_management_intent"]["origin"]["gesture_id"]
                    await writer.delete(await writer.get(TelegramCallbackRow, gesture))
                elif drift == "grant":
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
                elif drift == "editor":
                    parent.payload = {**parent.payload, "work_management_editor": {**parent.payload["work_management_editor"], "prompt_id": operation()}}
                else:
                    parts = deepcopy(parent.parts)
                    parts[-1]["message_ids"] = []
                    parent.parts = parts
        return settings

    monkeypatch.setattr(sources.settings, "get", move)
    before = len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"])
    e.channel.bot.request.enqueue("sendMessage", body={"ok": True, "result": {"message_id": 6301, "date": 1, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Prompt that must be refused"}})
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == before
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        prompt = await reader.get(TelegramCallbackRow, prompt_id)
        assert row.state != "sent" and prompt.message_id is None and prompt.payload["prompt_visibility"]["state"] == "pending"
