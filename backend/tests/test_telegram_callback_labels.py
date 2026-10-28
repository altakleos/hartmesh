"""Saved button labels retain their own values without changing action bodies."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_callbacks import callbacks_environment as callbacks_environment
from test_telegram_publication_custody import publication_environment as publication_environment

from app.telegram_bots.callbacks import CallbackSpec, canonical_keyboard
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow


def test_absent_label_values_preserve_old_descriptor_bytes():
    assert canonical_keyboard([[CallbackSpec("label.try_again", kind="retry_message", target_kind="thread", target_id="thread", payload={})]]) == [
        [
            {
                "label": "label.try_again",
                "kind": "retry_message",
                "target_kind": "thread",
                "target_id": "thread",
                "payload": {},
                "expires_at": None,
            }
        ]
    ]


@pytest.mark.asyncio
async def test_choice_label_values_are_frozen_beside_business_payload(callbacks_environment):
    e = callbacks_environment
    values = {"label": "Full choice <one>"}
    keyboard = [[{"label": "label.choice", "label_values": values, "kind": "clarify_option", "target_kind": "thread", "target_id": e.thread_id, "payload": {"request_id": "request", "option_id": "one", "value": "Full choice <one>"}}]]
    plan = await e.plan(keyboard=keyboard, message_id=902)
    values["label"] = "mutated"
    row = await e.callbacks.get(plan.group.callback_ids[0])
    assert dict(row.payload["label_values"]) == {"label": "Full choice <one>"}
    assert row.payload["value"] == "Full choice <one>"
    assert await e.callbacks.adopt(e.link, row.group_id, 902, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=plan.outbox_id, presentation_version=0, check_current=lambda: True)
    async with e.sessions.begin() as writer:
        saved = await writer.get(TelegramCallbackRow, row.id)
        saved.payload = {**saved.payload, "label_values": {"label": "different"}}
    assert not await e.callbacks.adopt(e.link, row.group_id, 902, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=plan.outbox_id, presentation_version=0, check_current=lambda: True)


def test_button_values_are_not_business_payload_and_are_strict_detached_json():
    with pytest.raises(ValueError):
        canonical_keyboard([[{"label": "label.choice", "kind": "clarify_option", "payload": {"label_values": {"label": "forged"}}}]])
    with pytest.raises(ValueError):
        canonical_keyboard([[{"label": "label.choice", "label_values": {"minutes": 1.5}, "kind": "clarify_option", "payload": {}}]])
