"""A fully shown Retry control may check only its original Work operation."""

import asyncio
from copy import deepcopy

from app.telegram_bots.callbacks import CallbackRefused, _topic
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import WORK_ACT_KINDS, same_body
from app.telegram_bots.work_outcome_delivery import acknowledged_parts, materialize
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def retry_source(link, parent, basis, parts, keyboard):
    try:
        if (
            parent is None
            or parent.state != "sent"
            or parent.class_ != "reply"
            or (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.instance_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref)
            or link.agent_kind != "instance"
            or not same_body(parent.payload.get("work_outcome_basis"), basis)
            or type(basis) is not dict
            or set(basis) != {"version", "context", "locale", "timezone", "parts", "keyboard", "destination", "input", "origin"}
            or type(basis["version"]) is not int
            or basis["version"] != 1
            or basis["context"]["act"]["state"] != "uncertain"
            or basis["context"]["act"]["kind"] not in WORK_ACT_KINDS
            or (parent.work_id, parent.request_id) != (basis["context"]["work_id"], basis["context"]["request_id"])
            or not same_body(basis["destination"], {"chat_id": parent.chat_id, "message_thread_id": parent.message_thread_id})
            or not same_body(basis["parts"], parts)
            or not same_body(basis["keyboard"], keyboard)
            or not same_body(parent.payload["product_keyboard_basis"], keyboard)
            or type(parent.payload["presentation_version"]) is not int
            or parent.payload["presentation_version"] != 0
            or len(parent.parts) != len(parts)
            or not parts
        ):
            raise ValueError()
        acknowledged_parts(parent, parts)
    except (KeyError, TypeError, ValueError):
        raise ProductSourceRetired() from None


class WorkActionRetries:
    def __init__(self, plans):
        self.plans, self.acts, self.callbacks = plans, plans.acts, plans.callbacks

    async def retry(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            async with self.plans.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                if callback is None or callback.link_id != link.id or callback.kind != "act_check":
                    raise CallbackRefused()
                parent = await reader.get(TelegramOutboxRow, callback.payload.get("anchor_outbox_id"))
                if parent is None or "work_outcome_basis" not in parent.payload:
                    raise CallbackRefused()
                basis = deepcopy(parent.payload["work_outcome_basis"])
            try:
                parts, keyboard, _ = await asyncio.to_thread(materialize, link, basis["context"], locale=basis["locale"], timezone=basis["timezone"], product=self.plans.product)
                retry_source(link, parent, basis, parts, keyboard)
            except (KeyError, TypeError, ValueError):
                raise ProductSourceRetired() from None
            composed = _RetryGuard(self, guard, link, callback_id, parent.id, basis, parts, keyboard)
            async with self.callbacks.writer(link, composed) as writer:
                await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=_topic(message_thread_id), guard=composed, session=writer)

            def current():
                try:
                    guard.current()
                except Exception:
                    return False
                return True

            return await self.acts.check(basis["context"]["act"]["id"], expected=basis["context"]["act"], attempt_current=current)

        return await await_drained(perform())


class _RetryGuard:
    def __init__(self, owner, base, link, callback_id, parent_id, basis, parts, keyboard):
        self.owner, self.base, self.link = owner, base, link
        self.callback_id, self.parent_id, self.basis, self.parts, self.keyboard = callback_id, parent_id, basis, parts, keyboard

    async def prepare(self):
        await prepare_publication_guard(self.base)

    def current(self):
        self.base.current()

    async def check(self, writer):
        current = await self.base.check(writer)
        parent, _rows, callback = await self.owner.callbacks._locked(writer, self.link, self.callback_id)
        retry_source(self.link, parent, self.basis, self.parts, self.keyboard)
        expected = self.basis["context"]["act"]
        saved = await writer.get(TelegramActRow, expected["id"], with_for_update=True, populate_existing=True)
        if (
            saved is None
            or parent.id != self.parent_id
            or callback.kind != "act_check"
            or callback.target_kind != "act"
            or callback.target_id != saved.id
            or callback.payload.get("act_id") != saved.id
            or (saved.link_id, saved.owner_user_id, saved.instance_id, saved.binding_generation, saved.work_id, saved.request_id)
            != (self.link.id, self.link.owner_user_id, self.link.agent_ref, self.link.binding_generation, parent.work_id, parent.request_id)
            or saved.kind != expected["kind"]
            or not same_body(saved.body, expected["body"])
        ):
            raise CallbackRefused()
        self.base.current()
        return current
