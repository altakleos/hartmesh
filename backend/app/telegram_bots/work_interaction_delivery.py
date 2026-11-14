"""Acknowledged Work controls publish unsubmitted replies, never service actions."""

from dataclasses import asdict
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import select

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, Prompts, _aware, _plain, _validate_rows
from app.telegram_bots.producer import ProductSourceRetired, _item, _safe_json, keyboard_cleanup_row
from app.telegram_bots.work_interactions import interaction_state, present_interaction
from app.telegram_bots.work_request_recovery import WorkRequestRecovery, _RecoveryGuard
from app.telegram_bots.work_request_source import same_displayed_basis
from deerflow.agent_instances.contract import AgentConflict
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained

CURRENT_REQUEST_KINDS = frozenset(
    {"hi_answer", "hi_choice", "hi_cant_open", "hi_cannot", "hi_wrong", "decide_choice", "decide_own", "decide_note", "decide_confirm", "decide_confirm_resume", "accept_open", "accept_confirm", "changes_open", "back"}
)


def transition(callback, source):
    """Derive a closed preview from the exact saved button, never caller text."""
    if callback.target_kind != "request" or callback.target_id != source.request["id"] or callback.payload.get("request_id") != source.request["id"] or callback.payload.get("work_id") != source.work["id"]:
        raise CallbackRefused()
    if _plain(callback.payload.get("seen")) != source.seen:
        raise AgentConflict("Review the current request and all responses")
    if callback.kind == "back":
        previous = interaction_state(_plain(callback.payload.get("interaction")), source)
        if previous["view"] not in {"cannot", "decision", "accept"}:
            raise CallbackRefused()
        state = {"view": "request"}
    elif callback.kind == "hi_cant_open":
        state = {"view": "cannot"}
    elif callback.kind == "accept_open":
        state = {"view": "accept"}
    elif callback.kind == "decide_choice":
        state = {"view": "decision", "decision": callback.payload.get("decision"), "note": None}
    elif callback.kind in {"hi_answer", "hi_cannot", "hi_wrong", "decide_own", "decide_note", "changes_open"}:
        purpose = {"hi_answer": "answer", "hi_cannot": "cannot", "hi_wrong": "wrong", "decide_own": "decision", "decide_note": "decision_note", "changes_open": "changes"}[callback.kind]
        state = {"view": "prompt", "purpose": purpose}
        if purpose == "decision_note":
            state["decision"] = callback.payload.get("interaction", {}).get("decision")
    else:
        raise CallbackRefused()
    return interaction_state(state, source)


def acknowledged_source(link, parent):
    from app.telegram_bots.work_request_source import frozen

    if parent is not None and parent.payload.get("work_outcome_basis", {}).get("version") == 5:
        from app.telegram_bots.work_comment_cards import acknowledged_comment

        return acknowledged_comment(link, parent)

    if parent is None or parent.state != "sent" or (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id):
        raise CallbackRefused()
    parts = [part for part in parent.parts if part.get("presentation_version") == parent.payload.get("presentation_version")]
    if not parts:
        raise CallbackRefused()
    captured = SimpleNamespace(link=link, item=_item(parent), part=parts[-1])
    basis, source, _recipe = frozen(captured, acknowledged=True)
    if basis["concealed"]:
        raise CallbackRefused()
    return source


def check_origin(link, parent, callbacks, basis, source, *, reopened_mark=None, outbox_id=None):
    """A complete ACK and consumed canonical group prove this direct reply."""
    if "reopen" in basis:
        from app.telegram_bots.work_reopen import check_reopen_origin

        check_reopen_origin(link, parent, callbacks, reopened_mark, basis, source, outbox_id)
        return
    origin = basis["origin"]
    if parent is None or parent.id != origin["anchor_outbox_id"] or parent.payload.get("presentation_version") != origin["presentation_version"]:
        raise ProductSourceRetired()
    selected = next((row for row in callbacks if row.id == origin["callback_id"]), None)
    if selected is None:
        raise ProductSourceRetired()
    previous = acknowledged_source(link, parent)
    refreshing = basis["interaction"]["view"] == "request" and selected.kind in {"decide_confirm", "decide_confirm_resume", "accept_confirm"} or basis["interaction"]["view"] == "current" and selected.kind in CURRENT_REQUEST_KINDS
    if refreshing:
        if (previous.request["id"], previous.work["id"]) != (source.request["id"], source.work["id"]):
            raise ProductSourceRetired()
    elif not same_displayed_basis(previous, source):
        raise ProductSourceRetired()
    group = [row for row in callbacks if row.group_id == selected.group_id]
    _validate_rows(parent, group)
    if any(row.consumed_at is None or row.closed_reason != ("work-refresh" if refreshing else "work-preview") or row.message_id != origin["message_id"] for row in group):
        raise ProductSourceRetired()
    if not refreshing and transition(selected, source) != basis["interaction"]:
        raise ProductSourceRetired()
    part = next((part for part in parent.parts if part.get("callback_group_id") == selected.group_id and part.get("presentation_version") == origin["presentation_version"]), None)
    if part is None or part.get("message_ids") != [origin["message_id"]]:
        raise ProductSourceRetired()


class WorkInteractionPlans:
    def __init__(self, plans):
        self.recovery = WorkRequestRecovery(plans)
        self.outbox, self.reader, self.callbacks = plans.outbox, plans.reader, plans.callbacks

    async def open(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                if callback is None or callback.link_id != link.id or callback.target_kind != "request":
                    raise CallbackRefused()
                request_id = callback.target_id
            source, settings = await self.recovery._source(link, request_id)
            admission = await prepare_attention(self.reader, link)
            composed = _RecoveryGuard(self.recovery, guard, admission, source)
            key = "notice:work-interaction:" + callback_id
            async with self.callbacks.writer(link, composed) as writer:
                existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key).with_for_update())
                if existing is not None:
                    basis = existing.payload.get("work_request_basis", {})
                    origin = basis.get("origin", {})
                    if (
                        existing.binding_generation != link.binding_generation
                        or existing.request_id != request_id
                        or (origin.get("callback_id"), origin.get("message_id"), existing.chat_id, existing.message_thread_id) != (callback_id, message_id, chat_id, message_thread_id)
                    ):
                        raise CallbackRefused()
                    return existing.id
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                if not same_displayed_basis(acknowledged_source(link, parent), source):
                    raise AgentConflict("Review the current request and all responses")
                state = transition(group.selected, source)
                return await self._publish(writer, link, group, parent, source, settings, state, key=key, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed)

        return await await_drained(perform())

    async def _publish(self, writer, link, group, parent, source, settings, state, *, key, chat_id, message_id, message_thread_id, guard, reason="work-preview", input_origin=None, reopen_origin=None):
        """Publish a source-checked transition inside its caller's guarded writer."""
        state = interaction_state(state, source)
        callback_id, request_id, composed = group.selected.id, source.request["id"], guard
        created_at, expires_at = group.selected.created_at, None
        if input_origin is not None and state["view"] == "changes":
            mark = await writer.get(TelegramInboundMarkRow, (input_origin["link_id"], input_origin["message_id"], input_origin["chat_id"]), populate_existing=True)
            if mark is None or mark.operation_ids != [input_origin["operation_id"]]:
                raise CallbackRefused()
            created_at = mark.accepted_at
            expires_at = _aware(created_at) + timedelta(hours=self.callbacks.config.callbacks.draft_ttl_hours)
        shown = present_interaction(source, state, locale=settings.locale, expires_at=expires_at)
        keyboard = [list(line) for line in shown.keyboard]
        recipes = [
            {**part, "source_ref": {"key": "work.request.presentation", "values": {"request_id": request_id, "part": index}}, **({"stored_keyboard": True} if index == len(shown.parts) - 1 and keyboard else {})}
            for index, part in enumerate(shown.parts)
        ]
        if state["view"] == "prompt":
            recipes[-1]["force_reply"] = True
        identity, _ = await self.outbox._add(
            link, dedupe_key=key, class_="reply", payload={"parts": recipes}, instance_id=link.agent_ref, work_id=source.work["id"], request_id=request_id, message_thread_id=message_thread_id, session=writer
        )
        row = await writer.get(TelegramOutboxRow, identity)
        snapshot = asdict(source)
        snapshot["responses"] = list(snapshot["responses"])
        cleanup = None
        if input_origin is None:
            cleanup = keyboard_cleanup_row(group.selected, owner_user_id=link.owner_user_id, now=self.outbox.clock(), dedupe_key="notice:work-interaction-cleanup:" + callback_id)
            cleanup.payload = {**cleanup.payload, "work_request_parent_id": identity}
            writer.add(cleanup)
        basis = {
            "version": 4 if input_origin is not None else 3,
            "source": snapshot,
            "locale": settings.locale,
            "concealed": False,
            "parts": recipes,
            "keyboard": keyboard,
            "reveal": None,
            "cleanup_id": cleanup.id if cleanup is not None else None,
            "interaction": state,
            "origin": {"callback_id": callback_id, "anchor_outbox_id": parent.id, "presentation_version": group.presentation_version, "message_id": message_id},
        }
        if reopen_origin is not None:
            basis["reopen"] = reopen_origin
        if input_origin is not None:
            basis["input"] = input_origin
        if state["view"] == "prompt":
            basis["prompt_id"] = uuid4().hex
        row.payload = _safe_json({**row.payload, "work_request_basis": basis, "product_keyboard_basis": keyboard})
        parts = self.outbox._parts(recipes, 0)
        if keyboard:
            fresh = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=identity, presentation_version=0, chat_id=chat_id, message_id=None, message_thread_id=message_thread_id, session=writer)
            parts[-1]["callback_group_id"] = fresh.group_id
            for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == fresh.group_id)):
                control.created_at = created_at
                control.payload = {**control.payload, "presentation_pending": True}
        row.parts = parts
        if state["view"] == "prompt":
            from app.telegram_bots.work_typing import PURPOSES

            prompts = Prompts(self.callbacks)
            payload = {"request_id": request_id, "work_id": source.work["id"], "seen": source.seen, "interaction": state}
            await prompts.open(link, PURPOSES[state["purpose"]], payload, chat_id=chat_id, message_thread_id=message_thread_id, message_id=None, prompt_id=basis["prompt_id"], guard=composed, session=writer, defer_visibility=True)
            await prompts.attach(link, basis["prompt_id"], outbox_id=identity, guard=composed, session=writer)
        if input_origin is None:
            await self.callbacks.consume_group(group, reason=reason, session=writer)
            if reason == "work-refresh":
                from app.telegram_bots.work_outcome_transfer import transfer_outcome

                await transfer_outcome(writer, link, callback_id, identity, outbox=self.outbox)
        return identity
