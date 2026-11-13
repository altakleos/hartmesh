"""Full acknowledged assignment drafts admit one immutable Save operation."""

from sqlalchemy import select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assignment_drafts import AssignmentDrafts, DraftProjection, snapshot_draft
from app.telegram_bots.assignment_sources import private_facts, private_snapshot
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.authority import CeilingRefused
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.work_actions import request_for, same_body, thaw
from app.telegram_bots.work_delegate_actions import delegate_source, draft_body_signature, require_delegate_scope
from app.telegram_bots.work_view_origins import acknowledged_view, anchor_signature
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def displayed_assignment(link, parent, callback):
    original = acknowledged_view(link, parent, callback.payload.get("presentation_version"))
    source = original.payload["work_news_basis"]["source"]
    final = original.parts[-1]
    if (
        source["key"] != "assignment.card"
        or source["work"]["phase"] != "draft"
        or original.payload["work_news_basis"]["concealed"]
        or callback.kind not in {"assign_start", "assign_save", "assign_edit", "assign_discard", "assign_answer"}
        or callback.target_kind != "draft"
        or (callback.target_id, callback.payload.get("draft_id")) != (source["work"]["draft_id"], source["work"]["draft_id"])
        or final.get("callback_group_id") != callback.group_id
        or final.get("message_ids") != [callback.message_id]
        or callback.message_thread_id != original.message_thread_id
    ):
        raise CallbackRefused()
    return original, source


def consent_origin(original, callback):
    return {"callback_id": callback.id, "anchor_outbox_id": original.id, "presentation_version": callback.payload["presentation_version"], "signature": anchor_signature(original)}


class AssignmentAdmissions:
    def __init__(self, plans, acts):
        self.plans, self.acts = plans, acts
        self.callbacks, self.outbox = plans.callbacks, plans.outbox
        if acts.session_factory.kw.get("bind") is not self.outbox.sessions.kw.get("bind"):
            raise ValueError("assignment-admission-requires-shared-storage")

    async def stage(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            await prepare_publication_guard(guard)
            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                parent = await reader.get(TelegramOutboxRow, callback.payload.get("anchor_outbox_id")) if callback else None
                if (
                    callback is None
                    or parent is None
                    or (callback.link_id, callback.chat_id, callback.binding_generation, callback.message_id, callback.message_thread_id) != (link.id, chat_id, link.binding_generation, message_id, message_thread_id)
                ):
                    raise CallbackRefused()
                original, source = displayed_assignment(link, parent, callback)
                if callback.kind not in {"assign_start", "assign_save"}:
                    raise CallbackRefused()
                origin, draft_id, start = consent_origin(original, callback), callback.target_id, callback.kind == "assign_start"
                saved = await reader.get(TelegramActRow, callback_id)
            if saved is not None:
                # An old press observes its original journal and proof only.
                async with self.callbacks.writer(link, guard) as writer:
                    control = await writer.get(TelegramCallbackRow, callback_id, populate_existing=True)
                    anchor = await writer.get(TelegramOutboxRow, origin["anchor_outbox_id"], populate_existing=True)
                    actual, _source = displayed_assignment(link, anchor, control)
                    controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == control.group_id)))
                    _validate_rows(actual, controls)
                    journal = await writer.get(TelegramActRow, callback_id, populate_existing=True)
                    act = _snapshot(journal)
                    require_delegate_scope(link, act)
                    stored = delegate_source(act)
                    parsed = request_for("delegate", act.body, identity=act.id)
                    original_draft = source["work"]["draft"]
                    if (
                        consent_origin(actual, control) != origin
                        or (control.link_id, control.chat_id, control.binding_generation, control.message_id, control.message_thread_id) != (link.id, chat_id, link.binding_generation, message_id, message_thread_id)
                        or any(row.payload.get("work_action_id") != act.id for row in controls)
                        or stored["draft_id"] != draft_id
                        or stored["start"] is not start
                        or stored["parent_id"] is not None
                        or stored["origin_kind"] != "callback"
                        or stored["draft_signature"] != draft_body_signature(link, private_snapshot(source["work"]["root"]))
                        or (parsed.objective, parsed.success_criteria) != (original_draft["objective"], original_draft["success_criteria"])
                        or not same_body(parsed.model_dump(mode="json")["due_at"], original_draft["due_at"])
                        or not same_body(parsed.model_dump(mode="json", exclude_unset=True), act.body)
                        or not same_body(act.continuation.get("delegate_origin"), origin)
                    ):
                        raise CallbackRefused()
                    return act
            current = await prepare_attention(self.plans.reader, link)
            drafts = AssignmentDrafts(self.acts)

            async def admit(writer, act):
                await guard.check(writer)
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                anchor = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                actual, frozen = displayed_assignment(link, anchor, group.selected)
                controls = [await writer.get(TelegramCallbackRow, row.id, populate_existing=True) for row in group.rows]
                root = await writer.get(TelegramActRow, draft_id, populate_existing=True)
                shown = snapshot_draft(link, root)
                expected_ids = [] if act is None else [act.id]
                if (
                    consent_origin(actual, group.selected) != origin
                    or not same_body(frozen, source)
                    or not same_body(root.body, source["work"]["root"]["body"])
                    or not same_body(private_facts(root), {**source["work"]["root"], "operation_ids": expected_ids})
                    or list(root.operation_ids) != expected_ids
                    or any(row.payload.get("work_action_id") is not None for row in controls)
                ):
                    raise CallbackRefused(None)
                if act is not None:
                    for row in controls:
                        row.payload = {**row.payload, "work_action_id": act.id}
                    journal = await writer.get(TelegramActRow, act.id, populate_existing=True)
                    journal.continuation = {**thaw(journal.continuation), "delegate_source": {**thaw(journal.continuation["delegate_source"]), "origin_kind": "callback"}, "delegate_origin": origin}
                    root.continuation = {"version": 1, "assignment_admission": origin}
                    if delegate_source(_snapshot(journal))["draft_signature"] != draft_body_signature(link, root):
                        raise CallbackRefused()
                    await writer.flush()
                projection = SaveProjection(link, _snapshot(root), start=start)
                await current(session=writer, projection=projection)
                drafts._age(shown)
                guard.current()

            return await self.acts.stage_delegate(link, draft_id, operation_id=callback_id, start=start, admission=admit)

        return await await_drained(perform())


class SaveProjection(DraftProjection):
    def __init__(self, link, saved, *, start):
        super().__init__(link, saved.body["origin"], saved=saved)
        self.start = start

    def check_with_permissions(self, rows, held):
        super().check_with_permissions(rows, held)
        if not ({"agents:write", "runs:create", "threads:write"} if self.start else {"agents:write"}).issubset(held):
            raise CeilingRefused()
