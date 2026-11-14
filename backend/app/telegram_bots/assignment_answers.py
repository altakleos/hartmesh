"""An acknowledged proposal choice owns one new message in its original chat."""

import re
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

from sqlalchemy import select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assignment_controls import consent_origin, displayed_assignment
from app.telegram_bots.assignment_drafts import AssignmentDrafts, DraftProjection, snapshot_draft
from app.telegram_bots.assignment_lineage import require_fresh_draft
from app.telegram_bots.assignment_proposal_records import ProposalInputs, validate_proposal_reply_basis
from app.telegram_bots.assignment_proposal_source import validate_proposal_basis
from app.telegram_bots.assignment_sources import private_facts
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.inbox import CALLBACK_PUBLICATION_KEY, SubmissionInputStage, _callback_owner, _identity_record, _submission_input, publication_identity, validate_publication_inputs
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.render import render_plain
from app.telegram_bots.work_actions import same_body
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


def _hex(value, length=32):
    return type(value) is str and re.fullmatch("[0-9a-f]{" + str(length) + "}", value) is not None


def validate_answer_document(data):
    """Closed choice syntax; a stored document never grants another press."""
    try:
        if (
            type(data) is not dict
            or set(data) != {"version", "text", "locale", "human_message_kwargs", "source_outbox_id", "parent_publication", "proposal_basis", "draft_id", "gesture"}
            or type(data["version"]) is not int
            or data["version"] != 2
            or data["locale"] not in {"en-US", "zh-CN"}
            or data["text"] != render_plain("assign.just_answer_message", data["locale"])
            or not _hex(data["source_outbox_id"])
            or not _hex(data["draft_id"])
        ):
            raise ValueError()
        parent = _identity_record(data["parent_publication"])
        source = data["proposal_basis"]
        record = replace(parent, dispatch_started=True, run_id=source["projection"]["output"]["run_id"])
        validate_proposal_basis(record, source)
        response = {"version": 1, "kind": "assignment_card_response", "tool_call_id": source["messages"][-1]["content"]["tool_call_id"], "choice": "answer_here"}
        gesture = data["gesture"]
        if (
            not same_body(data["human_message_kwargs"], {"assignment_card_response": response})
            or not same_body(source["publication"], data["parent_publication"])
            or type(gesture) is not dict
            or set(gesture) != {"kind", "callback_id", "callback_kind", "update_id", "origin"}
            or (gesture["kind"], gesture["callback_kind"]) != ("callback", "assign_answer")
            or not _hex(gesture["callback_id"])
            or type(gesture["update_id"]) is not int
            or gesture["update_id"] < 0
            or type(gesture["origin"]) is not dict
            or set(gesture["origin"]) != {"callback_id", "anchor_outbox_id", "presentation_version", "signature"}
            or gesture["origin"]["callback_id"] != gesture["callback_id"]
            or gesture["origin"]["anchor_outbox_id"] != data["source_outbox_id"]
            or type(gesture["origin"]["presentation_version"]) is not int
            or gesture["origin"]["presentation_version"] < 0
            or not _hex(gesture["origin"]["signature"], 64)
        ):
            raise ValueError()
        return deepcopy(data)
    except (ValueError, TypeError, KeyError, AttributeError):
        raise ValueError("assignment-answer-conflict") from None


class _AnswerFrame(DraftProjection):
    def __init__(self, link, root, parent, *, locale):
        super().__init__(link, root.body["origin"], saved=_snapshot(root))
        self.inputs = ProposalInputs(link, _item(parent))
        self.locale = locale

    def extend(self, query):
        result = self.inputs.extend(super().extend(query))
        self.locale_column = len(result.column_descriptions)
        return result.add_columns(TelegramPersonSettingsRow.locale).join(TelegramPersonSettingsRow, TelegramPersonSettingsRow.owner_user_id == self.link.owner_user_id)

    def check_with_permissions(self, rows, held):
        super().check_with_permissions(rows, held)
        self.inputs.check(rows)
        if rows[0][self.locale_column] != self.locale:
            raise ProductSourceRetired()


class _AnswerAdmissionGuard:
    def __init__(self, original, outbox):
        self.original, self.outbox, self.deadline = original, outbox, None

    @property
    def captured_link(self):
        return self.original.captured_link

    def freeze(self, draft, members):
        deadline = aware(draft.created_at) + timedelta(hours=self.outbox.config.callbacks.draft_ttl_hours)
        self.deadline = min([deadline, *[aware(member.expires_at) for member in members if member.expires_at is not None]])
        self.current()

    def current(self):
        result = self.original.current()
        if result is not None and result is not True or self.deadline is not None and self.outbox.clock() >= self.deadline:
            raise CallbackRefused("stale.draft")
        return True

    async def prepare(self):
        self.current()
        await prepare_publication_guard(self.original)
        self.current()

    async def check(self, writer):
        result = await self.original.check(writer)
        self.current()
        return result


class AssignmentAnswers:
    def __init__(self, assignments):
        self.assignments = assignments
        self.outbox, self.callbacks = assignments.outbox, assignments.callbacks
        self.drafts = AssignmentDrafts(assignments.reader.acts)

    async def stage(self, link, callback_id, *, update_id, chat_id, message_id, message_thread_id, guard):
        async def perform():
            if type(update_id) is not int or update_id < 0:
                raise CallbackRefused()
            settings = await self.assignments.settings.get(link.owner_user_id)
            if settings is None:
                raise CallbackRefused()
            current = await prepare_attention(self.assignments.reader, link)
            admission_guard = _AnswerAdmissionGuard(guard, self.outbox)
            async with self.callbacks.writer(link, admission_guard) as writer:
                control = await writer.get(TelegramCallbackRow, callback_id, with_for_update=True, populate_existing=True)
                parent = await writer.get(TelegramOutboxRow, control.payload.get("anchor_outbox_id"), with_for_update=True, populate_existing=True) if control else None
                if (
                    control is None
                    or parent is None
                    or control.kind != "assign_answer"
                    or (control.link_id, control.chat_id, control.binding_generation, control.message_id, control.message_thread_id) != (link.id, chat_id, link.binding_generation, message_id, message_thread_id)
                ):
                    raise CallbackRefused()
                original, shown = displayed_assignment(link, parent, control)
                proof = validate_proposal_reply_basis(_item(parent))
                if proof.record.scope.thread_id != parent.payload["reply_basis"]["publication"]["scope"]["thread_id"] or shown["work"]["source"] != "tool":
                    raise CallbackRefused()
                root = await writer.get(TelegramActRow, control.target_id, with_for_update=True, populate_existing=True)
                draft = snapshot_draft(link, root)
                members = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == control.group_id).order_by(TelegramCallbackRow.id).with_for_update().execution_options(populate_existing=True)))
                _validate_rows(original, members)
                origin = consent_origin(original, control)
                witness = {"version": 1, "assignment_navigation": {"kind": "answer_here", "origin": origin}}
                if root.operation_ids or root.continuation is not None or root.stop_dispatch is not None or root.cancel_requested_at is not None or any(member.payload.get("work_action_id") is not None for member in members):
                    raise CallbackRefused()
                frame = _AnswerFrame(link, root, parent, locale=settings.locale)
                await current(session=writer, projection=frame)
                if CALLBACK_PUBLICATION_KEY in control.payload:
                    if root.state != "refused" or root.reason != "answered-here" or not same_body(root.result, witness) or any(member.consumed_at is None or member.closed_reason != "answered-here" for member in members):
                        raise CallbackRefused()
                    saved = await self.outbox.inbox.lock_callback_input(link, callback_id, session=writer)
                    document = saved.submission
                    if document is None or document["version"] != 2 or document["gesture"]["origin"] != origin or document["draft_id"] != root.id or document["source_outbox_id"] != parent.id:
                        raise CallbackRefused()
                else:
                    group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=admission_guard, session=writer)
                    self.drafts._age(draft)
                    admission_guard.freeze(draft, members)
                    if root.state != "draft" or not same_body(private_facts(root), shown["work"]["root"]):
                        raise CallbackRefused()
                    await require_fresh_draft(writer, link, root.id)
                    if any(CALLBACK_PUBLICATION_KEY in member.payload for member in members):
                        raise CallbackRefused()
                    source_basis = deepcopy(parent.payload["reply_basis"]["outcome"]["proposal_basis"])
                    document = validate_answer_document(
                        {
                            "version": 2,
                            "text": render_plain("assign.just_answer_message", settings.locale),
                            "locale": settings.locale,
                            "human_message_kwargs": {"assignment_card_response": {"version": 1, "kind": "assignment_card_response", "tool_call_id": source_basis["messages"][-1]["content"]["tool_call_id"], "choice": "answer_here"}},
                            "source_outbox_id": parent.id,
                            "parent_publication": publication_identity(proof.record),
                            "proposal_basis": source_basis,
                            "draft_id": root.id,
                            "gesture": {"kind": "callback", "callback_id": callback_id, "callback_kind": "assign_answer", "update_id": update_id, "origin": origin},
                        }
                    )
                    inputs = deepcopy(parent.payload["reply_basis"]["inputs"]["inputs"])
                    historical = validate_publication_inputs(proof.record, parent.payload["reply_basis"]["inputs"])
                    owner = {
                        "version": 2,
                        "update_id": update_id,
                        "inputs": inputs,
                        "input_sha256": _submission_input(historical, document),
                        "submission": document,
                        "source_outbox_id": parent.id,
                        "source_publication": publication_identity(proof.record),
                        "thread_preparation": None,
                        "scope": None,
                        "turn_id": callback_id,
                        "publications": [],
                        "current_publication_id": None,
                        "confirmed_publication_id": None,
                        "assistant_message_id": None,
                        "outbox_id": None,
                        "delivery_ids": [],
                        "route_complete": False,
                        "state": "accepted",
                        "closed_at": None,
                    }
                    control.payload = {**control.payload, CALLBACK_PUBLICATION_KEY: owner}
                    _owner, saved = _callback_owner(control.payload, callback_id)
                    root.state, root.reason, root.result, root.updated_at = "refused", "answered-here", witness, self.outbox.clock()
                    await self.callbacks.consume_group(group, reason="answered-here", session=writer)
                    await writer.flush()
                await current(session=writer, projection=_AnswerFrame(link, root, parent, locale=settings.locale))
                admission_guard.current()
                result = SubmissionInputStage(saved.origins, saved.text, saved.human_message_kwargs, saved.thread_id, document["parent_publication"], callback_id, saved.record, saved.route_complete)
            await self.assignments.observe(link, root.id)
            return result

        return await await_drained(perform())
