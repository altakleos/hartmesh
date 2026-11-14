"""Immutable saved-assignment drafts retain their original command revisions."""

from collections.abc import Mapping
from datetime import timedelta

from sqlalchemy import and_
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.assignment_drafts import draft_scope
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body, thaw
from app.telegram_bots.work_management_actions import same_binding
from app.telegram_bots.work_result_identity import digest
from deerflow.agent_instances.contract import AgentConflict
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained


def edit_signature(row, origin_kind):
    return digest([row.id, {key: getattr(row, key) for key in _ACT_SCOPE}, thaw(row.body), origin_kind])


def snapshot_edit(link, row):
    try:
        body = thaw(row.body)
        source = row.continuation.get("edit_draft_source") if isinstance(row.continuation, Mapping) else None
        if (
            not same_binding(link, row)
            or row.kind != "edit"
            or row.work_id is None
            or row.request_id is not None
            or row.message_id is not None
            or row.state not in {"draft", "refused"}
            or row.attempts != 0
            or row.first_attempt_at is not None
            or row.stop_dispatch is not None
            or row.cancel_requested_at is not None
            or len(row.operation_ids) > 1
        ):
            raise ValueError()
        if (
            type(body) is not dict
            or set(body) != {"version", "draft", "original", "expected_revision", "expected_assignment_revision"}
            or type(body["version"]) is not int
            or body["version"] != 1
            or any(type(body[key]) is not int or not 1 <= body[key] <= 2147483647 for key in ("expected_revision", "expected_assignment_revision"))
        ):
            raise ValueError()
        for key in ("draft", "original"):
            parsed = DraftAssignment.model_validate(body[key])
            if not same_body(parsed.model_dump(mode="json"), body[key]):
                raise ValueError()
        if (
            not isinstance(source, Mapping)
            or set(source) != {"version", "origin_kind", "signature", "link_id"}
            or type(source["version"]) is not int
            or source["version"] != 1
            or source["origin_kind"] not in {"internal", "callback"}
            or source["link_id"] != row.link_id
            or source["signature"] != edit_signature(row, source["origin_kind"])
        ):
            raise ValueError()
        return _snapshot(row) if isinstance(row, TelegramActRow) else row
    except (ValueError, KeyError, TypeError, AttributeError):
        raise ProductSourceRetired() from None


class WorkEditDrafts:
    def __init__(self, acts):
        self.acts, self.work, self.links, self.authority, self.outbox = acts, acts.work, acts.links, acts.authority, acts.outbox

    def age(self, row):
        if row.state != "draft" or self.outbox.clock() >= aware(row.created_at) + timedelta(hours=self.outbox.config.callbacks.draft_ttl_hours):
            raise CallbackRefused("stale.draft")

    async def create(self, link, *, operation_id, work_id, draft=None, admission=None, confirmation_current=lambda: None):
        if draft is not None and type(draft) is not DraftAssignment:
            raise ValueError("invalid-edit-draft")

        async def perform():
            confirmation_current()
            async with self.outbox.sessions() as reader:
                saved = await reader.get(TelegramActRow, operation_id)
            if saved is not None:
                result = await self.get(link, operation_id)
                if result.work_id != work_id or draft is not None and not same_body(result.body["draft"], draft.model_dump(mode="json")):
                    raise ValueError("edit-source-conflict")
                confirmation_current()
                return result
            current = await self.work.get(actor=PrincipalRef("human", link.owner_user_id), instance_id=link.agent_ref, work_id=work_id)
            original = DraftAssignment(objective=current["objective"], success_criteria=current["success_criteria"], due_at=current["due_at"])
            body = {"version": 1, "draft": (draft or original).model_dump(mode="json"), "original": original.model_dump(mode="json"), "expected_revision": current["revision"], "expected_assignment_revision": current["assignment_revision"]}
            prepared = await prepare_attention(self, link)
            confirmation_current()
            async with self.outbox._writer(link.id) as (writer, _binding):
                if admission is not None:
                    await admission(writer, None)
                row = await writer.get(TelegramActRow, operation_id, with_for_update=True, populate_existing=True)
                confirmation_current()
                if row is not None:
                    if not same_body(row.body, body):
                        raise ValueError("edit-source-conflict")
                else:
                    now = self.outbox.clock()
                    row = TelegramActRow(id=operation_id, **{**draft_scope(link), "kind": "edit", "work_id": work_id}, body=body, state="draft", operation_ids=[], attempts=0, created_at=now, updated_at=now)
                    writer.add(row)
                    await writer.flush()
                    confirmation_current()
                    row.continuation = {"edit_draft_source": {"version": 1, "origin_kind": "internal", "link_id": link.id, "signature": edit_signature(row, "internal")}}
                    await writer.flush()
                result = snapshot_edit(link, row)
                self.age(result)
                await prepared(session=writer, projection=EditDraftProjection(link, result, staged=True))
                if admission is not None:
                    await admission(writer, result)
                result = snapshot_edit(link, row)
                await prepared(session=writer, projection=EditDraftProjection(link, result, staged=True))
                confirmation_current()
                self.age(result)
                return result

        return await await_drained(perform())

    async def get(self, link, identity):
        async def perform():
            async with self.outbox.sessions() as reader:
                row = await reader.get(TelegramActRow, identity)
                saved = snapshot_edit(link, row)
            current = await prepare_attention(self, link)
            await current(projection=EditDraftProjection(link, saved))
            return saved

        return await await_drained(perform())


class EditDraftProjection:
    def __init__(self, link, saved, *, staged=False):
        self.link, self.saved, self.staged = link, saved, staged

    def extend(self, query):
        self.column = len(query.column_descriptions)
        draft, work = aliased(TelegramActRow), aliased(AgentWorkRow)
        return query.add_columns(draft, work).join(draft, draft.id == self.saved.id).join(work, and_(work.id == draft.work_id, work.instance_id == self.link.agent_ref))

    def check_with_permissions(self, rows, held):
        row, work = rows[0][self.column : self.column + 2]
        snapshot_edit(self.link, row)
        if (
            rows[0][2] & 6 != 6
            or "agents:write" not in held
            or not same_binding(rows[0][0], self.saved)
            or any(getattr(row, key) != getattr(self.saved, key) for key in _ACT_SCOPE)
            or not same_body(row.body, self.saved.body)
            or not same_body(row.continuation, self.saved.continuation)
            or not same_body(row.operation_ids, self.saved.operation_ids)
            or row.state != self.saved.state
        ):
            raise ProductSourceRetired()
        if self.staged and ((work.revision, work.assignment_revision) != (row.body["expected_revision"], row.body["expected_assignment_revision"]) or work.status not in {"open", "blocked"}):
            raise AgentConflict("Review the current assignment", reason="changed")


class EditCommandProof:
    def __init__(self, act, reference, *, admitting=False, staged=False, clock=None, draft_ttl_hours=24):
        self.act, self.reference, self.admitting, self.staged = act, reference, admitting, staged
        self.clock, self.draft_ttl_hours = clock, draft_ttl_hours

    def extend(self, query):
        self.column = len(query.column_descriptions)
        root, member = aliased(TelegramActRow), aliased(TelegramActRow)
        return query.add_columns(root, member).join(root, root.id == self.reference["id"]).outerjoin(member, and_(member.kind == "edit", member.continuation["management_source"]["draft_ref"]["id"].as_string() == root.id))

    def check(self, rows):
        root = rows[0][self.column]
        snapshot_edit(rows[0][0], root)
        source = root.continuation["edit_draft_source"]
        if source["signature"] != self.reference["signature"] or source["origin_kind"] != "internal" or root.state != "draft" or any(getattr(root, key) != getattr(self.act, key) for key in _ACT_SCOPE if key != "kind"):
            raise ProductSourceRetired()
        expected = WorkCommand(
            operation_id=self.act.id,
            action="edit",
            expected_revision=root.body["expected_revision"],
            expected_assignment_revision=root.body["expected_assignment_revision"],
            objective=root.body["draft"]["objective"],
            success_criteria=root.body["draft"]["success_criteria"],
            due_at=root.body["draft"]["due_at"],
        ).model_dump(mode="json", exclude_unset=True)
        members = {row[self.column + 1].id for row in rows if row[self.column + 1] is not None}
        if not same_body(expected, self.act.body) or members != {self.act.id}:
            raise ProductSourceRetired()
        if self.admitting:
            if root.operation_ids or root.continuation.get("edit_admission") is not None:
                raise ProductSourceRetired()
        else:
            from app.telegram_bots.work_management_consent import edit_admission_for

            witness = edit_admission_for(self.act, self.reference)
            if list(root.operation_ids) != [self.act.id] or not same_body(root.continuation.get("edit_admission"), witness):
                raise ProductSourceRetired()
        if self.staged and (self.clock is None or self.clock() >= aware(root.created_at) + timedelta(hours=self.draft_ttl_hours)):
            raise CallbackRefused("stale.draft")
