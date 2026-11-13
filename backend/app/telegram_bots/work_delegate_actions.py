"""Explicit stored-draft delegation and exact no-due recovery, without launching."""

import hashlib
from datetime import datetime, timedelta

from pydantic import ValidationError
from sqlalchemy import and_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.authority import AccessOff, AuthorityUnavailable, CeilingRefused, require
from deerflow.agent_instances.contract import AgentConflict, AgentDenied, Refusal
from deerflow.agent_instances.work import INSPECT
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained


def no_due_id(identity):
    return hashlib.sha256((identity + ":no-due").encode()).hexdigest()[:32]


def private_draft(act):
    """Closed private drafts never become executable service operations."""
    from collections.abc import Mapping

    return (
        act.attempts == 0
        and act.first_attempt_at is None
        and isinstance(act.body, Mapping)
        and "operation_id" not in act.body
        and set(act.body) - {"criteria_origin"}
        in (
            {"version", "draft", "original", "source", "due_note", "origin"},
            {"version", "request", "source", "origin"},
        )
    )


def delegate_source(act):
    from app.telegram_bots.work_actions import thaw

    value = thaw(act.continuation)
    if type(value) is not dict or set(value) not in ({"version", "delegate_source"}, {"version", "delegate_source", "activation_continuation"}) or type(value["version"]) is not int or value["version"] != 1:
        raise ValueError("invalid-delegate-source")
    source = value["delegate_source"]
    if (
        type(source) is not dict
        or set(source) != {"draft_id", "draft_signature", "authorized_at", "start", "due_text", "parent_id"}
        or type(source["start"]) is not bool
        or type(source["draft_id"]) is not str
        or len(source["draft_id"]) != 32
        or type(source["draft_signature"]) is not str
        or len(source["draft_signature"]) != 64
        or source["due_text"] is not None
        and (type(source["due_text"]) is not str or not 1 <= len(source["due_text"]) <= 120)
        or source["parent_id"] is not None
        and (type(source["parent_id"]) is not str or len(source["parent_id"]) != 32)
        or type(source["authorized_at"]) is not str
        or datetime.fromisoformat(source["authorized_at"]).utcoffset() is None
    ):
        raise ValueError("invalid-delegate-source")
    return source


def draft_body_signature(link, row):
    from app.telegram_bots.assignment_drafts import draft_scope
    from app.telegram_bots.work_actions import thaw
    from app.telegram_bots.work_result_identity import digest

    return digest([row.id, draft_scope(link), thaw(row.body)])


def require_delegate_scope(link, act):
    from app.telegram_bots.assignment_drafts import draft_scope

    if link is None or any(getattr(act, key) != value for key, value in draft_scope(link).items()):
        raise AgentDenied("The saved delegation binding is no longer current", reason="not-visible")


def no_due_body(parent):
    from app.telegram_bots.work_actions import thaw

    source = delegate_source(parent)
    body = thaw(parent.body)
    if source["parent_id"] is not None or "due_at" not in body or body["due_at"] is None or set(body) != {"operation_id", "objective", "success_criteria", "due_at"}:
        raise ValueError("invalid-no-due-parent")
    return DelegateWork(operation_id=no_due_id(parent.id), objective=body["objective"], success_criteria=body["success_criteria"] + ("\n\n" + source["due_text"] if source["due_text"] else ""))


def qualify_no_due(parent, child):
    from app.telegram_bots.work_actions import _ACT_SCOPE, same_body

    expected = no_due_body(parent)
    first, second = delegate_source(parent), delegate_source(child)
    if (
        list(parent.operation_ids) != [child.id]
        or child.operation_ids
        or any(getattr(parent, key) != getattr(child, key) for key in _ACT_SCOPE)
        or child.id != expected.operation_id
        or not same_body(child.body, expected.model_dump(mode="json", exclude_unset=True))
        or second != {**first, "parent_id": parent.id, "start": False}
    ):
        raise ValueError("invalid-no-due-child")


class DelegateActionMethods:
    async def stage_delegate(self, link, draft_act_id, *, operation_id, start=False, admission=None):
        async def perform():
            async with self._work_serial(operation_id):
                return await self._stage_delegate(link, draft_act_id, operation_id=operation_id, start=start, admission=admission)

        return await await_drained(perform())

    async def delegate(self, link, draft_act_id, *, operation_id, start=False):
        async def perform():
            async with self._work_serial(operation_id):
                act = await self._stage_delegate(link, draft_act_id, operation_id=operation_id, start=start)
                return await self._attempt_delegate(act, explicit=True)

        return await await_drained(perform())

    async def _stage_delegate(self, link, draft_act_id, *, operation_id, start, admission=None):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.assignment_drafts import AssignmentDrafts, snapshot_draft
        from app.telegram_bots.work_actions import request_for
        from app.telegram_bots.work_authority import prepare_work_owner

        if type(start) is not bool:
            raise ValueError("invalid-delegate-mode")
        await self._work_link(link, kind="delegate", writable=True)
        async with self.session_factory() as reader:
            saved = await reader.get(TelegramActRow, operation_id)
            if saved is not None:
                act = _snapshot(saved)
                source = delegate_source(act)
                if (source["draft_id"], source["start"], source["parent_id"]) != (draft_act_id, start, None) or act.kind != "delegate" or act.link_id != link.id:
                    raise ValueError("act-source-conflict")
                request_for("delegate", act.body, identity=operation_id)
                from app.telegram_bots.assignment_drafts import draft_scope

                if any(getattr(act, key) != value for key, value in draft_scope(link).items()):
                    raise ValueError("act-source-conflict")
                return act
        drafts = AssignmentDrafts(self)
        shown = await drafts.get(link, draft_act_id)
        values = dict(operation_id=operation_id, objective=shown.draft.objective, success_criteria=shown.draft.success_criteria)
        if shown.draft.due_at is not None:
            values["due_at"] = shown.draft.due_at
        body = DelegateWork(**values)
        owner = await prepare_work_owner(self.authority, link.owner_user_id)
        async with self.outbox._writer(link.id) as (writer, _binding):
            if admission is not None:
                await admission(writer, None)
            root = await writer.get(TelegramActRow, draft_act_id, with_for_update=True, populate_existing=True)
            fresh = snapshot_draft(link, root)
            drafts._age(fresh)
            if fresh.signature != shown.signature:
                raise ValueError("draft-source-conflict")
            if root.operation_ids:
                raise ValueError("draft-already-admitted")
            await self._stage_work_act(link, kind="delegate", request_id=None, work_id=None, request=body, owner=owner, session=writer)
            saved = await writer.get(TelegramActRow, operation_id, populate_existing=True)
            saved.continuation = {
                "version": 1,
                "delegate_source": {"draft_id": root.id, "draft_signature": draft_body_signature(link, root), "authorized_at": self.outbox.clock().isoformat(), "start": start, "due_text": shown.original.due_text, "parent_id": None},
            }
            root.operation_ids = [saved.id]
            await writer.flush()
            act = _snapshot(saved)
            if admission is not None:
                await admission(writer, act)
            return act

    async def _delegate_receipt(self, act):
        from app.telegram_bots.work_actions import request_for

        if private_draft(act):
            return None
        delegate_source(act)
        actor = PrincipalRef("human", act.owner_user_id)
        body = request_for("delegate", act.body, identity=act.id)
        async with self.session_factory() as reader:
            parent, _view = await self.work._parent(reader, actor, act.instance_id, INSPECT)
            receipt = await self.work._receipt(reader, actor, parent, body, None)
        if receipt is not None:
            return receipt
        if act.operation_ids:
            child = await self._work_record(act.operation_ids[0])
            qualify_no_due(act, child)
            receipt = await self._delegate_receipt(child)
            if receipt is not None:
                return {"delegate_child_id": child.id, "receipt": receipt}
        return None

    async def _finish_delegate(self, act, result):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import _ACT_SCOPE, same_body, thaw

        async with self.outbox._writer(act.link_id) as (writer, _binding):
            saved = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
            if saved is None or any(getattr(saved, key) != getattr(act, key) for key in _ACT_SCOPE) or not same_body(saved.body, act.body):
                raise ValueError("act-source-conflict")
            if saved.state == "done" or saved.attempts != act.attempts:
                return _snapshot(saved)
            source = delegate_source(_snapshot(saved))
            saved.state, saved.reason, saved.result, saved.updated_at = "done", None, thaw(result), self.outbox.clock()
            if source["start"] and source["parent_id"] is None:
                record = result.get("receipt", result)
                if record.get("instance_id") != act.instance_id or record.get("creator_id") != act.owner_user_id or type(record.get("assignment_revision")) is not int:
                    raise ValueError("invalid-delegate-receipt")
                saved.continuation = {
                    **thaw(saved.continuation),
                    "activation_continuation": {
                        "version": 1,
                        "phase": "ready",
                        "authorized_at": source["authorized_at"],
                        "child_id": hashlib.sha256((saved.id + ":activate").encode()).hexdigest()[:32],
                        "work_id": record["id"],
                        "assignment_revision": record["assignment_revision"],
                    },
                }
            await writer.flush()
            return _snapshot(saved)

    async def _stage_no_due(self, act):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import _ACT_SCOPE, same_body

        body = no_due_body(act)
        link = await self.links.get(act.link_id)
        require_delegate_scope(link, act)
        owner = await self.authority.prepare_live_owner(act.owner_user_id)
        async with self.outbox._writer(act.link_id) as (writer, binding):
            from app.telegram_bots.work_actions import _SCOPE

            if binding is None or any(getattr(binding, key) != getattr(link, key) for key in _SCOPE):
                raise AgentDenied("The delegation binding changed", reason="not-visible")
            parent = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
            if parent is None or parent.attempts != act.attempts or any(getattr(parent, key) != getattr(act, key) for key in _ACT_SCOPE) or not same_body(parent.body, act.body) or parent.cancel_requested_at is not None:
                return None
            if await writer.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.instance_id == act.instance_id, AgentWorkEventRow.operation_id == act.id)) is not None:
                return None
            if not parent.operation_ids:
                await self._stage_work_act(link, kind="delegate", request_id=None, work_id=None, request=body, owner=owner, session=writer)
                child = await writer.get(TelegramActRow, body.operation_id, populate_existing=True)
                child.continuation = {"version": 1, "delegate_source": {**delegate_source(act), "parent_id": act.id, "start": False}}
                parent.operation_ids = [child.id]
                await writer.flush()
            child = await writer.get(TelegramActRow, body.operation_id, populate_existing=True)
            qualify_no_due(_snapshot(parent), _snapshot(child))
            return _snapshot(child)

    async def _attempt_delegate(self, act, *, explicit=False, attempt_current=lambda: True):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import _ACT_SCOPE, request_for, same_body
        from app.telegram_bots.work_authority import prepare_work_owner

        if private_draft(act):
            return act
        now = self.outbox.clock()
        expired = act.first_attempt_at is not None and now - act.first_attempt_at >= timedelta(hours=1) or act.attempts >= self.outbox.config.acts.max_attempts
        if act.attempts or act.state == "done" or act.first_attempt_at is None:
            try:
                receipt = await self.receipt_for(act)
            except Exception:
                return await self._work_outcome(act, state="uncertain") if expired and not explicit and act.state != "done" else act
            if receipt is not None:
                return await self._finish_delegate(act, receipt)
        retry = explicit and act.state in {"refused", "conflict"} and act.reason in {"agent-not-active", "work-off", "lifecycle-pending", "denied", "grant-required", "not-visible", "storage-busy"}
        if act.state == "done" or act.cancel_requested_at is not None or act.state in {"refused", "conflict"} and not retry:
            return act
        if act.operation_ids:
            try:
                child = await self._work_record(act.operation_ids[0])
                qualify_no_due(act, child)
            except (ValueError, KeyError, TypeError):
                return await self._work_outcome(act, state="uncertain", reason="invalid")
            if retry:
                async with self.outbox._writer(act.link_id) as (writer, _binding):
                    saved = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
                    if (
                        saved is None
                        or saved.state != act.state
                        or saved.reason != act.reason
                        or saved.attempts != act.attempts
                        or saved.cancel_requested_at is not None
                        or any(getattr(saved, key) != getattr(act, key) for key in _ACT_SCOPE)
                        or not same_body(saved.body, act.body)
                        or not same_body(saved.operation_ids, act.operation_ids)
                    ):
                        return _snapshot(saved) if saved is not None else act
                    saved.state, saved.reason, saved.updated_at = "pending", None, self.outbox.clock()
                    await writer.flush()
                    act = _snapshot(saved)
            async with self._work_serial(child.id):
                result = await self._attempt_delegate(child, explicit=explicit, attempt_current=attempt_current)
            if result.state == "done":
                return await self._finish_delegate(act, {"delegate_child_id": child.id, "receipt": result.result})
            if result.state in {"refused", "conflict", "uncertain"}:
                return await self._work_outcome(act, state=result.state, reason=result.reason, result={"delegate_child_id": child.id, "state": result.state, "reason": result.reason})
            return act
        if act.first_attempt_at is None or not explicit and (expired or act.state == "uncertain"):
            return await self._work_outcome(act, state="uncertain")
        try:
            link = await self.links.get(act.link_id)
            require_delegate_scope(link, act)
            await self._work_link(link, kind="delegate", writable=True)
            await require(act.owner_user_id, "agents:read", "agents:write")
            owner = await prepare_work_owner(self.authority, act.owner_user_id)
            if not attempt_current():
                return act
            async with self.outbox._writer(link.id) as (writer, binding):
                from app.telegram_bots.work_actions import _SCOPE

                if binding is None or any(getattr(binding, key) != getattr(link, key) for key in _SCOPE):
                    raise AgentDenied("The delegation binding changed", reason="not-visible")
                saved = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
                if (
                    saved is None
                    or saved.attempts != act.attempts
                    or saved.state == "done"
                    or saved.cancel_requested_at is not None
                    or saved.operation_ids
                    or any(getattr(saved, key) != getattr(act, key) for key in _ACT_SCOPE)
                    or not same_body(saved.body, act.body)
                ):
                    return _snapshot(saved) if saved else act
                if not attempt_current():
                    return _snapshot(saved)
                saved.state, saved.reason, saved.attempts, saved.updated_at = "pending", None, saved.attempts + 1, self.outbox.clock()
                await writer.flush()
                act = _snapshot(saved)
                await owner(act.owner_user_id, writer)
            admission = await prepare_delegate_admission(self, link, act, attempt_current=attempt_current)
            result = await self.work.delegate(actor=PrincipalRef("human", act.owner_user_id), instance_id=act.instance_id, request=request_for("delegate", act.body, identity=act.id), admission=admission)
        except AgentDenied as exc:
            if exc.reason == "manage-required" and delegate_source(act)["parent_id"] is None and "due_at" in act.body:
                try:
                    receipt = await self.receipt_for(act)
                    if receipt is not None:
                        return await self._finish_delegate(act, receipt)
                    child = await self._stage_no_due(act)
                    if child is not None:
                        parent = await self._work_record(act.id)
                        return await self._attempt_delegate(parent, explicit=explicit, attempt_current=attempt_current)
                except ValidationError:
                    return await self._work_outcome(act, state="refused", reason="draft-too-long")
                except Exception:
                    return act
                return act
            return await self._work_outcome(act, state="refused", reason=exc.reason or "denied")
        except AgentConflict as exc:
            return await self._work_outcome(act, state="conflict", reason=exc.reason or "changed")
        except (AccessOff, CeilingRefused, Refusal, ValueError) as exc:
            return await self._work_outcome(act, state="refused", reason=getattr(exc, "reason", None) or "denied")
        except Exception:
            return await self._work_outcome(act, state="uncertain") if act.attempts >= self.outbox.config.acts.max_attempts else act
        return await self._finish_delegate(act, result)


async def prepare_delegate_admission(acts, link, act, *, attempt_current):
    from app.telegram_bots.acts import _snapshot
    from app.telegram_bots.assignment_drafts import snapshot_draft
    from app.telegram_bots.attention_authority import prepare_attention

    source = delegate_source(act)
    async with acts.session_factory() as reader:
        root = await reader.get(TelegramActRow, source["draft_id"])
        snapshot_draft(link, root)
        if root.state != "draft" or draft_body_signature(link, root) != source["draft_signature"]:
            raise AuthorityUnavailable()
        original = _snapshot(root)
    prepared = await prepare_attention(acts, link)
    projection = DelegateProjection(link, act, original, attempt_current)

    async def admission(writer):
        acts._writer_required(writer)
        if not attempt_current():
            raise AuthorityUnavailable()
        await writer.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).with_for_update().execution_options(populate_existing=True))
        ids = [source["draft_id"], *([source["parent_id"]] if source["parent_id"] else []), act.id]
        for identity in ids:
            await writer.scalar(select(TelegramActRow).where(TelegramActRow.id == identity).with_for_update().execution_options(populate_existing=True))
        await prepared(session=writer, projection=projection)

    return admission


class DelegateProjection:
    def __init__(self, link, act, root, attempt_current):
        from app.telegram_bots.assignment_drafts import DraftProjection

        self.link, self.act, self.root, self.attempt_current = link, act, root, attempt_current
        self.draft = DraftProjection(link, root.body["origin"], saved=root)
        self.source = delegate_source(act)

    def extend(self, query):
        result = self.draft.extend(query)
        self.column = len(result.column_descriptions)
        action, parent, event = aliased(TelegramActRow), aliased(TelegramActRow), aliased(AgentWorkEventRow)
        return (
            result.add_columns(action, parent, event)
            .join(action, action.id == self.act.id)
            .outerjoin(parent, parent.id == self.source["parent_id"])
            .outerjoin(event, and_(event.instance_id == self.act.instance_id, event.operation_id == self.source["parent_id"]))
        )

    def check_with_permissions(self, rows, held):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import _ACT_SCOPE, same_body

        self.draft.check_with_permissions(rows, held)
        saved, parent, event = rows[0][self.column : self.column + 3]
        if not self.attempt_current() or "agents:write" not in held:
            raise CeilingRefused()
        if (
            saved.state != "pending"
            or saved.attempts != self.act.attempts
            or saved.cancel_requested_at is not None
            or saved.operation_ids
            or any(getattr(saved, key) != getattr(self.act, key) for key in _ACT_SCOPE)
            or not same_body(saved.body, self.act.body)
            or not same_body(saved.continuation, self.act.continuation)
            or list(self.root.operation_ids) != [self.source["parent_id"] or self.act.id]
        ):
            raise AuthorityUnavailable()
        if self.source["parent_id"] is not None:
            if parent is None or parent.state not in {"pending", "uncertain"} or parent.cancel_requested_at is not None or event is not None:
                raise AuthorityUnavailable()
            qualify_no_due(_snapshot(parent), _snapshot(saved))
        if "due_at" in saved.body and rows[0][2] & 6 != 6:
            raise AgentDenied("A due date requires current management access", reason="manage-required")
        if not self.attempt_current():
            raise AuthorityUnavailable()
