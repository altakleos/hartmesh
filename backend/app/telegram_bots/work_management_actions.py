"""Saved-work commands retain their original CAS and cancellation scope."""

from collections.abc import Mapping
from datetime import timedelta

from sqlalchemy import and_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.authority import AccessOff, AuthorityUnavailable, CeilingRefused
from app.telegram_bots.work_result_identity import digest
from deerflow.agent_instances.contract import AgentConflict, AgentDenied, Refusal
from deerflow.agent_instances.work import UNRESOLVED
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained

MANAGEMENT_KINDS = frozenset({"edit", "cancel"})
_SCOPE_FIELDS = ("attempt_id", "thread_id", "run_id")


def management_signature(act, scope, origin_kind, draft_ref=None):
    from app.telegram_bots.work_actions import _ACT_SCOPE, thaw

    return digest([act.id, {key: getattr(act, key) for key in _ACT_SCOPE}, thaw(act.body), thaw(scope), origin_kind, draft_ref])


def same_binding(link, act):
    return link is not None and (link.id, link.owner_user_id, link.bot_id, link.binding_generation, link.telegram_user_id, link.chat_id, link.agent_kind, link.agent_ref) == (
        act.link_id,
        act.owner_user_id,
        act.bot_id,
        act.binding_generation,
        act.telegram_user_id,
        act.chat_id,
        "instance",
        act.instance_id,
    )


def command_source(act):
    from app.telegram_bots.work_actions import thaw

    value = act.continuation.get("management_source") if isinstance(act.continuation, Mapping) else None
    if (
        not isinstance(value, Mapping)
        or set(value) != {"version", "origin_kind", "scope", "signature", "draft_ref", "link_id"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or value["origin_kind"] not in {"internal", "callback"}
        or set(act.continuation) != ({"management_source", "management_origin"} if value["origin_kind"] == "callback" else {"management_source"})
        or act.kind not in MANAGEMENT_KINDS
    ):
        raise ValueError("invalid-management-source")
    if value["link_id"] != act.link_id:
        raise ValueError("management-binding-conflict")
    reference = value["draft_ref"]
    if act.kind == "edit":
        if (
            not isinstance(reference, Mapping)
            or set(reference) != {"id", "signature"}
            or any(type(reference[key]) is not str or len(reference[key]) != size or any(ch not in "0123456789abcdef" for ch in reference[key]) for key, size in (("id", 32), ("signature", 64)))
        ):
            raise ValueError("invalid-edit-reference")
    elif reference is not None:
        raise ValueError("unexpected-edit-reference")
    scope = value["scope"]
    if scope is not None:
        if (
            not isinstance(scope, Mapping)
            or set(scope) != set(_SCOPE_FIELDS)
            or type(scope["attempt_id"]) is not str
            or not scope["attempt_id"]
            or type(scope["thread_id"]) is not str
            or not scope["thread_id"]
            or scope["run_id"] is not None
            and (type(scope["run_id"]) is not str or not scope["run_id"])
        ):
            raise ValueError("invalid-management-scope")
        if (
            act.stop_dispatch is None
            or act.stop_dispatch.get("attempt_id") != scope["attempt_id"]
            or act.stop_dispatch.get("thread_id") != scope["thread_id"]
            or scope["run_id"] is not None
            and act.stop_dispatch.get("run_id") != scope["run_id"]
        ):
            raise ValueError("management-scope-conflict")
    elif act.stop_dispatch is not None:
        raise ValueError("unexpected-management-scope")
    if value["signature"] != management_signature(act, scope, value["origin_kind"], thaw(reference)):
        raise ValueError("management-source-conflict")
    return thaw(value)


class ManagementActionMethods:
    async def cancel_management(self, act_id, *, admission=None, confirmation_current=lambda: None):
        """Fence logical retries without waiting for a live service attempt."""

        async def perform():
            from app.telegram_bots.acts import _snapshot
            from app.telegram_bots.work_actions import _ACT_SCOPE, request_for, same_body

            captured = await self._work_record(act_id)
            request_for(captured.kind, captured.body, identity=captured.id)
            command_source(captured)
            link = await self.links.get(captured.link_id)
            if not same_binding(link, captured):
                raise AgentDenied("The command binding changed", reason="not-visible")
            await self._work_link(link)
            owner = await self.authority.prepare_live_owner(captured.owner_user_id)
            async with self.outbox._writer(link.id) as (writer, binding):
                row = await writer.get(TelegramActRow, captured.id, with_for_update=True, populate_existing=True)
                if (
                    row is None
                    or not same_binding(binding, captured)
                    or any(getattr(row, key) != getattr(captured, key) for key in _ACT_SCOPE)
                    or not same_body(row.body, captured.body)
                    or not same_body(row.continuation, captured.continuation)
                ):
                    raise ValueError("act-source-conflict")
                command_source(_snapshot(row))
                if admission is not None:
                    await admission(writer, None)
                confirmation_current()
                if row.cancel_requested_at is None:
                    row.cancel_requested_at = row.updated_at = self.outbox.clock()
                    if row.state != "done":
                        conclusive = row.attempts == 0 or row.state in {"refused", "conflict"}
                        row.state, row.reason = ("refused", "cancelled") if conclusive else ("uncertain", "cancel-unconfirmed")
                        if conclusive and row.stop_dispatch is not None:
                            row.stop_dispatch = {**row.stop_dispatch, "state": "ended"}
                await writer.flush()
                result = _snapshot(row)
                if admission is not None:
                    await admission(writer, result)
                await owner(result.owner_user_id, writer)
                confirmation_current()
            try:
                receipt = await self.receipt_for(result)
            except Exception:
                return result
            return await self._management_outcome(result, state="done", result=receipt) if receipt is not None else result

        return await await_drained(perform())

    async def edit(self, link, draft_act_id, *, operation_id):
        async def perform():
            async with self._work_serial(operation_id):
                act = await self._stage_edit(link, draft_act_id, operation_id=operation_id)
                return await self._attempt_management(act, explicit=True)

        return await await_drained(perform())

    async def stage_edit(self, link, draft_act_id, *, operation_id, admission=None, origin=None, confirmation_current=lambda: None):
        async def perform():
            async with self._work_serial(operation_id):
                return await self._stage_edit(link, draft_act_id, operation_id=operation_id, admission=admission, origin=origin, confirmation_current=confirmation_current)

        return await await_drained(perform())

    async def _stage_edit(self, link, draft_act_id, *, operation_id, admission=None, origin=None, confirmation_current=lambda: None):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import request_for, same_body
        from app.telegram_bots.work_edit_drafts import WorkEditDrafts, snapshot_edit

        async with self.session_factory() as reader:
            existing = await reader.get(TelegramActRow, operation_id)
            if existing is not None:
                saved = _snapshot(existing)
                request_for("edit", saved.body, identity=saved.id)
                if saved.kind != "edit" or not same_binding(link, saved) or command_source(saved)["draft_ref"]["id"] != draft_act_id:
                    raise ValueError("act-source-conflict")
                return saved
        await self._work_link(link, kind="edit", writable=True)
        drafts = WorkEditDrafts(self)
        root = await drafts.get(link, draft_act_id)
        drafts.age(root)
        if root.operation_ids:
            raise ValueError("edit-already-admitted")
        source = root.continuation["edit_draft_source"]
        reference = {"id": root.id, "signature": source["signature"]}
        draft = root.body["draft"]
        request = WorkCommand(
            operation_id=operation_id,
            action="edit",
            expected_revision=root.body["expected_revision"],
            expected_assignment_revision=root.body["expected_assignment_revision"],
            objective=draft["objective"],
            success_criteria=draft["success_criteria"],
            due_at=draft["due_at"],
        )
        current = await self.work.get(actor=PrincipalRef("human", link.owner_user_id), instance_id=link.agent_ref, work_id=root.work_id)

        async def admit(writer, act):
            saved = await writer.get(TelegramActRow, root.id, with_for_update=True, populate_existing=True)
            snapshot_edit(link, saved)
            drafts.age(saved)
            if not same_body(saved.body, root.body) or saved.operation_ids or saved.continuation["edit_draft_source"] != source:
                raise ValueError("edit-source-conflict")
            if act is not None:
                saved.operation_ids = [act.id]
                from app.telegram_bots.work_management_consent import edit_admission_for

                witness = edit_admission_for(act, reference)
                saved.continuation = {**saved.continuation, "edit_admission": witness}
                await writer.flush()
            if admission is not None:
                await admission(writer, act)

        original = root.body["original"]
        original_body = WorkCommand(
            operation_id=operation_id,
            action="edit",
            expected_revision=root.body["expected_revision"],
            expected_assignment_revision=root.body["expected_assignment_revision"],
            objective=original["objective"],
            success_criteria=original["success_criteria"],
            due_at=original["due_at"],
        )
        unchanged = same_body(request.model_dump(mode="json", exclude_unset=True), original_body.model_dump(mode="json", exclude_unset=True))
        return await self._stage_management(
            link, request, work_id=root.work_id, current=current, draft_ref=reference, admission=admit, non_effect_reason="unchanged" if unchanged else None, origin=origin, confirmation_current=confirmation_current
        )

    async def cancel(self, link, *, operation_id, work_id):
        async def perform():
            async with self._work_serial(operation_id):
                act = await self._stage_cancel(link, operation_id=operation_id, work_id=work_id)
                return await self._attempt_management(act, explicit=True)

        return await await_drained(perform())

    async def stage_cancel(self, link, *, operation_id, work_id, expected_revision=None, expected_assignment_revision=None, admission=None, origin=None, confirmation_current=lambda: None):
        async def perform():
            async with self._work_serial(operation_id):
                return await self._stage_cancel(
                    link,
                    operation_id=operation_id,
                    work_id=work_id,
                    expected_revision=expected_revision,
                    expected_assignment_revision=expected_assignment_revision,
                    admission=admission,
                    origin=origin,
                    confirmation_current=confirmation_current,
                )

        return await await_drained(perform())

    async def _stage_cancel(self, link, *, operation_id, work_id, expected_revision=None, expected_assignment_revision=None, admission=None, origin=None, confirmation_current=lambda: None):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import request_for

        async with self.session_factory() as reader:
            row = await reader.get(TelegramActRow, operation_id)
            saved = _snapshot(row) if row is not None else None
        if saved is not None:
            if (saved.link_id, saved.owner_user_id, saved.instance_id, saved.work_id, saved.kind) != (link.id, link.owner_user_id, link.agent_ref, work_id, "cancel"):
                raise ValueError("act-source-conflict")
            request_for("cancel", saved.body, identity=saved.id)
            return saved
        await self._work_link(link, kind="cancel", writable=True)
        current = await self.work.get(actor=PrincipalRef("human", link.owner_user_id), instance_id=link.agent_ref, work_id=work_id)
        body = WorkCommand(
            operation_id=operation_id,
            expected_revision=current["revision"] if expected_revision is None else expected_revision,
            expected_assignment_revision=current["assignment_revision"] if expected_assignment_revision is None else expected_assignment_revision,
            action="cancel",
        )
        return await self._stage_management(link, body, work_id=work_id, current=current, admission=admission, origin=origin, confirmation_current=confirmation_current)

    async def _stage_management(self, link, body, *, work_id, current, draft_ref=None, admission=None, non_effect_reason=None, origin=None, confirmation_current=lambda: None):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_authority import prepare_work_owner

        attempt = current.get("attempt")
        scope = {key: attempt["id"] if key == "attempt_id" else attempt[key] for key in _SCOPE_FIELDS} if attempt is not None and attempt["status"] in UNRESOLVED else None
        owner = await prepare_work_owner(self.authority, link.owner_user_id)
        prepared = await prepare_attention(self, link)
        async with self.outbox._writer(link.id) as (writer, _binding):
            if admission is not None:
                await admission(writer, None)
            row = await self._stage_work_act(link, kind=body.action, request_id=None, work_id=work_id, request=body, owner=owner, session=writer)
            saved = await writer.get(TelegramActRow, row.id, populate_existing=True)
            saved.stop_dispatch = {**scope, "state": "awaiting_command", "attempts": 0, "not_before": None} if scope is not None else None
            mode = "callback" if origin is not None else "internal"
            saved.continuation = {"management_source": {"version": 1, "origin_kind": mode, "scope": scope, "draft_ref": draft_ref, "link_id": link.id, "signature": management_signature(saved, scope, mode, draft_ref)}}
            if origin is not None:
                saved.continuation = {**saved.continuation, "management_origin": origin}
            await writer.flush()
            staged = _snapshot(saved)
            await prepared(session=writer, projection=ManagementProjection(staged, lambda: True, staged=True, admitting=True, clock=self.outbox.clock, draft_ttl_hours=self.outbox.config.callbacks.draft_ttl_hours))
            if admission is not None:
                await admission(writer, staged)
            await prepared(session=writer, projection=ManagementProjection(_snapshot(saved), lambda: True, staged=True, clock=self.outbox.clock, draft_ttl_hours=self.outbox.config.callbacks.draft_ttl_hours))
            confirmation_current()
            if non_effect_reason is not None:
                if body.action != "edit" or non_effect_reason != "unchanged":
                    raise ValueError("invalid-non-effect")
                saved.state, saved.reason, saved.updated_at = "refused", non_effect_reason, self.outbox.clock()
                saved.first_attempt_at = None
                if saved.stop_dispatch is not None:
                    saved.stop_dispatch = {**saved.stop_dispatch, "state": "ended"}
                await writer.flush()
            confirmation_current()
            return _snapshot(saved)

    async def _management_outcome(self, act, *, state, reason=None, result=None):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import _ACT_SCOPE, same_body

        async with self.outbox._writer(act.link_id) as (writer, _binding):
            row = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
            if row is None or any(getattr(row, key) != getattr(act, key) for key in _ACT_SCOPE) or not same_body(row.body, act.body):
                raise ValueError("act-source-conflict")
            if state != "done" and row.attempts != act.attempts:
                return _snapshot(row)
            if row.state != "done":
                row.state, row.reason, row.result, row.updated_at = state, reason, result, self.outbox.clock()
                if row.stop_dispatch is not None and state in {"done", "refused", "conflict"}:
                    row.stop_dispatch = {**row.stop_dispatch, "state": "ready" if state == "done" else "ended"}
                await writer.flush()
            return _snapshot(row)

    async def _attempt_management(self, act, *, explicit=False, attempt_current=lambda: True):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import _ACT_SCOPE, request_for, same_body
        from app.telegram_bots.work_authority import prepare_work_owner

        now = self.outbox.clock()
        exhausted = act.attempts >= self.outbox.config.acts.max_attempts or act.first_attempt_at is not None and now - act.first_attempt_at >= timedelta(hours=1)
        if act.state == "done" or act.attempts:
            try:
                receipt = await self.receipt_for(act)
            except Exception:
                return await self._management_outcome(act, state="uncertain") if exhausted and not explicit else act
            if receipt is not None:
                saved = await self._management_outcome(act, state="done", result=receipt)
                return await self._dispatch_stop(saved, explicit=explicit, attempt_current=attempt_current)
        if act.state in {"done", "refused", "conflict"} or act.cancel_requested_at is not None or not explicit and (act.state != "pending" or act.reason is not None):
            return act
        if not explicit and exhausted:
            return await self._management_outcome(act, state="uncertain")
        try:
            source = command_source(act)
        except ValueError:
            return act
        known_receipt = None
        try:
            link = await self.links.get(act.link_id)
            if not same_binding(link, act):
                raise AgentDenied("The action's binding changed", reason="not-visible")
            await self._work_link(link, kind=act.kind, writable=True)
            body = request_for(act.kind, act.body, identity=act.id)
            owner = await prepare_work_owner(self.authority, link.owner_user_id)
            async with self.outbox._writer(link.id) as (writer, binding):
                saved = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
                if (
                    saved is None
                    or saved.state == "done"
                    or saved.attempts != act.attempts
                    or saved.cancel_requested_at is not None
                    or any(getattr(saved, key) != getattr(act, key) for key in _ACT_SCOPE)
                    or not same_body(saved.body, act.body)
                    or not same_body(saved.stop_dispatch, act.stop_dispatch)
                    or not same_body(saved.continuation, act.continuation)
                    or not attempt_current()
                ):
                    return _snapshot(saved) if saved is not None else act
                if binding is None or binding.acts_paused_since is not None:
                    raise AgentDenied("Actions are temporarily paused", reason="actions-paused")
                saved.attempts += 1
                saved.first_attempt_at, saved.updated_at = saved.first_attempt_at or now, now
                saved.state, saved.reason = "pending", None
                await writer.flush()
                act = _snapshot(saved)
                await owner(link.owner_user_id, writer)
            prepared = await prepare_attention(self, link)

            async def admission(writer):
                self._writer_required(writer)
                await writer.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).with_for_update())
                await writer.scalar(select(TelegramActRow).where(TelegramActRow.id == act.id).with_for_update())
                await prepared(session=writer, projection=ManagementProjection(act, attempt_current))

            known_receipt = await self.work.command(actor=PrincipalRef("human", act.owner_user_id), instance_id=act.instance_id, work_id=act.work_id, request=body, can_stop=source["scope"] is not None, admission=admission)
            return await self._dispatch_stop(await self._management_outcome(act, state="done", result=known_receipt), explicit=explicit, attempt_current=attempt_current)
        except AgentConflict as exc:
            return act if known_receipt is not None else await self._management_outcome(act, state="conflict", reason=exc.reason or "changed")
        except (AgentDenied, AccessOff, CeilingRefused, Refusal, ValueError) as exc:
            if known_receipt is not None:
                return act
            return await self._management_outcome(act, state="pending" if getattr(exc, "reason", None) == "actions-paused" else "refused", reason=getattr(exc, "reason", None) or "denied")
        except Exception:
            return act


class ManagementProjection:
    def __init__(self, act, attempt_current, *, staged=False, admitting=False, clock=None, draft_ttl_hours=24):
        self.act, self.attempt_current, self.staged = act, attempt_current, staged
        self.source = command_source(act)
        from app.telegram_bots.work_management_consent import ManagementModeWitness

        self.witness = ManagementModeWitness(act, admitting=admitting)
        self.consent = None
        if self.source["origin_kind"] == "callback":
            from app.telegram_bots.work_management_consent import ManagementConsent

            self.consent = ManagementConsent(act, admitting=admitting)
        self.edit = None
        if act.kind == "edit":
            from app.telegram_bots.work_edit_drafts import EditCommandProof

            self.edit = EditCommandProof(act, self.source["draft_ref"], admitting=admitting, staged=staged, clock=clock, draft_ttl_hours=draft_ttl_hours)

    def extend(self, query):
        self.column = len(query.column_descriptions)
        saved, work, attempt, control, event = aliased(TelegramActRow), aliased(AgentWorkRow), aliased(AgentWorkAttemptRow), aliased(TelegramCallbackRow), aliased(AgentWorkEventRow)
        result = (
            query.add_columns(saved, work, attempt, control, event)
            .join(saved, saved.id == self.act.id)
            .join(work, and_(work.id == self.act.work_id, work.instance_id == self.act.instance_id))
            .outerjoin(attempt, and_(attempt.work_id == work.id, attempt.status.in_(UNRESOLVED)))
            .outerjoin(control, control.id == self.act.id)
            .outerjoin(event, and_(event.instance_id == self.act.instance_id, event.work_id == self.act.work_id, event.operation_id == self.act.id))
        )
        if self.edit is not None:
            result = self.edit.extend(result)
        result = self.witness.extend(result)
        return self.consent.extend(result) if self.consent is not None else result

    def check_with_permissions(self, rows, held):
        from app.telegram_bots.work_actions import _ACT_SCOPE, same_body

        saved, work, attempt, control, event = rows[0][self.column : self.column + 5]
        if self.edit is not None:
            self.edit.check(rows)
        if rows[0][2] & 6 != 6 or not {"agents:write", *(("runs:cancel",) if self.source["scope"] is not None else ())}.issubset(held):
            raise CeilingRefused()
        if (
            not self.attempt_current()
            or not same_binding(rows[0][0], self.act)
            or rows[0][0].acts_paused_since is not None
            or any(getattr(saved, key) != getattr(self.act, key) for key in _ACT_SCOPE)
            or not same_body(saved.body, self.act.body)
            or not same_body(saved.continuation, self.act.continuation)
            or not same_body(saved.stop_dispatch, self.act.stop_dispatch)
            or saved.attempts != self.act.attempts
            or saved.state != "pending"
            or saved.cancel_requested_at is not None
            or saved.operation_ids
        ):
            raise AuthorityUnavailable()
        if self.consent is not None:
            self.consent.check(rows, allow_closed=not self.staged)
        elif self.source["origin_kind"] != "internal" or control is not None:
            raise AuthorityUnavailable()
        self.witness.check(rows)
        expected = self.act.body["expected_revision"], self.act.body["expected_assignment_revision"]
        if event is None:
            if (work.revision, work.assignment_revision) != expected:
                raise AgentConflict("Work changed", reason="changed")
        elif (
            self.staged
            or (event.action, event.actor_kind, event.actor_id) != (self.act.kind, "human", self.act.owner_user_id)
            or not same_body(event.request, self.act.body)
            or (event.revision, event.assignment_revision) != (expected[0] + 1, expected[1] + 1)
            or (work.revision, work.assignment_revision) != (event.revision, event.assignment_revision)
            or any(not same_body(event.result.get(key), value) for key, value in {"id": work.id, "revision": work.revision, "assignment_revision": work.assignment_revision, "status": work.status}.items())
        ):
            raise AuthorityUnavailable()
        scope = self.source["scope"]
        if (attempt is None) != (scope is None) or attempt is not None and ((attempt.id, attempt.thread_id) != (scope["attempt_id"], scope["thread_id"]) or scope["run_id"] is not None and attempt.run_id != scope["run_id"]):
            raise AgentConflict("The original attempt changed", reason="changed")
