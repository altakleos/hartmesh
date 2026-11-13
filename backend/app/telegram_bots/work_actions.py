"""Receipt-backed human Work actions over the canonical domain services.

Runtime installs the existing Work/HumanInput actions. Their transaction owns
each effect; the Telegram journal owns the
immutable request, local attempt serialization and uncertain-response recovery.
Stop and activation journal primitives await their chat controls.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from contextlib import asynccontextmanager
from datetime import timedelta

from sqlalchemy import and_, or_, select

from app.telegram_bots.authority import AccessOff, CeilingRefused, require
from app.telegram_bots.links import BotLink
from app.telegram_bots.work_start_actions import ActivationActionMethods
from app.telegram_bots.work_stop_actions import StopActionMethods
from deerflow.agent_instances.contract import AgentConflict, AgentDenied, Refusal
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.human_input_contract import RequestCommand, Respond
from deerflow.agent_instances.work import INSPECT
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.agent_instances.work_execution_contract import ActivateWork
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.telegram_bots.model import TelegramActRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained

_ACT_SCOPE = ("link_id", "owner_user_id", "bot_id", "binding_generation", "telegram_user_id", "chat_id", "instance_id", "work_id", "request_id", "kind", "message_id")

WORK_ACT_KINDS = frozenset({"respond", "decide", "accept", "changes_requested", "route", "stop", "activate"})
_SCOPE = ("id", "owner_user_id", "bot_id", "agent_kind", "agent_ref", "binding_generation", "token_revision", "telegram_user_id", "chat_id", "connection_id")


def thaw(value):
    if isinstance(value, Mapping):
        return {key: thaw(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [thaw(child) for child in value]
    return value


def same_body(left, right):
    """Compare immutable JSON without Python's bool/integer equivalence."""
    return json.dumps(thaw(left), sort_keys=True, separators=(",", ":")) == json.dumps(thaw(right), sort_keys=True, separators=(",", ":"))


def request_for(kind, body, *, identity=None):
    if kind not in WORK_ACT_KINDS:
        raise ValueError("unsupported-work-act")
    request = (Respond if kind == "respond" else RequestCommand if kind == "route" else ActivateWork if kind == "activate" else WorkCommand).model_validate(thaw(body))
    if identity is not None and request.operation_id != identity or kind not in {"respond", "activate"} and request.action != kind:
        raise ValueError("act-source-conflict")
    return request


class WorkActionMethods(StopActionMethods, ActivationActionMethods):
    """Methods mixed into the existing Acts journal; no independent lifecycle."""

    @asynccontextmanager
    async def _work_serial(self, identity):
        async with self._locks.hold(identity):
            self._work_active.add(identity)
            try:
                yield
            finally:
                self._work_active.discard(identity)

    async def _work_link(self, link, *, kind=None, writable=False):
        if self.work is None or self.authority is None or self.links is None:
            raise AgentDenied("Work actions are not available")
        if self.work._sf.kw.get("bind") is not self.session_factory.kw.get("bind"):
            raise AgentDenied("Work actions require the shared store")
        if getattr(getattr(self.authority.app, "state", None), "agent_instances", None) is not self.work.agents:
            raise AgentDenied("Work authority changed")
        await require(link.owner_user_id, "agents:read", *(("agents:write",) if writable else ()))
        current = await self.links.get(link.id)
        if current is None or any(getattr(current, field) != getattr(link, field) for field in _SCOPE) or current.agent_kind != "instance" or current.state != "connected" or current.health != "ok":
            raise AgentDenied("The bot binding is no longer current")
        if writable and kind in {"decide", "accept"} and current.acts_paused_since is not None:
            raise AgentDenied("Actions are temporarily paused", reason="actions-paused")
        if await self.authority.instance_send_policy(current, require_use=True, require_inspect=True) in {"off", "restoring"}:
            raise AgentDenied("The bot policy is unavailable")
        return current

    async def _work_record(self, identity):
        from app.telegram_bots.acts import _snapshot

        async with self.session_factory() as session:
            row = await session.get(TelegramActRow, identity)
            if row is None or row.kind not in WORK_ACT_KINDS:
                raise ValueError("unsupported-work-act")
            return _snapshot(row)

    async def receipt_for(self, act):
        """Read an exact service receipt without calling a command or activation."""
        return await await_drained(self._receipt_for(act))

    async def _receipt_for(self, act):
        link = await self.links.get(act.link_id) if act.link_id else None
        if link is None or (link.owner_user_id, link.bot_id, link.agent_ref, link.binding_generation, link.telegram_user_id, link.chat_id) != (
            act.owner_user_id,
            act.bot_id,
            act.instance_id,
            act.binding_generation,
            act.telegram_user_id,
            act.chat_id,
        ):
            raise AgentDenied("The action's binding is no longer current")
        await self._work_link(link)
        if act.kind == "stop" and act.operation_ids:
            return await self._stop_chain_receipt(act)
        actor = PrincipalRef("human", link.owner_user_id)
        body = request_for(act.kind, act.body, identity=act.id)
        if act.kind == "activate":
            from deerflow.agent_instances.work_execution import WorkExecution

            return await WorkExecution(self.work).activation_receipt(actor=actor, instance_id=act.instance_id, work_id=act.work_id, request=body)
        async with self.session_factory() as session:
            parent, _ = await self.work._parent(session, actor, act.instance_id, INSPECT)
            if act.kind in {"respond", "route"}:
                return await HumanInput(self.work)._receipt(session, actor, parent.id, body, request_id=act.request_id, work_id=act.work_id)
            return await self.work._receipt(session, actor, parent, body, act.work_id)

    async def _save_work_act(self, link, *, kind, request_id, work_id, request, admission=None):
        await self._work_link(link)
        owner = await self.authority.prepare_live_owner(link.owner_user_id)
        if admission is None:
            return await self._stage_work_act(link, kind=kind, request_id=request_id, work_id=work_id, request=request, owner=owner)
        async with self.outbox._writer(link.id) as (writer, _):
            await admission(writer, None)
            act = await self._stage_work_act(link, kind=kind, request_id=request_id, work_id=work_id, request=request, owner=owner, session=writer)
            await admission(writer, act)
            return act

    async def _stage_work_act(self, link, *, kind, request_id, work_id, request, owner, session=None, message_id=None):
        """Store an exact operation in its caller's guarded writer; do not act."""
        from app.telegram_bots.acts import _snapshot

        if message_id is not None and (kind != "respond" or type(message_id) is not int or not 0 < message_id < 2**63):
            raise ValueError("invalid-work-file-origin")
        request_for(kind, request.model_dump(mode="json", exclude_unset=True), identity=request.operation_id)
        body = request.model_dump(mode="json", exclude_unset=True)
        async with self.outbox._writer(link.id, session=session) as (session, current):
            if current is None or any(getattr(current, field) != getattr(link, field) for field in _SCOPE) or current.state != "connected" or current.health != "ok":
                raise AgentDenied("The bot binding is no longer current")
            row = await session.get(TelegramActRow, request.operation_id, with_for_update=True, populate_existing=True)
            scope = dict(
                link_id=link.id,
                bot_id=link.bot_id,
                owner_user_id=link.owner_user_id,
                binding_generation=link.binding_generation,
                telegram_user_id=link.telegram_user_id,
                chat_id=link.chat_id,
                instance_id=link.agent_ref,
                work_id=work_id,
                request_id=request_id,
                kind=kind,
                message_id=message_id,
            )
            if row is not None:
                if any(getattr(row, key) != value for key, value in scope.items()) or not same_body(row.body, body):
                    raise ValueError("act-source-conflict")
            else:
                now = self.outbox.clock()
                row = TelegramActRow(id=request.operation_id, **scope, body=body, state="pending", operation_ids=[], attempts=0, first_attempt_at=now, created_at=now, updated_at=now)
                session.add(row)
                await session.flush()
            result = _snapshot(row)
            await owner(link.owner_user_id, session)
        return result

    async def _work_outcome(self, captured, *, state, reason=None, result=None):
        if captured.kind == "activate":
            if state == "done":
                return await self._complete_activation(captured, result)
            return await self._activation_outcome(captured, state=state, reason=reason, result=result)
        from app.telegram_bots.acts import _snapshot

        async with self.outbox._writer(captured.link_id) as (session, link):
            row = await session.get(TelegramActRow, captured.id, with_for_update=True, populate_existing=True)
            if row is None:
                raise ValueError("unsupported-work-act")
            if row.state == "done" or row.attempts != captured.attempts or any(getattr(row, name) != getattr(captured, name) for name in _ACT_SCOPE) or not same_body(row.body, captured.body):
                return _snapshot(row)
            row.state, row.reason, row.result, row.updated_at = state, reason, result, self.outbox.clock()
            await session.flush()
            return _snapshot(row)

    async def _work_admission(self, link, act, *, attempt_current):
        from app.telegram_bots.work_effect_authority import prepare_effect_admission

        return await prepare_effect_admission(self, link, act, attempt_current=attempt_current)

    async def _work_refusal_retry_current(self, writer, link, act):
        from app.telegram_bots.callbacks import CallbackRefused
        from app.telegram_bots.work_outcome_transfer import transfer_receipt

        if transfer_receipt(act.continuation) is not None:
            return False
        try:
            parent, rows, selected = await self.callbacks._locked(writer, link, act.id)
            self.callbacks._usable(selected)
            if parent.state != "sent" or any(row.payload.get("work_action_id") != act.id for row in rows):
                return False
            # The exact acknowledged keyboard must still own this retry after
            # any receipt/provider await. A retired draft cannot be reopened by
            # a press that passed its earlier SDK precheck.
            part = next(part for part in parent.parts if part.get("callback_group_id") == selected.group_id and part["presentation_version"] == parent.payload["presentation_version"])
            return selected.message_id is not None and part["state"] == "sent" and part["message_ids"] == [selected.message_id]
        except (CallbackRefused, StopIteration):
            return False

    async def _attempt_work(self, act, *, explicit=False, attempt_current=lambda: True):
        from app.telegram_bots.acts import _snapshot, _utc

        if act.kind == "stop":
            return await self._run_stop_chain(act, explicit=explicit, attempt_current=attempt_current)
        if act.kind == "activate":
            return await self._attempt_activation(act, explicit=explicit, attempt_current=attempt_current)
        try:
            request_for(act.kind, act.body, identity=act.id)
        except ValueError:
            return await self._work_outcome(act, state="refused", reason="invalid")
        now = self.outbox.clock()
        expired = act.first_attempt_at is not None and (now - _utc(act.first_attempt_at) >= timedelta(hours=1) or act.attempts >= self.outbox.config.acts.max_attempts)
        if act.state == "done":
            # Replays still require current receipt/read authority.
            await self.receipt_for(act)
            return act
        retry_refusal = explicit and act.state == "refused" and act.reason == "storage-busy"
        if (act.state in {"conflict", "refused"} and not retry_refusal) or act.cancel_requested_at is not None:
            return act
        if act.attempts:
            try:
                receipt = await self.receipt_for(act)
            except Exception:
                # An unreadable receipt grants no permission to mutate.
                return await self._work_outcome(act, state="uncertain") if expired and not explicit else act
            if receipt is not None:
                return await self._work_outcome(act, state="done", result=receipt)
        now = self.outbox.clock()
        if not explicit and (act.reason is not None or act.state != "pending"):
            return act
        if not explicit and expired:
            return await self._work_outcome(act, state="uncertain")
        link = await self.links.get(act.link_id)
        try:
            if link is None or (link.owner_user_id, link.bot_id, link.agent_ref, link.binding_generation, link.telegram_user_id, link.chat_id) != (
                act.owner_user_id,
                act.bot_id,
                act.instance_id,
                act.binding_generation,
                act.telegram_user_id,
                act.chat_id,
            ):
                raise AgentDenied("The action's binding is no longer current")
            await self._work_link(link, kind=act.kind, writable=True)
            actor = PrincipalRef("human", link.owner_user_id)
            body = request_for(act.kind, act.body, identity=act.id)
            # Verify the action's recorded request belongs to this binding. A
            # replay receipt above is intentionally checked before open/CAS rules.
            async with self.session_factory() as reader:
                request = await reader.get(HumanInputRequestRow, act.request_id)
                if request is None or request.instance_id != act.instance_id or request.work_id != act.work_id:
                    raise AgentDenied("Request is unavailable")
            from app.telegram_bots.work_authority import prepare_work_owner

            owner = await prepare_work_owner(self.authority, link.owner_user_id)
            if not attempt_current():
                return act
            async with self.outbox._writer(link.id) as (writer, current):
                saved = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
                if saved.state == "done" or saved.attempts != act.attempts or saved.cancel_requested_at is not None or any(getattr(saved, field) != getattr(act, field) for field in _ACT_SCOPE) or not same_body(saved.body, act.body):
                    return _snapshot(saved)
                if retry_refusal and not await self._work_refusal_retry_current(writer, link, saved):
                    return _snapshot(saved)
                if current is None or any(getattr(current, field) != getattr(link, field) for field in _SCOPE) or current.state != "connected" or current.health != "ok":
                    raise AgentDenied("The bot binding is no longer current")
                if act.kind in {"decide", "accept"} and current.acts_paused_since is not None:
                    raise AgentDenied("Actions are temporarily paused", reason="actions-paused")
                saved.attempts += 1
                saved.first_attempt_at = saved.first_attempt_at or now
                saved.updated_at, saved.reason, saved.state = now, None, "pending"
                await writer.flush()
                act = _snapshot(saved)
                await owner(link.owner_user_id, writer)
            admission = await self._work_admission(link, act, attempt_current=attempt_current)
            if act.kind == "respond":
                result = await HumanInput(self.work).respond(actor=actor, request_id=act.request_id, request=body, admission=admission)
            elif act.kind == "route":
                result = await HumanInput(self.work).command(actor=actor, request_id=act.request_id, request=body, admission=admission)
            else:
                result = await self.work.command(actor=actor, instance_id=act.instance_id, work_id=act.work_id, request=body, can_stop=False, admission=admission)
            return await self._work_outcome(act, state="done", result=result)
        except AgentConflict as exc:
            reason = getattr(exc, "reason", None)
            return await self._work_outcome(act, state="pending" if reason == "attempt-unresolved" else "conflict", reason=reason or "changed")
        except (AgentDenied, AccessOff, CeilingRefused, Refusal, ValueError) as exc:
            if getattr(exc, "reason", None) == "actions-paused":
                return await self._work_outcome(act, state="pending", reason="actions-paused")
            return await self._work_outcome(act, state="refused", reason=getattr(exc, "reason", None) or ("access-off" if isinstance(exc, AccessOff) else "invalid" if isinstance(exc, ValueError) else "denied"))
        except Exception:
            # Do not invent a failure after an unknown service/commit result.
            if act.attempts >= self.outbox.config.acts.max_attempts:
                return await self._work_outcome(act, state="uncertain")
            return act

    async def _issue_work(self, link, *, kind, request_id, work_id, request, seen=None):
        async def perform():
            async with self._work_serial(request.operation_id):
                act = await self._save_work_act(link, kind=kind, request_id=request_id, work_id=work_id, request=request)
                if act.state == "pending" and not act.attempts and seen is not None:
                    try:
                        await self._validate_seen(link, work_id, request_id, seen)
                    except AgentConflict:
                        return await self._work_outcome(act, state="conflict", reason="changed")
                return await self._attempt_work(act, explicit=True)

        return await await_drained(perform())

    async def respond(self, link: BotLink, *, operation_id, request_id, request_revision, assignment_revision, disposition="supplied", text=None, choice=None):
        await self._work_link(link)
        actor = PrincipalRef("human", link.owner_user_id)
        current = await HumanInput(self.work).get(actor=actor, request_id=request_id)
        if current["instance_id"] != link.agent_ref:
            raise AgentDenied("Request is unavailable")
        body = Respond(operation_id=operation_id, expected_request_revision=request_revision, expected_assignment_revision=assignment_revision, disposition=disposition, text=text, choice=choice)
        return await self._issue_work(link, kind="respond", request_id=request_id, work_id=current["work_id"], request=body)

    async def route(self, link, *, operation_id, request_id, request_revision, recipient_id):
        async def perform():
            async with self._work_serial(operation_id):
                await self._work_link(link)
                if type(recipient_id) is not str or not recipient_id:
                    raise ValueError("invalid-route-recipient")
                async with self.session_factory() as reader:
                    saved = await reader.get(TelegramActRow, operation_id)
                    if saved is not None:
                        if saved.kind != "route" or saved.link_id != link.id or saved.request_id != request_id or saved.body.get("recipient_id") != recipient_id or saved.body.get("expected_request_revision") != request_revision:
                            raise ValueError("act-source-conflict")
                        body = request_for("route", saved.body, identity=operation_id)
                        work_id = saved.work_id
                    else:
                        body = None
                if body is None:
                    current = await HumanInput(self.work).get(actor=PrincipalRef("human", link.owner_user_id), request_id=request_id)
                    if current["instance_id"] != link.agent_ref:
                        raise AgentDenied("Request is unavailable")
                    work_id = current["work_id"]
                    body = RequestCommand(operation_id=operation_id, action="route", expected_revision=current["revision"], expected_request_revision=request_revision, recipient_id=recipient_id)
                act = await self._save_work_act(link, kind="route", request_id=request_id, work_id=work_id, request=body)
                return await self._attempt_work(act, explicit=True)

        return await await_drained(perform())

    async def _validate_seen(self, link, work_id, request_id, seen):
        actor = PrincipalRef("human", link.owner_user_id)
        human = HumanInput(self.work)
        current = await human.get(actor=actor, request_id=request_id)
        response_ids = []
        offset = 0
        while True:
            page = await human.responses(actor=actor, request_id=request_id, limit=100, offset=offset)
            response_ids.extend(row["id"] for row in page["responses"])
            if not page["has_more"]:
                break
            offset += len(page["responses"])
            if not page["responses"] or offset > 200:
                raise AgentConflict("The review changed")
        record = await self.work.get(actor=actor, instance_id=link.agent_ref, work_id=work_id)
        actual = dict(
            work_revision=record["revision"],
            assignment_revision=record["assignment_revision"],
            request_id=current["id"],
            revision=current["revision"],
            request_revision=current["request_revision"],
            response_ids=sorted(response_ids),
            outcome_id=record.get("outcome_id"),
            evidence_revision=(record.get("outcome") or {}).get("evidence_revision"),
            blocker_id=(record.get("blocker") or {}).get("id"),
            blocker_revision=(record.get("blocker") or {}).get("revision"),
        )
        expected = {**seen, "response_ids": sorted(seen["response_ids"])}
        if current["work_id"] != work_id or current["state"] == "closed" or actual != expected:
            raise AgentConflict("Review the current request and all responses")
        # Every human-content mutation advances Work.revision atomically. The
        # captured WorkCommand CAS closes changes after this read, including
        # changes_requested, which intentionally has no request_basis DTO field.

    @staticmethod
    def _command_basis(operation_id, request_id, seen):
        fields = {"work_revision", "assignment_revision", "request_id", "revision", "request_revision", "response_ids", "outcome_id", "evidence_revision", "blocker_id", "blocker_revision"}
        if type(seen) is not dict or set(seen) != fields or seen["request_id"] != request_id:
            raise ValueError("invalid-review-basis")
        return dict(operation_id=operation_id, expected_revision=seen["work_revision"], expected_assignment_revision=seen["assignment_revision"])

    def _decision_command(self, *, operation_id, request_id, decision, note, seen):
        base = self._command_basis(operation_id, request_id, seen)
        request = WorkCommand(
            **base,
            action="decide",
            note=decision + ("\n\n" + note if note else ""),
            blocker_id=seen["blocker_id"],
            blocker_revision=seen["blocker_revision"],
            request_basis=dict(id=request_id, revision=seen["revision"], request_revision=seen["request_revision"], response_ids=seen["response_ids"]),
        )
        return request

    async def decide(self, link, *, operation_id, request_id, work_id, decision, note, seen):
        request = self._decision_command(operation_id=operation_id, request_id=request_id, decision=decision, note=note, seen=seen)
        return await self._issue_work(link, kind="decide", request_id=request_id, work_id=work_id, request=request, seen=seen)

    def _acceptance_command(self, *, operation_id, request_id, outcome_id, evidence_revision, seen):
        base = self._command_basis(operation_id, request_id, seen)
        if (outcome_id, evidence_revision) != (seen["outcome_id"], seen["evidence_revision"]):
            raise ValueError("invalid-review-basis")
        request = WorkCommand(
            **base,
            action="accept",
            outcome_id=outcome_id,
            evidence_revision=evidence_revision,
            basis="outcome_statement",
            acknowledge_unchecked_sources=True,
            request_basis=dict(id=request_id, revision=seen["revision"], request_revision=seen["request_revision"], response_ids=seen["response_ids"]),
        )
        return request

    async def accept(self, link, *, operation_id, request_id, work_id, outcome_id, evidence_revision, seen):
        request = self._acceptance_command(operation_id=operation_id, request_id=request_id, outcome_id=outcome_id, evidence_revision=evidence_revision, seen=seen)
        return await self._issue_work(link, kind="accept", request_id=request_id, work_id=work_id, request=request, seen=seen)

    def _changes_command(self, *, operation_id, request_id, note, seen):
        return WorkCommand(**self._command_basis(operation_id, request_id, seen), action="changes_requested", note=note)

    async def request_changes(self, link, *, operation_id, request_id, work_id, note, seen):
        request = self._changes_command(operation_id=operation_id, request_id=request_id, note=note, seen=seen)
        return await self._issue_work(link, kind="changes_requested", request_id=request_id, work_id=work_id, request=request, seen=seen)

    async def reconcile_receipt(self, act_id):
        """Settle only an exact committed receipt; absence never authorizes retry."""

        async def perform():
            async with self._work_serial(act_id):
                act = await self._work_record(act_id)
                if act.kind == "stop":
                    return await self._reconcile_stop_receipt(act)
                receipt = await self.receipt_for(act)
                if receipt is None or act.state == "done":
                    return act
                return await self._work_outcome(act, state="done", result=receipt)

        return await await_drained(perform())

    async def check(self, act_id, *, expected=None, attempt_current=lambda: True):
        async def perform():
            async with self._work_serial(act_id):
                current = await self._work_record(act_id)
                if expected is not None:
                    if expected["id"] != current.id or expected["kind"] != current.kind or not same_body(expected["body"], current.body) or type(expected["attempts"]) is not int:
                        raise ValueError("work-retry-body-changed")
                    if any(key in expected and getattr(current, key) != expected[key] for key in _ACT_SCOPE):
                        raise ValueError("work-retry-scope-changed")
                    if current.kind == "stop" and "effective" in expected:
                        effective = current
                        if expected["effective"]["id"] != current.id:
                            children = await self._stop_children(current)
                            effective = next((child for child in children if child.id == expected["effective"]["id"]), None)
                        if (
                            effective is None
                            or not same_body(effective.body, expected["effective"]["body"])
                            or not same_body(effective.stop_dispatch, expected["effective"]["stop_dispatch"])
                            or effective.attempts != expected["effective"]["attempts"]
                        ):
                            return await self._reconcile_stop_receipt(current)
                    if expected["attempts"] != current.attempts:
                        if current.kind == "stop":
                            return await self._reconcile_stop_receipt(current)
                        receipt = await self.receipt_for(current)
                        if receipt is not None and current.state != "done":
                            return await self._work_outcome(current, state="done", result=receipt)
                        return current
                return await self._attempt_work(current, explicit=True, attempt_current=attempt_current)

        return await await_drained(perform())


class ActsWorker:
    """Retry only admitted Work service calls; settings/network/files stay owned."""

    def __init__(self, acts, *, available=lambda: True):
        self.acts = acts
        self.available = available
        self._task = None
        self._stopping = False

    async def stop(self):
        self._stopping = True
        if self._task is not None:
            await await_drained(self._task)

    async def tick(self):
        if self._stopping or self._task is not None or not self.available():
            return
        task = self._task = asyncio.create_task(self._tick())
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            self._stopping = True
            await await_drained(task)
            raise
        finally:
            self._task = None

    async def _tick(self):
        from app.telegram_bots.acts import _utc

        if not self.available():
            return
        now = self.acts.outbox.clock()
        async with self.acts.session_factory() as reader:
            ids = list(
                await reader.scalars(
                    select(TelegramActRow.id)
                    .where(
                        TelegramActRow.kind.in_(WORK_ACT_KINDS),
                        TelegramActRow.first_attempt_at.is_not(None),
                        or_(
                            and_(TelegramActRow.state == "pending", TelegramActRow.reason.is_(None)),
                            and_(TelegramActRow.kind == "stop", TelegramActRow.state == "conflict", TelegramActRow.reason == "changed"),
                            and_(TelegramActRow.kind == "activate", TelegramActRow.state == "waiting", TelegramActRow.reason == "busy"),
                        ),
                    )
                    .order_by(TelegramActRow.updated_at, TelegramActRow.id)
                    .limit(100)
                )
            )
        for identity in ids:
            if self._stopping or not self.available():
                break
            if identity in self.acts._work_active:
                continue
            async with self.acts._work_serial(identity):
                act = await self.acts._work_record(identity)
                retry_seconds = self.acts.outbox.config.queue.retry_seconds if act.kind == "activate" and act.state == "waiting" and act.reason == "busy" else self.acts.outbox.config.acts.retry_seconds
                if now - _utc(act.updated_at) >= timedelta(seconds=retry_seconds):
                    await await_drained(self.acts._attempt_work(act, attempt_current=lambda: not self._stopping and self.available()))
        await self.acts._recover_stop_dispatches(attempt_current=lambda: not self._stopping and self.available())
