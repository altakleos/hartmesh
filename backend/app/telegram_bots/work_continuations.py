"""Recover only the activation child authorized by a saved composite gesture."""

import logging
from datetime import datetime, timedelta

from sqlalchemy import select

from app.telegram_bots.notifications import aware
from app.telegram_bots.work_continuation_state import child_marker, continuation_state, require_child
from deerflow.persistence.telegram_bots.model import TelegramActRow
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


class _ContinuationEnded(Exception):
    pass


class ContinuationActionMethods:
    async def cancel_continuation(self, act_id, *, admission=None):
        async def perform():
            from app.telegram_bots.acts import _snapshot
            from app.telegram_bots.work_actions import _ACT_SCOPE, _SCOPE, same_body
            from app.telegram_bots.work_delegate_actions import delegate_source, delegation_children, qualify_no_due
            from app.telegram_bots.work_start_actions import link_matches
            from deerflow.agent_instances.contract import AgentDenied
            from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow

            captured = await self._work_record(act_id)
            original = continuation_state(captured)
            if captured.kind != "delegate" or not delegate_source(captured)["start"]:
                raise ValueError("invalid-composite-parent")
            link = await self.links.get(captured.link_id)
            if not link_matches(link, captured):
                raise AgentDenied("The action's binding is no longer current")
            await self._work_link(link)
            owner = await self.authority.prepare_live_owner(link.owner_user_id)
            child = None
            async with self.outbox._writer(link.id) as (writer, binding):
                if binding is None or any(getattr(binding, key) != getattr(link, key) for key in _SCOPE):
                    raise AgentDenied("The action's binding is no longer current")
                row = await writer.get(TelegramActRow, act_id, with_for_update=True, populate_existing=True)
                if row is None or any(getattr(row, key) != getattr(captured, key) for key in _ACT_SCOPE) or not same_body(row.body, captured.body):
                    raise ValueError("continuation-source-conflict")
                state = continuation_state(_snapshot(row))
                if any(not same_body(state[key], original[key]) for key in ("authorized_at", "child_id")):
                    raise ValueError("continuation-source-conflict")
                if admission is not None:
                    await admission(writer, None)
                members = [row]
                for identity in delegation_children(row):
                    member = await writer.get(TelegramActRow, identity, with_for_update=True, populate_existing=True)
                    if member is None:
                        raise ValueError("missing-delegate-child")
                    qualify_no_due(_snapshot(row), _snapshot(member))
                    members.append(member)
                if state["child_body"] is not None:
                    child_row = await writer.get(TelegramActRow, state["child_id"], with_for_update=True, populate_existing=True)
                    if child_row is not None:
                        require_child(_snapshot(row), _snapshot(child_row))
                        members.append(child_row)
                now = self.outbox.clock()
                for member in members:
                    if member.cancel_requested_at is None:
                        member.cancel_requested_at = member.updated_at = now
                    if member.state == "done":
                        continue
                    if member.kind == "activate":
                        reserved = await writer.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.work_id == member.work_id, AgentWorkAttemptRow.activation_id == member.id))
                    else:
                        reserved = await writer.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.instance_id == member.instance_id, AgentWorkEventRow.operation_id == member.id))
                    conclusive = reserved is None and (member.attempts == 0 or member.state in {"refused", "conflict", "waiting"})
                    member.state, member.reason = ("refused", "cancelled") if conclusive else ("uncertain", "cancel-unconfirmed")
                await writer.flush()
                parent = _snapshot(row)
                if state["child_body"] is not None and child_row is not None:
                    child = _snapshot(child_row)
                await owner(link.owner_user_id, writer)
                if admission is not None:
                    await admission(writer, parent)
            # Cancellation owns only a fence and receipt reads. A possible
            # native admission must never be handed to its mutation helper.
            primary_receipt = await self.receipt_for(parent)
            if primary_receipt is not None and parent.state != "done":
                parent = await self._work_outcome(parent, state="done", result=primary_receipt)
            if child is not None:
                receipt = await self.receipt_for(child)
                if receipt is not None:
                    child = await self._complete_activation(child, receipt)
                if child.state == "done":
                    return await self._continuation_phase(parent, "done")
                if child.state == "refused" and child.reason == "cancelled":
                    return await self._continuation_phase(parent, "cancelled")
                return parent
            if continuation_state(parent)["child_body"] is None:
                return await self._continuation_phase(parent, "cancelled")
            return parent

        return await await_drained(perform())

    async def resume_continuation(self, act_id, *, attempt_current=lambda: True, background=False):
        async def perform():
            async with self._work_serial(act_id):
                return await self._resume_continuation(act_id, attempt_current=attempt_current, background=background)

        return await await_drained(perform())

    async def _continuation_phase(self, captured, phase, *, refusal=None):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import _ACT_SCOPE, same_body, thaw

        async with self.outbox._writer(captured.link_id) as (writer, _link):
            row = await writer.get(TelegramActRow, captured.id, with_for_update=True, populate_existing=True)
            if row is None or any(getattr(row, key) != getattr(captured, key) for key in _ACT_SCOPE) or not same_body(row.body, captured.body):
                raise ValueError("continuation-source-conflict")
            if not same_body(row.continuation, captured.continuation):
                current, previous = continuation_state(_snapshot(row)), continuation_state(captured)
                if current["phase"] in {"done", "refused", "cancelled", "expired"} and all(same_body(current[key], previous[key]) for key in current if key != "phase"):
                    return _snapshot(row)
                raise ValueError("continuation-source-conflict")
            data = {**continuation_state(_snapshot(row)), "phase": phase}
            if phase != "refused":
                data.pop("refusal", None)
            elif refusal is not None:
                data["refusal"] = refusal
            row.continuation = {**thaw(row.continuation), "activation_continuation": data}
            continuation_state(_snapshot(row))
            row.updated_at = self.outbox.clock()
            await writer.flush()
            return _snapshot(row)

    async def _resume_continuation(self, act_id, *, attempt_current, background):
        from app.telegram_bots.work_actions import _ACT_SCOPE, same_body, thaw
        from app.telegram_bots.work_delegate_actions import DELEGATION_RECEIPT_FIELDS, delegate_source
        from app.telegram_bots.work_start_actions import link_matches

        parent = await self._work_record(act_id)
        state = continuation_state(parent)
        if parent.kind != "delegate" or delegate_source(parent)["start"] is not True or delegate_source(parent)["parent_id"] is not None:
            raise ValueError("invalid-composite-parent")
        if state["phase"] in {"done", "refused", "cancelled", "expired"} or not attempt_current():
            return parent
        if parent.state != "done":
            if parent.state in {"refused", "conflict"}:
                return await self._continuation_phase(parent, "refused")
            return parent
        receipt = await self.receipt_for(parent)
        if receipt is None:
            return parent
        saved_receipt, current_receipt = parent.result, receipt
        if "delegate_child_id" in saved_receipt:
            if current_receipt.get("delegate_child_id") != saved_receipt["delegate_child_id"]:
                return parent
            saved_receipt, current_receipt = saved_receipt["receipt"], current_receipt["receipt"]
        # A canonical delegation receipt may project today's live attempt.
        # Its immutable command fields, rather than that mutable projection,
        # qualify the primary; the final child admission still joins its event.
        if any(not same_body(saved_receipt.get(key), current_receipt.get(key)) for key in DELEGATION_RECEIPT_FIELDS):
            return parent
        link = await self.links.get(parent.link_id)
        if not link_matches(link, parent):
            return await self._continuation_phase(parent, "cancelled")
        await self._work_link(link)
        if state["phase"] == "ready":
            if parent.cancel_requested_at is not None:
                return await self._continuation_phase(parent, "cancelled")
            deadline = aware(datetime.fromisoformat(state["authorized_at"])) + timedelta(hours=1)
            if self.outbox.clock() >= deadline:
                return await self._continuation_phase(parent, "expired")
            async with self.session_factory() as reader:
                if await reader.get(TelegramActRow, state["child_id"]) is not None:
                    raise ValueError("orphan-activation-child")
            from app.telegram_bots.attention_authority import prepare_attention

            prepared = await prepare_attention(self, link)

            async def admit(writer, child):
                row = await writer.get(TelegramActRow, parent.id, with_for_update=True, populate_existing=True)
                if (
                    row is None
                    or any(getattr(row, key) != getattr(parent, key) for key in _ACT_SCOPE)
                    or row.state != "done"
                    or row.cancel_requested_at is not None
                    or not same_body(row.body, parent.body)
                    or not same_body(row.continuation, parent.continuation)
                    or not same_body(row.operation_ids, parent.operation_ids)
                ):
                    raise ValueError("continuation-source-conflict")
                if child is not None:
                    actual_child = await writer.get(TelegramActRow, child.id, populate_existing=True)
                    actual_child.continuation = {"version": 1, "activation_parent": child_marker(parent)}
                    row.operation_ids = [*row.operation_ids, child.id]
                    row.continuation = {**thaw(row.continuation), "activation_continuation": {**state, "phase": "child_planned", "child_body": thaw(child.body)}}
                    await writer.flush()
                await prepared(session=writer)
                if not attempt_current() or self.outbox.clock() >= deadline:
                    raise _ContinuationEnded()

            try:
                await self.stage_activation(link, operation_id=state["child_id"], work_id=state["work_id"], assignment_revision=state["assignment_revision"], admission=admit)
            except _ContinuationEnded:
                return await self._continuation_phase(parent, "expired") if attempt_current() else parent
            except Exception as error:
                from app.telegram_bots.authority import AccessOff, CeilingRefused
                from deerflow.agent_instances.contract import AgentConflict, AgentDenied, Refusal

                if not isinstance(error, (AgentConflict, AgentDenied, Refusal, AccessOff, CeilingRefused)):
                    raise
                kind = "conflict" if isinstance(error, AgentConflict) else "refused" if isinstance(error, Refusal) else "denied"
                reason = getattr(error, "reason", None)
                if type(reason) is not str or not 1 <= len(reason) <= 128:
                    reason = "denied"
                return await self._continuation_phase(parent, "refused", refusal={"kind": kind, "reason": reason})
            parent = await self._work_record(parent.id)
            state = continuation_state(parent)
        child = await self._work_record(state["child_id"])
        require_child(parent, child)
        if background and child.id in self._work_active:
            return parent
        async with self._work_serial(child.id):
            child = await self._work_record(child.id)
            interval = self.outbox.config.queue.retry_seconds if child.state == "waiting" and child.reason == "busy" else self.outbox.config.acts.retry_seconds
            if child.attempts and child.state in {"pending", "waiting"} and self.outbox.clock() - child.updated_at < timedelta(seconds=interval):
                # A receipt may already exist even while another mutation is
                # too early. Read it without accelerating the retry budget.
                receipt = await self.receipt_for(child)
                if receipt is None:
                    return await self._work_record(parent.id)
                child = await self._complete_activation(child, receipt)
            else:
                child = await self._attempt_activation(child, attempt_current=attempt_current)
        if child.state == "done":
            return await self._continuation_phase(parent, "done")
        if child.state == "refused" and child.reason == "cancelled":
            return await self._continuation_phase(parent, "cancelled")
        if child.state in {"refused", "conflict"} or child.state == "waiting" and child.reason == "work-limit":
            return await self._continuation_phase(parent, "refused")
        return await self._work_record(parent.id)

    async def _recover_continuations(self, *, after="", attempt_current=lambda: True):
        async with self.session_factory() as reader:
            query = select(TelegramActRow.id).where(
                TelegramActRow.kind.in_(("delegate", "respond", "decide")),
                TelegramActRow.continuation["activation_continuation"]["phase"].as_string().in_(("ready", "child_planned")),
            )
            ids = list(await reader.scalars(query.where(TelegramActRow.id > after).order_by(TelegramActRow.id).limit(100)))
            if not ids and after:
                ids = list(await reader.scalars(query.order_by(TelegramActRow.id).limit(100)))
        for identity in ids:
            if not attempt_current():
                break
            if identity in self._work_active:
                continue
            try:
                await self.resume_continuation(identity, attempt_current=attempt_current, background=True)
            except Exception as error:
                # Missing/ambiguous custody cannot authorize reconstruction;
                # another eligible family must still receive its own turn.
                logger.warning("Composite continuation deferred (%s)", type(error).__name__)
        return ids[-1] if ids else ""
