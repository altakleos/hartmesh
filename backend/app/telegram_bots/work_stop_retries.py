"""Bounded Stop conflict children retain their own immutable command and receipt."""

import hashlib
from collections.abc import Mapping
from datetime import timedelta

from deerflow.agent_instances.contract import AgentDenied
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.telegram_bots.model import TelegramActRow
from deerflow.spaces.contract import PrincipalRef

STOP_REPEATS = 3


def child_identity(parent, ordinal):
    return hashlib.sha256((parent + f":stop:{ordinal}").encode()).hexdigest()[:32]


def parent_of(act):
    value = act.continuation
    if not isinstance(value, Mapping) or "stop_retry" not in value:
        return None
    if set(value) != {"version", "stop_retry"} or type(value["version"]) is not int or value["version"] != 1:
        raise ValueError("invalid-stop-retry")
    retry = value["stop_retry"]
    if not isinstance(retry, Mapping) or set(retry) != {"parent_id", "ordinal"} or type(retry["ordinal"]) is not int or retry["ordinal"] not in {2, 3}:
        raise ValueError("invalid-stop-retry")
    if child_identity(retry["parent_id"], retry["ordinal"]) != act.id:
        raise ValueError("invalid-stop-retry")
    return retry


def validate_child(parent, child, ordinal):
    from app.telegram_bots.work_actions import _ACT_SCOPE

    ref = parent_of(child)
    if (
        ref is None
        or ref["parent_id"] != parent.id
        or ref["ordinal"] != ordinal
        or child.id != child_identity(parent.id, ordinal)
        or any(getattr(parent, key) != getattr(child, key) for key in _ACT_SCOPE)
        or child.body.get("attempt_id") != parent.body.get("attempt_id")
        or child.operation_ids
    ):
        raise ValueError("invalid-stop-retry")


class StopRetryMethods:
    async def _stop_children(self, parent):
        if len(parent.operation_ids) >= STOP_REPEATS:
            raise ValueError("invalid-stop-retry")
        children = []
        for ordinal, identity in enumerate(parent.operation_ids, 2):
            if identity != child_identity(parent.id, ordinal):
                raise ValueError("invalid-stop-retry")
            child = await self._work_record(identity)
            validate_child(parent, child, ordinal)
            children.append(child)
        return children

    async def _stop_chain_receipt(self, parent):
        from app.telegram_bots.work_actions import thaw

        for child in await self._stop_children(parent):
            receipt = await self.receipt_for(child)
            if receipt is not None:
                return {"stop_child_id": child.id, "receipt": thaw(receipt)}
        return None

    async def _reconcile_stop_receipt(self, act):
        if not act.operation_ids:
            receipt = await self.receipt_for(act)
            return await self._finish_stop(act, receipt) if receipt is not None else act
        for child in await self._stop_children(act):
            receipt = await self.receipt_for(child)
            if receipt is not None:
                async with self._work_serial(child.id):
                    child = await self._finish_stop(await self._work_record(child.id), receipt)
                return await self._finish_stop_parent(act, child)
        return act

    async def _end_stop_scope(self, act):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import same_body

        async with self.outbox._writer(act.link_id) as (writer, _link):
            row = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
            if row is None:
                raise ValueError("missing-stop-act")
            if row.state in {"refused", "conflict"} and same_body(row.body, act.body) and row.stop_dispatch is not None and row.stop_dispatch["state"] == "awaiting_command":
                row.stop_dispatch = {**row.stop_dispatch, "state": "ended"}
                await writer.flush()
            return _snapshot(row)

    async def _finish_stop_parent(self, parent, child):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import same_body, thaw

        async with self.outbox._writer(parent.link_id) as (writer, _link):
            row = await writer.get(TelegramActRow, parent.id, with_for_update=True, populate_existing=True)
            actual = await writer.get(TelegramActRow, child.id, with_for_update=True, populate_existing=True)
            if row is None or actual is None or not same_body(row.body, parent.body) or actual.state != "done":
                raise ValueError("invalid-stop-retry")
            validate_child(_snapshot(row), _snapshot(actual), parent_of(child)["ordinal"])
            if child.id not in row.operation_ids:
                raise ValueError("invalid-stop-retry")
            row.state, row.reason = "done", None
            row.result, row.updated_at = {"stop_child_id": child.id, "receipt": thaw(actual.result)}, self.outbox.clock()
            await writer.flush()
            return _snapshot(row)

    async def _plan_stop_child(self, parent, ordinal, *, attempt_current):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
        from app.telegram_bots.work_authority import prepare_work_owner

        link = await self.links.get(parent.link_id)
        await self._work_link(link, kind="stop", writable=True)
        current = await self.work.get(actor=PrincipalRef("human", parent.owner_user_id), instance_id=parent.instance_id, work_id=parent.work_id)
        attempt = current["attempt"]
        if not attempt or attempt["id"] != parent.body["attempt_id"] or attempt["status"] not in {"starting", "running"}:
            return None
        scope = parent.stop_dispatch
        if scope is None or attempt["thread_id"] != scope["thread_id"] or scope["run_id"] is not None and attempt["run_id"] != scope["run_id"]:
            raise AgentDenied("The original stop scope changed")
        body = WorkCommand(operation_id=child_identity(parent.id, ordinal), action="stop", attempt_id=attempt["id"], expected_revision=current["revision"], expected_assignment_revision=current["assignment_revision"])
        owner = await prepare_work_owner(self.authority, parent.owner_user_id)
        async with self.outbox._writer(link.id) as (writer, _link):
            row = await writer.get(TelegramActRow, parent.id, with_for_update=True, populate_existing=True)
            if (
                row is None
                or any(getattr(row, key) != getattr(parent, key) for key in _ACT_SCOPE)
                or not same_body(row.body, parent.body)
                or tuple(row.operation_ids) != parent.operation_ids
                or row.cancel_requested_at is not None
                or not attempt_current()
            ):
                raise ValueError("invalid-stop-retry")
            if len(row.operation_ids) != ordinal - 2:
                raise ValueError("invalid-stop-retry")
            if await writer.get(TelegramActRow, body.operation_id) is not None:
                raise ValueError("orphaned-stop-retry")
            await self._stage_work_act(link, kind="stop", request_id=None, work_id=parent.work_id, request=body, owner=owner, session=writer)
            child = await writer.get(TelegramActRow, body.operation_id, with_for_update=True, populate_existing=True)
            child.stop_dispatch = {"attempt_id": attempt["id"], "thread_id": attempt["thread_id"], "run_id": attempt["run_id"], "state": "awaiting_command", "attempts": 0, "not_before": None}
            child.continuation = {"version": 1, "stop_retry": {"parent_id": parent.id, "ordinal": ordinal}}
            row.operation_ids = [*row.operation_ids, child.id]
            row.state, row.reason, row.updated_at = "pending", None, self.outbox.clock()
            await writer.flush()
            await owner(parent.owner_user_id, writer)
            return _snapshot(row), _snapshot(child)

    async def _run_stop_chain(self, parent, *, explicit=False, attempt_current=lambda: True):
        if parent_of(parent) is not None:
            return await self._attempt_stop(parent, explicit=explicit, attempt_current=attempt_current)
        children = await self._stop_children(parent)
        # Reconcile every stored child before any mutable latest-Work check.
        for child in children:
            try:
                receipt = await self.receipt_for(child)
            except Exception:
                expired = child.first_attempt_at is not None and self.outbox.clock() - child.first_attempt_at >= timedelta(hours=1)
                return await self._work_outcome(parent, state="uncertain") if not explicit and (expired or child.attempts >= self.outbox.config.acts.max_attempts) else parent
            if receipt is not None:
                async with self._work_serial(child.id):
                    child = await self._finish_stop(await self._work_record(child.id), receipt)
                    child = await self._dispatch_stop(child, explicit=explicit, attempt_current=attempt_current)
                return await self._finish_stop_parent(parent, child)
        if children:
            async with self._work_serial(children[-1].id):
                result = await self._attempt_stop(await self._work_record(children[-1].id), explicit=explicit, attempt_current=attempt_current)
            parent = await self._work_record(parent.id)
        else:
            result = await self._attempt_stop(parent, explicit=explicit, attempt_current=attempt_current)
        while result.state == "conflict" and result.reason == "changed" and attempt_current():
            ordinal = len(parent.operation_ids) + 2
            if ordinal > STOP_REPEATS:
                return await self._work_outcome(parent, state="conflict", reason="stop-conflicts")
            if not explicit and parent.first_attempt_at is not None and self.outbox.clock() - parent.first_attempt_at >= timedelta(hours=1):
                return await self._work_outcome(parent, state="conflict", reason="stop-conflicts")
            planned = await self._plan_stop_child(parent, ordinal, attempt_current=attempt_current)
            if planned is None:
                return await self._work_outcome(parent, state="refused", reason="no-running-attempt")
            parent, child = planned
            async with self._work_serial(child.id):
                result = await self._attempt_stop(child, explicit=explicit, attempt_current=attempt_current)
            parent = await self._work_record(parent.id)
            if result.state == "done":
                return await self._finish_stop_parent(parent, result)
        if children or parent.operation_ids:
            if result.state == "done":
                return await self._finish_stop_parent(parent, result)
            return await self._work_outcome(parent, state=result.state, reason=result.reason, result={"stop_child_id": result.id})
        return result
