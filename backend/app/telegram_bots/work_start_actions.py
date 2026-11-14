"""Saved explicit activation intents over the existing Gateway admission owner.

Chat controls and capacity offers install these journal APIs separately.
"""

from collections.abc import Mapping
from datetime import timedelta

from sqlalchemy import select

from app.telegram_bots.authority import AccessOff, AuthorityUnavailable, CeilingRefused, internal_request, require
from deerflow.agent_instances.contract import AgentConflict, AgentDenied, AgentPermission, Refusal
from deerflow.agent_instances.work import UNRESOLVED
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.agent_instances.work_execution_contract import ActivateWork
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained

START_REFUSALS = frozenset({"busy", "work-limit", "run-limit", "restarting", "execution-unavailable", "storage-busy"})


class ActivationActionMethods:
    async def activate(self, link, *, operation_id, work_id, assignment_revision):
        """Prepare once, then replay only the original saved activation body."""

        async def perform():
            async with self._work_serial(operation_id):
                act = await self._stage_activation(link, operation_id=operation_id, work_id=work_id, assignment_revision=assignment_revision)
                return await self._attempt_activation(act, explicit=True)

        return await await_drained(perform())

    async def stage_activation(self, link, *, operation_id, work_id, assignment_revision, admission=None):
        """Save intent without launching, borrowing a fresh callback admission.

        Conversation preparation happens before the journal writer. The trusted
        admission hook can join a callback receipt to that writer and veto both.
        An exact saved replay only observes its original intent.
        """

        async def perform():
            async with self._work_serial(operation_id):
                return await self._stage_activation(link, operation_id=operation_id, work_id=work_id, assignment_revision=assignment_revision, admission=admission)

        return await await_drained(perform())

    async def _stage_activation(self, link, *, operation_id, work_id, assignment_revision, admission=None):
        from app.telegram_bots.work_actions import _ACT_SCOPE

        await self._work_link(link)
        if type(assignment_revision) is not int or not 1 <= assignment_revision <= 2**31 - 1:
            raise ValueError("invalid-assignment-revision")
        async with self.session_factory() as reader:
            saved = await reader.get(TelegramActRow, operation_id)
            if saved is not None:
                expected = dict(
                    link_id=link.id,
                    owner_user_id=link.owner_user_id,
                    bot_id=link.bot_id,
                    binding_generation=link.binding_generation,
                    telegram_user_id=link.telegram_user_id,
                    chat_id=link.chat_id,
                    instance_id=link.agent_ref,
                    work_id=work_id,
                    request_id=None,
                    kind="activate",
                    message_id=None,
                )
                if any(getattr(saved, key) != expected[key] for key in _ACT_SCOPE) or saved.body.get("expected_assignment_revision") != assignment_revision:
                    raise ValueError("act-source-conflict")
                from app.telegram_bots.acts import _snapshot

                act = _snapshot(saved)
            else:
                act = None
        if act is None:
            await require(link.owner_user_id, "agents:read", "agents:write", "runs:create")
            actor = PrincipalRef("human", link.owner_user_id)
            record = await self.work.get(actor=actor, instance_id=link.agent_ref, work_id=work_id)
            if record["assignment_revision"] != assignment_revision:
                raise AgentConflict("The displayed assignment changed", reason="work-changed")
            _startable(record)
            conversations = self.authority.app.state.agent_conversations
            thread_id = (record.get("attempt") or {}).get("thread_id")
            if thread_id is None or not all([await conversations.allowed(actor=actor, thread_id=thread_id, permission=permission) for permission in (AgentPermission.USE, AgentPermission.INSPECT)]):
                await require(link.owner_user_id, "threads:write")
                conversation = await conversations.create(
                    actor=actor,
                    instance_id=link.agent_ref,
                    creation_id=operation_id,
                    display_name=record["objective"][:80],
                    metadata={"channel_source": {"type": "im_channel", "provider": "telegram", "connection_id": link.connection_id}, "work_id": work_id},
                )
                thread_id = conversation["thread_id"]
            body = ActivateWork(operation_id=operation_id, expected_revision=record["revision"], expected_assignment_revision=assignment_revision, thread_id=thread_id)
            act = await self._save_work_act(link, kind="activate", request_id=None, work_id=work_id, request=body, admission=admission)
        return act

    async def _activation_outcome(self, captured, *, state, reason=None, result=None, answered=False):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import _ACT_SCOPE, same_body

        async with self.outbox._writer(captured.link_id) as (writer, link):
            saved = await writer.get(TelegramActRow, captured.id, with_for_update=True, populate_existing=True)
            if saved is None:
                raise ValueError("unsupported-work-act")
            if saved.state == "done" or saved.attempts != captured.attempts or any(getattr(saved, key) != getattr(captured, key) for key in _ACT_SCOPE) or not same_body(saved.body, captured.body):
                return _snapshot(saved)
            # A late refusal/exception must not erase a concurrently committed
            # Cancel. An exact admitted receipt can still settle that fence.
            if saved.cancel_requested_at is not None and state != "done":
                reserved = await writer.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.work_id == saved.work_id, AgentWorkAttemptRow.activation_id == saved.id))
                if not answered or reserved is not None:
                    return _snapshot(saved)
                # The owned call has now returned a definitive no-admission
                # result, and no original promise exists. The fence blocks any
                # future call; this is sufficient evidence for cancelled.
                state, reason = "refused", "cancelled"
            saved.state, saved.reason, saved.result, saved.updated_at = state, reason, result, self.outbox.clock()
            if answered:
                from app.telegram_bots.work_actions import thaw

                saved.continuation = {**(thaw(saved.continuation) or {}), "version": 1, "activation_retry": {"answered": min(captured.attempts, answered_calls(captured) + 1)}}
            if state == "done":
                from app.telegram_bots.work_start_status import seed_activation_status

                await seed_activation_status(self, writer, link, saved, result)
            await writer.flush()
            return _snapshot(saved)

    async def _complete_activation(self, act, result):
        try:
            return await self._activation_outcome(act, state="done", result=result)
        except Exception:
            # Host admission already succeeded. A source/journal failure must
            # preserve receipt recovery and let unrelated Acts make progress.
            return await self._work_record(act.id)

    async def _attempt_activation(self, act, *, explicit=False, attempt_current=lambda: True):
        from app.gateway.work_execution import activate_work
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import _ACT_SCOPE, _SCOPE, request_for, same_body

        if act.attempts:
            try:
                receipt = await self.receipt_for(act)
            except Exception:
                elapsed = self.outbox.clock() - act.first_attempt_at
                expired = elapsed >= timedelta(hours=1) or act.attempts - answered_calls(act) >= self.outbox.config.acts.max_attempts
                if not explicit and expired:
                    return await self._activation_outcome(act, state="uncertain")
                return await self._work_record(act.id)
            if receipt is not None:
                return await self._complete_activation(act, receipt)
        held = act.state in {"conflict", "refused"} or act.state == "waiting" and act.reason == "work-limit"
        if held and explicit and act.cancel_requested_at is None:
            # A refused exact replay can leave an original runless reservation.
            # Explicit recovery may complete only that same stored operation;
            # ordinary refusals and capacity waits never become new launches.
            held = not await WorkExecution(self.work).activation_exists(act.work_id, act.id)
        if act.cancel_requested_at is not None or act.state == "done" or held:
            return act
        now = self.outbox.clock()
        if act.state == "waiting" and act.reason == "busy" and now - act.first_attempt_at >= timedelta(seconds=self.outbox.config.queue.max_wait_seconds):
            return await self._activation_outcome(act, state="refused", reason="busy")
        if not explicit and (act.state == "uncertain" or now - act.first_attempt_at >= timedelta(hours=1) or act.state != "waiting" and act.attempts - answered_calls(act) >= self.outbox.config.acts.max_attempts):
            return await self._activation_outcome(act, state="uncertain")
        try:
            link = await self.links.get(act.link_id)
            if not link_matches(link, act):
                raise AgentDenied("The action's binding is no longer current")
            await self._work_link(link, kind="activate", writable=True)
            await require(link.owner_user_id, "agents:read", "agents:write", "runs:create")
            body = request_for("activate", act.body, identity=act.id)
            # The saved body is the authorization. In particular an unresolved
            # original reservation must reach the helper's exact replay path,
            # rather than the fresh-action attempt-running preflight.
            from app.telegram_bots.work_authority import prepare_work_owner

            owner = await prepare_work_owner(self.authority, link.owner_user_id)
            if not attempt_current():
                return act
            async with self.outbox._writer(link.id) as (writer, current):
                saved = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
                if saved is None:
                    raise ValueError("unsupported-work-act")
                if saved.state == "done" or saved.attempts != act.attempts or saved.cancel_requested_at is not None or any(getattr(saved, key) != getattr(act, key) for key in _ACT_SCOPE) or not same_body(saved.body, act.body):
                    return _snapshot(saved)
                if current is None or any(getattr(current, key) != getattr(link, key) for key in _SCOPE) or current.state != "connected" or current.health != "ok":
                    raise AgentDenied("The bot binding is no longer current")
                if not attempt_current():
                    return _snapshot(saved)
                saved.attempts += 1
                saved.state, saved.reason, saved.updated_at = "pending", None, now
                await writer.flush()
                act = _snapshot(saved)
                await owner(link.owner_user_id, writer)
            admission = await activation_admission(self, link, act, attempt_current=attempt_current)
            result = await activate_work(internal_request(self.authority.app, link.owner_user_id), instance_id=act.instance_id, work_id=act.work_id, body=body, admission=admission)
        except AgentConflict as exc:
            return await self._activation_outcome(act, state="conflict", reason=exc.reason or "work-changed", answered=True)
        except Refusal as exc:
            if exc.reason in {"busy", "work-limit"}:
                return await self._activation_outcome(act, state="waiting", reason=exc.reason, answered=True)
            return await self._activation_outcome(act, state="refused", reason=exc.reason or "denied", answered=True)
        except (AgentDenied, AccessOff, CeilingRefused, ValueError) as exc:
            return await self._activation_outcome(act, state="refused", reason=getattr(exc, "reason", None) or "denied", answered=True)
        except Exception:
            # Includes response loss after reservation/run creation. A fresh
            # operation or a changed body would duplicate the human's intent.
            if self.outbox.clock() - act.first_attempt_at >= timedelta(hours=1) or act.attempts - answered_calls(act) >= self.outbox.config.acts.max_attempts:
                return await self._activation_outcome(act, state="uncertain")
            return await self._work_record(act.id)
        else:
            return await self._complete_activation(act, result)

    async def cancel_activation(self, act_id, *, admission=None):
        """Fence future attempts without waiting for an in-flight provider call."""

        async def perform():
            from app.telegram_bots.acts import _snapshot
            from app.telegram_bots.work_actions import _ACT_SCOPE, _SCOPE, same_body

            act = await self._work_record(act_id)
            if act.kind != "activate":
                raise ValueError("unsupported-work-act")
            link = await self.links.get(act.link_id)
            if not link_matches(link, act):
                raise AgentDenied("The action's binding is no longer current")
            await self._work_link(link)
            reserved = await WorkExecution(self.work).activation_exists(act.work_id, act.id)
            owner = await self.authority.prepare_live_owner(act.owner_user_id)
            async with self.outbox._writer(act.link_id) as (writer, current):
                if current is None or any(getattr(current, key) != getattr(link, key) for key in _SCOPE) or current.state != "connected" or current.health != "ok":
                    raise AgentDenied("The bot binding is no longer current")
                saved = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
                if saved is None or any(getattr(saved, key) != getattr(act, key) for key in _ACT_SCOPE) or not same_body(saved.body, act.body):
                    raise ValueError("act-source-conflict")
                if admission is not None:
                    await admission(writer, None)
                if saved.state != "done" and saved.cancel_requested_at is None:
                    saved.cancel_requested_at = saved.updated_at = self.outbox.clock()
                    conclusive = not reserved and saved.attempts == act.attempts and (not saved.attempts or saved.state in {"waiting", "refused", "conflict"})
                    saved.state, saved.reason = ("refused", "cancelled") if conclusive else ("uncertain", "cancel-unconfirmed")
                    await writer.flush()
                result = _snapshot(saved)
                await owner(act.owner_user_id, writer)
                if admission is not None:
                    await admission(writer, result)
            # Only observe. Even a runless exact reservation must never be
            # handed back to the launching helper after this fence.
            receipt = await self.receipt_for(result)
            return await self._complete_activation(result, receipt) if receipt is not None else result

        return await await_drained(perform())


def link_matches(link, act):
    return link is not None and (link.owner_user_id, link.bot_id, link.agent_ref, link.binding_generation, link.telegram_user_id, link.chat_id) == (
        act.owner_user_id,
        act.bot_id,
        act.instance_id,
        act.binding_generation,
        act.telegram_user_id,
        act.chat_id,
    )


def answered_calls(act):
    """Conclusive call refusals do not consume the unknown-result retry budget."""
    value = act.continuation
    if value is None:
        return 0
    if (
        not isinstance(value, Mapping)
        or set(value) - {"activation_retry", "activation_parent"} != {"version"}
        or not {"activation_retry", "activation_parent"}.intersection(value)
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise ValueError("invalid-activation-retry")
    if "activation_parent" in value:
        parent = value["activation_parent"]
        if not isinstance(parent, Mapping) or set(parent) != {"version", "act_id", "source_signature", "authorized_at"} or type(parent["version"]) is not int or parent["version"] != 1:
            raise ValueError("invalid-activation-parent")
        if "activation_retry" not in value:
            return 0
    retry = value["activation_retry"]
    if not isinstance(retry, Mapping) or set(retry) != {"answered"} or type(retry["answered"]) is not int or not 0 <= retry["answered"] <= act.attempts:
        raise ValueError("invalid-activation-retry")
    return retry["answered"]


def _startable(record):
    if not record["work_enabled"]:
        raise AgentDenied("Work is unavailable", reason="work-off")
    if record["needs_mandate_reconciliation"]:
        raise AgentDenied("The mandate requires confirmation", reason="brief-changed")
    if record["status"] in {"completed", "cancelled"}:
        raise AgentDenied("The assignment is closed", reason="work-closed")
    if record["status"] == "submitted":
        raise AgentDenied("The result needs review", reason="awaiting-review")
    attempt = record.get("attempt")
    if attempt and attempt["status"] in UNRESOLVED:
        raise AgentConflict("The prior attempt is unresolved", reason="attempt-running" if attempt["status"] in {"starting", "running"} else "attempt-unsettled")


async def activation_admission(acts, link, act, *, attempt_current):
    from app.telegram_bots.attention_authority import prepare_attention

    prepared = await prepare_attention(acts, link)
    projection = ActivationProjection(act, attempt_current)
    await projection.parent.prepare(acts, link)

    async def admission(writer):
        acts._writer_required(writer)
        if not attempt_current():
            raise AuthorityUnavailable()
        # Resource/thread/instance/Work locks already belong to the host.
        # Keep the same instance -> bot -> Act order as other Work effects.
        await writer.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).with_for_update().execution_options(populate_existing=True))
        await writer.scalar(select(TelegramActRow).where(TelegramActRow.id == act.id).with_for_update().execution_options(populate_existing=True))
        await prepared(session=writer, projection=projection)
        if not attempt_current():
            raise AuthorityUnavailable()

    return admission


class ActivationProjection:
    def __init__(self, act, attempt_current):
        from app.telegram_bots.work_continuation_sources import ActivationParentProjection

        self.act, self.attempt_current = act, attempt_current
        self.parent = ActivationParentProjection(self.act)

    def extend(self, query):
        self.column = len(query.column_descriptions)
        return self.parent.extend(query.add_columns(TelegramActRow).join(TelegramActRow, TelegramActRow.id == self.act.id))

    def check_with_permissions(self, rows, held):
        from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
        from app.telegram_bots.work_outcome_transfer import transfer_receipt

        saved = rows[0][self.column]
        if not self.attempt_current():
            raise AuthorityUnavailable()
        if not {"agents:write", "runs:create"} <= held:
            raise CeilingRefused()
        if (
            any(getattr(saved, key) != getattr(self.act, key) for key in _ACT_SCOPE)
            or not same_body(saved.body, self.act.body)
            or saved.attempts != self.act.attempts
            or saved.state != "pending"
            or saved.cancel_requested_at is not None
            or transfer_receipt(saved.continuation) is not None
        ):
            raise AgentDenied("The activation admission is no longer current")
        self.parent.check(rows, saved, held)
