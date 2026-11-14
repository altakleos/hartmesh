"""Durable original-attempt cancellation; commands and dispatch settle separately."""

from datetime import timedelta

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.authority import AccessOff, AuthorityUnavailable, CeilingRefused, internal_request, require
from app.telegram_bots.work_stop_retries import STOP_REPEATS as STOP_REPEATS
from app.telegram_bots.work_stop_retries import StopRetryMethods
from deerflow.agent_instances.contract import AgentConflict, AgentDenied, Refusal
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained

ACT_AUTOMATIC_WINDOW_SECONDS = 3600


def original_attempt_id(act):
    if act.kind == "stop":
        return act.body["attempt_id"]
    if act.kind in {"edit", "cancel"}:
        from app.telegram_bots.work_management_actions import command_source

        scope = command_source(act)["scope"]
        if scope is not None:
            return scope["attempt_id"]
    raise ValueError("missing-original-attempt")


class StopActionMethods(StopRetryMethods):
    async def stop(self, link, *, operation_id, work_id, attempt_id):
        async def perform():
            async with self._work_serial(operation_id):
                act = await self._stage_stop(link, operation_id=operation_id, work_id=work_id, attempt_id=attempt_id)
                return await self._run_stop_chain(act, explicit=True)

        return await await_drained(perform())

    async def stage_stop(self, link, *, operation_id, work_id, attempt_id, admission=None):
        """Admit an exact Stop intent without issuing its command or dispatch."""

        async def perform():
            async with self._work_serial(operation_id):
                return await self._stage_stop(link, operation_id=operation_id, work_id=work_id, attempt_id=attempt_id, admission=admission)

        return await await_drained(perform())

    async def _stage_stop(self, link, *, operation_id, work_id, attempt_id, admission=None):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import request_for
        from app.telegram_bots.work_authority import prepare_work_owner

        await self._work_link(link, kind="stop", writable=True)
        await require(link.owner_user_id, "agents:read", "agents:write", "runs:cancel")
        async with self.session_factory() as reader:
            saved = await reader.get(TelegramActRow, operation_id)
            act = _snapshot(saved) if saved is not None else None
        if act is not None:
            if (act.link_id, act.work_id, act.kind, act.body.get("attempt_id")) != (link.id, work_id, "stop", attempt_id):
                raise ValueError("act-source-conflict")
            request_for("stop", act.body, identity=operation_id)
        else:
            current = await self.work.get(actor=PrincipalRef("human", link.owner_user_id), instance_id=link.agent_ref, work_id=work_id)
            attempt = current["attempt"]
            body = WorkCommand(operation_id=operation_id, expected_revision=current["revision"], expected_assignment_revision=current["assignment_revision"], action="stop", attempt_id=attempt_id)
            scope = None
            if attempt is not None and attempt["id"] == attempt_id and attempt["status"] in {"starting", "running"}:
                scope = {"attempt_id": attempt_id, "thread_id": attempt["thread_id"], "run_id": attempt["run_id"], "state": "awaiting_command", "attempts": 0, "not_before": None}
            owner = await prepare_work_owner(self.authority, link.owner_user_id)
            async with self.outbox._writer(link.id) as (writer, _current):
                if admission is not None:
                    await admission(writer, None)
                await self._stage_work_act(link, kind="stop", request_id=None, work_id=work_id, request=body, owner=owner, session=writer)
                row = await writer.get(TelegramActRow, operation_id, populate_existing=True)
                row.stop_dispatch = scope
                if scope is None:
                    row.state, row.reason = "refused", "no-running-attempt"
                await writer.flush()
                act = _snapshot(row)
                if admission is not None:
                    await admission(writer, act)
        return act

    async def _finish_stop(self, act, receipt):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import _ACT_SCOPE, same_body

        async with self.outbox._writer(act.link_id) as (writer, _link):
            row = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
            if row is None or any(getattr(row, key) != getattr(act, key) for key in _ACT_SCOPE) or not same_body(row.body, act.body):
                raise ValueError("act-source-conflict")
            if row.state != "done":
                row.state, row.reason, row.result, row.updated_at = "done", None, receipt, self.outbox.clock()
                row.stop_dispatch = {**row.stop_dispatch, "state": "ready"}
                await writer.flush()
            return _snapshot(row)

    async def _attempt_stop(self, act, *, explicit=False, attempt_current=lambda: True):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import _ACT_SCOPE, request_for, same_body
        from app.telegram_bots.work_authority import prepare_work_owner

        now = self.outbox.clock()
        expired = act.first_attempt_at is not None and now - act.first_attempt_at >= timedelta(seconds=ACT_AUTOMATIC_WINDOW_SECONDS)
        exhausted = expired or act.attempts >= self.outbox.config.acts.max_attempts
        if act.state == "done" or act.attempts:
            try:
                receipt = await self.receipt_for(act)
            except Exception:
                return await self._work_outcome(act, state="uncertain") if exhausted and not explicit else act
            if receipt is not None:
                return await self._dispatch_stop(await self._finish_stop(act, receipt), explicit=explicit, attempt_current=attempt_current)
        if act.state in {"done", "refused", "conflict"} or act.cancel_requested_at is not None:
            return act
        if not explicit and (act.state != "pending" or act.reason is not None):
            return act
        if not explicit and exhausted:
            return await self._work_outcome(act, state="uncertain")
        known_receipt = None
        try:
            link = await self.links.get(act.link_id)
            await self._work_link(link, kind="stop", writable=True)
            await require(act.owner_user_id, "agents:read", "agents:write", "runs:cancel")
            body = request_for("stop", act.body, identity=act.id)
            owner = await prepare_work_owner(self.authority, link.owner_user_id)
            async with self.outbox._writer(link.id) as (writer, current):
                row = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
                if row is None or row.state == "done" or row.attempts != act.attempts or any(getattr(row, key) != getattr(act, key) for key in _ACT_SCOPE) or not same_body(row.body, act.body):
                    return _snapshot(row) if row is not None else act
                if current is None or current.binding_generation != act.binding_generation or row.cancel_requested_at is not None or not attempt_current():
                    return _snapshot(row)
                row.attempts += 1
                row.first_attempt_at, row.updated_at = row.first_attempt_at or now, now
                row.state, row.reason = "pending", None
                from app.telegram_bots.work_stop_retries import parent_of, validate_child

                retry = parent_of(_snapshot(row))
                if retry is not None:
                    parent = await writer.get(TelegramActRow, retry["parent_id"], with_for_update=True, populate_existing=True)
                    if parent is None:
                        raise ValueError("missing-stop-retry-parent")
                    validate_child(_snapshot(parent), _snapshot(row), retry["ordinal"])
                    if parent.cancel_requested_at is not None or parent.state not in {"pending", "uncertain"} or parent.operation_ids[-1] != row.id:
                        raise ValueError("invalid-stop-retry")
                    parent.attempts += 1
                    parent.updated_at = now
                await writer.flush()
                act = _snapshot(row)
                await owner(link.owner_user_id, writer)
            admission = await stop_admission(self, link, act, attempt_current=attempt_current)
            known_receipt = await self.work.command(actor=PrincipalRef("human", act.owner_user_id), instance_id=act.instance_id, work_id=act.work_id, request=body, can_stop=True, admission=admission)
            return await self._dispatch_stop(await self._finish_stop(act, known_receipt), explicit=explicit, attempt_current=attempt_current)
        except AgentConflict as exc:
            if known_receipt is not None:
                return act
            return await self._end_stop_scope(await self._work_outcome(act, state="conflict", reason=exc.reason or "changed"))
        except (AgentDenied, AccessOff, CeilingRefused, Refusal, ValueError) as exc:
            if known_receipt is not None:
                return act
            return await self._end_stop_scope(await self._work_outcome(act, state="refused", reason=getattr(exc, "reason", None) or "denied"))
        except Exception:
            return act

    async def resume_stop_dispatch(self, act):
        async def perform():
            async with self._work_serial(act.id):
                return await self._dispatch_stop(await self._work_record(act.id))

        return await await_drained(perform())

    async def _recover_stop_dispatches(self, *, attempt_current):
        now = self.outbox.clock()
        async with self.session_factory() as reader:
            identities = list(
                await reader.scalars(
                    select(TelegramActRow.id)
                    .where(
                        TelegramActRow.kind.in_(("stop", "edit", "cancel")),
                        TelegramActRow.state == "done",
                        TelegramActRow.stop_dispatch["state"].as_string().in_(("ready", "requested", "uncertain")),
                        or_(TelegramActRow.stop_dispatch["not_before"].as_string().is_(None), TelegramActRow.stop_dispatch["not_before"].as_string() <= now.isoformat()),
                    )
                    .order_by(TelegramActRow.updated_at, TelegramActRow.id)
                    .limit(100)
                )
            )
        for identity in identities:
            if not attempt_current():
                break
            if identity not in self._work_active:
                async with self._work_serial(identity):
                    await self._dispatch_stop(await self._work_record(identity), attempt_current=attempt_current)

    async def _dispatch_stop(self, act, *, explicit=False, attempt_current=lambda: True):
        from app.gateway.work_execution import dispatch_work_stop
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
        from app.telegram_bots.work_management_actions import same_binding

        scope = act.stop_dispatch
        if (
            act.state != "done"
            or scope is None
            or scope["state"] not in {"ready", "requested", "uncertain", "ended"}
            or scope["state"] == "ended"
            and not (explicit and scope.get("ended_from") in {"ready", "uncertain"})
            or not attempt_current()
        ):
            return act
        try:
            if scope["attempt_id"] != original_attempt_id(act) or not isinstance(scope["thread_id"], str) or not scope["thread_id"]:
                raise ValueError("act-source-conflict")
            if await self.receipt_for(act) is None:
                return act
            link = await self.links.get(act.link_id)
            await self._work_link(link)
            if (link.owner_user_id, link.bot_id, link.agent_ref, link.binding_generation, link.telegram_user_id, link.chat_id) != (act.owner_user_id, act.bot_id, act.instance_id, act.binding_generation, act.telegram_user_id, act.chat_id):
                raise AgentDenied("The action's binding is no longer current")
            if not attempt_current():
                return act
            if scope["state"] == "requested" or scope["state"] == "uncertain" and not explicit:
                original = await self.work.attempt_for_stop(actor=PrincipalRef("human", act.owner_user_id), instance_id=act.instance_id, work_id=act.work_id, attempt_id=scope["attempt_id"])
                if original is not None and (original["thread_id"] != scope["thread_id"] or scope["run_id"] is not None and original["run_id"] != scope["run_id"]):
                    raise AgentDenied("Work stop binding is unavailable")
                settled = original is not None and original["status"] in {"succeeded", "failed", "cancelled"} and original["settled_at"] is not None
                updated = {**scope, "state": "settled" if settled else scope["state"], "run_id": original["run_id"] if original is not None else scope["run_id"]}
            else:
                await self._work_link(link, kind="stop", writable=True)
                await require(act.owner_user_id, "agents:read", "agents:write", "runs:cancel")
                guard = await stop_dispatch_admission(self, link, act, attempt_current=attempt_current)
                result = await dispatch_work_stop(
                    internal_request(self.authority.app, act.owner_user_id),
                    self.work,
                    actor=PrincipalRef("human", act.owner_user_id),
                    instance_id=act.instance_id,
                    work_id=act.work_id,
                    expected_attempt_id=scope["attempt_id"],
                    expected_thread_id=scope["thread_id"],
                    expected_run_id=scope["run_id"],
                    before_dispatch=guard,
                )
                updated = {**scope, "state": "settled" if result.state == "settled" else "requested" if result.state == "requested" else "ready", "run_id": result.run_id}
        except (AgentDenied, AccessOff, CeilingRefused):
            updated = {**scope, "state": scope["state"] if scope["state"] in {"requested", "uncertain"} else "ended"}
            if updated["state"] == "ended" and scope["state"] == "ready":
                updated["ended_from"] = "ready"
        except Exception:
            if not attempt_current():
                return act
            count = scope["attempts"] + 1
            # An acknowledged cancel never becomes permission to dispatch it
            # again. Failed settlement reads retain that acknowledged phase.
            state = scope["state"] if scope["state"] in {"requested", "uncertain"} else "uncertain" if count >= self.outbox.config.acts.max_attempts else "ready"
            updated = {**scope, "attempts": count, "state": state}
        updated["not_before"] = (self.outbox.clock() + timedelta(seconds=self.outbox.config.acts.retry_seconds)).isoformat()
        if updated["state"] != "ended":
            updated.pop("ended_from", None)
        if not attempt_current():
            return act
        async with self.outbox._writer(act.link_id) as (writer, binding):
            row = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
            if (
                attempt_current()
                and same_binding(binding, act)
                and binding.state == "connected"
                and binding.health == "ok"
                and row is not None
                and row.state == "done"
                and all(getattr(row, key) == getattr(act, key) for key in _ACT_SCOPE)
                and same_body(row.body, act.body)
                # Stop reply metadata has its own owner; recording an actual
                # physical ACK must not leave permission to cancel again.
                # Management continuations additionally seal command consent.
                and (act.kind == "stop" or same_body(row.continuation, act.continuation))
                and same_body(row.stop_dispatch, scope)
                and row.cancel_requested_at == act.cancel_requested_at
            ):
                row.stop_dispatch, row.updated_at = updated, self.outbox.clock()
                await writer.flush()
                return _snapshot(row)
        return act


async def stop_admission(acts, link, act, *, attempt_current):
    from app.telegram_bots.attention_authority import prepare_attention
    from app.telegram_bots.work_command_stop_origins import stop_mode_proof

    prepared = await prepare_attention(acts, link)
    projection = StopProjection(act, attempt_current, command=await stop_mode_proof(acts, link, act))

    async def admission(writer):
        acts._writer_required(writer)
        await writer.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).with_for_update().execution_options(populate_existing=True))
        await writer.scalar(select(TelegramActRow).where(TelegramActRow.id == act.id).with_for_update().execution_options(populate_existing=True))
        held, _basis = await prepared(session=writer, projection=projection)
        if not {"agents:write", "runs:cancel"}.issubset(held):
            raise CeilingRefused()

    return admission


class StopProjection:
    def __init__(self, act, attempt_current, *, command=None):
        self.act, self.attempt_current = act, attempt_current
        self.command = command
        from app.telegram_bots.work_stop_retries import parent_of

        self.retry = parent_of(act) if act.kind == "stop" else None
        self.management_consent = None
        self.management_witness = None
        if act.kind in {"edit", "cancel"}:
            from app.telegram_bots.work_management_actions import command_source
            from app.telegram_bots.work_management_consent import ManagementModeWitness

            self.management_witness = ManagementModeWitness(act)

            if command_source(act)["origin_kind"] == "callback":
                from app.telegram_bots.work_management_consent import ManagementConsent

                self.management_consent = ManagementConsent(act)

    def extend(self, query):
        self.column = len(query.column_descriptions)
        result = (
            query.add_columns(TelegramActRow, AgentWorkRow, AgentWorkAttemptRow)
            .join(TelegramActRow, TelegramActRow.id == self.act.id)
            .join(AgentWorkRow, and_(AgentWorkRow.id == self.act.work_id, AgentWorkRow.instance_id == TelegramBotLinkRow.agent_ref))
            .join(AgentWorkAttemptRow, and_(AgentWorkAttemptRow.id == original_attempt_id(self.act), AgentWorkAttemptRow.work_id == AgentWorkRow.id))
        )
        if self.act.kind in {"edit", "cancel"}:
            self.management_control_column = len(result.column_descriptions)
            control = aliased(TelegramCallbackRow)
            result = result.add_columns(control).outerjoin(control, control.id == self.act.id)
            result = self.management_witness.extend(result)
            return self.management_consent.extend(result) if self.management_consent is not None else result
        if self.retry is not None:
            self.parent_column = len(result.column_descriptions)
            parent = aliased(TelegramActRow)
            result = result.add_columns(parent).join(parent, parent.id == self.retry["parent_id"])
        return self.command.extend(result) if self.command is not None else result

    def check_parent(self, rows, saved, *, dispatch=False):
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.work_actions import same_body
        from app.telegram_bots.work_stop_retries import parent_of, validate_child

        if self.act.kind in {"edit", "cancel"}:
            from app.telegram_bots.work_management_actions import command_source

            source, expected = command_source(_snapshot(saved)), command_source(self.act)
            if source != expected or saved.operation_ids or parent_of(_snapshot(saved)) is not None:
                raise AgentDenied("The management cancellation source changed")
            self.management_witness.check(rows)
            if self.management_consent is not None:
                self.management_consent.check(rows, allow_closed=dispatch)
            elif source["origin_kind"] != "internal" or rows[0][self.management_control_column] is not None:
                raise AgentDenied("The management cancellation source changed")
            return
        if self.retry is None:
            if parent_of(_snapshot(saved)) is not None:
                raise AgentDenied("The stop retry identity changed")
            if self.command is not None:
                self.command.check(rows, saved)
            return
        if not same_body(saved.continuation, self.act.continuation):
            raise AgentDenied("The stop retry identity changed")
        parent = rows[0][self.parent_column]
        validate_child(_snapshot(parent), _snapshot(saved), self.retry["ordinal"])
        if self.command is not None:
            self.command.check(rows, parent)
        if (
            parent.cancel_requested_at is not None
            or not parent.operation_ids
            or parent.operation_ids[-1] != saved.id
            or parent.state not in ({"pending", "uncertain", "done"} if dispatch else {"pending", "uncertain"})
            or parent.state == "done"
            and (parent.result or {}).get("stop_child_id") != saved.id
        ):
            raise AgentDenied("The original stop retry admission changed")

    def check(self, rows):
        from app.telegram_bots.work_actions import _ACT_SCOPE, same_body

        saved, _work, attempt = rows[0][self.column : self.column + 3]
        self.check_parent(rows, saved)
        if not self.attempt_current():
            raise AuthorityUnavailable()
        if (
            any(getattr(saved, key) != getattr(self.act, key) for key in _ACT_SCOPE)
            or not same_body(saved.body, self.act.body)
            or not same_body(saved.stop_dispatch, self.act.stop_dispatch)
            or saved.attempts != self.act.attempts
            or saved.state != "pending"
            or saved.cancel_requested_at is not None
            or attempt.thread_id != saved.stop_dispatch["thread_id"]
            or saved.stop_dispatch["run_id"] is not None
            and attempt.run_id != saved.stop_dispatch["run_id"]
        ):
            raise AgentDenied("The stop admission is no longer current")


async def stop_dispatch_admission(acts, link, act, *, attempt_current):
    from app.telegram_bots.attention_authority import prepare_attention
    from app.telegram_bots.work_command_stop_origins import stop_mode_proof

    prepared = await prepare_attention(acts, link)
    command = await stop_mode_proof(acts, link, act) if act.kind == "stop" else None

    async def admission(original):
        projection = StopDispatchProjection(act, original, attempt_current, command=command)
        held, _basis = await prepared(projection=projection)
        if not {"agents:write", "runs:cancel"}.issubset(held):
            raise CeilingRefused()
        if not attempt_current():
            raise AuthorityUnavailable()
        return True

    return admission


class StopDispatchProjection(StopProjection):
    def __init__(self, act, original, attempt_current, *, command=None):
        super().__init__(act, attempt_current, command=command)
        self.original = original

    def extend(self, query):
        from deerflow.persistence.agent_instances.work import AgentWorkEventRow

        result = super().extend(query)
        self.event_column = len(result.column_descriptions)
        return result.add_columns(AgentWorkEventRow).join(AgentWorkEventRow, and_(AgentWorkEventRow.instance_id == self.act.instance_id, AgentWorkEventRow.work_id == self.act.work_id, AgentWorkEventRow.operation_id == self.act.id))

    def check(self, rows):
        from app.telegram_bots.work_actions import _ACT_SCOPE, same_body

        saved, _work, attempt = rows[0][self.column : self.column + 3]
        event = rows[0][self.event_column]
        self.check_parent(rows, saved, dispatch=True)
        permissions = rows[0][2]
        original = self.original
        if not self.attempt_current():
            raise AuthorityUnavailable()
        if (
            any(getattr(saved, key) != getattr(self.act, key) for key in _ACT_SCOPE)
            or saved.state != "done"
            or not same_body(saved.body, self.act.body)
            or not same_body(saved.stop_dispatch, self.act.stop_dispatch)
            # Cancelling logical retries cannot abandon the independent Stop
            # obligation of an already proved edit/withdraw command.
            or saved.cancel_requested_at is not None
            and saved.kind not in {"edit", "cancel"}
            or (attempt.id, attempt.thread_id, attempt.run_id, attempt.status, attempt.requester_id) != (original["id"], original["thread_id"], original["run_id"], original["status"], original["requester_id"])
            or permissions & 6 != 6
            and attempt.requester_id != self.act.owner_user_id
            or (event.action, event.actor_kind, event.actor_id) != (self.act.kind, "human", self.act.owner_user_id)
            or not same_body(event.request, self.act.body)
        ):
            raise AgentDenied("The stop dispatch admission is no longer current")
