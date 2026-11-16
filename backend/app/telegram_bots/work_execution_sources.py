"""Own Start/Stop outcomes reuse full-ACK Work delivery and current source fences."""

from collections.abc import Mapping
from dataclasses import asdict
from datetime import datetime

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.execution_notices import execution_notice
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.settings import format_time
from app.telegram_bots.work_actions import _ACT_SCOPE, request_for, same_body, thaw
from app.telegram_bots.work_command_stop_origins import CommandStopProof, source_marker
from app.telegram_bots.work_details import WorkDetailProjection, _fields
from app.telegram_bots.work_news_sources import WorkNewsRead
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_start_controls import displayed_start
from app.telegram_bots.work_stop_controls import displayed_stop
from app.telegram_bots.work_stop_retries import parent_of, validate_child
from app.telegram_bots.work_view_origins import anchor_signature
from app.telegram_bots.work_view_sources import WorkViewReader, detail_read
from deerflow.agent_instances.work import _date
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.persistence.user.model import UserRow
from deerflow.utils.file_io import await_drained

EXECUTION_KEYS = frozenset({"execution.activate", "execution.stop"})
_ATTEMPT = ("id", "work_id", "activation_id", "requester_id", "status", "run_id", "thread_id", "settled_at")


def act_facts(act):
    """No saved Work result, candidate or file reference enters the presentation."""
    return thaw(
        {
            **{key: getattr(act, key) for key in ("id", *_ACT_SCOPE, "body", "state", "reason", "attempts", "operation_ids", "stop_dispatch", "continuation")},
            "cancel_requested_at": _date(act.cancel_requested_at) if act.cancel_requested_at is not None else None,
            "first_attempt_at": _date(act.first_attempt_at) if act.first_attempt_at is not None else None,
        }
    )


def require_execution_scope(link, act):
    expected = dict(
        link_id=link.id,
        owner_user_id=link.owner_user_id,
        bot_id=link.bot_id,
        binding_generation=link.binding_generation,
        telegram_user_id=link.telegram_user_id,
        chat_id=link.chat_id,
        instance_id=link.agent_ref,
        work_id=act.work_id,
        request_id=None,
        kind=act.kind,
        message_id=None,
    )
    if act.kind not in {"activate", "stop"} or any(getattr(act, key) != expected[key] for key in _ACT_SCOPE):
        raise ProductSourceRetired()


def qualify_execution_origin(link, act, callback, parent, controls, *, activation_parent=None, command=None, context_root=None):
    try:
        if act.kind == "activate_context":
            from app.telegram_bots.work_start_conversations import require_context_child, source_signature

            if context_root is None:
                raise ValueError("missing-activation-context-root")
            require_context_child(context_root, act)
            original = qualify_execution_origin(link, context_root, callback, parent, controls, activation_parent=activation_parent)
            return {**original, "activation_context_root_id": context_root.id, "activation_context_signature": source_signature(context_root)}
        if command is not None:
            require_execution_scope(link, act)
            if activation_parent is not None or act.kind != "stop" or command.act.id != act.id or not same_body(source_marker(act), command.marker):
                raise ValueError()
            return command.origin
        if act.kind not in {"activate", "stop"} or callback is None or parent is None:
            raise ValueError()
        voice_parent = activation_parent is not None and activation_parent.kind == "respond"
        if (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.instance_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, None if voice_parent else link.agent_ref) or (
            callback.link_id,
            callback.binding_generation,
            callback.chat_id,
            callback.message_thread_id,
        ) != (link.id, link.binding_generation, link.chat_id, parent.message_thread_id):
            raise ValueError()
        require_execution_scope(link, act)
        parsed = request_for(act.kind, act.body, identity=act.id)
        if not same_body(parsed.model_dump(mode="json", exclude_unset=True), act.body):
            raise ValueError()
        if activation_parent is not None:
            from app.telegram_bots.work_continuation_state import require_child

            require_child(activation_parent, act)
            if activation_parent.kind in {"decide", "respond"}:
                from app.telegram_bots.work_request_continuations import request_activation, request_signature

                source = request_activation(activation_parent)
                origin = source["origin"]
                _validate_rows(parent, controls)
                if (
                    callback.id != activation_parent.id
                    or callback.kind != ("voice_save_resume" if voice_parent else "decide_confirm_resume")
                    or parent.work_id != (None if voice_parent else act.work_id)
                    or parent.id != origin["anchor_outbox_id"]
                    or callback.payload.get("presentation_version") != origin["presentation_version"]
                    or request_signature(parent) != origin["signature"]
                    or not controls
                    or any(control.payload.get("work_action_id") != activation_parent.id for control in controls)
                ):
                    raise ValueError()
                return {**origin, "activation_parent_id": activation_parent.id}
            from app.telegram_bots.assignment_consent import delegate_origin
            from app.telegram_bots.assignment_controls import consent_origin, displayed_assignment

            origin = delegate_origin(activation_parent)
            original, _source = displayed_assignment(link, parent, callback)
            _validate_rows(original, controls)
            if (
                callback.id != activation_parent.id
                or callback.kind != "assign_start"
                or parent.work_id is not None
                or consent_origin(original, callback) != origin
                or not controls
                or any(control.payload.get("work_action_id") != activation_parent.id for control in controls)
            ):
                raise ValueError()
            return {**origin, "activation_parent_id": activation_parent.id}
        if callback.id != act.id or isinstance(act.continuation, Mapping) and "activation_parent" in act.continuation:
            raise ValueError()
        original, *basis = (displayed_start if act.kind == "activate" else displayed_stop)(link, parent, callback)
        from app.telegram_bots.work_status_control_origins import carrier_source

        source = carrier_source(original)
        expected_parent_work = None if act.kind == "activate" and source["key"] == "assignment.card" and source["work"]["phase"] == "saved" else act.work_id
        if parent.work_id != expected_parent_work:
            raise ValueError()
        if act.kind == "activate":
            if basis[0]["id"] != act.work_id or basis[0]["assignment_revision"] != parsed.expected_assignment_revision:
                raise ValueError()
        elif basis != [act.work_id, parsed.attempt_id]:
            raise ValueError()
        _validate_rows(original, controls)
        if not controls or any(control.payload.get("work_action_id") != act.id for control in controls):
            raise ValueError()
        return {"callback_id": callback.id, "anchor_outbox_id": parent.id, "presentation_version": callback.payload["presentation_version"], "signature": anchor_signature(original)}
    except (KeyError, TypeError, ValueError, CallbackRefused):
        raise ProductSourceRetired() from None


async def execution_origin_rows(reader, journal):
    """Locate the actual human gesture; a composite child has no own button."""
    context_root = None
    if journal.kind == "activate_context":
        from app.telegram_bots.work_start_conversations import require_context_child

        marker = (journal.continuation or {}).get("activation_context_parent")
        if not isinstance(marker, Mapping) or type(marker.get("root_id")) is not str:
            raise ProductSourceRetired()
        row = await reader.get(TelegramActRow, marker["root_id"], populate_existing=True)
        if row is None:
            raise ProductSourceRetired()
        context_root = _snapshot(row)
        require_context_child(context_root, _snapshot(journal))
        journal = row
    marker = journal.continuation.get("activation_parent") if isinstance(journal.continuation, Mapping) else None
    primary = await reader.get(TelegramActRow, marker.get("act_id"), populate_existing=True) if isinstance(marker, Mapping) and type(marker.get("act_id")) is str else None
    if marker is not None and primary is None:
        raise ProductSourceRetired()
    callback = await reader.get(TelegramCallbackRow, primary.id if primary is not None else journal.id, populate_existing=True)
    parent = await reader.get(TelegramOutboxRow, callback.payload.get("anchor_outbox_id"), populate_existing=True) if callback else None
    controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == callback.group_id).execution_options(populate_existing=True))) if callback else []
    return callback, parent, controls, _snapshot(primary) if primary is not None else None, context_root


def qualify_stop_receipt(event, act):
    """One immutable command/receipt proof for delivery and retention."""
    try:
        receipt = thaw(act.result)
        fields = ("id", "instance_id", "creator_id", "revision", "assignment_revision")
        if (
            event is None
            or (event.instance_id, event.work_id, event.operation_id, event.action, event.actor_kind, event.actor_id) != (act.instance_id, act.work_id, act.id, "stop", "human", act.owner_user_id)
            or not same_body(event.request, act.body)
            or type(receipt) is not dict
            or type(event.result) is not dict
            or (receipt.get("id"), receipt.get("instance_id")) != (act.work_id, act.instance_id)
            or not same_body({key: event.result.get(key) for key in fields}, {key: receipt.get(key) for key in fields})
            or any(type(value.get(key)) is not int for value in (event.result, receipt) for key in ("revision", "assignment_revision"))
            or event.revision != act.body["expected_revision"] + 1
            or event.assignment_revision != act.body["expected_assignment_revision"]
            or event.result.get("revision") != event.revision
            or event.result.get("assignment_revision") != event.assignment_revision
        ):
            raise ValueError()
        return digest([event.id, event.operation_id, event.request, {key: event.result.get(key) for key in fields}])
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ProductSourceRetired() from None


class WorkExecutionReader(WorkViewReader):
    def __init__(self, *, capacity=None, **kwargs):
        super().__init__(**kwargs)
        self.capacity = capacity

    async def prepare_clock(self, link, item):
        clock = await super().prepare_clock(link, item)
        if self.capacity is not None:
            async with self.links.session_factory() as reader:
                act = await reader.get(TelegramActRow, item.payload["work_execution_intent"]["act_id"])
                waiting = act is not None and act.kind == "activate" and act.state == "waiting" and act.reason == "work-limit"
            if waiting:
                from app.telegram_bots.work_capacity_sources import CapacityClock

                return CapacityClock(clock.settings, clock.zone, clock.now, await self.capacity.read(link))
        from app.telegram_bots.work_execution_readiness import prepare_refusal_clock

        return await prepare_refusal_clock(self, link, item, clock)

    async def inspect(self, link, item):
        async def perform():
            intent = item.payload.get("work_execution_intent")
            if type(intent) is not dict or set(intent) != {"version", "act_id", "origin"} or type(intent["version"]) is not int or intent["version"] != 1:
                raise ProductSourceRetired()
            detail = await self.detail(link, item.work_id)
            clock = await self.prepare_clock(link, item)
            projection = WorkExecutionProjection(self, link, item, detail=detail, clock=clock, allow_empty=True)
            await projection.prepare_source()
            admission = await prepare_attention(self, link)
            await admission(projection=projection)
            return projection

        return await await_drained(perform())

    async def read(self, link, item):
        projection = await self.inspect(link, item)
        if projection.source is None:
            raise ProductSourceRetired()
        return projection.source


class WorkExecutionProjection:
    def __init__(self, reader, link, item, source=None, *, detail=None, clock, allow_empty=False, origin_only=False, recovery_only=False):
        self.reader, self.link, self.item, self.source, self.clock = reader, link, item, source, clock
        self.origin_only, self.recovery_only = origin_only, recovery_only
        if recovery_only and not origin_only:
            raise ProductSourceRetired()
        self.allow_empty = allow_empty
        self.expected_act = None
        self.detail = detail or detail_read(source.work["detail"])
        self.projection = None if origin_only else WorkDetailProjection(reader, link, self.detail.record, self.detail.request, reusable=self.detail.can_reuse_conversation, expected=self.detail)
        self.capacity = getattr(clock, "capacity", None)
        self.capacity_projection = None
        self.activation_parent = None
        self.command = None
        self.stop_refusal = None
        self.stop_requester = None
        if self.capacity is not None:
            from app.telegram_bots.work_capacity_reads import CapacityProjection

            self.capacity_projection = CapacityProjection(link, self.capacity.capacity, expected=self.capacity)

    async def prepare_source(self):
        from app.telegram_bots.work_continuation_sources import ActivationParentProjection

        async with self.reader.links.session_factory() as reader:
            act = await reader.get(TelegramActRow, self.item.payload["work_execution_intent"]["act_id"])
            if act is None:
                raise ProductSourceRetired()
            captured = _snapshot(act)
        if source_marker(captured) is not None:
            self.command = CommandStopProof(self.link, captured)
        if captured.kind == "stop":
            from app.telegram_bots.work_stop_refusals import StopRefusalProof, permission_refusal

            if permission_refusal(captured):
                self.stop_refusal = StopRefusalProof(captured)
                if self.command is None:
                    from app.telegram_bots.work_detail_cards import full_detail
                    from app.telegram_bots.work_status_control_origins import carrier_source, status_record

                    async with self.reader.links.session_factory() as reader:
                        callback, parent, _controls, _primary, _context = await execution_origin_rows(reader, captured)
                        original, _work_id, _attempt_id = displayed_stop(self.link, parent, callback)
                    carrier = carrier_source(original)
                    record = status_record(carrier) if carrier["key"].startswith("status.") else full_detail(carrier)["record"]
                    self.stop_requester = (record.get("attempt") or {}).get("requester_id")
                    if type(self.stop_requester) is not str:
                        raise ProductSourceRetired()
        if captured.kind == "activate":
            self.activation_parent = ActivationParentProjection(captured, for_effect=False)
            try:
                await self.activation_parent.prepare(self.reader.acts, self.link)
            except (ValueError, KeyError, TypeError):
                raise ProductSourceRetired() from None
        return self

    def with_item(self, item):
        from copy import copy

        current = copy(self)
        current.item = item
        return current

    def extend(self, query):
        result = self.projection.extend(query) if self.projection is not None else query
        self.delivery_column = len(result.column_descriptions)
        delivery, act, callback, parent, control, original, child, event, settings, requester = (
            aliased(TelegramOutboxRow),
            aliased(TelegramActRow),
            aliased(TelegramCallbackRow),
            aliased(TelegramOutboxRow),
            aliased(TelegramCallbackRow),
            aliased(AgentWorkAttemptRow),
            aliased(TelegramActRow),
            aliased(AgentWorkEventRow),
            aliased(TelegramPersonSettingsRow),
            aliased(UserRow),
        )
        operation = case((act.kind == "stop", act.body["attempt_id"].as_string()), else_=None)
        context_operation = act.continuation["activation_context"]["child_id"].as_string()
        supervisor = aliased(UserRow)
        result = (
            result.add_columns(delivery, act, callback, parent, control, original, child, event, settings, requester.email, supervisor.email)
            .join(delivery, delivery.id == self.item.id)
            .join(act, act.id == self.item.payload["work_execution_intent"]["act_id"])
            .outerjoin(callback, callback.id == self.item.payload["work_execution_intent"]["origin"].get("callback_id"))
            .outerjoin(parent, parent.id == callback.payload["anchor_outbox_id"].as_string())
            .outerjoin(control, control.group_id == callback.group_id)
            .outerjoin(
                original,
                and_(or_(context_operation.is_not(None), original.work_id == act.work_id), case((act.kind == "activate", original.activation_id == func.coalesce(context_operation, act.id)), else_=original.id == operation)),
            )
            .outerjoin(child, child.id == func.coalesce(act.result["stop_child_id"].as_string(), act.continuation["activation_context"]["child_id"].as_string()))
            .outerjoin(event, and_(or_(context_operation.is_not(None), and_(event.instance_id == act.instance_id, event.work_id == act.work_id)), event.operation_id == case((child.id.is_not(None), child.id), else_=act.id)))
            .join(settings, settings.owner_user_id == self.link.owner_user_id)
            .outerjoin(requester, requester.id == original.requester_id)
            .outerjoin(supervisor, supervisor.id == AgentInstanceRow.supervisor_id)
        )
        if self.activation_parent is not None:
            result = self.activation_parent.extend(result)
        if self.command is not None:
            result = self.command.extend(result)
        result = self.capacity_projection.extend(result) if self.capacity_projection is not None else result
        from app.telegram_bots.work_execution_readiness import RefusalClock

        self.readiness_column = None
        if isinstance(self.clock, RefusalClock):
            self.readiness_column = len(result.column_descriptions)
            result = result.add_columns(
                select(AgentWorkAttemptRow.id).where(or_(AgentWorkAttemptRow.activation_id == act.id, AgentWorkAttemptRow.activation_id == context_operation)).exists(),
                select(AgentWorkEventRow.id).where(or_(AgentWorkEventRow.operation_id == act.id, AgentWorkEventRow.operation_id == context_operation)).exists(),
            )
        return self.stop_refusal.extend(result) if self.stop_refusal is not None else result

    def check(self, rows):
        raise ProductSourceRetired()

    def check_with_permissions(self, rows, held):
        if self.projection is not None:
            self.projection.check_with_permissions(rows, held)
        detail = self.detail if self.origin_only else self.projection.source
        if self.capacity_projection is not None:
            self.capacity_projection.check(rows)
        delivery, journal, callback, parent, _control, original, child, event, settings, person, supervisor = rows[0][self.delivery_column : self.delivery_column + 11]
        if (
            _item(delivery) != self.item
            or (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.instance_id, delivery.work_id, delivery.request_id, delivery.event_id, delivery.class_)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, self.link.agent_ref, self.detail.record["id"], None, None, "reply")
            or settings.timezone != self.clock.settings.timezone
            or settings.locale != self.clock.settings.locale
        ):
            raise ProductSourceRetired()
        act = _snapshot(journal)
        self.act = act
        facts = act_facts(act)
        if self.expected_act is not None and not same_body(facts, self.expected_act):
            raise ProductSourceRetired()
        self.expected_act = facts
        controls = list({row[self.delivery_column + 4].id: row[self.delivery_column + 4] for row in rows if row[self.delivery_column + 4] is not None}.values())
        primary = None
        if self.activation_parent is not None:
            try:
                self.activation_parent.check(rows, journal, held)
            except (ValueError, KeyError, TypeError):
                raise ProductSourceRetired() from None
            primary_row = rows[0][self.activation_parent.column]
            primary = _snapshot(primary_row) if primary_row is not None else None
        elif act.kind == "activate":
            raise ProductSourceRetired()
        if self.command is not None:
            self.command.check(rows)
        origin = qualify_execution_origin(self.link, act, callback, parent, controls, activation_parent=primary, command=self.command)
        if self.item.dedupe_key != "notice:work-execution:" + act.id or not same_body(origin, self.item.payload["work_execution_intent"]["origin"]):
            raise ProductSourceRetired()
        if delivery.message_thread_id != (self.command.topic if self.command is not None else parent.message_thread_id):
            raise ProductSourceRetired()
        effective = act
        event_proof = None
        if child is not None:
            effective = _snapshot(child)
            try:
                if act.kind == "activate":
                    from app.telegram_bots.work_start_conversations import qualify_context_receipt, require_context_child

                    require_context_child(act, effective)
                    if act.state == "done":
                        qualify_context_receipt(act, effective, event, original, act.result["receipt"])
                        if act.result.get("activation_context_child_id") != effective.id:
                            raise ValueError()
                else:
                    validate_child(act, effective, parent_of(effective)["ordinal"])
                    if effective.id not in act.operation_ids or act.result.get("stop_child_id") != effective.id:
                        raise ValueError()
                if act.state == "done" and (effective.state != "done" or not same_body(act.result.get("receipt"), effective.result)):
                    raise ValueError()
            except (TypeError, ValueError, KeyError):
                raise ProductSourceRetired() from None
        elif act.kind == "stop" and act.operation_ids and act.state == "done":
            raise ProductSourceRetired()
        if original is not None:
            if act.kind == "activate" and ((original.requester_id, original.thread_id) != (effective.owner_user_id, effective.body["thread_id"]) or not same_body(original.request, effective.body)):
                raise ProductSourceRetired()
            if act.kind == "stop" and (original.id != act.body["attempt_id"] or act.stop_dispatch is not None and original.thread_id != act.stop_dispatch["thread_id"]):
                raise ProductSourceRetired()
            if act.kind == "stop" and effective.stop_dispatch is not None and effective.stop_dispatch.get("run_id") is not None and original.run_id != effective.stop_dispatch["run_id"]:
                raise ProductSourceRetired()
        if act.kind == "stop" and act.state == "done":
            event_proof = qualify_stop_receipt(event, effective)
        original_facts = _fields(original, _ATTEMPT)
        if act.kind == "activate" and act.operation_ids:
            from dataclasses import replace

            from app.telegram_bots.work_start_conversations import context_deadline, exact_runless_promise, qualify_child_operation

            if child is None:
                if self.allow_empty:
                    self.source = None
                    return
                raise ProductSourceRetired()
            attempts = {value.id: value for row in rows if (value := row[self.delivery_column + 5]) is not None}
            events = {value.id: value for row in rows if (value := row[self.delivery_column + 7]) is not None}
            if len(attempts) > 1 or len(events) > 1:
                raise ProductSourceRetired()
            try:
                qualify_child_operation(effective, event, original, allow_absent=True)
                promise = exact_runless_promise(effective, event, original, deadline=context_deadline(act))
            except ValueError:
                raise ProductSourceRetired() from None
            notice_act = replace(effective, kind="activate")
        else:
            notice_act = act
        notice = execution_notice(notice_act, original=original_facts, dispatch=effective.stop_dispatch)
        from app.telegram_bots.work_stop_refusals import permission_refusal

        if permission_refusal(act) and self.stop_refusal is None:
            raise ProductSourceRetired()
        if self.stop_refusal is not None:
            from app.telegram_bots.execution_notices import ExecutionNotice

            self.stop_refusal.check(rows, act)
            if original is None or original.status not in {"starting", "running"}:
                raise ProductSourceRetired()
            if self.stop_requester is not None and original.requester_id != self.stop_requester:
                raise ProductSourceRetired()
            if not {"agents:write", "runs:cancel"}.issubset(held):
                notice = ExecutionNotice("denied.other")
            elif not detail.permissions & 4 and original.requester_id != self.link.owner_user_id:
                if person is None:
                    raise ProductSourceRetired()
                notice = ExecutionNotice("stop.not_yours")
            else:
                notice = ExecutionNotice("tap.failed", preview=True)
        readiness = None
        from app.telegram_bots.work_execution_readiness import NO_EFFECT_REASONS

        if notice_act.kind == "activate" and notice is not None and notice.key in {"unavailable.fix", "denied.start", "tap.failed"} and effective.reason in NO_EFFECT_REASONS:
            from dataclasses import replace

            from app.telegram_bots.work_execution_readiness import RefusalClock, readiness_signature

            if not isinstance(self.clock, RefusalClock) or self.clock.signature != readiness_signature(act, effective) or any(rows[0][self.readiness_column : self.readiness_column + 2]):
                raise ProductSourceRetired()
            from app.telegram_bots.work_views import work_start_permissions

            if work_start_permissions(detail.record, can_reuse_conversation=detail.can_reuse_conversation).issubset(held):
                failure = self.clock.failure
                readiness = {"key": failure.text_key, "values": failure.values} if failure is not None else {"key": "ready", "values": {}}
                notice = replace(notice, key=failure.text_key if failure is not None else "tap.failed", preview=failure is None)
            else:
                notice = replace(notice, key="denied.start", preview=False)
        if notice is not None and act.kind == "activate" and act.operation_ids and self.clock.now() >= context_deadline(act) and not promise:
            from dataclasses import replace

            notice = replace(notice, retry=False)
        if notice is None:
            if self.allow_empty:
                if act.kind == "activate" and act.state == "done":
                    from app.telegram_bots.work_start_retention import closed_activation, closed_start

                    if act.operation_ids:
                        closed = child is not None and closed_activation(child, self.link, original, work_id=act.work_id)
                    else:
                        closed = closed_activation(journal, self.link, original, work_id=act.work_id) if primary is not None else closed_start(callback, journal, self.link, original)
                    if not closed:
                        raise ProductSourceRetired()
                self.source = None
                return
            raise ProductSourceRetired()
        if self.origin_only:
            expected = self.source.work
            proof = {"act": facts, "original": original_facts, "dispatch": effective.stop_dispatch, "effective": act_facts(effective), "origin": origin, "event_proof": event_proof, "notice": asdict(notice)}
            if not (notice.retry if self.recovery_only else notice.preview) or any(not same_body(expected.get(key), value) for key, value in proof.items()):
                raise ProductSourceRetired()
            return
        record = detail.record
        values = dict(
            name=rows[0][1].name,
            objective=record["objective"],
            person=person,
            supervisor=supervisor,
            time=format_time(aware(original.settled_at), self.clock.settings, now=self.clock.now(), zone=self.clock.zone) if original is not None and original.settled_at is not None else "",
        )
        if readiness is not None:
            user = rows[0][5]
            from deerflow.persistence.user.access import issuer_key

            limited = any(row[7] is not None and row[7].role != "admin" and (not user.oauth_issuer or row[7].issuer == issuer_key(user.oauth_issuer)) for row in rows) if user.oauth_id and user.oauth_provider else False
            values.update(custody=rows[0][1].custody, reader_is_administrator=user.system_role == "admin" and not limited, **readiness["values"])
        if notice.key == "busy.work_limit" and self.capacity is not None:
            from app.telegram_bots.render import render
            from app.telegram_bots.work_capacity_text import waiting_html

            values["capacity_html"] = waiting_html(
                self.capacity.capacity,
                objective=record["objective"],
                locale=settings.locale,
                people={self.capacity.capacity.running[0].requester_id: self.capacity.person} if self.capacity.capacity.running else {},
                time=lambda value: format_time(value, self.clock.settings, now=self.clock.now(), zone=self.clock.zone),
            ) or render("busy.place_free", settings.locale, objective=record["objective"])

        def date(value):
            return datetime.fromisoformat(value).astimezone(self.clock.zone).strftime("%Y-%m-%d %H:%M") if value else None

        data = thaw(
            dict(
                product=self.reader.product,
                wait_seconds=self.reader.acts.outbox.config.queue.max_wait_seconds,
                detail_dates={"date": date(record["created_at"]), "due": date(record["due_at"])},
                detail=asdict(detail),
                act=act_facts(act),
                original=original_facts,
                dispatch=effective.stop_dispatch,
                effective=act_facts(effective),
                origin=origin,
                event_proof=event_proof,
                timezone=settings.timezone,
                notice=asdict(notice),
                **({"readiness": readiness} if readiness is not None else {}),
            )
        )
        source = WorkNewsRead(act.work_id, "execution." + act.kind, values, detail.protected, data, digest([self.link.id, self.link.binding_generation, act.id, act.kind, thaw(act.body), origin]), digest([data, values]))
        if self.source is not None and not same_body(asdict(self.source), asdict(source)):
            raise ProductSourceRetired()
        self.source = source
