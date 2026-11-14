"""Real recovery gestures fence or retry only their original saved command."""

from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assignment_sources import private_snapshot
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
from app.telegram_bots.work_management_actions import same_binding
from app.telegram_bots.work_management_consent import ManagementConsent, ManagementModeWitness
from app.telegram_bots.work_management_origins import origin_for
from app.telegram_bots.work_view_origins import acknowledged_view
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def displayed_recovery(link, parent, control):
    original = acknowledged_view(link, parent, control.payload.get("presentation_version"))
    source = original.payload["work_news_basis"]["source"]
    data = source["work"]
    act = data.get("act")
    if (
        source["key"] not in {"management.edit", "management.withdraw"}
        or act is None
        or control.kind not in {"act_check", "cancel_wait"}
        or control.target_kind != "act"
        or (control.target_id, control.payload.get("act_id")) != (act["id"], act["id"])
        or not data["notice"]["cancel" if control.kind == "cancel_wait" else "retry"]
        or original.parts[-1].get("callback_group_id") != control.group_id
        or original.parts[-1].get("message_ids") != [control.message_id]
        or original.message_thread_id != control.message_thread_id
    ):
        raise CallbackRefused()
    return original, source


def recovery_for(item):
    value = item.payload.get("work_management_recovery")
    if value is not None and (type(value) is not dict or set(value) != {"version", "kind", "act_id", "origin"} or type(value["version"]) is not int or value["version"] != 1 or value["kind"] not in {"cancel", "retry"}):
        raise ProductSourceRetired()
    return value


class ManagementRecoveryProof:
    def __init__(self, link, item):
        self.link, self.item, self.value = link, item, recovery_for(item)

    def extend(self, query):
        if self.value is None:
            return query
        self.column = len(query.column_descriptions)
        control, member = aliased(TelegramCallbackRow), aliased(TelegramCallbackRow)
        return query.add_columns(control, member).join(control, control.id == self.value["origin"]["callback_id"]).outerjoin(member, member.group_id == control.group_id)

    def check(self, rows, parent, act):
        if self.value is None:
            return
        try:
            control = rows[0][self.column]
            members = list({row[self.column + 1].id: row[self.column + 1] for row in rows if row[self.column + 1] is not None}.values())
            original, source = displayed_recovery(self.link, parent, control)
            _validate_rows(original, members)
            if (
                act is None
                or self.value["act_id"] != act.id
                or source["work"]["act"]["id"] != act.id
                or origin_for(original, control) != self.value["origin"]
                or control.kind != ("cancel_wait" if self.value["kind"] == "cancel" else "act_check")
                or any(member.consumed_at is None or member.closed_reason != "work-" + self.value["kind"] for member in members)
            ):
                raise ValueError()
        except (ValueError, TypeError, KeyError, AttributeError, CallbackRefused):
            raise ProductSourceRetired() from None


class RecoveryAuthority:
    def __init__(self, act, *, retry):
        self.act, self.retry = act, retry
        self.consent, self.witness = ManagementConsent(act), ManagementModeWitness(act)

    def extend(self, query):
        self.column = len(query.column_descriptions)
        saved = aliased(TelegramActRow)
        return self.consent.extend(self.witness.extend(query.add_columns(saved).join(saved, saved.id == self.act.id)))

    def check_with_permissions(self, rows, held):
        saved = rows[0][self.column]
        if not same_binding(rows[0][0], self.act) or any(getattr(saved, key) != getattr(self.act, key) for key in _ACT_SCOPE) or not same_body(saved.body, self.act.body) or not same_body(saved.continuation, self.act.continuation):
            raise ProductSourceRetired()
        if self.retry and saved.state != "done" and (rows[0][2] & 6 != 6 or "agents:write" not in held):
            raise AgentDenied("Current management permission is required", reason="denied")
        if self.retry and saved.state != "done" and rows[0][0].acts_paused_since is not None:
            raise AgentDenied("Actions are temporarily paused", reason="actions-paused")
        self.witness.check(rows)
        self.consent.check(rows, allow_closed=True)


class WorkManagementRecovery:
    def __init__(self, plans):
        self.plans, self.acts, self.callbacks, self.outbox = plans, plans.acts, plans.callbacks, plans.outbox

    async def operate(self, link, callback_id, *, retry, chat_id, message_id, message_thread_id, guard):
        async def perform():
            await prepare_publication_guard(guard)
            async with self.outbox.sessions() as reader:
                control = await reader.get(TelegramCallbackRow, callback_id)
                parent = await reader.get(TelegramOutboxRow, control.payload.get("anchor_outbox_id")) if control else None
                if (
                    control is None
                    or parent is None
                    or (control.link_id, control.chat_id, control.binding_generation, control.message_id, control.message_thread_id) != (link.id, chat_id, link.binding_generation, message_id, message_thread_id)
                ):
                    raise CallbackRefused()
                original, source = displayed_recovery(link, parent, control)
                if control.kind != ("act_check" if retry else "cancel_wait"):
                    raise CallbackRefused()
                shown = private_snapshot(source["work"]["act"])
                expected = {key: source["work"]["act"][key] for key in ("id", *_ACT_SCOPE, "body", "attempts", "continuation", "stop_dispatch", "state")}
                origin = origin_for(original, control)
                identity = parent.id
            prepared = await prepare_attention(self.plans.reader, link)
            witness = {"version": 1, "kind": "retry" if retry else "cancel", "act_id": shown.id, "origin": origin}
            replay = False
            consumed_at = None

            async def admission(writer, result):
                nonlocal replay, consumed_at
                await guard.check(writer)
                native = await writer.get(TelegramActRow, shown.id, populate_existing=True)
                if native is None or not same_body(native.body, shown.body):
                    raise CallbackRefused()
                await prepared(session=writer, projection=RecoveryAuthority(_snapshot(native), retry=retry))
                current = await writer.get(TelegramCallbackRow, callback_id, with_for_update=True, populate_existing=True)
                parent = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
                actual, _source = displayed_recovery(link, parent, current)
                if origin_for(actual, current) != origin:
                    raise CallbackRefused()
                if current.consumed_at is not None:
                    members = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == current.group_id)))
                    _validate_rows(actual, members)
                    if not same_body(recovery_for(parent), witness) or any(member.consumed_at is None or aware(member.consumed_at) != aware(current.consumed_at) or member.closed_reason != "work-" + witness["kind"] for member in members):
                        raise CallbackRefused()
                    replay = True
                    consumed_at = current.consumed_at
                    return
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
                if retry or result is not None:
                    await self.callbacks.consume_group(group, reason="work-" + witness["kind"], session=writer)
                    consumed_at = (await writer.get(TelegramCallbackRow, callback_id, populate_existing=True)).consumed_at
                    parent.payload = {**parent.payload, "work_management_recovery": witness}
                    await writer.flush()
                guard.current()

            def current():
                guard.current()
                self.callbacks._unexpired(control)

            if retry:
                async with self.callbacks.writer(link, guard) as writer:
                    await admission(writer, None)
                    if not replay:
                        current()
                deadline = aware(consumed_at) + timedelta(hours=1)
                # Logical attempts advance their counter before calling Work.
                # A consumed physical Retry has no such pre-call witness and
                # therefore remains read-only after any interruption.
                resume = not replay or shown.state != "done" and self.outbox.clock() < deadline
                result = await self.acts.check(shown.id, expected=expected, attempt_current=lambda: _current(guard) and self.outbox.clock() < deadline) if resume else await self.acts._work_record(shown.id)
            else:
                result = await self.acts.cancel_management(shown.id, admission=admission, confirmation_current=current)
            await self.plans.observe(link, identity, guard=guard)
            return result

        return await await_drained(perform())


def _current(guard):
    try:
        guard.current()
        return True
    except Exception:
        return False
