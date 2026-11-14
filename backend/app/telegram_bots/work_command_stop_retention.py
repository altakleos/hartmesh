"""Retain complete typed-Stop families until their original promises settle."""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from sqlalchemy import select

from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_command_stop_origins import command_basis, operation_id, selected_targets, source_marker
from app.telegram_bots.work_execution_retention import closed_stop
from app.telegram_bots.work_management_edit_records import read_incoming, read_origin
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


def closed_marker(row):
    """Structural historical provenance only; this never grants live authority."""
    try:
        marker = source_marker(row)
        if marker is None:
            return False
        basis, target = marker["basis"], marker["target"]
        incoming = read_incoming(basis["input"])
        selection = basis["selection"]
        if type(selection) is not dict or selection.get("kind") not in {"command", "callback"} or set(selection) != {"kind", "choice"} | ({"origin"} if selection["kind"] == "callback" else set()):
            return False
        if selection["kind"] == "callback":
            read_origin(selection["origin"])
        elif len(basis["targets"]) != 1 or selection["choice"] != "attempt:" + target["attempt_id"]:
            return False
        return (
            set(basis) == {"version", "input", "owner_user_id", "instance_id", "answer", "targets", "selection"}
            and type(basis["version"]) is int
            and basis["version"] == 1
            and basis["answer"] is None
            and basis["instance_id"] == row.instance_id
            and basis["owner_user_id"] == row.owner_user_id
            and target in selected_targets(basis)
            and (row.link_id, row.chat_id, row.binding_generation) == (incoming["link_id"], incoming["chat_id"], incoming["binding_generation"])
            and row.id == operation_id(incoming, target)
            and (row.work_id, row.body.get("attempt_id")) == (target["work_id"], target["attempt_id"])
        )
    except (ProductSourceRetired, ValueError, TypeError, KeyError, AttributeError):
        return False


@dataclass
class CommandStopDependencies:
    inputs: set = field(default_factory=set)
    deliveries: set = field(default_factory=set)
    groups: set = field(default_factory=set)
    acts: set = field(default_factory=set)


def _identities(values):
    return {value for value in values if type(value) is str} if isinstance(values, (list, tuple)) else set()


def _scope(row, mark, basis, *, cleanup=False):
    return (row.link_id, row.binding_generation, row.chat_id, getattr(row, "message_thread_id", mark.message_thread_id)) == (mark.link_id, mark.binding_generation, mark.chat_id, mark.message_thread_id) and (
        not hasattr(row, "owner_user_id") or (row.owner_user_id, row.instance_id) == (basis["owner_user_id"], None if cleanup else basis["instance_id"])
    )


def _anchors(mark, basis, members, deliveries, controls):
    """Corrupt references hold their input, without adopting foreign UI."""
    from app.telegram_bots.work_result_identity import digest

    found = set()
    for row in deliveries.values():
        if type(row.payload) is not dict:
            continue
        cleanup = row.payload.get("kind") == "keyboard_cleanup"
        if not _scope(row, mark, basis, cleanup=cleanup):
            continue
        choice = row.payload.get("command_stop_intent")
        execution = row.payload.get("work_execution_intent")
        if type(choice) is dict and choice.get("message_id") == mark.message_id and choice.get("chat_id") == mark.chat_id and row.dedupe_key == "notice:command-stop:" + digest(basis["input"]):
            found.add(row.id)
        elif (
            type(execution) is dict
            and execution.get("act_id") in members
            and row.dedupe_key == "notice:work-execution:" + execution["act_id"]
            and type(execution.get("origin")) is dict
            and same_body(execution["origin"].get("input"), basis["input"])
        ):
            found.add(row.id)
    groups = {control.group_id: control for control in controls if _scope(control, mark, basis) and control.payload.get("anchor_outbox_id") in found}
    for row in deliveries.values():
        if type(row.payload) is not dict or row.payload.get("kind") != "keyboard_cleanup" or not _scope(row, mark, basis, cleanup=True):
            continue
        group = groups.get(row.dedupe_key.removeprefix("notice:spent-keyboard:"))
        if group is not None and row.payload.get("target_message_id") == group.message_id:
            found.add(row.id)
    return found


def _damaged_anchors(mark, basis, members, deliveries):
    """Independent original locators preserve damaged UI for repair only."""
    from app.telegram_bots.work_result_identity import digest

    locators = _identities(mark.delivery_ids)
    origin = (basis.get("selection") or {}).get("origin", {})
    if type(origin) is dict and type(origin.get("anchor_outbox_id")) is str:
        locators.add(origin["anchor_outbox_id"])
    execution = {"notice:work-execution:" + operation_id(basis["input"], target): target["work_id"] for target in basis["targets"] if operation_id(basis["input"], target) in members}
    return {
        row.id
        for row in deliveries.values()
        if row.id in locators and _scope(row, mark, basis) and (row.dedupe_key == "notice:command-stop:" + digest(basis["input"]) and row.work_id is None or row.dedupe_key in execution and row.work_id == execution[row.dedupe_key])
    }


def _pin_family(result, mark, basis, members, journals, deliveries, controls):
    anchors = _anchors(mark, basis, members, deliveries, controls) | _damaged_anchors(mark, basis, members, deliveries)
    result.deliveries.update(anchors)
    result.acts.update(members)
    for identity in members:
        result.acts.update(child for child in _identities(journals[identity].operation_ids) if child in journals and _scope(journals[child], mark, basis))
    result.groups.update(control.group_id for control in controls if _scope(control, mark, basis) and control.payload.get("anchor_outbox_id") in anchors)


def _pin_unknown(result, mark, journals, deliveries, controls):
    """Historical provenance can retain evidence, never complete an input."""
    for root in journals.values():
        if not closed_marker(root):
            continue
        basis = source_marker(root)["basis"]
        incoming = basis["input"]
        if any(getattr(mark, key) != incoming[key] for key in ("link_id", "message_id", "chat_id", "binding_generation", "message_thread_id", "update_id")) or not _scope(root, mark, basis):
            continue
        members = {root.id} | {child for child in _identities(root.operation_ids) if child in journals and _scope(journals[child], mark, basis)}
        _pin_family(result, mark, basis, members, journals, deliveries, controls)
    # An unselected choice has no journal yet. Its complete stored recipe is
    # another historical witness, usable for retention only.
    from app.telegram_bots.producer import _item
    from app.telegram_bots.work_news_delivery import frozen_news
    from app.telegram_bots.work_result_identity import digest

    for row in deliveries.values():
        if type(row.payload) is not dict or "command_stop_intent" not in row.payload or not row.parts:
            continue
        try:
            _frozen, source, _part = frozen_news(SimpleNamespace(link=SimpleNamespace(id=mark.link_id), item=_item(row), part=None))
            basis = source.work["basis"]
            incoming = read_incoming(basis["input"])
            if (
                source.key != "command.stop"
                or basis["selection"] is not None
                or not _scope(row, mark, basis)
                or any(getattr(mark, key) != incoming[key] for key in ("link_id", "message_id", "chat_id", "binding_generation", "message_thread_id", "update_id"))
            ):
                continue
            if row.dedupe_key != "notice:command-stop:" + digest(incoming) or not same_body(row.payload["command_stop_intent"], {"version": 1, "message_id": mark.message_id, "chat_id": mark.chat_id, "signature": digest(basis)}):
                continue
            _pin_family(result, mark, basis, set(), journals, deliveries, controls)
        except (ProductSourceRetired, ValueError, TypeError, KeyError, AttributeError):
            continue


async def command_stop_dependencies(session, now=None, *, max_age_days=180):
    now = aware(now or datetime.now(UTC))
    result = CommandStopDependencies()
    marks = list(await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.route_kind == "command_stop")))
    journals = {row.id: row for row in await session.scalars(select(TelegramActRow).where(TelegramActRow.kind == "stop", TelegramActRow.continuation["command_stop_source"].as_string().is_not(None)))}
    if not marks and not journals:
        return result
    referenced = set().union(*(_identities(mark.operation_ids) for mark in marks), *(_identities(row.operation_ids) for row in journals.values()))
    ordered = sorted(referenced - journals.keys())
    for start in range(0, len(ordered), 500):
        for row in await session.scalars(select(TelegramActRow).where(TelegramActRow.id.in_(ordered[start : start + 500]))):
            journals[row.id] = row
    roots, by_input = {}, {}
    for row in journals.values():
        try:
            marker = source_marker(row)
            if marker is not None:
                incoming = marker["basis"]["input"]
                key = incoming["link_id"], incoming["message_id"], incoming["chat_id"]
                by_input.setdefault(key, set()).add(row.id)
                roots[row.id] = key
        except (ProductSourceRetired, TypeError, KeyError, AttributeError):
            result.acts.add(row.id)
    links = {mark.link_id for mark in marks} | {row.link_id for row in journals.values()}
    deliveries = {row.id: row for row in await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id.in_(sorted(links))))}
    controls = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.link_id.in_(sorted(links)))))
    attempts, events = {}, {}
    target_ids = sorted({row.body["attempt_id"] for row in journals.values() if type(row.body) is dict and type(row.body.get("attempt_id")) is str})
    operation_ids = sorted(journals)
    for start in range(0, len(target_ids), 500):
        for row in await session.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.id.in_(target_ids[start : start + 500]))):
            attempts[row.id] = row
    for start in range(0, len(operation_ids), 500):
        for row in await session.scalars(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id.in_(operation_ids[start : start + 500]))):
            events[row.instance_id, row.operation_id] = row
    seen = set()
    for mark in marks:
        key = mark.link_id, mark.message_id, mark.chat_id
        seen.add(key)
        referenced_members = _identities(mark.operation_ids) | by_input.get(key, set())
        try:
            basis = command_basis(mark)
        except (ProductSourceRetired, ValueError, TypeError, KeyError, AttributeError):
            result.inputs.add(key)
            _pin_unknown(result, mark, journals, deliveries, controls)
            continue
        members = {identity for identity in referenced_members if identity in journals and _scope(journals[identity], mark, basis)}
        anchors = _anchors(mark, basis, members, deliveries, controls)
        damaged = _damaged_anchors(mark, basis, members, deliveries) - anchors
        closed = False
        live_controls = any(
            _scope(control, mark, basis)
            and control.payload.get("anchor_outbox_id") in anchors
            and control.consumed_at is None
            and now < aware(control.created_at) + timedelta(days=max_age_days)
            and (control.expires_at is None or now < aware(control.expires_at))
            for control in controls
        )
        try:
            basis = command_basis(mark)
            requested = {operation_id(basis["input"], target) for target in selected_targets(basis)}
            from app.telegram_bots.work_command_stop_lifetime import unselected_closed

            no_effect = not members and unselected_closed(mark, basis, now, max_age_days)
            closed = (
                (bool(requested) or no_effect)
                and requested == members
                and referenced_members == members
                and (no_effect or _identities(mark.delivery_ids) <= anchors)
                and not damaged
                and mark.state == "done"
                and mark.route_complete
                and not live_controls
                and all(identity in journals and closed_marker(journals[identity]) and same_body(source_marker(journals[identity])["basis"], basis) and closed_stop(journals[identity], journals, attempts, events) for identity in requested)
                and all(identity in deliveries and deliveries[identity].state in {"sent", "skipped"} for identity in anchors)
            )
        except (ProductSourceRetired, ValueError, TypeError, KeyError, AttributeError):
            pass
        if not closed:
            result.inputs.add(key)
            _pin_family(result, mark, basis, members, journals, deliveries, controls)
    for identity, key in roots.items():
        if key not in seen and (not closed_marker(journals[identity]) or not closed_stop(journals[identity], journals, attempts, events)):
            result.acts.add(identity)
            root = journals[identity]
            basis = source_marker(root)["basis"]
            mark = SimpleNamespace(**basis["input"], delivery_ids=[])
            if (root.link_id, root.binding_generation, root.chat_id, root.instance_id, root.owner_user_id) != (mark.link_id, mark.binding_generation, mark.chat_id, basis["instance_id"], basis["owner_user_id"]):
                continue
            _pin_family(result, mark, basis, {identity}, journals, deliveries, controls)
    return result
