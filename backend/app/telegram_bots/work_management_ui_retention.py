"""Read-only custody edges for saved-work previews and their exact journals."""

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta

from sqlalchemy import select

from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_management_retention import closed_command, closed_edit, discarded_root, historical, private_root
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow


@dataclass
class ManagementDependencies:
    groups: set = field(default_factory=set)
    deliveries: set = field(default_factory=set)
    acts: set = field(default_factory=set)


def mapping(value):
    return value if isinstance(value, Mapping) else {}


def identities(*values):
    return {value for value in values if type(value) is str}


def sources(row):
    basis = mapping(mapping(row.payload).get("work_news_basis"))
    yield mapping(basis.get("source"))
    for old in basis.get("history", []) if isinstance(basis.get("history"), list) else []:
        yield mapping(mapping(mapping(old).get("basis")).get("source"))


def openings(row, gestures):
    yield mapping(mapping(mapping(row.payload).get("work_management_intent")).get("origin"))
    for source in sources(row):
        yield mapping(mapping(mapping(source.get("work")).get("intent")).get("origin"))
    for gesture in gestures:
        yield mapping(gesture.payload)


def family_closed(rows, events, attempts, now, ttl):
    try:
        values = [historical(row) for row in rows]
        roots = [row for row in values if "edit_draft_source" in mapping(row.continuation)]
        if len(values) == 2 and len(roots) == 1:
            command = next(row for row in values if row.id != roots[0].id)
            return command.kind == "edit" and closed_edit(roots[0], command, events, attempts)
        if len(values) == 1 and not roots:
            return values[0].kind == "cancel" and closed_command(values[0], events, attempts)
        if len(values) == 1:
            root = roots[0]
            private_root(root)
            return (
                not root.operation_ids
                and set(root.continuation) == {"edit_draft_source"}
                and events.get((root.instance_id, root.id)) is None
                and (discarded_root(root) or root.result is None and (root.state == "draft" or root.state == "refused" and root.reason == "draft-expired") and aware(now) >= aware(root.created_at) + timedelta(hours=ttl))
            )
        return False
    except (ValueError, TypeError, KeyError, AttributeError, ProductSourceRetired):
        return False


async def management_dependencies(session, now, *, max_age_days=180, draft_ttl_hours=24):
    """Missing or conflicting custody holds a complete family without renewing it.

    This reader takes no locks: callback/outbox sweepers already own their rows.
    The Act sweeper invokes it only after taking the admission Link -> Act locks.
    Historical locators are used solely for garbage collection, never dispatch.
    """
    result = ManagementDependencies()
    journals = {row.id: row for row in await session.scalars(select(TelegramActRow).where(TelegramActRow.kind.in_(("edit", "cancel"))))}
    controls = list(await session.scalars(select(TelegramCallbackRow)))
    native_anchors = {anchor for row in journals.values() if type(anchor := mapping(mapping(row.continuation).get("management_origin")).get("anchor_outbox_id")) is str}
    native_anchors.update(control.target_id for control in controls if control.kind == "work_management_gesture" and control.target_kind == "outbox")
    native_anchors.update(mapping(control.payload).get("anchor_outbox_id") for control in controls if control.kind in {"work_change_save", "work_withdraw_confirm"})
    parents = {}
    for row in await session.scalars(select(TelegramOutboxRow)):
        if row.id in native_anchors or "work_management_intent" in mapping(row.payload) or any(source.get("key") in {"management.edit", "management.withdraw"} for source in sources(row)):
            parents[row.id] = row
    if not parents:
        return result
    links = {row.id: row for row in await session.scalars(select(TelegramBotLinkRow))}
    by_anchor, by_group, gestures = defaultdict(list), defaultdict(list), defaultdict(list)
    for control in controls:
        anchor = mapping(control.payload).get("anchor_outbox_id")
        if type(anchor) is str:
            by_anchor[anchor].append(control)
        by_group[control.group_id].append(control)
        if control.kind == "work_management_gesture" and control.target_kind == "outbox":
            gestures[control.target_id].append(control)
    graph, parent_members = defaultdict(set), {}

    def connect(left, right):
        graph[left].add(right)
        graph[right].add(left)

    for identity, row in journals.items():
        graph[("act", identity)]
        ref = mapping(mapping(row.continuation).get("management_source")).get("draft_ref")
        members = identities(mapping(ref).get("id"))
        if isinstance(row.operation_ids, list):
            members.update(value for value in row.operation_ids if type(value) is str)
        for member in members:
            connect(("act", identity), ("act", member))
    for identity, parent in parents.items():
        payload = mapping(parent.payload)
        intent = mapping(payload.get("work_management_intent"))
        members = identities(intent.get("draft_id"), intent.get("act_id"))
        for source in sources(parent):
            data = mapping(source.get("work"))
            members.update(identities(data.get("draft_id")))
            for key in ("root", "act"):
                frozen = mapping(data.get(key))
                members.update(identities(frozen.get("id")))
                if isinstance(frozen.get("operation_ids"), list):
                    members.update(value for value in frozen["operation_ids"] if type(value) is str)
        for control in by_anchor[identity]:
            members.update(identities(mapping(control.payload).get("work_action_id")))
            if control.target_kind == "draft" and control.kind in {"work_change_save", "assign_edit", "assign_discard"}:
                members.update(identities(control.target_id))
        parent_members[identity] = members
        graph[("outbox", identity)]
        for member in members:
            connect(("outbox", identity), ("act", member))
        for origin in openings(parent, gestures[identity]):
            for control in controls:
                if control.id == origin.get("gesture_id") and control.kind == "work_management_gesture" and control.target_kind == "outbox":
                    connect(("outbox", identity), ("outbox", control.target_id))
    # A native origin also retains its owner after a damaged outer reservation.
    for identity, row in journals.items():
        anchor = mapping(mapping(row.continuation).get("management_origin")).get("anchor_outbox_id")
        if type(anchor) is str:
            connect(("act", identity), ("outbox", anchor))
    # A foreign journal can keep this native command unresolved. Preserve the
    # command's evidence too, rather than letting the two sweepers split it.
    for row in await session.scalars(select(TelegramActRow).where(TelegramActRow.kind.not_in(("edit", "cancel")))):
        references = {value for value in row.operation_ids if type(value) is str} if isinstance(row.operation_ids, list) else set()
        continuation = mapping(row.continuation)
        for key, fields in (("stop_retry", ("parent_id",)), ("delegate_source", ("parent_id", "draft_id")), ("activation_continuation", ("child_id",))):
            references.update(mapping(continuation.get(key)).get(field) for field in fields if type(mapping(continuation.get(key)).get(field)) is str)
        for member in references:
            if ("act", member) in graph:
                connect(("act", row.id), ("act", member))
    events, attempts = {}, {}
    ordered = sorted(identity for kind, identity in graph if kind == "act")
    for start in range(0, len(ordered), 500):
        for event in await session.scalars(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id.in_(ordered[start : start + 500]))):
            events[event.instance_id, event.operation_id] = event
    targets = sorted({scope["attempt_id"] for row in journals.values() if type(scope := row.stop_dispatch) is dict and type(scope.get("attempt_id")) is str})
    for start in range(0, len(targets), 500):
        for attempt in await session.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.id.in_(targets[start : start + 500]))):
            attempts[attempt.id] = attempt
    seen = set()
    for first in list(graph):
        if first in seen:
            continue
        component, pending = set(), [first]
        while pending:
            node = pending.pop()
            if node in component:
                continue
            component.add(node)
            pending.extend(graph[node] - component)
        seen.update(component)
        owners = [parents[identity] for kind, identity in component if kind == "outbox" and identity in parents]
        if not owners:
            continue
        members = {identity for kind, identity in component if kind == "act"}
        closed = members.issubset(journals) and (family_closed([journals[identity] for identity in members], events, attempts, now, draft_ttl_hours) if members else True)
        held = not closed or len(owners) != 1
        owned = {control.id: control for parent in owners for control in by_anchor[parent.id]}
        for parent in owners:
            payload = mapping(parent.payload)
            intent = mapping(payload.get("work_management_intent"))
            # An unadmitted withdrawal may expire, but a damaged/missing intent
            # or a conflicting cross-parent reservation cannot establish closure.
            from app.telegram_bots.work_management_sources import management_intent

            try:
                management_intent(parent)
                expected_members = identities(intent["draft_id"], intent["act_id"])
                if expected_members != parent_members[parent.id]:
                    held = True
                for identity in members & journals.keys():
                    journal = journals[identity]
                    if "management_source" not in mapping(journal.continuation):
                        continue
                    from app.telegram_bots.work_management_consent import management_origin

                    if management_origin(historical(journal))["anchor_outbox_id"] != parent.id or any(
                        getattr(journal, key) != getattr(parent, key) for key in ("link_id", "owner_user_id", "binding_generation", "chat_id", "instance_id", "work_id")
                    ):
                        held = True
            except (ValueError, TypeError, KeyError, AttributeError, ProductSourceRetired):
                held = True
            held |= parent.state in {"pending", "sending", "failed"}
            current = links.get(parent.link_id)
            current_scope = current is not None and current.binding_generation == parent.binding_generation
            held |= current_scope and any(
                control.consumed_at is None and aware(now) < aware(control.created_at) + timedelta(days=max_age_days) and (control.expires_at is None or aware(now) < aware(control.expires_at)) for control in by_anchor[parent.id]
            )
            # The opening ledger and original full Work preview are distinct
            # evidence from the later Save/Confirm preview.
            for origin in openings(parent, gestures[parent.id]):
                for anchor in identities(origin.get("anchor_outbox_id")):
                    for control in by_anchor[anchor]:
                        owned[control.id] = control
                for control in controls:
                    if control.id in identities(origin.get("gesture_id"), origin.get("callback_id")):
                        owned[control.id] = control
            owned.update((control.id, control) for control in gestures[parent.id])
            for part in parent.parts if isinstance(parent.parts, list) else []:
                for control in by_group[mapping(part).get("callback_group_id")]:
                    owned[control.id] = control
        if held:
            result.acts.update(members)
            result.deliveries.update(identity for kind, identity in component if kind == "outbox")
            for parent in owners:
                for origin in openings(parent, gestures[parent.id]):
                    result.deliveries.update(identities(origin.get("anchor_outbox_id")))
            result.groups.update(control.group_id for control in owned.values())
    return result
