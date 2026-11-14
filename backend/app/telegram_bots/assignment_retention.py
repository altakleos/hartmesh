"""Retain whole assignment families while any exact custody remains unresolved."""

from collections import defaultdict, deque
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import timedelta
from hashlib import sha256
from types import SimpleNamespace

from sqlalchemy import delete, select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assignment_drafts import draft_scope, snapshot_draft
from app.telegram_bots.assignment_journals import exact_delegation, saved_receipt
from app.telegram_bots.assignment_sources import private_snapshot
from app.telegram_bots.links import BotLink
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_delegate_actions import delegate_source, delegation_children, private_draft
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow


@dataclass
class AssignmentDependencies:
    groups: set = field(default_factory=set)
    deliveries: set = field(default_factory=set)
    acts: set = field(default_factory=set)
    inputs: set = field(default_factory=set)
    expired_drafts: set = field(default_factory=set)
    expired_history: set = field(default_factory=set)


@dataclass
class Family:
    root: object
    scope: object
    members: set
    deliveries: list
    controls: list
    inputs: set
    closed: bool
    hold: bool
    actual: bool


def raw_source(row):
    value = row.continuation
    source = value.get("delegate_source") if isinstance(value, Mapping) else None
    return source if isinstance(source, Mapping) else {}


def retained_scope(root):
    """Read-only historical identity, never a live provider or SDK authority."""
    body = root.body if isinstance(root.body, Mapping) else {}
    origin = body.get("origin", {})
    identity = root.link_id or (origin.get("link_id") if isinstance(origin, Mapping) else None)
    if type(identity) is not str:
        return None
    return SimpleNamespace(id=identity, owner_user_id=root.owner_user_id, bot_id=root.bot_id, binding_generation=root.binding_generation, telegram_user_id=root.telegram_user_id, chat_id=root.chat_id, agent_ref=root.instance_id)


def historical(row, scope):
    # SET NULL on link deletion must not erase the original signature's locator.
    value = _snapshot(row)
    return replace(value, link_id=scope.id) if value.link_id is None else value


def in_scope(row, scope):
    return (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.instance_id) == (scope.id, scope.owner_user_id, scope.binding_generation, scope.chat_id, scope.agent_ref)


def valid_root(root, scope):
    try:
        origin = root.body["origin"]
        if not isinstance(origin, Mapping) or (origin.get("link_id"), origin.get("chat_id"), origin.get("binding_generation")) != (scope.id, scope.chat_id, scope.binding_generation):
            return False
        if root.body["version"] == 1:
            snapshot_draft(scope, root)
        else:
            from app.telegram_bots.assignment_requests import request_snapshot

            value = replace(root, state="draft") if root.state == "refused" and root.reason == "draft-expired" else root
            request_snapshot(scope, value)
        return True
    except (ValueError, TypeError, KeyError, AttributeError, ProductSourceRetired):
        return False


def closed_family(scope, root, parents, journals, reverse, forward, events, attempts):
    try:
        if len(parents) != 1 or list(root.operation_ids) != [parents[0].id]:
            return False
        parent = parents[0]
        children = set(delegation_children(parent))
        if children != reverse[parent.id] or any(forward[identity] != {parent.id} for identity in children):
            return False
        if root.stop_dispatch is not None or parent.stop_dispatch is not None:
            return False
        source = delegate_source(parent)
        continuation = parent.continuation.get("activation_continuation")
        activation_id = sha256((parent.id + ":activate").encode()).hexdigest()[:32]
        if source["start"] and continuation is None and (parent.state == "done" or activation_id in attempts):
            return False
        if continuation is not None:
            from app.telegram_bots.work_start_retention import closed_activation

            if (
                not isinstance(continuation, Mapping)
                or type(continuation.get("version")) is not int
                or continuation["version"] != 1
                or continuation.get("child_id") != activation_id
                or continuation.get("phase") not in {"done", "refused", "cancelled", "expired"}
            ):
                return False
            activation, attempt = journals.get(activation_id), attempts.get(activation_id)
            if activation is not None:
                from app.telegram_bots.work_continuation_state import require_child

                require_child(parent, activation)
                if not closed_activation(activation, scope, attempt, work_id=activation.work_id):
                    return False
            elif continuation.get("child_body") is not None:
                return False
            elif attempt is not None:
                return False
            elif continuation["phase"] not in {"expired", "cancelled"}:
                from app.telegram_bots.work_continuation_state import continuation_state

                ended = continuation_state(parent)
                if ended["phase"] != "refused" or ended["child_body"] is not None or "refusal" not in ended:
                    return False
        delegated = delegation_children(parent)
        child = journals.get(delegated[0]) if len(delegated) == 1 else None
        if child is not None and child.stop_dispatch is not None:
            return False
        effective = exact_delegation(scope, root, parent, child)
        original_event = events.get((parent.instance_id, parent.id))
        if child is not None and original_event is not None:
            return False
        event = events.get((effective.instance_id, effective.id))
        receipt = saved_receipt(scope, effective, event)
        if parent.state == "done" and receipt is not None:
            if continuation is not None and (continuation.get("work_id"), continuation.get("assignment_revision")) != (receipt["id"], receipt["assignment_revision"]):
                return False
            return child is None or same_body(parent.result, {"delegate_child_id": child.id, "receipt": child.result})
        return event is None and parent.state in {"refused", "conflict"} and effective.state in {"refused", "conflict"} and effective.reason not in {"cancel-unconfirmed", "invalid", "missing"}
    except (ValueError, TypeError, KeyError, AttributeError, ProductSourceRetired):
        return False


def frozen_roots(delivery):
    body = delivery.payload.get("work_news_basis", {}) if isinstance(delivery.payload, Mapping) else {}
    source = body.get("source", {}) if isinstance(body, Mapping) else {}
    work = source.get("work", {}) if isinstance(source, Mapping) else {}
    value = work.get("root") if isinstance(work, Mapping) else None
    return value if isinstance(value, Mapping) else None


def pin_only_root(identity, delivery, frozen):
    """Keep custody locators without inventing a valid or expirable draft."""
    value = frozen if isinstance(frozen, Mapping) else {}
    return SimpleNamespace(
        id=identity,
        link_id=delivery.link_id,
        owner_user_id=delivery.owner_user_id,
        bot_id=value.get("bot_id"),
        binding_generation=delivery.binding_generation,
        telegram_user_id=value.get("telegram_user_id"),
        chat_id=delivery.chat_id,
        instance_id=delivery.instance_id,
        body=value.get("body", {}),
        operation_ids=value.get("operation_ids", []) if isinstance(value.get("operation_ids", []), list) else [],
        state="unknown",
        created_at=None,
    )


async def assignment_dependencies(session, now, *, max_age_days, draft_ttl_hours=24):
    now, result = aware(now), AssignmentDependencies()
    links = {row.id: BotLink.from_row(row) for row in await session.scalars(select(TelegramBotLinkRow).execution_options(populate_existing=True))}
    journals = {row.id: row for row in await session.scalars(select(TelegramActRow).where(TelegramActRow.kind == "delegate").execution_options(populate_existing=True))}
    card_rows = list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string().is_not(None)).execution_options(populate_existing=True)))
    root_ids = {
        row.id
        for row in journals.values()
        if private_draft(_snapshot(row)) or row.attempts == 0 and row.first_attempt_at is None and isinstance(row.body, Mapping) and "operation_id" not in row.body and ("draft" in row.body or "request" in row.body)
    }
    by_root, parent_refs, child_refs, by_anchor, by_target = (defaultdict(list) for _ in range(5))
    reverse, forward = defaultdict(set), defaultdict(set)
    anchors, anchor_roots = set(), defaultdict(set)
    for row in journals.values():
        source = raw_source(row)
        identity = source.get("draft_id")
        if type(identity) is str:
            root_ids.add(identity)
            (parent_refs if source.get("parent_id") is None else child_refs)[identity].append(row.id)
        parent_id = source.get("parent_id")
        if type(parent_id) is str:
            reverse[parent_id].add(row.id)
        if row.id not in root_ids:
            for child in row.operation_ids:
                if type(child) is str:
                    forward[child].add(row.id)
        origin = row.continuation.get("delegate_origin") if isinstance(row.continuation, Mapping) else None
        if isinstance(origin, Mapping) and type(origin.get("anchor_outbox_id")) is str:
            anchors.add(origin["anchor_outbox_id"])
            if type(identity) is str:
                anchor_roots[origin["anchor_outbox_id"]].add(identity)
    present = {row.id for row in card_rows}
    ordered = sorted(anchors - present)
    for start in range(0, len(ordered), 500):
        card_rows.extend(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.id.in_(ordered[start : start + 500])).execution_options(populate_existing=True)))
    for row in card_rows:
        intent = row.payload.get("assignment_intent", {}) if isinstance(row.payload, Mapping) else {}
        frozen = frozen_roots(row)
        identities = [intent.get("draft_id") if isinstance(intent, Mapping) else None, frozen.get("id") if frozen else None, *anchor_roots[row.id]]
        for identity in set(value for value in identities if type(value) is str):
            if type(identity) is str:
                root_ids.add(identity)
                by_root[identity].append(row)
    if not root_ids:
        return result
    controls = list(await session.scalars(select(TelegramCallbackRow).execution_options(populate_existing=True)))
    by_group = defaultdict(list)
    for control in controls:
        if isinstance(control.payload, Mapping):
            by_anchor[control.payload.get("anchor_outbox_id")].append(control)
        by_group[control.group_id].append(control)
        if control.target_kind == "outbox":
            by_target[control.target_id].append(control)
    continuation_ids = {value.get("child_id") for row in journals.values() if isinstance(row.continuation, Mapping) and isinstance(value := row.continuation.get("activation_continuation"), Mapping) and type(value.get("child_id")) is str}
    ordered = sorted(continuation_ids - journals.keys())
    for start in range(0, len(ordered), 500):
        for row in await session.scalars(select(TelegramActRow).where(TelegramActRow.id.in_(ordered[start : start + 500])).execution_options(populate_existing=True)):
            journals[row.id] = row
    events, attempts = {}, {}
    ordered = sorted(journals)
    for start in range(0, len(ordered), 500):
        for row in await session.scalars(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id.in_(ordered[start : start + 500]))):
            events[row.instance_id, row.operation_id] = row
    ordered = sorted(set(journals) | continuation_ids | {sha256((identity + ":activate").encode()).hexdigest()[:32] for ids in parent_refs.values() for identity in ids})
    for start in range(0, len(ordered), 500):
        for row in await session.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id.in_(ordered[start : start + 500]))):
            attempts[row.activation_id] = row
    from app.telegram_bots.work_execution_retention import execution_dependencies
    from app.telegram_bots.work_start_retention import reserved_starts

    held_groups, held_anchors = await reserved_starts(session)
    execution_groups, execution_anchors, held_execution_acts = await execution_dependencies(session)
    held_groups.update(execution_groups)
    held_anchors.update(execution_anchors)
    # A separately pending child outcome owns its whole original family. A raw
    # foreign-generation/target reference cannot inherit that custody.
    ordered_anchors = sorted(execution_anchors)
    for start in range(0, len(ordered_anchors), 500):
        for delivery in await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.id.in_(ordered_anchors[start : start + 500]))):
            payload = delivery.payload if isinstance(delivery.payload, Mapping) else {}
            intent = payload.get("work_execution_intent", payload.get("capacity_offer_intent"))
            child = journals.get(intent.get("act_id")) if isinstance(intent, Mapping) else None
            if (
                child is not None
                and child.id in held_execution_acts
                and (child.link_id, child.owner_user_id, child.binding_generation, child.chat_id, child.instance_id, child.work_id)
                == (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.instance_id, delivery.work_id)
            ):
                result.acts.add(child.id)
    from app.telegram_bots.assignment_answer_retention import answer_dependencies

    answer_groups, answer_anchors = await answer_dependencies(session, controls, links, now, max_age_days=max_age_days)
    held_groups.update(answer_groups)
    held_anchors.update(answer_anchors)
    result.groups.update(answer_groups)
    result.deliveries.update(answer_anchors)
    families = []
    for identity in sorted(root_ids):
        raw = journals.get(identity)
        actual = raw is not None
        root = _snapshot(raw) if actual else None
        if root is None:
            for delivery in by_root[identity]:
                frozen = frozen_roots(delivery)
                try:
                    candidate = private_snapshot(frozen) if frozen is not None else None
                    if candidate is not None and candidate.id == identity:
                        root = candidate
                        break
                except (ValueError, KeyError, TypeError):
                    continue
            if root is None and by_root[identity]:
                delivery = by_root[identity][0]
                root = pin_only_root(identity, delivery, frozen_roots(delivery))
        member_ids = {identity, *parent_refs[identity], *child_refs[identity]}
        if root is not None:
            member_ids.update(member for member in root.operation_ids if type(member) is str)
            original_scope = retained_scope(root)
            for parent_id in parent_refs[identity]:
                parent = journals.get(parent_id)
                continuation = parent.continuation.get("activation_continuation") if parent is not None and isinstance(parent.continuation, Mapping) else None
                child_id = continuation.get("child_id") if isinstance(continuation, Mapping) else None
                child = journals.get(child_id) if type(child_id) is str else None
                if child is not None and original_scope is not None and (child.link_id is None or in_scope(child, original_scope)):
                    member_ids.add(child.id)
        pending_members = list(member_ids)
        visited = set()
        while pending_members:
            member = pending_members.pop()
            if member in visited:
                continue
            visited.add(member)
            children = set(reverse[member])
            journal = journals.get(member)
            if journal is not None:
                children.update(child for child in journal.operation_ids if type(child) is str)
            pending_members.extend(children - member_ids)
            member_ids.update(children)
        if root is None:
            result.acts.update(member_ids)
            continue
        scope = retained_scope(root)
        if scope is None:
            result.acts.update(member_ids)
            continue
        relevant = set(member_ids)
        for parent_id in parent_refs[identity]:
            parent = journals.get(parent_id)
            continuation = parent.continuation.get("activation_continuation") if parent is not None and isinstance(parent.continuation, Mapping) else None
            if isinstance(continuation, Mapping) and type(continuation.get("child_id")) is str:
                relevant.add(continuation["child_id"])
        normalized = {key: historical(journals[key], scope) for key in relevant if key in journals and (journals[key].link_id is None or in_scope(journals[key], scope))}
        if actual:
            root = normalized.get(identity, root)
        parents = [normalized[key] for key in parent_refs[identity] if key in normalized]
        deliveries = [row for row in by_root[identity] if in_scope(row, scope)]
        current = links.get(scope.id)
        current_scope = current is not None and actual and raw.link_id is not None and all(getattr(raw, key) == value for key, value in draft_scope(current).items())
        owned = {
            control.id: control
            for row in deliveries
            for control in [*by_anchor[row.id], *by_target[row.id], *[control for part in (row.parts if isinstance(row.parts, list) else []) if isinstance(part, Mapping) for control in by_group[part.get("callback_group_id")]]]
            if (control.link_id, control.chat_id, control.binding_generation, control.message_thread_id) == (scope.id, scope.chat_id, scope.binding_generation, row.message_thread_id)
        }
        live = current_scope and any(control.consumed_at is None and now < aware(control.created_at) + timedelta(days=max_age_days) and (control.expires_at is None or now < aware(control.expires_at)) for control in owned.values())
        pending = any(row.state in {"pending", "sending"} for row in deliveries)
        other_held = any(control.group_id in held_groups for control in owned.values()) or any(row.id in held_anchors for row in deliveries)
        valid = actual and valid_root(root, scope)
        unadmitted = (
            valid
            and not parents
            and not child_refs[identity]
            and not root.operation_ids
            and root.continuation is None
            and root.stop_dispatch is None
            and root.cancel_requested_at is None
            and not any(not isinstance(control.payload, Mapping) or control.payload.get("work_action_id") is not None for control in owned.values())
        )
        closed = valid and (closed_family(scope, root, parents, normalized, reverse, forward, events, attempts) if parents else unadmitted)
        old_draft = root.created_at is not None and now >= aware(root.created_at) + timedelta(hours=draft_ttl_hours)
        if unadmitted and root.state == "draft" and old_draft:
            result.expired_drafts.add(identity)
        inputs = set()
        bodies = [root.body, *[value["body"] for row in deliveries if (value := frozen_roots(row)) is not None and isinstance(value.get("body"), Mapping)]]
        for body in bodies:
            if not isinstance(body, Mapping):
                continue
            for key in ("origin", "criteria_origin"):
                origin = body.get(key)
                if isinstance(origin, Mapping) and (origin.get("link_id"), origin.get("chat_id"), origin.get("binding_generation")) == (scope.id, scope.chat_id, scope.binding_generation) and type(origin.get("message_id")) is int:
                    inputs.add((scope.id, origin["message_id"], scope.chat_id))
        # Proposal cards seal every input in the original human turn. The Root
        # names only the first one; raw same-scope locators pin custody here,
        # without becoming authorization or a replacement for a missing row.
        for delivery in deliveries:
            basis = delivery.payload.get("reply_basis", {})
            receipt = basis.get("inputs", {}) if isinstance(basis, Mapping) else {}
            originals = receipt.get("inputs", []) if isinstance(receipt, Mapping) else []
            if not isinstance(basis, Mapping) or basis.get("version") != 9 or not isinstance(originals, list):
                continue
            for origin in originals:
                if isinstance(origin, Mapping) and (origin.get("link_id"), origin.get("chat_id"), origin.get("binding_generation")) == (scope.id, scope.chat_id, scope.binding_generation) and type(origin.get("message_id")) is int:
                    inputs.add((scope.id, origin["message_id"], scope.chat_id))
        hold = not closed or live or pending or other_held or root.state == "draft" and not old_draft
        families.append(Family(root, scope, member_ids, deliveries, list(owned.values()), inputs, closed, hold, actual))
    owners = defaultdict(set)
    for ordinal, family in enumerate(families):
        for member in family.members:
            owners[member].add(ordinal)
    queue = deque(index for index, family in enumerate(families) if family.hold or family.members & result.acts)
    # A shared or conflicting child retains both complete original families.
    seen = set()
    while queue:
        ordinal = queue.popleft()
        if ordinal in seen:
            continue
        seen.add(ordinal)
        families[ordinal].hold = True
        for member in families[ordinal].members:
            for neighbour in owners[member] - seen:
                queue.append(neighbour)
    for family in families:
        if family.hold:
            result.acts.update(family.members)
            result.deliveries.update(row.id for row in family.deliveries)
            result.groups.update(control.group_id for control in family.controls)
            result.inputs.update(family.inputs)
        elif family.actual and family.closed and all(member in journals and aware(journals[member].created_at) < now - timedelta(days=400) for member in family.members):
            result.expired_history.update(family.members)
    result.expired_history.difference_update(result.acts)
    return result


async def expire_assignments(session, now, *, max_age_days, draft_ttl_hours):
    # Lock in the same Link -> Act order as admissions before deriving closure.
    # A retry or newly attached child cannot invalidate the deletion snapshot.
    candidates = list(await session.execute(select(TelegramActRow.id, TelegramActRow.link_id).where(TelegramActRow.kind == "delegate")))
    ordered_links = sorted({link_id for _, link_id in candidates if link_id is not None})
    for start in range(0, len(ordered_links), 500):
        list(await session.scalars(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id.in_(ordered_links[start : start + 500])).order_by(TelegramBotLinkRow.id).with_for_update().execution_options(populate_existing=True)))
    ordered_acts = sorted(identity for identity, _ in candidates)
    for start in range(0, len(ordered_acts), 500):
        list(await session.scalars(select(TelegramActRow).where(TelegramActRow.id.in_(ordered_acts[start : start + 500])).order_by(TelegramActRow.id).with_for_update().execution_options(populate_existing=True)))
    dependencies = await assignment_dependencies(session, now, max_age_days=max_age_days, draft_ttl_hours=draft_ttl_hours)
    for identity in sorted(dependencies.expired_drafts):
        root = await session.get(TelegramActRow, identity, with_for_update=True, populate_existing=True)
        if root is not None and root.state == "draft" and not root.operation_ids and root.continuation is None and aware(root.created_at) + timedelta(hours=draft_ttl_hours) <= aware(now):
            root.state, root.reason, root.updated_at = "refused", "draft-expired", now
    removed = 0
    ordered = sorted(dependencies.expired_history)
    for start in range(0, len(ordered), 500):
        removed += (await session.execute(delete(TelegramActRow).where(TelegramActRow.id.in_(ordered[start : start + 500])))).rowcount
    return removed
