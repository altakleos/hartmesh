"""Retire only complete, closed saved-assignment command families."""

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

from sqlalchemy import delete, select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import _ACT_SCOPE, request_for, same_body
from app.telegram_bots.work_edit_drafts import snapshot_edit
from app.telegram_bots.work_management_actions import command_source
from app.telegram_bots.work_stop_retention import original_settled
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow


def historical(row):
    """Recover a sealed locator for retention reads, never for live authority."""
    value = _snapshot(row)
    continuation = value.continuation if isinstance(value.continuation, Mapping) else {}
    source = continuation.get("edit_draft_source", continuation.get("management_source"))
    if not isinstance(source, Mapping) or type(source.get("link_id")) is not str or len(source["link_id"]) != 32:
        raise ValueError("unknown-management-locator")
    return replace(value, link_id=source["link_id"]) if value.link_id is None else value


def private_root(row):
    scope = SimpleNamespace(
        id=row.link_id, owner_user_id=row.owner_user_id, bot_id=row.bot_id, binding_generation=row.binding_generation, telegram_user_id=row.telegram_user_id, chat_id=row.chat_id, agent_kind="instance", agent_ref=row.instance_id
    )
    snapshot_edit(scope, row)
    if row.continuation["edit_draft_source"]["origin_kind"] != "internal":
        raise ValueError("unqualified-edit-origin")


def discarded_root(root):
    value = root.result
    origin = value.get("origin") if isinstance(value, Mapping) else None
    return (
        root.state == "refused"
        and root.reason == "discarded"
        and isinstance(value, Mapping)
        and set(value) == {"version", "kind", "origin"}
        and type(value["version"]) is int
        and value["version"] == 1
        and value["kind"] == "discard"
        and isinstance(origin, Mapping)
        and set(origin) == {"callback_id", "anchor_outbox_id", "presentation_version", "signature"}
        and all(type(origin[key]) is str and len(origin[key]) == size and all(ch in "0123456789abcdef" for ch in origin[key]) for key, size in (("callback_id", 32), ("anchor_outbox_id", 32), ("signature", 64)))
        and type(origin["presentation_version"]) is int
        and origin["presentation_version"] >= 0
    )


def closed_command(row, events, attempts):
    source = command_source(row)
    if source["origin_kind"] == "callback":
        from app.telegram_bots.work_management_consent import management_origin

        management_origin(row)
    body = request_for(row.kind, row.body, identity=row.id)
    if row.request_id is not None or row.message_id is not None or row.operation_ids or not same_body(row.body, body.model_dump(mode="json", exclude_unset=True)):
        return False
    event = events.get((row.instance_id, row.id))
    if row.state in {"refused", "conflict"}:
        known_failure = row.state == "refused" and row.reason == "unchanged" and row.kind == "edit" and row.attempts == 0 and row.first_attempt_at is None
        known_failure = known_failure or row.state == "conflict" and row.reason == "changed" and row.attempts > 0 and row.first_attempt_at is not None
        known_failure = known_failure or row.state == "refused" and row.reason == "cancelled" and row.attempts == 0 and row.cancel_requested_at is not None
        return known_failure and event is None and row.result is None and (source["scope"] is None or row.stop_dispatch["state"] == "ended")
    if row.state != "done" or row.reason is not None or event is None or not isinstance(row.result, Mapping) or not isinstance(event.result, Mapping):
        return False
    expected = (body.expected_revision + 1, body.expected_assignment_revision + 1)
    if (
        (event.work_id, event.action, event.actor_kind, event.actor_id) != (row.work_id, row.kind, "human", row.owner_user_id)
        or not same_body(event.request, row.body)
        or (event.revision, event.assignment_revision) != expected
        or any(not same_body(row.result.get(key), event.result.get(key)) for key in ("id", "instance_id", "revision", "assignment_revision", "status", "objective", "success_criteria", "due_at"))
        or (event.result.get("id"), event.result.get("instance_id"), event.result.get("revision"), event.result.get("assignment_revision")) != (row.work_id, row.instance_id, *expected)
    ):
        return False
    if source["scope"] is None:
        return True
    scope = row.stop_dispatch
    if (
        not isinstance(scope, Mapping)
        or set(scope) - {"ended_from"} != {"attempt_id", "thread_id", "run_id", "state", "attempts", "not_before"}
        or scope["state"] not in {"settled", "ended", "ready", "requested", "uncertain"}
        or "ended_from" in scope
        and (scope["state"] != "ended" or scope["ended_from"] not in {"ready", "uncertain"})
        or type(scope["attempts"]) is not int
        or scope["attempts"] < 0
    ):
        return False
    return original_settled(row, attempts.get(scope["attempt_id"]))


def closed_edit(root, command, events, attempts):
    from app.telegram_bots.work_management_consent import edit_admission_for

    private_root(root)
    source = command_source(command)
    reference = source["draft_ref"]
    if (
        list(root.operation_ids) != [command.id]
        or not (root.state == "draft" or command.reason == "unchanged" and discarded_root(root))
        or set(root.continuation) != {"edit_draft_source", "edit_admission"}
        or any(getattr(root, key) != getattr(command, key) for key in _ACT_SCOPE if key != "kind")
        or not same_body(reference, {"id": root.id, "signature": root.continuation["edit_draft_source"]["signature"]})
        or not same_body(root.continuation["edit_admission"], edit_admission_for(command, reference))
    ):
        return False
    draft = root.body["draft"]
    expected = WorkCommand(
        operation_id=command.id,
        action="edit",
        expected_revision=root.body["expected_revision"],
        expected_assignment_revision=root.body["expected_assignment_revision"],
        objective=draft["objective"],
        success_criteria=draft["success_criteria"],
        due_at=draft["due_at"],
    )
    if events.get((root.instance_id, root.id)) is not None:
        return False
    if command.reason == "unchanged":
        original = WorkCommand.model_validate({**expected.model_dump(mode="json", exclude_unset=True), **{key: root.body["original"][key] for key in ("objective", "success_criteria", "due_at")}})
        if not same_body(original.model_dump(mode="json", exclude_unset=True), expected.model_dump(mode="json", exclude_unset=True)):
            return False
    return same_body(command.body, expected.model_dump(mode="json", exclude_unset=True)) and closed_command(command, events, attempts)


async def expire_management(session, now, *, draft_ttl_hours, pending_outcome):
    """Lock Link -> Act before qualifying complete reciprocal components."""
    candidates = list(await session.execute(select(TelegramActRow.id, TelegramActRow.link_id).where(TelegramActRow.kind.in_(("edit", "cancel")))))
    links = sorted({link_id for _, link_id in candidates if link_id is not None})
    for start in range(0, len(links), 500):
        list(await session.scalars(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id.in_(links[start : start + 500])).order_by(TelegramBotLinkRow.id).with_for_update().execution_options(populate_existing=True)))
    identities = sorted(identity for identity, _ in candidates)
    journals, controls = {}, set()
    events, attempts = {}, {}
    for start in range(0, len(identities), 500):
        batch = identities[start : start + 500]
        for row in await session.scalars(select(TelegramActRow).where(TelegramActRow.id.in_(batch)).order_by(TelegramActRow.id).with_for_update().execution_options(populate_existing=True)):
            journals[row.id] = row
        controls.update(await session.scalars(select(TelegramCallbackRow.id).where(TelegramCallbackRow.id.in_(batch))))
        for event in await session.scalars(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id.in_(batch))):
            events[event.instance_id, event.operation_id] = event
    targets = sorted({scope["attempt_id"] for row in journals.values() if isinstance((scope := row.stop_dispatch), Mapping) and type(scope.get("attempt_id")) is str})
    for start in range(0, len(targets), 500):
        for attempt in await session.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.id.in_(targets[start : start + 500]))):
            attempts[attempt.id] = attempt
    graph = defaultdict(set)
    for identity, row in journals.items():
        graph[identity]
        source = row.continuation.get("management_source") if isinstance(row.continuation, Mapping) else None
        reference = source.get("draft_ref") if isinstance(source, Mapping) else None
        neighbours = {reference["id"]} if isinstance(reference, Mapping) and type(reference.get("id")) is str else set()
        if isinstance(row.result, Mapping) and "edit_draft_source" in (row.continuation or {}) and type(row.result.get("draft_id")) is str:
            neighbours.add(row.result["draft_id"])
        if isinstance(row.operation_ids, list):
            neighbours.update(member for member in row.operation_ids if type(member) is str)
        for member in neighbours:
            graph[identity].add(member)
            graph[member].add(identity)
    held = set(await session.scalars(select(TelegramActRow.id).where(TelegramActRow.kind.in_(("edit", "cancel")), pending_outcome)))
    blocked = held | controls
    from app.telegram_bots.work_management_ui_retention import management_dependencies

    ui = await management_dependencies(session, now, draft_ttl_hours=draft_ttl_hours)
    blocked.update(ui.acts)
    foreign = select(TelegramActRow.operation_ids, TelegramActRow.continuation).where(TelegramActRow.kind.not_in(("edit", "cancel")))
    for references, continuation in await session.execute(foreign):
        if isinstance(references, list):
            blocked.update(member for member in references if type(member) is str and member in graph)
        if isinstance(continuation, Mapping):
            for key, fields in (("stop_retry", ("parent_id",)), ("delegate_source", ("parent_id", "draft_id")), ("activation_continuation", ("child_id",))):
                source = continuation.get(key)
                if isinstance(source, Mapping):
                    blocked.update(source[field] for field in fields if type(source.get(field)) is str and source[field] in graph)
    seen, expired = set(), []
    for identity in sorted(graph):
        if identity in seen:
            continue
        component, pending = set(), [identity]
        while pending:
            member = pending.pop()
            if member in component:
                continue
            component.add(member)
            pending.extend(graph[member] - component)
        seen.update(component)
        if component & blocked or not component.issubset(journals):
            continue
        try:
            rows = [historical(journals[member]) for member in sorted(component)]
            roots = [row for row in rows if "edit_draft_source" in row.continuation]
            if len(rows) == 1 and roots:
                root = roots[0]
                private_root(root)
                closed = not root.operation_ids and set(root.continuation) == {"edit_draft_source"} and events.get((root.instance_id, root.id)) is None
                if closed and root.state == "draft" and aware(now) >= aware(root.created_at) + timedelta(hours=draft_ttl_hours):
                    raw = journals[root.id]
                    raw.state, raw.reason, raw.updated_at = "refused", "draft-expired", now
                closed = closed and (journals[root.id].state == "refused" and journals[root.id].reason == "draft-expired" or discarded_root(root))
            elif len(rows) == 2 and len(roots) == 1:
                command = next(row for row in rows if row.id != roots[0].id)
                closed = command.kind == "edit" and closed_edit(roots[0], command, events, attempts)
            else:
                from app.telegram_bots.work_management_edit_retention import family_closed

                closed = family_closed(rows, events, attempts, now, draft_ttl_hours)
                if closed:
                    for root in roots:
                        if root.state == "draft" and not root.operation_ids and aware(now) >= aware(root.created_at) + timedelta(hours=draft_ttl_hours):
                            raw = journals[root.id]
                            raw.state, raw.reason, raw.updated_at = "refused", "draft-expired", now
            if closed and all(aware(row.created_at) < aware(now) - timedelta(days=400) for row in rows):
                expired.extend(component)
        except (ValueError, TypeError, KeyError, AttributeError, ProductSourceRetired):
            continue
    removed = 0
    for start in range(0, len(expired), 500):
        removed += (await session.execute(delete(TelegramActRow).where(TelegramActRow.id.in_(expired[start : start + 500])))).rowcount
    return removed
