"""Historical closure of complete immutable saved-edit replacement chains."""

from dataclasses import replace
from datetime import timedelta

from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body, thaw
from app.telegram_bots.work_management_edit_records import hex_identity, read_editor, read_incoming
from app.telegram_bots.work_management_retention import closed_command, closed_edit, discarded_root, historical, private_root
from app.telegram_bots.work_result_identity import digest


def replacement_for(root):
    value = thaw(root.result)
    if root.state != "refused" or root.reason != "replaced" or type(value) is not dict or set(value) != {"version", "editor", "input", "prompt_outbox_id", "draft_id"} or type(value["version"]) is not int or value["version"] != 1:
        raise ValueError("unknown-edit-replacement")
    editor, incoming = read_editor(value["editor"]), read_incoming(value["input"])
    if (
        editor is None
        or editor["draft_id"] != root.id
        or not hex_identity(value["prompt_outbox_id"])
        or not hex_identity(value["draft_id"])
        or value["draft_id"] == root.id
        or value["draft_id"] != digest(["management-edit", incoming])[:32]
        or (incoming["link_id"], incoming["chat_id"], incoming["binding_generation"]) != (root.link_id, root.chat_id, root.binding_generation)
    ):
        raise ValueError("conflicting-edit-replacement")
    return value


def unadmitted_closed(root, events, now, ttl):
    private_root(root)
    return (
        not root.operation_ids
        and set(root.continuation) == {"edit_draft_source"}
        and events.get((root.instance_id, root.id)) is None
        and (discarded_root(root) or root.result is None and (root.state == "draft" or root.state == "refused" and root.reason == "draft-expired") and aware(now) >= aware(root.created_at) + timedelta(hours=ttl))
    )


def family_closed(rows, events, attempts, now, ttl):
    """Qualify history only; this function grants no current source authority."""
    try:
        values = {value.id: value for row in rows for value in [historical(row)]}
        roots = {key: row for key, row in values.items() if "edit_draft_source" in row.continuation}
        if not roots:
            return len(values) == 1 and next(iter(values.values())).kind == "cancel" and closed_command(next(iter(values.values())), events, attempts)
        next_roots = {}
        for key, root in roots.items():
            private_root(root)
            if root.state == "refused" and root.reason == "replaced":
                next_roots[key] = replacement_for(root)["draft_id"]
        starts = set(roots) - set(next_roots.values())
        if len(starts) != 1 or len(next_roots.values()) != len(set(next_roots.values())):
            return False
        visited, used = set(), set()
        key = starts.pop()
        while key not in visited:
            root = roots.get(key)
            if root is None or events.get((root.instance_id, root.id)) is not None:
                return False
            visited.add(key)
            used.add(key)
            if key in next_roots:
                following = roots.get(next_roots[key])
                if (
                    following is None
                    or any(getattr(root, field) != getattr(following, field) for field in _ACT_SCOPE)
                    or any(not same_body(root.body[field], following.body[field]) for field in ("version", "original", "expected_revision", "expected_assignment_revision"))
                    or aware(root.created_at) != aware(following.created_at)
                ):
                    return False
                if root.operation_ids:
                    if len(root.operation_ids) != 1:
                        return False
                    native = values.get(root.operation_ids[0])
                    normalized = replace(root, state="draft", reason=None, result=None)
                    if native is None or native.state != "refused" or native.reason != "unchanged" or native.attempts != 0 or not closed_edit(normalized, native, events, attempts):
                        return False
                    used.add(native.id)
                elif set(root.continuation) != {"edit_draft_source"}:
                    return False
                key = following.id
                continue
            if root.operation_ids:
                if len(root.operation_ids) != 1:
                    return False
                native = values.get(root.operation_ids[0])
                if native is None or not closed_edit(root, native, events, attempts):
                    return False
                used.add(native.id)
            elif not unadmitted_closed(root, events, now, ttl):
                return False
            return visited == set(roots) and used == set(values)
        return False
    except (ValueError, TypeError, KeyError, AttributeError, ProductSourceRetired):
        return False
