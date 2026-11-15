"""Pin actual historical SDK ancestry; these reads never supply effect authority."""

from collections import defaultdict
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import timedelta

from sqlalchemy import or_, select

from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_news_carriers import news_witness_id
from app.telegram_bots.work_view_origins import anchor_signature
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


def mapping(value):
    return value if isinstance(value, Mapping) else {}


def identifier(value):
    return value if type(value) is str and len(value) == 32 and all(char in "0123456789abcdef" for char in value) else None


def scope(row):
    return row.link_id, row.binding_generation, row.chat_id, row.message_thread_id


def origins(row):
    payload = mapping(row.payload)
    yield mapping(mapping(payload.get("work_view_intent")).get("origin"))
    yield mapping(mapping(mapping(mapping(payload.get("work_news_basis")).get("source")).get("work")).get("origin"))


def stored_anchor(parent, version):
    """Reconstruct stored bytes for the independent gesture digest, without I/O."""
    item = _item(parent)
    current = item.payload["presentation_version"]
    if type(version) is not int or not 0 <= version <= current:
        raise ProductSourceRetired()
    if version == current:
        return item
    if "work_status_update" in item.payload:
        from app.telegram_bots.callbacks import canonical_keyboard
        from app.telegram_bots.work_status_update_controls import presentation

        body = item.payload["work_status_update"]
        old = body["history"][version]
        saved = deepcopy(old["basis"])
        saved["history"] = deepcopy(body["history"][:version])
        return replace(
            item,
            parts=[part for part in item.parts if part["presentation_version"] < version] + deepcopy(old["parts"]),
            payload={**item.payload, "presentation_version": version, "work_status_update": saved, "product_keyboard_basis": canonical_keyboard(presentation(saved["source"]).keyboard)},
        )
    keys = {"summary_basis", "summary_view_basis"}.intersection(item.payload)
    if len(keys) > 1:
        raise ProductSourceRetired()
    key = next(iter(keys)) if keys else "work_news_basis"
    old = item.payload[key]["history"][version]
    saved = deepcopy(old["basis"])
    if saved.get("version") == (2 if keys else 3):
        saved["history"] = deepcopy(item.payload[key]["history"][:version])
    parts = [old["part"]] if keys else old["parts"]
    return replace(
        item,
        class_=item.class_ if keys else old["class"],
        parts=[part for part in item.parts if part["presentation_version"] < version] + deepcopy(parts),
        payload={**item.payload, "presentation_version": version, key: saved, "product_keyboard_basis": saved["keyboard"]},
    )


@dataclass
class RefreshDependencies:
    groups: set = field(default_factory=set)
    callbacks: set = field(default_factory=set)
    deliveries: set = field(default_factory=set)


async def refresh_dependencies(session, now, *, max_age_days=180):
    result = RefreshDependencies()
    delivery = TelegramOutboxRow
    admitted = select(TelegramActRow.continuation["news_origin"]["anchor_outbox_id"].as_string()).where(TelegramActRow.kind == "activate")
    witnessed = select(TelegramCallbackRow.payload["news_origin"]["anchor_outbox_id"].as_string()).where(TelegramCallbackRow.kind == "work_activation_origin")
    rows = list(
        await session.scalars(
            select(delivery).where(
                or_(
                    delivery.dedupe_key.startswith("notice:work-start-refresh:"),
                    delivery.payload["work_view_intent"]["origin"]["refusal"].as_string() == "start.changed",
                    delivery.payload["work_news_basis"]["source"]["work"]["origin"]["refusal"].as_string() == "start.changed",
                    delivery.id.in_(admitted),
                    delivery.id.in_(witnessed),
                )
            )
        )
    )
    views = {row.id: row for row in rows}
    if not views:
        return result
    link_ids = {row.link_id for row in rows}
    controls = {row.id: row for row in await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.link_id.in_(link_ids)))}
    gestures = defaultdict(list)
    for row in controls.values():
        if row.kind == "work_view_gesture" and row.target_kind == "outbox" and row.group_id == row.id:
            gestures[row.target_id].append(row)
    all_views = {
        row.id: row for row in await session.scalars(select(delivery).where(delivery.link_id.in_(link_ids), or_(delivery.payload["work_view_intent"]["version"].as_integer() == 1, delivery.dedupe_key.startswith("notice:work-view:"))))
    }
    all_views.update(views)
    parents = dict(all_views)
    locators = {
        identity for view in all_views.values() for origin in [*origins(view), *(mapping(row.payload) for row in gestures[view.id])] for key in ("anchor_outbox_id", "cleanup_id") if (identity := identifier(origin.get(key))) is not None
    }
    ordered = sorted(locators - parents.keys())
    for start in range(0, len(ordered), 500):
        parents.update({row.id: row for row in await session.scalars(select(delivery).where(delivery.id.in_(ordered[start : start + 500])))})
    group_ids = {value for control in controls.values() if (value := identifier(control.group_id)) is not None}
    group_ids.update(value for parent in parents.values() for part in (parent.parts if isinstance(parent.parts, list) else []) if isinstance(part, Mapping) if (value := identifier(part.get("callback_group_id"))) is not None)
    by_group = defaultdict(list)
    ordered = sorted(group_ids)
    for start in range(0, len(ordered), 500):
        for row in await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(ordered[start : start + 500]))):
            by_group[row.group_id].append(row)

    def owned_group(identity, view, *, anchor):
        members = by_group.get(identity, [])
        return bool(members) and all(scope(row) == scope(view) and mapping(row.payload).get("anchor_outbox_id") == anchor for row in members)

    seeds = {view.id for view in views.values() if view.state in {"pending", "sending", "failed"}}
    for row in controls.values():
        anchor = row.target_id if row.kind == "work_view_gesture" and row.target_kind == "outbox" else mapping(row.payload).get("anchor_outbox_id")
        view = views.get(identifier(anchor))
        if view is not None and scope(row) == scope(view) and row.consumed_at is None and aware(row.created_at) > now - timedelta(days=max_age_days) and (row.expires_at is None or aware(row.expires_at) > now):
            seeds.add(view.id)
        if row.kind == "work_activation_origin":
            action = identifier(mapping(row.payload).get("work_action_id"))
            view = views.get(identifier(mapping(mapping(row.payload).get("news_origin")).get("anchor_outbox_id")))
            if action is not None and row.id == row.group_id == news_witness_id(action) and view is not None and scope(row) == scope(view) and (row.target_kind, row.target_id) == ("work", view.work_id):
                seeds.add(view.id)
    for act in await session.scalars(select(TelegramActRow).where(TelegramActRow.kind == "activate", TelegramActRow.link_id.in_(link_ids))):
        view = views.get(identifier(mapping(mapping(act.continuation).get("news_origin")).get("anchor_outbox_id")))
        if view is not None and (act.link_id, act.owner_user_id, act.binding_generation, act.chat_id, act.instance_id, act.work_id) == (
            view.link_id,
            view.owner_user_id,
            view.binding_generation,
            view.chat_id,
            view.instance_id,
            view.work_id,
        ):
            seeds.add(view.id)
    for identity in seeds:
        view, visited = views[identity], set()
        while view.id not in visited and len(visited) < 32:
            visited.add(view.id)
            result.deliveries.add(view.id)
            for part in view.parts if isinstance(view.parts, list) else []:
                group = identifier(mapping(part).get("callback_group_id"))
                if group is not None and owned_group(group, view, anchor=view.id):
                    result.groups.add(group)
            candidates = [row for row in gestures[view.id] if scope(row) == scope(view) and mapping(row.payload).get("gesture_id") == row.id]
            if len(candidates) != 1:
                break
            gesture = candidates[0]
            result.callbacks.add(gesture.id)
            origin = mapping(gesture.payload)
            parent = parents.get(identifier(origin.get("anchor_outbox_id")))
            if parent is None or scope(parent) != scope(view) or (parent.owner_user_id, parent.instance_id) != (view.owner_user_id, view.instance_id):
                break
            try:
                original = stored_anchor(parent, origin.get("presentation_version"))
                if anchor_signature(original) != origin.get("anchor_signature"):
                    break
                parts = [part for part in original.parts if part["presentation_version"] == origin["presentation_version"]]
                if not parts or any(part["state"] != "sent" for part in parts) or parts[-1]["message_ids"] != [gesture.message_id]:
                    break
                kind = "work_start" if view.id in views else "work_details"
                specs = [spec for line in original.payload["product_keyboard_basis"] for spec in line]
                if not any((spec["kind"], spec["target_kind"], spec["target_id"], spec["payload"].get("work_id")) == (kind, "work", view.work_id, view.work_id) for spec in specs):
                    break
            except (ProductSourceRetired, KeyError, TypeError, ValueError, AttributeError, IndexError):
                break
            cleanup = parents.get(identifier(origin.get("cleanup_id")))
            if cleanup is not None and scope(cleanup) == scope(view) and cleanup.owner_user_id == view.owner_user_id and mapping(cleanup.payload).get("work_request_parent_id") == view.id:
                result.deliveries.add(cleanup.id)
            group = identifier(parts[-1].get("callback_group_id"))
            if group is not None and owned_group(group, view, anchor=parent.id):
                result.groups.add(group)
            result.deliveries.add(parent.id)
            if parent.id not in all_views:
                break
            view = parent
    return result
