"""Actual summary receipts behind read-only native Work navigation."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

from sqlalchemy import select

from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.summary_delivery import frozen_summary
from app.telegram_bots.summary_views import frozen_view
from app.telegram_bots.work_digest_views import parent_proof
from app.telegram_bots.work_request_source import require_show
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


def family(payload):
    keys = {"summary_basis", "summary_view_basis"}.intersection(payload)
    if len(keys) != 1:
        raise ProductSourceRetired()
    return next(iter(keys))


def acknowledged_summary(link, parent, version):
    item = _item(parent)
    key = family(item.payload)
    current = item.payload["presentation_version"]
    if type(version) is not int or not 0 <= version <= current:
        raise ProductSourceRetired()
    if version != current:
        try:
            old = item.payload[key]["history"][version]
            basis = deepcopy(old["basis"])
            if basis.get("version") == 2:
                basis["history"] = deepcopy(item.payload[key]["history"][:version])
            item = replace(
                item, parts=[part for part in item.parts if part["presentation_version"] < version] + [old["part"]], payload={**item.payload, "presentation_version": version, key: basis, "product_keyboard_basis": basis["keyboard"]}
            )
        except (KeyError, IndexError, TypeError):
            raise ProductSourceRetired() from None
    frozen = frozen_summary if key == "summary_basis" else frozen_view
    basis, _ = frozen(SimpleNamespace(link=link, item=item, part=None), acknowledged=True)
    summary = basis if key == "summary_basis" else basis["summary"]
    if summary.get("work_controls") != 1 or key == "summary_basis" and basis["concealed"]:
        raise ProductSourceRetired()
    return item


def qualify_summary_parent(link, anchor, root, controls, clock):
    """A protected Show retains its real original source and consumed Show."""
    if "summary_view_basis" not in anchor.payload:
        return
    basis = anchor.payload["summary_view_basis"]
    if root is None or root.id != basis["parent_id"]:
        raise ProductSourceRetired()
    original = _item(root)
    frozen, _ = frozen_summary(SimpleNamespace(link=link, item=original, part=None), acknowledged=True)
    if not frozen["concealed"] or parent_proof(original) != basis["parent_proof"] or original.parts[-1]["message_ids"] != [basis["target"]]:
        raise ProductSourceRetired()
    try:
        _validate_rows(original, list(controls.values()))
        require_show(controls.get(basis["callback_id"]), original)
        for control in controls.values():
            if control.consumed_at is None or control.closed_reason != "revealed" or control.expires_at is not None and aware(control.expires_at) <= clock():
                raise CallbackRefused()
    except CallbackRefused:
        raise ProductSourceRetired() from None


async def qualify_summary_anchor(link, original, writer, clock):
    if "summary_view_basis" not in original.payload:
        return
    basis = original.payload["summary_view_basis"]
    root = await writer.get(TelegramOutboxRow, basis["parent_id"])
    show = await writer.get(TelegramCallbackRow, basis["callback_id"])
    controls = {} if show is None else {control.id: control for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == show.group_id))}
    qualify_summary_parent(link, original, root, controls, clock)
