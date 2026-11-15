"""Reconstruct actual status update presentations without changing their owner."""

from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace

from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_status import STATUS_KEYS, WorkStatusRead
from app.telegram_bots.work_status_delivery import notice
from app.telegram_bots.work_status_update_controls import presentation
from app.telegram_bots.work_status_updates import validate_update
from app.telegram_bots.work_view_sources import detail_read


def is_status_carrier(parent):
    """Independent delivery identity survives loss of optional source metadata."""
    from collections.abc import Mapping

    if parent is None:
        return False
    source = parent.payload.get("work_news_basis", {}).get("source", {})
    return (
        "work_status_update" in parent.payload
        or isinstance(source, Mapping)
        and source.get("key") == "status.stopped"
        or parent.work_id is not None
        and parent.dedupe_key.startswith("progress:")
        or any(isinstance(part, Mapping) and isinstance(part.get("source_ref"), Mapping) and part["source_ref"].get("key") == "work.status.update" for part in parent.parts)
    )


def acknowledged_update(parent, version):
    item = _item(parent)
    basis = validate_update(item)
    current = item.payload["presentation_version"]
    if type(version) is not int or not 0 <= version <= current or basis["version"] != 2:
        raise ProductSourceRetired()
    if version != current:
        saved = deepcopy(basis["history"][version]["basis"])
        saved["history"] = deepcopy(basis["history"][:version])
        from app.telegram_bots.callbacks import canonical_keyboard

        item = replace(
            item,
            parts=[part for part in item.parts if part["presentation_version"] < version] + deepcopy(basis["history"][version]["parts"]),
            payload={**item.payload, "presentation_version": version, "work_status_update": saved, "product_keyboard_basis": canonical_keyboard(presentation(saved["source"]).keyboard)},
        )
    validate_update(item, acknowledged=True)
    return item


def carrier_source(item):
    if "work_status_update" not in item.payload:
        return item.payload["work_news_basis"]["source"]
    source = validate_update(item, acknowledged=True)["source"]
    return asdict(notice(WorkStatusRead(**source["status"]), SimpleNamespace(work_id=item.work_id), detail=detail_read(source["detail"]), dates=source["detail_dates"]))


def status_record(source):
    if source["key"] not in STATUS_KEYS or type(source["work"].get("detail")) is not dict:
        raise ProductSourceRetired()
    record = source["work"]["detail"]["record"]
    if (record.get("attempt") or {}).get("id") != source["work"]["attempt_id"]:
        raise ProductSourceRetired()
    return record


class StatusResumeOrigin:
    """An ended original attempt survives this activation's own revision writes."""

    def __init__(self, link, item, original, clock):
        from app.telegram_bots.work_status import StatusProjection

        self.status = StatusProjection(link, item, clock)
        self.item, self.original = item, original

    def extend(self, query):
        return self.status.extend(query)

    def check_with_permissions(self, rows, held):
        self.check(rows)

    def check(self, rows):
        # The service calls admission before and after reserving its new
        # attempt. Re-derive the original status across that own revision write;
        # its complete immutable event proofs below must remain unchanged.
        self.status.check(rows, compare_source=False)
        fresh = self.status.source
        original = self.original
        proofs = original["work"].get("event_proofs")
        if (
            original["key"] != "status.stopped"
            or fresh.key != "status.stopped"
            or original["event_signature"] != fresh.origin_signature
            or original["work"].get("attempt_id") != fresh.attempt_id
            or type(proofs) is not dict
            or not proofs
            or any(fresh.event_proofs.get(identity) != signature for identity, signature in proofs.items())
        ):
            raise ProductSourceRetired()


async def resume_projection(reader, link, parent, original):
    from app.telegram_bots.settings import SettingsRepository
    from app.telegram_bots.work_status_delivery import StatusNoticeReader

    clock = reader.outbox.clock if hasattr(reader, "outbox") else reader.clock
    status = StatusNoticeReader(reader, settings=SettingsRepository(reader.links.session_factory, reader.links.config), clock=clock)
    return StatusResumeOrigin(link, _item(parent), carrier_source(original), await status.prepare(link))
