"""Dormant Work review media plans, sharing one outbox with their final card."""

from app.telegram_bots.work_request_delivery import WorkRequestPlans
from app.telegram_bots.work_result_files import present_review_files, result_options
from app.telegram_bots.work_result_ledger import initial_ledger


class WorkReviewPlans(WorkRequestPlans):
    def _replay_basis(self, basis):
        return super()._replay_basis(basis) and type(basis.get("result_files")) is dict and bool(result_options(basis["result_files"]))

    def _extras(self, source):
        return {"result_files": {"version": 1, "capacity": self.outbox.config.files.in_memory_max_bytes, "max_bytes": self.outbox.config.files.max_outbound_bytes}}

    def _presentation(self, source, settings, identity):
        return present_review_files(source, locale=settings.locale, concealed=source.protected, outbox_id=identity, **result_options(self._extras(source)["result_files"]))

    async def _freeze(self, writer, link, row, source, settings, *, pending=True):
        from app.telegram_bots.work_result_identity import result_identity
        from app.telegram_bots.work_result_navigation import capture_navigation
        from deerflow.persistence.agent_instances.work import AgentWorkRow

        identity = await super()._freeze(writer, link, row, source, settings, pending=True)
        work = await writer.get(AgentWorkRow, source.work["id"], populate_existing=True)
        navigation = await capture_navigation(writer, link, source)
        row.payload = {**row.payload, "work_result_ledger": initial_ledger(row.parts), "work_result_identity": result_identity(work), "work_result_navigation": navigation}
        await writer.flush()
        return identity


async def change_result_part(outbox, captured, *, hashes=None, templates=None, reason=None):
    """Fence metadata updates by the exact original sending DTO and source."""
    from deerflow.utils.file_io import await_drained

    return await await_drained(_change_result_part(outbox, captured, hashes=hashes, templates=templates, reason=reason))


def frozen_result(captured, **kwargs):
    if "result_notice_basis" in captured.item.payload:
        from app.telegram_bots.result_notice_delivery import frozen_notice

        return frozen_notice(captured, **kwargs)
    from app.telegram_bots.work_request_source import frozen

    return frozen(captured, **kwargs)


async def _change_result_part(outbox, captured, *, hashes=None, templates=None, reason=None):
    from copy import deepcopy
    from types import SimpleNamespace

    from app.telegram_bots.producer import DeliveryClaimRetired, _item
    from app.telegram_bots.work_result_history import qualified_history
    from app.telegram_bots.work_result_ledger import qualify_result_parts, replace_media
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    async with outbox._writer(captured.link.id) as (writer, binding):
        row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
        current = outbox._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
        if current != captured:
            raise DeliveryClaimRetired()
        basis, source, _part = frozen_result(current)
        if "result_files" not in basis:
            raise DeliveryClaimRetired()
        if hashes is not None:
            if templates is not None or type(hashes) is not dict or not hashes:
                raise ValueError("invalid-result-file-hashes")
            parts = deepcopy(row.parts)
            selected = next(part for part in parts if part["part_id"] == current.part["part_id"])
            previous = selected.get("result_file_hashes", {})
            if any(previous.get(key, value) != value for key, value in hashes.items()):
                return False
            selected["result_file_hashes"] = {**previous, **hashes}
            row.parts = parts
            frozen_result(SimpleNamespace(link=current.link, item=_item(row), part=selected))
        else:
            replace_media(row, current.part["part_id"], templates, reason=reason)
            carried, hashes = qualified_history(_item(row), source)
            qualify_result_parts(_item(row), basis["parts"], carried=carried, known_hashes=hashes)
            row.state, row.not_before = "pending", outbox.clock()
            row.claim_owner = row.claim_id = row.claimed_at = None
        return True
