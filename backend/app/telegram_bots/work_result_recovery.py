"""Reveal protected review files after clearing their acknowledged teaser."""

from types import SimpleNamespace

from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired, _item
from app.telegram_bots.work_request_recovery import WorkRequestRecovery
from app.telegram_bots.work_request_source import frozen
from app.telegram_bots.work_result_delivery import WorkReviewPlans
from app.telegram_bots.work_result_files import present_review_files, result_options
from app.telegram_bots.work_result_ledger import initial_ledger


class WorkResultRecovery(WorkRequestRecovery):
    def __init__(self, plans):
        super().__init__(WorkReviewPlans(outbox=plans.outbox, reader=plans.reader, callbacks=plans.callbacks))

    def _owns_basis(self, basis):
        return "result_files" in basis

    def _separate_reveal(self, reveal, fresh):
        return reveal is not None

    def _extras(self, source):
        return self.plans._extras(source)

    def _presentation(self, source, settings, link, row, concealed):
        return present_review_files(source, locale=settings.locale, concealed=concealed, outbox_id=row.id, **result_options(self._extras(source)["result_files"]))

    async def _replace(self, writer, link, row, source, settings, *, reveal=None, force_new=False, choice=None):
        # This transition owns only the first Show. Refreshing a partly sent
        # result needs a separate proof that carries its file receipts forward.
        basis = row.payload.get("work_request_basis", {})
        if reveal is None or choice is not None or not basis.get("concealed") or not self._owns_basis(basis):
            raise ProductSourceRetired()
        parts = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]]
        if len(parts) != 1 or parts[0]["kind"] != "text":
            raise ProductSourceRetired()
        try:
            frozen(SimpleNamespace(link=link, item=_item(row), part=parts[0]), acknowledged=True)
        except PermanentDeliveryError:
            raise ProductSourceRetired() from None
        await super()._replace(writer, link, row, source, settings, reveal=reveal, force_new=True)
        fresh = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]]
        row.payload = {**row.payload, "work_result_ledger": initial_ledger(fresh)}
        await writer.flush()
