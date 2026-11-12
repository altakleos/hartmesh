"""Refresh review cards while retaining original media delivery evidence."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired, _item
from app.telegram_bots.work_request_recovery import WorkRequestRecovery, _RecoveryProjection
from app.telegram_bots.work_request_source import frozen
from app.telegram_bots.work_result_delivery import WorkReviewPlans
from app.telegram_bots.work_result_files import present_review_files, result_options
from app.telegram_bots.work_result_history import archive_media, qualified_history, read_media_history
from app.telegram_bots.work_result_identity import result_identity
from app.telegram_bots.work_result_ledger import carry_media, initial_ledger
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.work import AgentWorkRow


class WorkResultRecovery(WorkRequestRecovery):
    def __init__(self, plans):
        super().__init__(WorkReviewPlans(outbox=plans.outbox, reader=plans.reader, callbacks=plans.callbacks))

    def _owns_basis(self, basis):
        return "result_files" in basis

    async def _source(self, link, request_id, *, intent=None):
        source, settings = await super()._source(link, request_id, intent=intent)
        async with self.outbox.sessions() as reader:
            work = await reader.get(AgentWorkRow, source.work["id"])
            if work is None or (work.instance_id, work.revision) != (link.agent_ref, source.work["revision"]):
                raise ProductSourceRetired()
            identity = result_identity(work)
        return replace(source, work={**source.work, "result_identity": identity}), settings

    async def _admit(self, admission, writer, source):
        held, _target = await admission(session=writer, projection=_ResultRecoveryProjection(source))
        if "agents:write" not in held:
            raise AgentDenied("The request is unavailable")

    def _separate_reveal(self, reveal, fresh):
        return reveal is not None

    def _extras(self, source):
        return self.plans._extras(source)

    def _presentation(self, source, settings, link, row, concealed):
        return present_review_files(source, locale=settings.locale, concealed=concealed, outbox_id=row.id, **result_options(self._extras(source)["result_files"]))

    async def _replace(self, writer, link, row, source, settings, *, reveal=None, force_new=False, choice=None):
        basis = row.payload.get("work_request_basis", {})
        if choice is not None or not self._owns_basis(basis) or reveal is not None and not basis.get("concealed"):
            raise ProductSourceRetired()
        if source.work.get("result_identity") != row.payload.get("work_result_identity"):
            raise ProductSourceRetired()
        parts = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]]
        selected = next((part for part in parts if part["state"] == "sending"), parts[-1] if row.state == "sent" and parts else None)
        if selected is None:
            raise ProductSourceRetired()
        try:
            item = _item(row)
            _basis, previous, _recipe = frozen(SimpleNamespace(link=link, item=item, part=selected), acknowledged=selected["state"] == "sent")
            earlier, hashes = qualified_history(item, previous)
            history = deepcopy(item.payload.get("work_result_history", []))
            if not basis["concealed"]:
                proof = archive_media(item, previous, locale=basis["locale"], options=basis["result_files"], earlier=earlier)
                known, remembered = read_media_history(item, proof, source, earlier=earlier)
                # Card-only refreshes must not grow a chain of redundant proofs.
                if any(earlier.get(index) != receipt for index, receipt in known.items()) or any(hashes.get(index) != digest for index, digest in remembered.items()):
                    history.append(proof)
        except (PermanentDeliveryError, ValueError):
            raise ProductSourceRetired() from None
        await super()._replace(writer, link, row, source, settings, reveal=reveal, force_new=True)
        fresh = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]]
        updated = row.payload["work_request_basis"]
        # A refresh before the first Show cleanup finishes cannot bypass it.
        if reveal is None and basis.get("reveal") is not None:
            updated = {**updated, "reveal": basis["reveal"], "cleanup_id": basis["cleanup_id"]}
        row.payload = {**row.payload, "work_request_basis": updated, "work_result_ledger": initial_ledger(fresh), "work_result_history": history}
        carried, hashes = qualified_history(_item(row), source)
        documents = set()
        for proof in history:
            for event in proof["ledger"]["events"]:
                if event["reason"] == "photo-rejected":
                    parent = next(part for part in proof["parts"] if part["part_id"] == event["old"])
                    documents.update(file["source_index"] for file in parent["files"])
        carry_media(row, carried, hashes, documents=documents)
        await writer.flush()


class _ResultRecoveryProjection(_RecoveryProjection):
    def check(self, rows):
        if result_identity(rows[0][8]) != self.source.work.get("result_identity"):
            raise ProductSourceRetired()
        super().check(rows)
