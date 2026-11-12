"""Refresh review cards while retaining original media delivery evidence."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

from sqlalchemy import and_
from sqlalchemy.orm import aliased

from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired, _item
from app.telegram_bots.work_request_recovery import WorkRequestRecovery, _RecoveryGuard, _RecoveryProjection
from app.telegram_bots.work_request_source import frozen
from app.telegram_bots.work_result_delivery import WorkReviewPlans
from app.telegram_bots.work_result_files import present_review_files, result_options
from app.telegram_bots.work_result_history import archive_media, qualified_history, read_media_history
from app.telegram_bots.work_result_identity import result_identity
from app.telegram_bots.work_result_ledger import carry_media, initial_ledger
from app.telegram_bots.work_result_navigation import NavigationOrigin, capture_navigation, require_navigation
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.utils.file_io import await_drained


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
            navigation = await capture_navigation(reader, link, source)
        return replace(source, work={**source.work, "result_identity": identity, "result_navigation": navigation}), settings

    async def _admit(self, admission, writer, source):
        held, _target = await admission(session=writer, projection=_ResultRecoveryProjection(source))
        if "agents:write" not in held:
            raise AgentDenied("The request is unavailable")

    async def repair(self, link, outbox_id, *, guard):
        """Repair only a held result failure, never an active or uncertain send."""
        from app.telegram_bots.attention_authority import prepare_attention

        async def perform():
            async with self.outbox.sessions() as reader:
                row = await reader.get(TelegramOutboxRow, outbox_id)
                if row is None or (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id):
                    raise CallbackRefused()
                if not self._repairable(row):
                    return None
                version, request_id = row.payload["presentation_version"], row.request_id
            source, settings = await self._source(link, request_id, intent=row.payload.get("work_event_intent"))
            admission = await prepare_attention(self.reader, link)
            composed = _RecoveryGuard(self, guard, admission, source)
            async with self.callbacks.writer(link, composed) as writer:
                row = await writer.get(TelegramOutboxRow, outbox_id, with_for_update=True, populate_existing=True)
                if not self._repairable(row) or row.payload["presentation_version"] != version:
                    return None
                await self._replace(writer, link, row, source, settings, repair=True)
                return outbox_id

        return await await_drained(perform())

    @staticmethod
    def _repairable(row):
        # Current review notifications originate in General. A retained topic
        # needs its own destination transition, not an unchanged blind retry.
        return row is not None and row.state == "failed" and row.failed_reason == "work-result-refresh-required" and all(getattr(row, field) is None for field in ("claim_owner", "claim_id", "claimed_at", "message_thread_id"))

    def _separate_reveal(self, reveal, fresh):
        return reveal is not None

    def _extras(self, source):
        return self.plans._extras(source)

    def _presentation(self, source, settings, link, row, concealed):
        return present_review_files(source, locale=settings.locale, concealed=concealed, outbox_id=row.id, **result_options(self._extras(source)["result_files"]))

    async def _replace(self, writer, link, row, source, settings, *, reveal=None, force_new=False, choice=None, repair=False):
        basis = row.payload.get("work_request_basis", {})
        if choice is not None or not self._owns_basis(basis) or reveal is not None and not basis.get("concealed"):
            raise ProductSourceRetired()
        if source.work.get("result_identity") != row.payload.get("work_result_identity"):
            raise ProductSourceRetired()
        parts = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]]
        if repair:
            if not self._repairable(row) or reveal is not None:
                raise ProductSourceRetired()
            selected = next((part for part in parts if part["state"] == "pending"), None)
        else:
            selected = next((part for part in parts if part["state"] == "sending"), parts[-1] if row.state == "sent" and parts else None)
        if selected is None:
            raise ProductSourceRetired()
        try:
            item = _item(row)
            _basis, previous, _recipe = frozen(SimpleNamespace(link=link, item=item, part=selected), acknowledged=selected["state"] == "sent", pending=repair)
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
        row.payload = {**row.payload, "work_request_basis": updated, "work_result_ledger": initial_ledger(fresh), "work_result_history": history, "work_result_navigation": source.work["result_navigation"]}
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
    def __init__(self, source):
        super().__init__(source)
        self.navigation_origin = NavigationOrigin()

    def extend(self, query):
        result = super().extend(query)
        self.navigation_column = len(result.column_descriptions)
        attempt = aliased(AgentWorkAttemptRow)
        return result.add_columns(attempt).outerjoin(attempt, and_(attempt.id == self.source.work["result_navigation"]["attempt_id"], attempt.work_id == AgentWorkRow.id))

    def check(self, rows):
        self.navigation_origin.check(self.source.work["result_navigation"])
        if result_identity(rows[0][8]) != self.source.work.get("result_identity"):
            raise ProductSourceRetired()
        try:
            require_navigation(self.source.work["result_navigation"], rows[0][self.navigation_column], self.source.work["id"])
        except ValueError:
            raise ProductSourceRetired() from None
        super().check(rows)
