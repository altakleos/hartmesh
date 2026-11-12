"""Current full-source qualification for an acknowledged summary's controls."""

from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace

from sqlalchemy.orm import aliased

from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired, _item
from app.telegram_bots.summary_delivery import frozen_summary
from app.telegram_bots.work_digest_members import claim_of
from app.telegram_bots.work_digest_navigation import _same_item
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class SummaryNavigationSnapshot:
    owner: object
    link: object
    item: object
    source: object
    settings: object
    callback_id: str
    closed_reason: str | None

    async def authorize(self, *, session=None):
        return await self.source.authorize(session=session, projection=SummaryNavigationProjection(self))


class SummaryNavigation:
    def __init__(self, plans):
        self.plans = plans

    async def read(self, link, identity, *, callback_id, closed_reason=None):
        async def perform():
            if closed_reason not in {None, "revealed"}:
                raise ProductSourceRetired()
            try:
                async with self.plans.outbox.sessions() as reader:
                    row = await reader.get(TelegramOutboxRow, identity)
                    if row is None:
                        raise ProductSourceRetired()
                    item = _item(row)
                basis, _ = frozen_summary(SimpleNamespace(link=link, item=item, part=None), acknowledged=True)
                if basis["concealed"] is not True:
                    raise ProductSourceRetired()
                source = await self.plans.reader.read(link, as_of=datetime.fromisoformat(basis["as_of"]), since=datetime.fromisoformat(basis["since"]))
                settings = await self.plans.settings.get(link.owner_user_id)
                if settings is None:
                    raise ProductSourceRetired()
                snapshot = SummaryNavigationSnapshot(self, link, item, source, settings, callback_id, closed_reason)
                await snapshot.authorize()
                return snapshot
            except (ValueError, TypeError, KeyError, PermanentDeliveryError):
                raise ProductSourceRetired() from None

        return await await_drained(perform())


class SummaryNavigationProjection:
    def __init__(self, snapshot):
        self.snapshot = snapshot

    def extend(self, query):
        self.column = len(query.column_descriptions)
        parent, control, settings = aliased(TelegramOutboxRow), aliased(TelegramCallbackRow), TelegramPersonSettingsRow
        return (
            query.add_columns(parent, control, settings)
            .join(parent, parent.id == self.snapshot.item.id)
            .outerjoin(control, control.group_id == self.snapshot.item.parts[-1]["callback_group_id"])
            .join(settings, settings.owner_user_id == parent.owner_user_id)
        )

    def check_with_permissions(self, rows, held):
        snapshot = self.snapshot
        parent, _, settings = rows[0][self.column : self.column + 3]
        if not _same_item(parent, snapshot.item) or parent.state != "sent" or claim_of(parent) != (None, None, None):
            raise ProductSourceRetired()
        if (settings.locale, settings.timezone, settings.summary_minute) != (snapshot.settings.locale, snapshot.settings.timezone, snapshot.settings.summary_minute):
            raise ProductSourceRetired()
        if parent.message_id != parent.parts[-1]["message_ids"][0]:
            raise ProductSourceRetired()
        controls = {row[self.column + 1].id: row[self.column + 1] for row in rows if row[self.column + 1] is not None}
        selected = controls.get(snapshot.callback_id)
        try:
            _validate_rows(parent, list(controls.values()))
            for control in controls.values():
                if snapshot.closed_reason is None:
                    snapshot.owner.plans.callbacks._usable(control)
                else:
                    if control.consumed_at is None or control.closed_reason != snapshot.closed_reason:
                        raise CallbackRefused()
                    snapshot.owner.plans.callbacks._unexpired(control)
                if control.message_id != parent.message_id or aware(control.created_at) != aware(parent.created_at):
                    raise CallbackRefused()
            if selected is None or (selected.kind, selected.target_kind, selected.target_id) != ("reveal", "outbox", parent.id):
                raise CallbackRefused()
        except CallbackRefused:
            raise ProductSourceRetired() from None
