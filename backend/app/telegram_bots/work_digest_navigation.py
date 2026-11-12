"""Current source admission for controls on an acknowledged digest card."""

from dataclasses import dataclass
from types import SimpleNamespace

from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.digests import _members
from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_delivery import frozen_digest, member_proof
from app.telegram_bots.work_digest_members import claim_of
from app.telegram_bots.work_digest_projection import DigestProjection
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class DigestNavigationSnapshot:
    owner: object
    link: object
    item: object
    siblings: tuple
    members: tuple
    locale: str
    callback_id: str
    member_id: str | None
    closed_reason: str | None = None

    async def authorize(self, *, session=None):
        async def perform():
            admission = await prepare_attention(self.owner.plans.reader, self.link)
            return await admission(session=session, projection=_NavigationProjection(self))

        return await await_drained(perform())


class DigestNavigation:
    def __init__(self, plans):
        self.plans = plans

    async def read(self, link, identity, *, callback_id, member_id=None, closed_reason=None):
        """Read previews without consuming controls or changing the batch ACK."""

        async def perform():
            if closed_reason not in {None, "revealed"}:
                raise ProductSourceRetired()
            try:
                async with self.plans.outbox.sessions() as reader:
                    parent = await reader.get(TelegramOutboxRow, identity)
                    if parent is None or (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.instance_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref):
                        raise ProductSourceRetired()
                    item = _item(parent)
                    basis, _ = frozen_digest(SimpleNamespace(link=link, item=item, part=None), acknowledged=True)
                    siblings = tuple(_item(row) for row in await _members(reader, parent))
                    if member_id is not None and member_id not in {row.id for row in siblings}:
                        raise ProductSourceRetired()
                settings = await self.plans.settings.get(link.owner_user_id)
                if settings is None:
                    raise ProductSourceRetired()
                selected = siblings if member_id is None else tuple(row for row in siblings if row.id == member_id)
                members = tuple([await self.plans.members.read(link, row, locale=settings.locale) for row in selected])
                origins = {proof["id"]: proof["origin"] for proof in basis["members"]}
                if any(member_proof(member)["origin"] != origins[member.item.id] for member in members):
                    raise ProductSourceRetired()
                snapshot = DigestNavigationSnapshot(self, link, item, siblings, members, settings.locale, callback_id, member_id, closed_reason)
                await snapshot.authorize()
                return snapshot
            except (ValueError, TypeError, KeyError, PermanentDeliveryError):
                raise ProductSourceRetired() from None

        return await await_drained(perform())


class _NavigationProjection(DigestProjection):
    def __init__(self, snapshot):
        super().__init__(snapshot.link, [member.projection for member in snapshot.members])
        self.snapshot = snapshot

    def extend(self, query):
        query = super().extend(query)
        self.column = len(query.column_descriptions)
        parent, child, control, settings = TelegramOutboxRow, aliased(TelegramOutboxRow), TelegramCallbackRow, TelegramPersonSettingsRow
        return (
            query.add_columns(parent, child, control, settings)
            .join(parent, parent.id == self.snapshot.item.id)
            .join(child, child.id.in_([row.id for row in self.snapshot.siblings]))
            .outerjoin(control, control.group_id == self.snapshot.item.parts[0]["callback_group_id"])
            .join(settings, settings.owner_user_id == parent.owner_user_id)
        )

    def check_with_permissions(self, rows, held):
        super().check_with_permissions(rows, held)
        snapshot = self.snapshot
        parent, _, _, settings = rows[0][self.column : self.column + 4]
        if not _same_item(parent, snapshot.item) or parent.state != "sent" or claim_of(parent) != (None, None, None) or settings.locale != snapshot.locale:
            raise ProductSourceRetired()
        part = parent.parts[0]
        if parent.message_id != part["message_ids"][0]:
            raise ProductSourceRetired()
        children = {row[self.column + 1].id: row[self.column + 1] for row in rows}
        if set(children) != {item.id for item in snapshot.siblings}:
            raise ProductSourceRetired()
        for item in snapshot.siblings:
            child = children[item.id]
            if not _same_item(child, item) or (child.state, child.skip_reason) != ("skipped", "in-digest") or claim_of(child) != (None, None, None):
                raise ProductSourceRetired()
        controls = {row[self.column + 2].id: row[self.column + 2] for row in rows if row[self.column + 2] is not None}
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
                if control.message_id != parent.message_id:
                    raise CallbackRefused()
            if selected is None or selected.target_kind != "outbox":
                raise CallbackRefused()
            if snapshot.member_id is None or snapshot.closed_reason == "revealed":
                if selected.kind != "reveal" or selected.target_id != parent.id or not parent.payload["work_digest_basis"]["concealed"]:
                    raise CallbackRefused()
            elif selected.kind != "digest_item" or selected.target_id != snapshot.member_id:
                raise CallbackRefused()
        except CallbackRefused:
            raise ProductSourceRetired() from None


def _same_item(row, expected):
    actual = _item(row)
    return actual == expected and same_body(actual.payload, expected.payload) and same_body(actual.parts, expected.parts)
