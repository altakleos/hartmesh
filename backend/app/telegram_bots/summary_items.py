"""Explicit summary origins resolve current requests, never synthetic deliveries."""

from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace

from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired, _item
from app.telegram_bots.summary_delivery import frozen_summary
from app.telegram_bots.summary_sources import SummaryProjection
from app.telegram_bots.summary_views import frozen_view
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_members import _REQUEST_FIELDS, claim_of
from app.telegram_bots.work_digest_projection import DigestProjection
from app.telegram_bots.work_digest_views import parent_proof
from app.telegram_bots.work_navigation_origin import NavigationTarget, validate_origin
from app.telegram_bots.work_request_recovery import WorkRequestRecovery, _RecoveryProjection
from app.telegram_bots.work_request_source import require_show
from app.telegram_bots.work_result_recovery import WorkResultRecovery, _ResultRecoveryProjection
from app.telegram_bots.work_source_spaces import ReferencedSpaces
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow


@dataclass(frozen=True)
class SummaryItemSnapshot:
    link: object
    item: object
    source: object
    locale: str
    show_id: str | None


@dataclass(frozen=True)
class SummaryItemRead:
    owner: object
    snapshot: SummaryItemSnapshot
    anchor: object
    value: dict
    target: NavigationTarget

    async def authorize(self, *, session=None):
        admission = await prepare_attention(self.owner.plans.reader, self.snapshot.link)
        return await admission(session=session, projection=SummaryItemProjection(self))


class SummaryItemsNavigation:
    def __init__(self, plans, *, requests, reviews):
        if requests is None:
            raise ValueError("summary-navigation-requires-request-owner")
        if any(owner is not None and (owner.outbox is not plans.outbox or owner.callbacks is not plans.callbacks) for owner in (requests, reviews)):
            raise ValueError("summary-navigation-requires-shared-owners")
        self.plans, self.requests, self.reviews = plans, requests, reviews

    async def read(self, link, anchor, callback, query_key, *, expected=None):
        try:
            captured = SimpleNamespace(link=link, item=anchor, part=None)
            families = {"summary_basis", "summary_view_basis", "work_digest_basis", "work_digest_view_basis"}.intersection(anchor.payload)
            if families == {"summary_basis"}:
                basis, _ = frozen_summary(captured, acknowledged=True)
                if basis["concealed"]:
                    raise ProductSourceRetired()
                parent, show_id = anchor, None
            elif families == {"summary_view_basis"}:
                view, _ = frozen_view(captured, acknowledged=True)
                async with self.plans.outbox.sessions() as reader:
                    row = await reader.get(TelegramOutboxRow, view["parent_id"])
                    if row is None:
                        raise ProductSourceRetired()
                    parent = _item(row)
                basis, _ = frozen_summary(SimpleNamespace(link=link, item=parent, part=None), acknowledged=True)
                if not basis["concealed"] or parent_proof(parent) != view["parent_proof"] or parent.parts[-1]["message_ids"] != [view["target"]]:
                    raise ProductSourceRetired()
                show_id = view["callback_id"]
            else:
                raise ProductSourceRetired()
            if callback.target_kind != "request":
                raise ProductSourceRetired()
            source = await self.plans.reader.read(link, as_of=datetime.fromisoformat(basis["as_of"]), since=datetime.fromisoformat(basis["since"]))
            selected = next((entry for entry in source.sections["waiting"] if entry.get("request_id") == callback.target_id), None)
            if selected is None:
                raise ProductSourceRetired()
            review = selected["kind"] == "review"
            if review and self.reviews is None:
                raise ProductSourceRetired()
            recovery = WorkResultRecovery(self.reviews) if review else WorkRequestRecovery(self.requests)
            request, settings = await recovery._source(link, callback.target_id)
            if request.work["id"] != selected["work_id"]:
                raise ProductSourceRetired()
            value = validate_origin(
                {
                    "version": 1,
                    "kind": "summary",
                    "summary_id": parent.id,
                    "summary_proof": parent_proof(parent),
                    "anchor_outbox_id": anchor.id,
                    "anchor_proof": parent_proof(anchor),
                    "presentation_version": anchor.payload["presentation_version"],
                    "message_id": anchor.parts[-1]["message_ids"][0],
                    "callback_id": callback.id,
                    "member_id": callback.target_id,
                    "query_key": query_key,
                }
            )
            if expected is not None and not same_body(value, expected):
                raise ProductSourceRetired()
            target = NavigationTarget(request, "work_request_basis", request.work["id"], request.request["id"], None, review=review)
            snapshot = SummaryItemSnapshot(link, parent, source, settings.locale, show_id)
            result = SummaryItemRead(self, snapshot, anchor, value, target)
            await result.authorize()
            return result
        except (KeyError, TypeError, ValueError, PermanentDeliveryError):
            raise ProductSourceRetired() from None


class SummaryItemProjection(DigestProjection):
    def __init__(self, navigation):
        self.navigation = navigation
        request = _SummaryRequestProjection(navigation)
        super().__init__(navigation.snapshot.link, [SummaryProjection(navigation.snapshot.source), request])

    def extend(self, query):
        query = super().extend(query)
        self.column = len(query.column_descriptions)
        parent, proof, anchor, control, settings = aliased(TelegramOutboxRow), aliased(TelegramCallbackRow), aliased(TelegramOutboxRow), aliased(TelegramCallbackRow), TelegramPersonSettingsRow
        self.callback_column = self.column + 3
        return (
            query.add_columns(parent, proof, anchor, control, settings)
            .join(parent, parent.id == self.navigation.snapshot.item.id)
            .outerjoin(proof, proof.group_id == self.navigation.snapshot.item.parts[-1]["callback_group_id"])
            .join(anchor, anchor.id == self.navigation.anchor.id)
            .outerjoin(control, control.group_id == self.navigation.anchor.parts[-1]["callback_group_id"])
            .join(settings, settings.owner_user_id == parent.owner_user_id)
        )

    def check_with_permissions(self, rows, held):
        super().check_with_permissions(rows, held)
        n = self.navigation
        parent, _, anchor, _, settings = rows[0][self.column : self.column + 5]
        if "agents:write" not in held or settings.locale != n.snapshot.locale:
            raise ProductSourceRetired()
        for row, item, proof in ((parent, n.snapshot.item, n.value["summary_proof"]), (anchor, n.anchor, n.value["anchor_proof"])):
            if _item(row) != item or parent_proof(_item(row)) != proof or row.state != "sent" or claim_of(row) != (None, None, None) or row.message_id != row.parts[-1]["message_ids"][0]:
                raise ProductSourceRetired()
        originals = {row[self.column + 1].id: row[self.column + 1] for row in rows if row[self.column + 1] is not None}
        controls = {row[self.callback_column].id: row[self.callback_column] for row in rows if row[self.callback_column] is not None}
        try:
            _validate_rows(parent, list(originals.values()))
            _validate_rows(anchor, list(controls.values()))
            if n.snapshot.show_id is not None:
                require_show(originals.get(n.snapshot.show_id), parent)
            for control in originals.values():
                if aware(control.created_at) != aware(parent.created_at) or control.message_id != parent.message_id:
                    raise CallbackRefused()
                if n.snapshot.show_id is not None:
                    n.owner.plans.callbacks._unexpired(control)
                    if control.closed_reason != "revealed" or control.consumed_at is None:
                        raise CallbackRefused()
            for control in controls.values():
                if control.kind == "summary_off" and control.consumed_at is not None and control.closed_reason == "done":
                    n.owner.plans.callbacks._unexpired(control)
                else:
                    n.owner.plans.callbacks._usable(control)
                if control.message_id != anchor.message_id or aware(control.created_at) != aware(parent.created_at):
                    raise CallbackRefused()
            selected = controls.get(n.value["callback_id"])
            if selected is None or (selected.kind, selected.target_kind, selected.target_id) != ("digest_item", "request", n.target.request_id):
                raise CallbackRefused()
        except CallbackRefused:
            raise ProductSourceRetired() from None


class _SummaryRequestProjection:
    """Full current request semantics and visible references without a delivery row."""

    def __init__(self, navigation):
        self.source = navigation.target.source
        self.base = _ResultRecoveryProjection(self.source) if navigation.target.review else _RecoveryProjection(self.source)
        reader = navigation.owner.requests.reader
        self.spaces = ReferencedSpaces(reader, reader.links.session_factory, self.source)

    def extend(self, query):
        query = self.base.extend(query)
        self.column = len(query.column_descriptions)
        return query.add_columns(self.spaces.expression())

    def check_with_permissions(self, rows, held):
        self.base.check([row[: self.column] for row in rows])
        work, request = rows[0][8:10]
        if (
            "agents:write" not in held
            or work.objective != self.source.work["objective"]
            or rows[0][1].name != self.source.request["instance_name"]
            or not same_body({key: getattr(request, key) for key in _REQUEST_FIELDS}, {key: self.source.request.get(key) for key in _REQUEST_FIELDS})
        ):
            raise ProductSourceRetired()
        self.spaces.check(rows[0][self.column])
