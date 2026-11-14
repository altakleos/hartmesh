"""Explicit, reusable navigation origins for current full Work previews."""

from copy import deepcopy
from dataclasses import dataclass
from types import SimpleNamespace

from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_delivery import frozen_digest
from app.telegram_bots.work_digest_members import claim_of
from app.telegram_bots.work_digest_navigation import DigestNavigation, _NavigationProjection
from app.telegram_bots.work_digest_views import frozen_view, parent_proof
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def validate_origin(value):
    if type(value) is not dict or value.get("kind") not in {"digest", "summary", "view"}:
        raise ProductSourceRetired()
    root = value["kind"]
    if set(value) != {"version", "kind", root + "_id", root + "_proof", "anchor_outbox_id", "anchor_proof", "presentation_version", "message_id", "callback_id", "member_id", "query_key"}:
        raise ProductSourceRetired()
    if type(value["version"]) is not int or value["version"] != 1 or type(value["presentation_version"]) is not int or value["presentation_version"] < 0 or type(value["message_id"]) is not int or value["message_id"] <= 0:
        raise ProductSourceRetired()
    for key in (root + "_id", "anchor_outbox_id", "callback_id", "member_id", root + "_proof", "anchor_proof", "query_key"):
        size = 64 if key in {root + "_proof", "anchor_proof", "query_key"} else 32
        if type(value[key]) is not str or len(value[key]) != size or any(char not in "0123456789abcdef" for char in value[key]):
            raise ProductSourceRetired()
    return value


def frozen_anchor(link, item):
    captured = SimpleNamespace(link=link, item=item, part=None)
    try:
        families = {"work_digest_basis", "work_digest_view_basis"}.intersection(item.payload)
        if families == {"work_digest_basis"}:
            frozen_digest(captured, acknowledged=True)
            return item.id, None
        if families == {"work_digest_view_basis"}:
            basis, _ = frozen_view(captured, acknowledged=True)
            return basis["parent_id"], basis["callback_id"]
    except (ValueError, TypeError, KeyError, PermanentDeliveryError):
        pass
    raise ProductSourceRetired()


@dataclass(frozen=True)
class NavigationTarget:
    source: object
    family: str
    work_id: str
    request_id: str | None
    event_id: str | None
    review: bool = False
    routing: bool = False
    intent: dict | None = None


@dataclass(frozen=True)
class NavigationRead:
    owner: object
    snapshot: object
    anchor: object
    value: dict

    @property
    def member(self):
        return self.snapshot.members[0]

    @property
    def target(self):
        item = self.member.item
        family = next(key for key in ("work_request_basis", "work_news_basis", "result_notice_basis") if key in item.payload)
        basis = item.payload[family]
        return NavigationTarget(
            self.member.source,
            family,
            item.work_id,
            item.request_id,
            item.event_id,
            review="result_files" in basis,
            routing=family == "work_request_basis" and basis["version"] in {5, 6},
            intent={key: deepcopy(item.payload[key]) for key in ("work_event_intent", "work_event_source") if key in item.payload},
        )

    async def authorize(self, *, session=None):
        async def perform():
            admission = await prepare_attention(self.owner.plans.reader, self.snapshot.link)
            return await admission(session=session, projection=NavigationOriginProjection(self))

        return await await_drained(perform())


class WorkNavigation:
    def __init__(self, plans, *, summaries=None, requests=None, reviews=None, views=None):
        self.plans, self.digests = plans, DigestNavigation(plans)
        self.views = {}
        if views is not None:
            from app.telegram_bots.work_detail_cards import DETAIL_INTENTS
            from app.telegram_bots.work_view_navigation import WorkViewNavigation

            if type(views) is not dict or not views or not set(views).issubset(DETAIL_INTENTS):
                raise ValueError("invalid-work-detail-owners")
            self.views = {key: WorkViewNavigation(owner, requests=requests, reviews=reviews) for key, owner in views.items()}
        self.summaries = None
        if summaries is not None:
            from app.telegram_bots.summary_items import SummaryItemsNavigation

            self.summaries = SummaryItemsNavigation(summaries, requests=requests, reviews=reviews)

    async def read(self, link, *, callback_id, query_id):
        if type(query_id) is not str or not 1 <= len(query_id) <= 256 or type(callback_id) is not str or len(callback_id) != 32 or any(char not in "0123456789abcdef" for char in callback_id):
            raise CallbackRefused()
        # A later press of the same reusable button is a fresh publication;
        # redelivery of one Telegram query keeps its original identity.
        key = digest([link.id, link.binding_generation, callback_id, query_id])
        return await await_drained(self._read(link, callback_id, key))

    async def restore(self, link, value):
        expected = deepcopy(validate_origin(value))
        return await await_drained(self._read(link, expected["callback_id"], expected["query_key"], expected=expected))

    async def _read(self, link, callback_id, query_key, *, expected=None):
        async with self.plans.outbox.sessions() as reader:
            callback = await reader.get(TelegramCallbackRow, callback_id)
            if callback is None or callback.kind != "digest_item" or callback.target_kind not in {"outbox", "request"} or callback.link_id != link.id or type(callback.payload) is not dict:
                raise ProductSourceRetired()
            row = await reader.get(TelegramOutboxRow, callback.payload.get("anchor_outbox_id"))
            if row is None or (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.instance_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref):
                raise ProductSourceRetired()
            item, member_id = _item(row), callback.target_id
        if self.summaries is not None and {"summary_basis", "summary_view_basis"}.intersection(item.payload):
            return await self.summaries.read(link, item, callback, query_key, expected=expected)
        if set(self.views).intersection(item.payload):
            from app.telegram_bots.work_detail_cards import detail_card_owner

            return await detail_card_owner(item, self.views).read(link, item, callback, query_key, expected=expected)
        if callback.target_kind != "outbox":
            raise ProductSourceRetired()
        digest_id, show_id = frozen_anchor(link, item)
        snapshot = await self.digests.read(link, digest_id, callback_id=show_id or callback_id, member_id=member_id, closed_reason="revealed" if show_id else None)
        if show_id and item.payload["work_digest_view_basis"]["parent_proof"] != parent_proof(snapshot.item):
            raise ProductSourceRetired()
        value = validate_origin(
            {
                "version": 1,
                "kind": "digest",
                "digest_id": digest_id,
                "digest_proof": parent_proof(snapshot.item),
                "anchor_outbox_id": item.id,
                "anchor_proof": parent_proof(item),
                "presentation_version": item.payload["presentation_version"],
                "message_id": item.parts[-1]["message_ids"][0],
                "callback_id": callback_id,
                "member_id": member_id,
                "query_key": query_key,
            }
        )
        if expected is not None and not same_body(value, expected):
            raise ProductSourceRetired()
        result = NavigationRead(self, snapshot, item, value)
        await result.authorize()
        return result


class _DigestOriginProjection(_NavigationProjection):
    """Original source plus the exact acknowledged navigation group, one read."""

    def __init__(self, navigation):
        super().__init__(navigation.snapshot)
        self.navigation = navigation

    def extend(self, query):
        query = super().extend(query)
        if self.navigation.anchor.id == self.snapshot.item.id:
            self.origin_column = None
            return query
        self.origin_column = len(query.column_descriptions)
        parent, control = aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        return query.add_columns(parent, control).join(parent, parent.id == self.navigation.anchor.id).outerjoin(control, control.group_id == self.navigation.anchor.parts[-1]["callback_group_id"])

    def check_with_permissions(self, rows, held):
        super().check_with_permissions(rows, held)
        navigation = self.navigation
        if parent_proof(_item(rows[0][self.column])) != navigation.value["digest_proof"]:
            raise ProductSourceRetired()
        if self.origin_column is None:
            parent = rows[0][self.column]
            if _item(parent) != navigation.anchor or parent_proof(_item(parent)) != navigation.value["anchor_proof"]:
                raise ProductSourceRetired()
            frozen_anchor(navigation.snapshot.link, _item(parent))
            return
        parent = rows[0][self.origin_column]
        if _item(parent) != navigation.anchor or parent_proof(_item(parent)) != navigation.value["anchor_proof"] or parent.state != "sent" or claim_of(parent) != (None, None, None) or parent.message_id != navigation.value["message_id"]:
            raise ProductSourceRetired()
        frozen_anchor(navigation.snapshot.link, _item(parent))
        controls = {control.id: control for *_, control in rows if control is not None}
        selected = controls.get(navigation.value["callback_id"])
        try:
            _validate_rows(parent, list(controls.values()))
            for control in controls.values():
                navigation.owner.plans.callbacks._usable(control)
                if control.message_id != parent.message_id:
                    raise CallbackRefused()
            if selected is None or selected.kind != "digest_item" or selected.target_kind != "outbox" or selected.target_id != navigation.member.item.id:
                raise CallbackRefused()
        except CallbackRefused:
            raise ProductSourceRetired() from None


class NavigationOriginProjection:
    """Compose an explicitly typed origin with each existing preview owner."""

    def __init__(self, navigation):
        self.navigation = navigation
        if navigation.value["kind"] == "view":
            from app.telegram_bots.work_view_navigation import WorkViewNavigationProjection

            self.delegate = WorkViewNavigationProjection(navigation)
        elif navigation.value["kind"] == "summary":
            from app.telegram_bots.summary_items import SummaryItemProjection

            self.delegate = SummaryItemProjection(navigation)
        else:
            self.delegate = _DigestOriginProjection(navigation)

    def extend(self, query):
        return self.delegate.extend(query)

    @property
    def callback_column(self):
        if self.navigation.value["kind"] in {"summary", "view"}:
            return self.delegate.callback_column
        delegate = self.delegate
        return delegate.column + 2 if delegate.origin_column is None else delegate.origin_column + 1

    def check_with_permissions(self, rows, held):
        self.delegate.check_with_permissions(rows, held)
