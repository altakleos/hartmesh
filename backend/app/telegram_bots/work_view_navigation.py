"""Full current request previews from actual acknowledged Work details."""

from dataclasses import dataclass

from sqlalchemy.orm import aliased

from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_detail_cards import full_detail
from app.telegram_bots.work_digest_members import claim_of
from app.telegram_bots.work_navigation_origin import NavigationRead, NavigationTarget, validate_origin
from app.telegram_bots.work_news_delivery import prepare_news_projection
from app.telegram_bots.work_view_origins import acknowledged_view, anchor_signature
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow


@dataclass(frozen=True)
class WorkViewNavigationSnapshot:
    link: object
    item: object
    source: object
    locale: str
    projection: object


@dataclass(frozen=True)
class WorkViewNavigationRead:
    owner: object
    snapshot: WorkViewNavigationSnapshot
    anchor: object
    value: dict
    target: NavigationTarget

    authorize = NavigationRead.authorize


class WorkViewNavigation:
    def __init__(self, plans, *, requests, reviews):
        if requests is None or requests.outbox is not plans.outbox or requests.callbacks is not plans.callbacks:
            raise ValueError("work-detail-navigation-requires-shared-owners")
        self.plans, self.requests, self.reviews = plans, requests, reviews

    async def read(self, link, item, callback, query_key, *, expected=None):
        original = acknowledged_view(link, item, callback.payload.get("presentation_version"))
        basis = original.payload["work_news_basis"]
        if callback.kind != "digest_item" or callback.target_kind != "request":
            raise ProductSourceRetired()
        displayed = full_detail(basis["source"])
        request = displayed.get("request")
        if request is None or callback.target_id != request["id"] or callback.payload.get("work_id") != displayed["record"]["id"] or callback.payload.get("request_id") != request["id"]:
            raise ProductSourceRetired()
        source = await self.plans.reader.read(link, item)
        current = full_detail(source).get("request")
        if current is None or current["id"] != request["id"]:
            raise ProductSourceRetired()
        preview = await self.requests.reader.read_request(link, request["id"])
        if preview.request["state"] != "pending" or preview.request["needs_routing"] or preview.request["recipient_id"] != link.owner_user_id or preview.request["purpose"] == "review":
            raise ProductSourceRetired()
        if not (preview.request["can_respond"] if preview.request["purpose"] == "information" else preview.request["can_manage"]):
            raise ProductSourceRetired()
        value = validate_origin(
            {
                "version": 1,
                "kind": "view",
                "view_id": item.id,
                "view_proof": anchor_signature(original),
                "anchor_outbox_id": item.id,
                "anchor_proof": anchor_signature(original),
                "presentation_version": original.payload["presentation_version"],
                "message_id": original.parts[-1]["message_ids"][0],
                "callback_id": callback.id,
                "member_id": request["id"],
                "query_key": query_key,
            }
        )
        if expected is not None and not same_body(value, expected):
            raise ProductSourceRetired()
        clock = await self.plans.reader.prepare_clock(link, item)
        projection = await prepare_news_projection(self.plans.projection(self.plans.reader, link, item, source, clock=clock))
        snapshot = WorkViewNavigationSnapshot(link, item, source, basis["locale"], projection)
        result = WorkViewNavigationRead(self, snapshot, original, value, NavigationTarget(preview, "work_request_basis", preview.work["id"], request["id"], None))
        await result.authorize()
        return result


class WorkViewNavigationProjection:
    def __init__(self, navigation):
        snapshot = navigation.snapshot
        self.navigation, self.delegate, self.link = navigation, snapshot.projection, snapshot.link

    def extend(self, query):
        query = self.delegate.extend(query)
        self.delivery_column = self.delegate.delivery_column
        self.callback_column = len(query.column_descriptions)
        control = aliased(TelegramCallbackRow)
        return query.add_columns(control).outerjoin(control, control.group_id == self.navigation.anchor.parts[-1]["callback_group_id"])

    def check_with_permissions(self, rows, held):
        self.delegate.check_with_permissions(rows, held)
        navigation = self.navigation
        parent = rows[0][self.delivery_column]
        original = acknowledged_view(self.link, parent, navigation.value["presentation_version"])
        if original != navigation.anchor or claim_of(parent) != (None, None, None) or anchor_signature(original) != navigation.value["anchor_proof"]:
            raise ProductSourceRetired()
        controls = {control.id: control for row in rows if (control := row[self.callback_column]) is not None}
        try:
            _validate_rows(original, list(controls.values()))
            for control in controls.values():
                navigation.owner.plans.callbacks._usable(control)
            selected = controls.get(navigation.value["callback_id"])
            if (
                selected is None
                or selected.kind != "digest_item"
                or selected.target_kind != "request"
                or selected.target_id != navigation.target.request_id
                or selected.message_id != navigation.value["message_id"]
                or selected.message_thread_id != original.message_thread_id
            ):
                raise CallbackRefused()
        except CallbackRefused:
            raise ProductSourceRetired() from None
