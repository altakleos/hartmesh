"""Complete assignment previews beneath source-owned event handovers."""

from dataclasses import replace
from datetime import datetime

from app.telegram_bots.attempt_text import ATTEMPT_KEYS

ACTION_NEWS_KEYS = frozenset({"decision.recorded", "result.changes", "attempt.stopped", "attempt.stopped_changed", "work.proposed", "request.answered"}) | ATTEMPT_KEYS


async def read_news_detail(reader, link, item):
    if item.payload.get("work_event_intent", {}).get("key") not in ACTION_NEWS_KEYS:
        return None
    return await reader.detail(link, item.work_id)


def detail_dates(detail, clock):
    def date(value):
        return datetime.fromisoformat(value).astimezone(clock.zone).strftime("%Y-%m-%d %H:%M") if value else None

    return {"date": date(detail.record["created_at"]), "due": date(detail.record["due_at"])}


def present_news_detail(source, basis, link, view):
    """Rendering reuse does not change the event carrier's source identity."""
    from app.telegram_bots.split import split_html, visible_text
    from app.telegram_bots.work_view_sources import present_view
    from app.telegram_bots.work_views import WorkViewPresentation

    if basis["concealed"] or source.key not in ACTION_NEWS_KEYS or "detail" not in source.work:
        return view
    if source.key == "request.answered" and source.values.get("open_assignment") is not True:
        return view
    detail = present_view(replace(source, key="view.detail", values=source.work["detail_dates"]), basis, link)
    allowed = {"work_start", "work_change"} if source.key == "attempt.no_result" and basis.get("manager_navigation") == 1 else {"work_start"}
    controls = tuple(tuple(button for button in line if button.get("kind") in allowed) for line in detail.keyboard)
    controls = tuple(line for line in controls if line)
    if not controls:
        return view
    html = "\n\n".join("".join(part["html"] for part in presentation.parts) for presentation in (view, detail))
    parts = tuple(dict(kind="text", html=chunk, visible_text=visible_text(chunk), protect_content=source.protected) for chunk in split_html(html))
    return WorkViewPresentation(parts, (*view.keyboard, *controls))


async def news_origin_projection(reader, link, parent, original, *, activation_id=None, historical=False):
    """Prepare the actual native event behind this acknowledged news carrier."""
    from app.telegram_bots.producer import ProductSourceRetired, _item
    from app.telegram_bots.work_news_sources import WorkNewsProjection, WorkNewsRead, WorkNewsReader
    from app.telegram_bots.work_status_control_origins import carrier_source, resume_projection

    value = carrier_source(original)
    if "work_outcome_basis" in original.payload:
        from app.telegram_bots.work_outcome_origins import OutcomeOriginProjection

        return OutcomeOriginProjection(link, original)
    if value["key"] == "status.stopped":
        if "work_event_intent" not in original.payload:
            raise ProductSourceRetired()
        return await resume_projection(reader, link, parent, original)
    if value["key"] not in ACTION_NEWS_KEYS or "work_event_intent" not in original.payload:
        return None
    source_reader = WorkNewsReader(work=reader.work, authority=reader.authority, links=reader.links)
    item = _item(parent)
    return WorkNewsProjection(source_reader, link, item, origin=WorkNewsRead(**value), clock=await source_reader.prepare_clock(link, item), activation_id=activation_id, historical=historical)


async def prepare_news_origin(reader, link, parent, original):
    from app.telegram_bots.attention_authority import prepare_attention

    projection = await news_origin_projection(reader, link, parent, original)
    if projection is None:
        return None
    admission = await prepare_attention(reader, link)

    async def check(*, session):
        await admission(session=session, projection=projection)

    return check


def activation_news_origin(original):
    from app.telegram_bots.work_status_control_origins import carrier_source
    from app.telegram_bots.work_view_origins import anchor_signature

    return {
        "version": 1,
        "anchor_outbox_id": original.id,
        "presentation_version": original.payload["presentation_version"],
        "signature": anchor_signature(original),
        "event_signature": carrier_source(original)["event_signature"],
    }


def require_news_origin(value):
    from collections.abc import Mapping

    if (
        not isinstance(value, Mapping)
        or set(value) != {"version", "anchor_outbox_id", "presentation_version", "signature", "event_signature"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or type(value["presentation_version"]) is not int
        or value["presentation_version"] < 0
        or any(type(value[key]) is not str or len(value[key]) != size or any(char not in "0123456789abcdef" for char in value[key]) for key, size in (("anchor_outbox_id", 32), ("signature", 64), ("event_signature", 64)))
    ):
        raise ValueError("invalid-activation-news-origin")
    return value


def news_witness_id(act_id):
    from app.telegram_bots.work_result_identity import digest

    return digest([act_id, "news-activation-origin"])[:32]


async def save_news_witness(writer, link, act, original):
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    identity = news_witness_id(act.id)
    if await writer.get(TelegramCallbackRow, identity) is not None:
        raise ProductSourceRetired()
    writer.add(
        TelegramCallbackRow(
            id=identity,
            group_id=identity,
            link_id=link.id,
            binding_generation=link.binding_generation,
            chat_id=link.chat_id,
            message_thread_id=original.message_thread_id,
            message_id=None,
            kind="work_activation_origin",
            target_kind="work",
            target_id=act.work_id,
            payload={
                "news_origin": activation_news_origin(original),
                "work_action_id": act.id,
                "anchor_outbox_id": original.id,
                "source_group_id": original.parts[-1]["callback_group_id"],
                "assignment_revision": act.body["expected_assignment_revision"],
            },
            created_at=act.created_at,
        )
    )


async def news_witness_dependencies(session, control, journal, binding):
    """Damaged locators hold their own witness without pinning another owner."""
    from sqlalchemy import select

    from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired
    from app.telegram_bots.work_actions import same_body
    from app.telegram_bots.work_view_origins import acknowledged_view
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

    try:
        # Retain the witness's own original receipt even when its admitted
        # journal is lost. This never supplies permission to execute an Act.
        action_id = control.payload.get("work_action_id")
        marker = require_news_origin(control.payload.get("news_origin"))
        if type(action_id) is not str or len(action_id) != 32 or control.id != news_witness_id(action_id) or control.group_id != control.id:
            return set(), set()
        parent = await session.get(TelegramOutboxRow, marker["anchor_outbox_id"])
        if (
            parent is None
            or control.payload.get("anchor_outbox_id") != parent.id
            or (control.link_id, control.binding_generation, control.chat_id, control.kind, control.target_kind) != (binding.id, binding.binding_generation, binding.chat_id, "work_activation_origin", "work")
            or (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.instance_id, parent.work_id, parent.message_thread_id)
            != (binding.id, binding.owner_user_id, binding.binding_generation, binding.chat_id, binding.agent_ref, control.target_id, control.message_thread_id)
        ):
            return set(), set()
        original = acknowledged_view(binding, parent, marker["presentation_version"])
        group_id = original.parts[-1]["callback_group_id"]
        if not same_body(marker, activation_news_origin(original)) or control.payload.get("source_group_id") != group_id:
            return set(), set()
        selected = await session.get(TelegramCallbackRow, action_id)
        if selected is not None and (
            selected.group_id != group_id
            or selected.payload.get("anchor_outbox_id") != parent.id
            or selected.payload.get("presentation_version") != marker["presentation_version"]
            or (selected.kind, selected.target_kind, selected.target_id) != ("work_start", "work", control.target_id)
        ):
            return set(), set()
        members = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group_id)))
        # Missing public controls cannot renew consent. The actual complete
        # SDK receipt and independent scoped witness still retain its history.
        if any(
            (row.link_id, row.binding_generation, row.chat_id, row.message_thread_id, row.payload.get("anchor_outbox_id"), row.payload.get("work_action_id"))
            != (binding.id, binding.binding_generation, binding.chat_id, control.message_thread_id, parent.id, action_id)
            for row in members
        ):
            return set(), set()
        return {group_id}, {parent.id}
    except (KeyError, TypeError, ValueError, ProductSourceRetired, PermanentDeliveryError):
        return set(), set()


class ActivationNewsOrigin:
    """The original news event and SDK acknowledgements survive to native entry."""

    def __init__(self, act, *, for_effect=True):
        self.act, self.projection = act, None
        self.for_effect = for_effect

    async def prepare(self, acts, link):
        from collections.abc import Mapping

        from app.telegram_bots.producer import ProductSourceRetired
        from app.telegram_bots.work_actions import same_body
        from app.telegram_bots.work_view_origins import acknowledged_view
        from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

        marker = self.act.continuation.get("news_origin") if isinstance(self.act.continuation, Mapping) else None
        self.marker = marker
        async with acts.session_factory() as reader:
            callback = await reader.get(TelegramCallbackRow, self.act.id)
            witness = await reader.get(TelegramCallbackRow, news_witness_id(self.act.id))
            parent = await reader.get(TelegramOutboxRow, callback.payload.get("anchor_outbox_id")) if callback else None
            from app.telegram_bots.work_status_control_origins import is_status_carrier

            status_parent = is_status_carrier(parent)
            from app.telegram_bots.work_outcome_origins import is_outcome_carrier

            outcome_parent = is_outcome_carrier(parent)
            if outcome_parent and "work_outcome_basis" not in parent.payload:
                raise ProductSourceRetired()
            if not outcome_parent and (parent is None or "work_event_intent" not in parent.payload or not {"work_news_basis", "work_status_update"}.intersection(parent.payload)):
                if marker is not None or witness is not None or status_parent:
                    raise ProductSourceRetired()
                return
            original = acknowledged_view(link, parent, callback.payload.get("presentation_version"))
        self.projection = await news_origin_projection(acts, link, parent, original, activation_id=self.act.id, historical=not self.for_effect)
        if self.projection is not None and (witness is None or not same_body(marker, activation_news_origin(original))):
            raise ProductSourceRetired()
        self.link = link

    def extend(self, query):
        if self.projection is None:
            return query
        from sqlalchemy import and_
        from sqlalchemy.orm import aliased

        from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

        callback, parent, control, witness = aliased(TelegramCallbackRow), aliased(TelegramOutboxRow), aliased(TelegramCallbackRow), aliased(TelegramCallbackRow)
        self.column = len(query.column_descriptions)
        result = (
            query.add_columns(callback, parent, control, witness)
            .outerjoin(callback, callback.id == self.act.id)
            .outerjoin(parent, parent.id == callback.payload["anchor_outbox_id"].as_string())
            .outerjoin(control, and_(control.group_id == callback.group_id, control.payload["anchor_outbox_id"].as_string() == parent.id))
            .outerjoin(witness, witness.id == news_witness_id(self.act.id))
        )
        return self.projection.extend(result)

    def check(self, rows, actual, held):
        if self.projection is None:
            return
        from app.telegram_bots.acts import _snapshot
        from app.telegram_bots.producer import ProductSourceRetired
        from app.telegram_bots.work_actions import same_body
        from app.telegram_bots.work_execution_sources import qualify_execution_origin

        callback, parent, _, witness = rows[0][self.column : self.column + 4]
        if not same_body((actual.continuation or {}).get("news_origin"), self.marker):
            raise ProductSourceRetired()
        if (
            witness is None
            or (witness.id, witness.group_id, witness.link_id, witness.binding_generation, witness.chat_id, witness.message_thread_id, witness.kind, witness.target_kind, witness.target_id, witness.message_id)
            != (news_witness_id(actual.id), news_witness_id(actual.id), self.link.id, self.link.binding_generation, self.link.chat_id, parent.message_thread_id if parent else None, "work_activation_origin", "work", actual.work_id, None)
            or witness.consumed_at is not None
            or witness.expires_at is not None
            or witness.closed_reason is not None
            or not same_body(
                witness.payload,
                {
                    "news_origin": self.marker,
                    "work_action_id": actual.id,
                    "anchor_outbox_id": self.marker["anchor_outbox_id"],
                    "source_group_id": callback.group_id if callback else None,
                    "assignment_revision": actual.body["expected_assignment_revision"],
                },
            )
        ):
            raise ProductSourceRetired()
        controls = {value.id: value for row in rows if (value := row[self.column + 2]) is not None}
        qualify_execution_origin(self.link, _snapshot(actual), callback, parent, list(controls.values()))
        self.projection.check_with_permissions(rows, held)
