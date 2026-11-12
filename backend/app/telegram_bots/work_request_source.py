"""Source proof and one final SQL boundary for saved Work request delivery."""

import asyncio
import json
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from sqlalchemy import JSON, and_, false, func, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import current_attention
from app.telegram_bots.authority import AccessOff, CeilingRefused
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.notifications import aware, preference_skip
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryRetry, PermanentDeliveryError, ProductSourceRetired
from app.telegram_bots.settings import SettingsRepository, _snapshot
from app.telegram_bots.work_event_source import require_event_source, unsolicited_event
from app.telegram_bots.work_requests import WorkRequestRead, present_request, seen_basis
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


def content(source):
    request_fields = (
        "id",
        "instance_id",
        "work_id",
        "purpose",
        "question",
        "reason",
        "expected_response",
        "choices",
        "sources",
        "recipient_id",
        "revision",
        "request_revision",
        "assignment_revision",
        "state",
        "basis_id",
        "basis_revision",
        "instance_name",
    )
    work_fields = ("id", "instance_id", "objective", "assignment_revision", "outcome", "outcome_id", "blocker", "status")
    return ({key: source.request.get(key) for key in request_fields}, {key: source.work.get(key) for key in work_fields}, source.responses, source.people)


def same_displayed_basis(left, right):
    """Compare the complete preview/CAS, excluding unrelated execution probes."""

    def basis(source):
        return json.dumps((content(source), source.seen, source.protected), sort_keys=True, separators=(",", ":"))

    return basis(left) == basis(right)


def _references(value):
    if isinstance(value, dict):
        if value.get("kind") == "space_file":
            yield value["space_id"]
        for child in value.values():
            yield from _references(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            yield from _references(child)


def frozen(captured, *, acknowledged=False, pending=False):
    """A class label or literal template cannot manufacture a Work source."""
    try:
        basis = captured.item.payload["work_request_basis"]
        if pending and acknowledged:
            raise ValueError()
        if (
            type(basis) is not dict
            or set(basis)
            != {"version", "source", "locale", "concealed", "parts", "keyboard"}
            | ({"reveal", "cleanup_id"} if basis.get("version") in {2, 3, 4, 6} else set())
            | ({"interaction", "origin"} if basis.get("version") in {3, 4} else set())
            | ({"input"} if basis.get("version") == 4 else set())
            | ({"reopen"} if basis.get("version") == 3 and "reopen" in basis else set())
            | ({"routing"} if basis.get("version") in {5, 6} else set())
            | ({"reply_origin"} if basis.get("version") in {2, 6} and "reply_origin" in basis else set())
            | ({"choice_origin"} if basis.get("version") == 6 and "choice_origin" in basis else set())
            | ({"result_files"} if basis.get("version") in {1, 2} and "result_files" in basis else set())
            | ({"prompt_id"} if basis.get("version") == 3 and type(basis.get("interaction")) is dict and basis["interaction"].get("view") == "prompt" else set())
            or type(basis["version"]) is not int
            or basis["version"] not in {1, 2, 3, 4, 5, 6}
        ):
            raise ValueError()
        if (
            captured.item.class_ != ("reply" if basis["version"] in {3, 4} or basis.get("reply_origin") is not None or basis.get("choice_origin") is not None else "question")
            or type(basis["concealed"]) is not bool
            or type(basis["locale"]) is not str
            or type(captured.item.payload.get("presentation_version")) is not int
            or captured.item.payload["presentation_version"] < 0
        ):
            raise ValueError()
        if "reopen" in basis:
            from app.telegram_bots.work_reopen import validate_reopen

            validate_reopen(basis["reopen"])
            if basis.get("interaction", {}).get("view") != "prompt":
                raise ValueError()
        reply_origin = basis.get("reply_origin")
        if reply_origin is not None and (type(reply_origin) is not str or len(reply_origin) != 32 or any(ch not in "0123456789abcdef" for ch in reply_origin)):
            raise ValueError()
        choice_origin = basis.get("choice_origin")
        if "choice_origin" in basis and (type(choice_origin) is not str or len(choice_origin) != 32 or any(ch not in "0123456789abcdef" for ch in choice_origin)):
            raise ValueError()
        reveal = basis.get("reveal")
        if reveal is not None and (type(reveal) is not str or len(reveal) != 32 or any(ch not in "0123456789abcdef" for ch in reveal)):
            raise ValueError()
        if reveal is not None and basis["concealed"]:
            raise ValueError()
        cleanup_id = basis.get("cleanup_id")
        if cleanup_id is not None and (reveal is None and basis["version"] not in {3, 4} or type(cleanup_id) is not str or len(cleanup_id) != 32 or any(ch not in "0123456789abcdef" for ch in cleanup_id)):
            raise ValueError()
        data = basis["source"]
        source = WorkRequestRead(**{**data, "responses": tuple(data["responses"])})
        if (
            source.request["instance_id"] != captured.link.agent_ref
            or captured.link.agent_kind != "instance"
            or source.request["work_id"] != source.work["id"]
            or (basis["version"] not in {5, 6} and source.request["recipient_id"] != captured.link.owner_user_id)
        ):
            raise ValueError()
        if (captured.item.instance_id, captured.item.work_id, captured.item.request_id) != (source.request["instance_id"], source.work["id"], source.request["id"]):
            raise ValueError()
        if type(source.protected) is not bool or json.dumps(source.seen, sort_keys=True) != json.dumps(seen_basis(source.work, source.request, source.responses), sort_keys=True):
            raise ValueError()
        if source.protected and not basis["concealed"] and reveal is None and reply_origin is None and choice_origin is None and basis["version"] not in {3, 4}:
            raise ValueError()
        if basis["version"] in {3, 4}:
            from app.telegram_bots.work_interactions import present_interaction

            origin = basis["origin"]
            if (
                type(origin) is not dict
                or set(origin) != {"callback_id", "anchor_outbox_id", "presentation_version", "message_id"}
                or any(type(origin[key]) is not str or len(origin[key]) != 32 or any(ch not in "0123456789abcdef" for ch in origin[key]) for key in ("callback_id", "anchor_outbox_id"))
            ):
                raise ValueError()
            if (
                type(origin["presentation_version"]) is not int
                or origin["presentation_version"] < 0
                or type(origin["message_id"]) is not int
                or origin["message_id"] <= 0
                or reveal is not None
                or basis["concealed"]
                or basis["version"] == 3
                and cleanup_id is None
            ):
                raise ValueError()
            if basis["version"] == 4:
                from app.telegram_bots.work_input_previews import validate_input_basis

                validate_input_basis(basis)
            expires_at = basis["keyboard"][0][0]["expires_at"] if basis["version"] == 4 and basis["interaction"]["view"] == "changes" else None
            presentation = present_interaction(source, basis["interaction"], locale=basis["locale"], expires_at=expires_at)
            if basis["interaction"]["view"] == "prompt" and (type(basis["prompt_id"]) is not str or len(basis["prompt_id"]) != 32 or any(ch not in "0123456789abcdef" for ch in basis["prompt_id"])):
                raise ValueError()
        elif basis["version"] in {5, 6}:
            from app.telegram_bots.work_routing import RoutingRead, present_routing

            routing = basis["routing"]
            if (basis["version"] == 5 and captured.item.payload["presentation_version"] != 0) or (basis["version"] == 6 and captured.item.payload["presentation_version"] < 1) or captured.item.message_thread_id is not None:
                raise ValueError()
            if (
                type(routing) is not dict
                or set(routing) != {"recipients", "after_wrong", "public_base_url"} | ({"response_id"} if "response_id" in routing else set())
                or type(routing["after_wrong"]) is not bool
                or type(routing["recipients"]) is not list
                or (routing["public_base_url"] is not None and type(routing["public_base_url"]) is not str)
            ):
                raise ValueError()
            if "response_id" in routing:
                intent = captured.item.payload.get("work_event_intent", {})
                if type(routing["response_id"]) is not str or not routing["response_id"] or intent.get("key") != "request.unrouted" or routing["response_id"] != intent.get("response_id"):
                    raise ValueError()
            presentation = present_routing(
                RoutingRead(source, tuple(routing["recipients"]), routing["after_wrong"], routing.get("response_id")),
                locale=basis["locale"],
                link=captured.link,
                public_base_url=routing["public_base_url"],
                concealed=basis["concealed"],
                outbox_id=captured.item.id,
            )
        elif "result_files" in basis:
            from app.telegram_bots.work_result_files import present_review_files, result_options

            presentation = present_review_files(source, locale=basis["locale"], concealed=basis["concealed"], outbox_id=captured.item.id, **result_options(basis["result_files"]))
        else:
            presentation = present_request(source, locale=basis["locale"], concealed=basis["concealed"], outbox_id=captured.item.id)
        if reveal is not None and (len(presentation.parts) > 1 or "result_files" in basis) and cleanup_id is None:
            raise ValueError()
        expected = []
        for index, part in enumerate(presentation.parts):
            recipe = {**part, "source_ref": {"key": "work.request.presentation", "values": {"request_id": source.request["id"], "part": index}}}
            if presentation.keyboard and index == len(presentation.parts) - 1:
                recipe["stored_keyboard"] = True
            if basis["version"] == 3 and basis["interaction"]["view"] == "prompt" and index == len(presentation.parts) - 1:
                recipe["force_reply"] = True
            expected.append(recipe)
        if basis["parts"] != expected or basis["keyboard"] != [list(line) for line in presentation.keyboard] or captured.item.payload.get("product_keyboard_basis") != basis["keyboard"]:
            raise ValueError()
        if "result_files" in basis:
            from app.telegram_bots.work_result_history import qualified_history
            from app.telegram_bots.work_result_identity import qualify_identity
            from app.telegram_bots.work_result_ledger import qualify_result_parts
            from app.telegram_bots.work_result_navigation import qualify_navigation

            qualify_identity(captured.item.payload.get("work_result_identity"), source)
            if "work_result_navigation" in captured.item.payload:
                qualify_navigation(captured.item.payload["work_result_navigation"], captured.link, source)
            carried, hashes = qualified_history(captured.item, source)
            recipe = qualify_result_parts(captured.item, expected, selected=captured.part, acknowledged=acknowledged, carried=carried, known_hashes=hashes, pending=pending)
            return basis, source, recipe
        index = captured.part["source_ref"]["values"]["part"]
        if type(index) is not int or not 0 <= index < len(expected):
            raise ValueError()
        version = captured.item.payload["presentation_version"]
        historical = [part for part in captured.item.parts if part["presentation_version"] < version]
        if any(part["state"] not in {"sent", "retired", "unavailable"} for part in historical):
            raise ValueError()
        parts = [part for part in captured.item.parts if part["presentation_version"] == version]
        if len(historical) + len(parts) != len(captured.item.parts):
            raise ValueError()
        if len(parts) != len(expected) or parts[index] != captured.part or captured.part.get("state") != ("sent" if acknowledged else "pending" if pending else "sending"):
            raise ValueError()
        identities = {part["part_id"] for part in historical}
        if len(identities) != len(historical):
            raise ValueError()
        receipts = {
            "part_id",
            "presentation_version",
            "ordinal",
            "state",
            "message_ids",
            "callback_group_id",
            "sent_at",
            "dispatched_at",
            "plain_fallback",
            "invalid_token_verification_pending",
            "target_message_id",
            "target_protect_content",
        }
        for ordinal, (part, recipe) in enumerate(zip(parts, expected, strict=True)):
            identity = part.get("part_id")
            if type(identity) is not str or len(identity) != 32 or identity in identities or any(ch not in "0123456789abcdef" for ch in identity):
                raise ValueError()
            identities.add(identity)
            if type(part.get("ordinal")) is not int or part["ordinal"] != ordinal or type(part.get("presentation_version")) is not int or part["presentation_version"] != version:
                raise ValueError()
            if part.get("kind") == "edit":
                anchor = next((old for old in historical if old["state"] == "sent" and part.get("target_message_id") in old.get("message_ids", [])), None)
                if cleanup_id is not None or ordinal != 0 or anchor is None or bool(anchor.get("protect_content")) != part.get("target_protect_content") or source.protected and not anchor.get("protect_content"):
                    raise ValueError()
            elif part.get("kind") != "text" or "target_message_id" in part or "target_protect_content" in part:
                raise ValueError()
            if set(part) - set(recipe) - receipts or any(part.get(key) != value for key, value in recipe.items() if key not in {"protect_content", "kind"}):
                raise ValueError()
            if source.protected and not part.get("protect_content") or ordinal < len(parts) - 1 and part.get("callback_group_id"):
                raise ValueError()
            if (acknowledged or ordinal < index and not pending) and (part["state"] != "sent" or not part.get("sent_at") or not part.get("message_ids") or any(type(identity) is not int or identity <= 0 for identity in part["message_ids"])):
                raise ValueError()
            if pending and (part["state"] != "pending" or part.get("message_ids") or part.get("sent_at") or part.get("dispatched_at")):
                raise ValueError()
        recipe = expected[index]
        return basis, source, recipe
    except (KeyError, TypeError, ValueError, IndexError):
        raise PermanentDeliveryError("invalid-work-request-source") from None


def require_show(callback, parent):
    """The consumed Show belongs to this exact presentation and binding."""
    if (
        callback is None
        or callback.kind != "reveal"
        or callback.consumed_at is None
        or callback.closed_reason != "revealed"
        or (callback.link_id, callback.chat_id, callback.binding_generation, callback.target_kind, callback.target_id, callback.payload.get("outbox_id"), callback.payload.get("anchor_outbox_id"))
        != (parent.link_id, parent.chat_id, parent.binding_generation, "outbox", parent.id, parent.id, parent.id)
    ):
        raise ProductSourceRetired()


async def acknowledge_request(session, row):
    """Incomplete or malformed terminal history cannot arm working controls."""
    from app.telegram_bots.links import BotLink
    from app.telegram_bots.producer import _item

    binding = await session.get(TelegramBotLinkRow, row.link_id)
    if binding is None:
        raise ProductSourceRetired()
    parts = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]]
    if not parts:
        raise ProductSourceRetired()
    frozen(SimpleNamespace(link=BotLink.from_row(binding), item=_item(row), part=parts[-1]), acknowledged=True)
    if "result_files" in row.payload["work_request_basis"]:
        return
    messages = set()
    for part in parts:
        if len(part["message_ids"]) != 1 or part["message_ids"][0] in messages or datetime.fromisoformat(part["sent_at"]).utcoffset() is None:
            raise ProductSourceRetired()
        messages.update(part["message_ids"])


class WorkRequestSources:
    projection_type = None

    def __init__(self, *, reader, outbox, callbacks, recovery=None):
        self.recovery = recovery
        self.reader, self.outbox, self.callbacks = reader, outbox, callbacks
        self.settings = SettingsRepository(outbox.sessions, outbox.config)
        if reader.links.session_factory.kw.get("bind") is not outbox.sessions.kw.get("bind") or callbacks.sessions.kw.get("bind") is not outbox.sessions.kw.get("bind"):
            raise ValueError("work-source-requires-shared-storage")

    async def _prepare(self, captured):
        basis, source, part = frozen(captured)
        if basis["version"] in {5, 6}:
            raise ProductSourceRetired()
        current = await self.reader.read_request(captured.link, source.request["id"])
        if unsolicited_event(captured.item, basis):
            require_event_source(captured.item, current)
        if current.request["state"] != "pending" or current.request["needs_routing"]:
            raise ProductSourceRetired()
        if not (current.request["can_respond"] if current.request["purpose"] == "information" else current.request["can_manage"]):
            raise ProductSourceRetired()
        revealed = bool(basis.get("reveal") or basis.get("reply_origin") or basis.get("choice_origin"))
        changed = not same_displayed_basis(current, source) or current.protected != basis["concealed"] and not revealed and basis["version"] not in {3, 4}
        if basis["version"] in {3, 4} and changed:
            raise ProductSourceRetired()
        if self.recovery is not None and basis["version"] not in {3, 4}:
            settings = await self.settings.get(captured.link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            if changed or settings.locale != basis["locale"]:
                await self.recovery.refresh(captured)
                raise DeliveryClaimRetired()
        elif content(current) != content(source):
            raise ProductSourceRetired()
        elif changed:
            raise DeliveryRetry(timedelta(seconds=5))
        return basis, current, part

    async def read_part(self, captured):
        async def read():
            _basis, _current, part = await self._prepare(captured)
            return part["html"], part["visible_text"]

        try:
            return await await_drained(read())
        except (AccessOff, CeilingRefused, AgentDenied, AgentConflict):
            raise ProductSourceRetired() from None

    async def authorize(self, captured, *, silent, now):
        async def read():
            basis, source, _part = await self._prepare(captured)
            settings = await self.settings.get(captured.link.owner_user_id)
            if settings is None:
                raise DeliveryClaimRetired()
            zone = await asyncio.to_thread(ZoneInfo, settings.timezone)
            projection = (self.projection_type or _RequestProjection)(self, captured, basis, source, settings, zone, silent, now)
            held, _target = await current_attention(self.reader, captured.link, projection=projection)
            if "agents:write" not in held:
                raise ProductSourceRetired()

        try:
            await await_drained(read())
        except (AccessOff, CeilingRefused, AgentDenied, AgentConflict, CallbackRefused):
            raise ProductSourceRetired() from None


class _RequestProjection:
    """Synchronous projection of facts joined to the final account/grant read."""

    def __init__(self, host, captured, basis, source, settings, zone, silent, now):
        self.host, self.captured, self.basis, self.source = host, captured, basis, source
        self.settings, self.zone, self.silent, self.now = settings, zone, silent, now
        from app.telegram_bots.work_source_spaces import ReferencedSpaces

        self.references = ReferencedSpaces(host.reader, host.outbox.sessions, source)
        # The last part owns markup, but its complete control group must remain
        # usable before *any* chunk of this presentation reaches Telegram.
        self.controls_part = next((part for part in captured.item.parts if part["presentation_version"] == captured.item.payload["presentation_version"] and part.get("stored_keyboard")), None)
        self.event_column = None

    def extend(self, query):
        self.column = len(query.column_descriptions)
        bot, delivery, settings = TelegramBotLinkRow, TelegramOutboxRow, TelegramPersonSettingsRow
        request, work, callback = HumanInputRequestRow, AgentWorkRow, TelegramCallbackRow
        sqlite = self.host.outbox.sessions.kw["bind"].dialect.name == "sqlite"

        def aggregate(fields):
            args = []
            for key, column in fields.items():
                args.extend((key, func.json(column) if sqlite and isinstance(column.type, JSON) else column))
            return func.json_group_array(func.json_object(*args)) if sqlite else func.json_agg(func.json_build_object(*args))

        # Correlated compact arrays keep the complete outbox JSON out of the
        # Space x callback product. These facts still share the final statement.
        spaces = self.references.expression()
        origin = self.basis.get("origin", {})
        origin_group = select(TelegramCallbackRow.group_id).where(TelegramCallbackRow.id == origin.get("callback_id")).scalar_subquery()
        callbacks = (
            select(aggregate({column.name: column for column in callback.__table__.columns}))
            .where(
                or_(
                    callback.group_id == self.controls_part.get("callback_group_id") if self.controls_part is not None else false(),
                    callback.id.in_({self.basis.get("reveal"), self.basis.get("reply_origin"), self.basis.get("choice_origin")} - {None}),
                    callback.group_id == origin_group if origin else false(),
                    callback.id == self.basis.get("prompt_id") if self.basis.get("prompt_id") else false(),
                    callback.id == self.basis.get("reopen", {}).get("prompt_id") if self.basis.get("reopen") else false(),
                )
            )
            .scalar_subquery()
        )
        cleanup = aliased(TelegramOutboxRow)
        origin_parent = aliased(TelegramOutboxRow)
        result = (
            query.add_columns(work, request, delivery, settings, spaces, callbacks, cleanup, origin_parent)
            .outerjoin(origin_parent, origin_parent.id == origin.get("anchor_outbox_id") if origin else false())
            .outerjoin(cleanup, cleanup.id == self.basis.get("cleanup_id") if self.basis.get("cleanup_id") else false())
            .join(work, and_(work.id == self.source.work["id"], work.instance_id == bot.agent_ref))
            .join(request, and_(request.id == self.source.request["id"], request.instance_id == bot.agent_ref, request.work_id == work.id))
            .join(delivery, and_(delivery.id == self.captured.item.id, delivery.link_id == bot.id))
            .join(settings, settings.owner_user_id == bot.owner_user_id)
        )
        if self.basis["version"] == 4:
            from app.telegram_bots.work_input_previews import blocked_input_actions

            identity, mark = self.basis["input"], aliased(TelegramInboundMarkRow)
            result = result.add_columns(mark, blocked_input_actions(identity)).outerjoin(mark, and_(mark.link_id == identity["link_id"], mark.chat_id == identity["chat_id"], mark.message_id == identity["message_id"]))
        if "reopen" in self.basis:
            identity, mark = self.basis["reopen"]["input"], aliased(TelegramInboundMarkRow)
            if self.basis["reopen"]["version"] == 2:
                from app.telegram_bots.work_album_reopen import RecoveryGroupProof

                origin = SimpleNamespace(**identity, route_basis={"work_album_recovery": self.basis["reopen"]["album"]})
                # The selector only needs identity; column names are fixed by
                # the projection, never supplied by this detached locator.
                result = result.add_columns(RecoveryGroupProof(origin, sqlite=sqlite).expression())
            else:
                result = result.add_columns(mark).outerjoin(mark, and_(mark.link_id == identity["link_id"], mark.chat_id == identity["chat_id"], mark.message_id == identity["message_id"]))
        if unsolicited_event(self.captured.item, self.basis):
            from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
            from deerflow.persistence.agent_instances.work import AgentWorkEventRow

            self.event_column = len(result.column_descriptions)
            event, receipt = aliased(AgentWorkEventRow), aliased(HumanInputEventRow)
            result = result.add_columns(event, receipt).outerjoin(event, event.id == delivery.event_id).outerjoin(receipt, and_(receipt.instance_id == event.instance_id, receipt.operation_id == event.operation_id))
        return result

    @staticmethod
    def objects(value):
        values = json.loads(value) if isinstance(value, str) else value or []
        result = []
        for fields in values:
            for key in ("created_at", "expires_at", "consumed_at"):
                if isinstance(fields.get(key), str):
                    fields[key] = datetime.fromisoformat(fields[key])
            result.append(SimpleNamespace(**fields))
        return result

    def check(self, rows):
        current, target, permissions, policy = rows[0][:4]
        if self.event_column is not None:
            event, receipt = rows[0][self.event_column : self.event_column + 2]
            require_event_source(self.captured.item, self.source, event=event, receipt=receipt, final=True)
        # File admission may prepend its own source/Space facts. Keep this
        # projection composable without moving the shared authority columns.
        rows = [(*row[:8], *row[self.column :]) for row in rows]
        work, request, delivery, saved_settings = rows[0][8:12]
        latest = self.host.outbox._sending(current, delivery, link_id=current.id, part_id=self.captured.part["part_id"], claim_owner=self.captured.claim_owner, claim_id=self.captured.claim_id, token_revision=self.captured.token_revision)
        if (delivery.instance_id, delivery.work_id, delivery.request_id) != (current.agent_ref, work.id, request.id):
            raise ProductSourceRetired()
        if latest != self.captured:
            raise DeliveryClaimRetired()
        if (request.revision, request.request_revision, request.state, request.recipient_id) != (
            self.source.request["revision"],
            self.source.request["request_revision"],
            self.source.request["state"] if self.basis["version"] in {5, 6} else "pending",
            self.source.request["recipient_id"] if self.basis["version"] in {5, 6} else current.owner_user_id,
        ):
            raise ProductSourceRetired()
        if work.revision != self.source.work["revision"]:
            raise DeliveryRetry(timedelta(seconds=5))
        if (self.basis["version"] in {5, 6} or self.source.request["purpose"] != "information") and permissions & 6 != 6:
            raise ProductSourceRetired()
        protected = target.custody == "company" and policy == "protected"
        if (
            protected != self.source.protected
            or protected != self.basis["concealed"]
            and not (self.basis.get("reveal") or self.basis.get("reply_origin") or self.basis.get("choice_origin"))
            and self.basis["version"] not in {3, 4}
            or protected
            and not self.captured.part.get("protect_content")
        ):
            raise DeliveryRetry(timedelta(seconds=5))
        actual = _snapshot(saved_settings)
        if self.basis["version"] in {3, 4} and actual.locale != self.basis["locale"]:
            raise ProductSourceRetired()
        if actual.locale != self.basis["locale"] or actual.timezone != self.settings.timezone:
            raise DeliveryRetry(timedelta(seconds=5))
        if preference_skip(latest.item, actual, self.now):
            raise ProductSourceRetired()
        local = self.now.astimezone(self.zone)
        minute, start, end = local.hour * 60 + local.minute, actual.quiet_start_minute, actual.quiet_end_minute
        quiet = actual.quiet_enabled and (minute >= start or minute < end if start > end else start <= minute < end)
        recent = current.last_update_at is not None and self.now - aware(current.last_update_at) <= timedelta(seconds=self.host.outbox.config.outbox.recent_act_seconds)
        if bool(delivery.silent or self.captured.part.get("silent") or quiet and not recent) != bool(self.silent):
            raise DeliveryClaimRetired()
        self.references.check(rows[0][12])
        callback_facts = self.objects(rows[0][13])
        for identity in {self.basis.get("reveal"), self.basis.get("reply_origin")} - {None}:
            reveal = next((row for row in callback_facts if row.id == identity), None)
            require_show(reveal, delivery)
        if self.basis.get("choice_origin") is not None:
            from app.telegram_bots.work_routing_origin import require_choice

            choice = next((row for row in callback_facts if row.id == self.basis["choice_origin"]), None)
            require_choice(choice, delivery)
        if not cleanup_ready(delivery, rows[0][14]):
            raise DeliveryRetry(timedelta(seconds=5))
        if self.basis["version"] == 3:
            from app.telegram_bots.work_interaction_delivery import check_origin

            check_origin(current, rows[0][15], callback_facts, self.basis, self.source, reopened_mark=rows[0][16] if "reopen" in self.basis else None, outbox_id=delivery.id)
        elif self.basis["version"] == 4:
            from app.telegram_bots.work_input_previews import check_input_origin

            check_input_origin(current, rows[0][15], callback_facts, rows[0][16], rows[0][17], self.basis, self.source, draft_ttl_hours=self.host.callbacks.config.callbacks.draft_ttl_hours)
        reveal = next((row for row in callback_facts if row.id == self.basis.get("reveal")), None)
        cleanup_target = self.basis["origin"]["message_id"] if self.basis["version"] in {3, 4} else reveal.message_id if self.basis.get("reveal") else None
        if self.basis.get("cleanup_id") and rows[0][14].payload["target_message_id"] != cleanup_target:
            raise ProductSourceRetired()
        if self.basis.get("prompt_id"):
            from app.telegram_bots.callbacks import _visibility
            from app.telegram_bots.work_typing import validate_prompt

            prompt = next((row for row in callback_facts if row.id == self.basis["prompt_id"]), None)
            parts = validate_prompt(latest.item, prompt)
            visibility = _visibility(prompt.payload.get("prompt_visibility"))
            if (
                prompt.consumed_at is not None
                or "prompt_successor" in prompt.payload
                or visibility["state"] != "pending"
                or visibility["part_id"] != parts[-1]["part_id"]
                or prompt.payload.get("anchor_outbox_id") != delivery.id
                or prompt.payload.get("presentation_version") != delivery.payload["presentation_version"]
            ):
                raise ProductSourceRetired()
            origin = next(row for row in callback_facts if row.id == self.basis["origin"]["callback_id"])
            age_now = self.host.callbacks.clock()
            if age_now >= aware(origin.created_at) + timedelta(days=self.host.callbacks.config.callbacks.max_age_days) or origin.expires_at is not None and age_now >= aware(origin.expires_at):
                raise ProductSourceRetired()
        if self.controls_part is not None:
            callbacks = {row.id: row for row in callback_facts if row.group_id == self.controls_part["callback_group_id"]}
            _validate_rows(delivery, list(callbacks.values()), part=self.controls_part, allow_pending=True)
            for callback in callbacks.values():
                if (
                    callback.link_id != current.id
                    or callback.binding_generation != current.binding_generation
                    or callback.chat_id != current.chat_id
                    or callback.message_thread_id != delivery.message_thread_id
                    or callback.payload.get("anchor_outbox_id") != delivery.id
                    or callback.payload.get("presentation_version") != delivery.payload["presentation_version"]
                    or type(callback.payload.get("presentation_pending")) is not bool
                ):
                    raise DeliveryClaimRetired()
                self.host.callbacks._usable(callback)


def cleanup_ready(parent, cleanup):
    """Require an acknowledged clear or its exact host-recorded missing target."""
    identity = parent.payload.get("work_news_basis", parent.payload.get("result_notice_basis", parent.payload.get("work_request_basis", {}))).get("cleanup_id")
    if identity is None:
        return True
    if (
        cleanup is None
        or cleanup.id != identity
        or (cleanup.link_id, cleanup.owner_user_id, cleanup.binding_generation, cleanup.chat_id) != (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id)
        or cleanup.class_ != "reply"
        or cleanup.payload.get("kind") != "keyboard_cleanup"
        or cleanup.payload.get("work_request_parent_id") != parent.id
        or type(cleanup.parts) is not list
        or len(cleanup.parts) != 1
        or type(cleanup.parts[0]) is not dict
    ):
        raise ProductSourceRetired()
    part = cleanup.parts[0]
    target = cleanup.payload.get("target_message_id")
    if type(target) is not int or target <= 0 or part.get("kind") != "edit" or part.get("remove_keyboard") is not True or part.get("target_message_id") != target:
        raise ProductSourceRetired()
    if (
        type(part.get("part_id")) is not str
        or len(part["part_id"]) != 32
        or any(ch not in "0123456789abcdef" for ch in part["part_id"])
        or type(part.get("presentation_version")) is not int
        or part["presentation_version"] != cleanup.payload.get("presentation_version", 0)
    ):
        raise ProductSourceRetired()
    if cleanup.state == "sent":
        try:
            datetime.fromisoformat(part.get("sent_at"))
        except (ValueError, TypeError):
            raise ProductSourceRetired() from None
        if part.get("state") != "sent" or not part.get("sent_at") or part.get("message_ids") != [target]:
            raise ProductSourceRetired()
        return True
    if cleanup.state == "skipped":
        receipt = cleanup.payload.get("cleanup_absent")
        if cleanup.skip_reason != "superseded" or type(receipt) is not dict or receipt != {"part_id": part.get("part_id"), "presentation_version": part.get("presentation_version"), "target_message_id": target, "at": receipt.get("at")}:
            raise ProductSourceRetired()
        try:
            datetime.fromisoformat(receipt["at"])
        except (ValueError, TypeError):
            raise ProductSourceRetired() from None
        return True
    return False
