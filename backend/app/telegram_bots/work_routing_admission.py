"""An acknowledged routing choice reserves one original service command."""

from sqlalchemy import and_, or_
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.authority import AccessOff, _PreparedLiveOwner
from app.telegram_bots.callbacks import CallbackRefused, _topic
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.work_actions import request_for
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from app.telegram_bots.work_request_recovery import _RecoveryProjection
from app.telegram_bots.work_routing_followups import FollowupAdmission, acknowledged_followup
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.human_input_contract import RequestCommand
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.persistence.user.access import DisabledIdentityRow
from deerflow.persistence.user.model import UserRow
from deerflow.utils.file_io import await_drained


def route_choice(callback, source):
    payload = callback.payload
    if (
        callback.kind != "route_to"
        or callback.target_kind != "request"
        or callback.target_id != source.request["id"]
        or payload.get("request_id") != source.request["id"]
        or type(payload.get("request_revision")) is not int
        or payload["request_revision"] != source.request["request_revision"]
        or type(payload.get("recipient_id")) is not str
        or not payload["recipient_id"]
    ):
        raise CallbackRefused()
    return payload["recipient_id"]


class WorkRoutingAdmissions:
    def __init__(self, plans, acts):
        self.plans, self.acts = plans, acts
        self.callbacks, self.outbox = plans.callbacks, plans.outbox
        if acts.session_factory.kw.get("bind") is not self.outbox.sessions.kw.get("bind"):
            raise ValueError("work-routing-requires-shared-storage")

    async def stage(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        if type(chat_id) is not int or type(message_id) is not int or message_id <= 0:
            raise CallbackRefused()
        message_thread_id = _topic(message_thread_id)

        async def perform():
            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                existing = await reader.get(TelegramActRow, callback_id)
                if callback is None or callback.link_id != link.id or callback.kind != "route_to" or callback.target_kind != "request":
                    raise CallbackRefused()
                request_id = callback.target_id
                parent = await reader.get(TelegramOutboxRow, callback.payload.get("anchor_outbox_id"))
                followup = FollowupAdmission(link, parent) if parent is not None and "work_outcome_basis" in parent.payload else None
            await self.acts._work_link(link)
            owner = await self.acts.authority.prepare_live_owner(link.owner_user_id)
            if existing is None:
                source = admission = projection = preparation_error = None
                try:
                    value = await self.plans.reader.read_routing(link, request_id)
                    source = value.source
                    if type(callback.payload.get("request_revision")) is not int or callback.payload["request_revision"] != source.request["request_revision"]:
                        raise AgentConflict("The request changed")
                    recipient = next((person for person in value.recipients if person["user_id"] == callback.payload.get("recipient_id")), None)
                    if recipient is None:
                        raise AgentDenied("The recipient can no longer answer", reason="recipient-ineligible")
                    admission = await prepare_attention(self.plans.reader, link)
                    projection = _RoutingActionProjection(self.plans, source, callback_id, recipient, followup)
                except (AgentDenied, AgentConflict, AccessOff, CallbackRefused) as error:
                    # A concurrent press may already own the command. Decide
                    # replay under the writer before applying fresh-source rules.
                    preparation_error = error
                composed = _RoutingGuard(guard, admission, projection, callback_id, preparation_error)
            else:
                source, composed = None, guard
            async with self.callbacks.writer(link, composed) as writer:
                if existing is None and not composed.replay:
                    group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                    parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                    rows = [await writer.get(TelegramCallbackRow, row.id) for row in group.rows]
                    selected = next(row for row in rows if row.id == callback_id)
                    saved = await writer.get(TelegramActRow, callback_id, with_for_update=True, populate_existing=True)
                else:
                    saved = await writer.get(TelegramActRow, callback_id, with_for_update=True, populate_existing=True)
                    if saved is None:
                        raise CallbackRefused()
                    parent, rows, selected = await self.callbacks._locked(writer, link, callback_id)
                if followup is not None:
                    captured = acknowledged_followup(link, parent)
                elif parent.payload.get("work_request_basis", {}).get("version") in {5, 6}:
                    captured = acknowledged_source(link, parent)
                else:
                    raise CallbackRefused()
                recipient_id = route_choice(selected, captured)
                if (selected.chat_id, selected.message_id, selected.message_thread_id) != (chat_id, message_id, message_thread_id):
                    raise CallbackRefused()
                if any(row.payload.get("work_action_id") not in {None, callback_id} for row in rows):
                    raise CallbackRefused(None)
                if saved is None and any("work_action_id" in row.payload for row in rows):
                    raise CallbackRefused()
                if saved is None:
                    if selected.payload["request_revision"] != source.request["request_revision"] or captured.work["id"] != source.work["id"]:
                        raise AgentConflict("The request changed")
                    body = RequestCommand(operation_id=callback_id, action="route", expected_revision=source.request["revision"], expected_request_revision=selected.payload["request_revision"], recipient_id=recipient_id)
                else:
                    body = request_for("route", saved.body, identity=callback_id)
                    if saved.kind != "route" or body.recipient_id != recipient_id or body.expected_request_revision != selected.payload["request_revision"]:
                        raise CallbackRefused()
                result = await self.acts._stage_work_act(link, kind="route", request_id=request_id, work_id=captured.work["id"], request=body, owner=owner, session=writer)
                for row in rows:
                    row.payload = {**row.payload, "work_action_id": result.id}
                return result

        return await await_drained(perform())


class _RoutingGuard:
    def __init__(self, base, admission, projection, identity, preparation_error):
        if not callable(getattr(base, "current", None)):
            raise CallbackRefused()
        self.base, self.admission, self.projection = base, admission, projection
        self.identity, self.preparation_error, self.replay = identity, preparation_error, None

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        if self.replay is None:
            self.replay = await writer.get(TelegramActRow, self.identity, with_for_update=True, populate_existing=True) is not None
        if not self.replay:
            if self.preparation_error is not None:
                raise self.preparation_error
            held, _target = await self.admission(session=writer, projection=self.projection)
            if "agents:write" not in held:
                raise AgentDenied("Routing is unavailable")
        self.base.current()
        return current


class _RoutingActionProjection(_RecoveryProjection):
    def __init__(self, plans, source, identity, recipient, followup=None):
        from app.gateway.deps import bind_user_repo
        from deerflow.config.app_config import peek_loaded_app_config

        super().__init__(source, allowed_act_id=identity)
        self.recipient, self.followup = recipient, followup
        self.config = peek_loaded_app_config()
        if self.config is None:
            raise AgentDenied("Routing is unavailable")
        self.sign_on_only = self.config.auth.local.enabled is False
        self.recipient_owner = _PreparedLiveOwner(recipient["user_id"], "", self.sign_on_only, bind_user_repo(plans.outbox.sessions), plans.outbox.sessions.kw["bind"].sync_engine)

    def extend(self, query):
        query = super().extend(query)
        user, grant, disabled = aliased(UserRow), aliased(AgentInstanceGrantRow), aliased(DisabledIdentityRow)
        result = (
            query.add_columns(user, grant.permissions, disabled)
            .outerjoin(user, user.id == self.recipient["user_id"])
            .outerjoin(grant, and_(grant.instance_id == TelegramBotLinkRow.agent_ref, grant.user_id == user.id))
            .outerjoin(disabled, or_(disabled.subject == user.id, disabled.subject == user.oauth_id))
        )

        return self.followup.extend(result) if self.followup is not None else result

    def check(self, rows):
        from deerflow.config.app_config import peek_loaded_app_config

        bot, target, permissions, policy = rows[0][:4]
        work, request, unresolved, user, grant = rows[0][8:13]
        if permissions & 6 != 6 or request.state == "closed":
            raise AgentDenied("Routing is unavailable")
        if work.revision != self.source.seen["work_revision"] or request.revision != self.source.seen["revision"] or (target.custody == "company" and policy == "protected") != self.source.protected:
            raise AgentConflict("The request changed")
        if unresolved:
            raise CallbackRefused(None)
        required = 3 if self.source.request["purpose"] == "information" else 6
        if user is None or type(grant) is not int or grant & required != required or user.email != self.recipient["name"]:
            raise AgentDenied("The recipient can no longer answer", reason="recipient-ineligible")
        try:
            self.recipient_owner.check_snapshot(user, [row[13] for row in rows])
        except AccessOff:
            raise AgentDenied("The recipient can no longer answer", reason="recipient-ineligible") from None
        if peek_loaded_app_config() is not self.config or (self.config.auth.local.enabled is False) != self.sign_on_only:
            raise AgentDenied("Routing authority changed")
        if self.followup is not None:
            self.followup.check(rows)
