"""Current eligible-recipient reads and dormant routing presentation."""

from dataclasses import dataclass

from app.channels.execution import ChannelExecutionProtocolError
from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.pages import page_address
from app.telegram_bots.render import render
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_requests import WorkRequestPresentation, WorkRequestRead, WorkRequestReader, present_request
from deerflow.agent_instances.contract import AgentDenied, AgentPermission
from deerflow.utils.file_io import await_drained

ROUTE_BUTTONS_MAX = 6


@dataclass(frozen=True)
class RoutingRead:
    source: WorkRequestRead
    recipients: tuple[dict, ...]
    after_wrong: bool


class WorkRoutingReader(WorkRequestReader):
    def __init__(self, *, client, **kwargs):
        super().__init__(**kwargs)
        self.client = client

    async def read_routing(self, link, request_id):
        return await await_drained(self._read_routing(link, request_id))

    async def _read_routing(self, link, request_id):
        from app.gateway.auth.mode import account_refusal
        from app.gateway.deps import bind_user_repo
        from deerflow.config.app_config import peek_loaded_app_config

        source = await self.read_request(link, request_id)
        if source.request["state"] == "closed" or not source.request["can_manage"]:
            raise AgentDenied("Routing is unavailable")
        authority = await self._current(link)

        async def check():
            if await self._current(link) != authority:
                raise AgentDenied("Routing authority changed")

        body = await self.client.get_agent_instance_grants(link.owner_user_id, instance_id=link.agent_ref, before_dispatch=check)
        rows = body.get("grants") if type(body) is dict else None
        if type(rows) is not list:
            raise ChannelExecutionProtocolError()
        required = 3 if source.request["purpose"] == "information" else 6
        latest = max((row for row in source.responses if row["disposition"] == "wrong_recipient"), key=lambda row: (row["created_at"], row["id"]), default=None)
        excluded = latest["actor_id"] if latest else None
        users = bind_user_repo(self.links.session_factory)
        config = peek_loaded_app_config()
        if config is None:
            raise AgentDenied("Routing authority is unavailable")
        mode = config.auth.local.enabled is False
        recipients, seen = [], set()
        for row in rows:
            if type(row) is not dict or type(row.get("user_id")) is not str or not row["user_id"] or row["user_id"] in seen or type(row.get("permissions")) is not int or not 1 <= row["permissions"] <= 7:
                raise ChannelExecutionProtocolError()
            identity = row["user_id"]
            seen.add(identity)
            if identity == excluded or row["permissions"] & required != required:
                continue
            user = await users.get_user_by_id(identity)
            if user is None or account_refusal(user, sign_on_only_mode=mode) is not None:
                continue
            name = getattr(user, "name", None) or user.email
            recipients.append({"user_id": identity, "name": name, "permissions": row["permissions"]})
        current = await self.read_request(link, request_id)
        await check()
        from app.telegram_bots.work_request_source import same_displayed_basis

        if not same_displayed_basis(current, source) or peek_loaded_app_config() is not config or (config.auth.local.enabled is False) != mode:
            raise AgentDenied("Routing source changed")
        recipients.sort(key=lambda row: (row["user_id"] != link.owner_user_id, row["name"].casefold(), row["user_id"]))
        return RoutingRead(source, tuple(recipients), excluded == link.owner_user_id and latest["request_revision"] == source.request["request_revision"] and latest["assignment_revision"] == source.request["assignment_revision"])


def present_routing(value, *, locale, link, public_base_url, product="HartMesh", concealed=False, outbox_id=None):
    source, recipients = value.source, value.recipients
    if concealed:
        return present_request(source, locale=locale, concealed=True, outbox_id=outbox_id)
    request = source.request
    values = {"objective": source.work["objective"], "name": request["instance_name"], "question": request["question"], "product": product}
    key = "request.route_none" if not recipients else "request.route_ask" if value.after_wrong else "request.unrouted"
    html = render(key, locale, **values)
    keyboard = [
        [
            {
                "label": "label.choice",
                "label_values": {"label": row["name"]},
                "kind": "route_to",
                "target_kind": "request",
                "target_id": request["id"],
                "payload": {"request_id": request["id"], "request_revision": request["request_revision"], "recipient_id": row["user_id"]},
            }
        ]
        for row in recipients[:ROUTE_BUTTONS_MAX]
    ]
    if not recipients or len(recipients) > ROUTE_BUTTONS_MAX:
        address = page_address("web.access" if not recipients else "web.request", link, request_id=request["id"], work_id=source.work["id"], permissions=AgentPermission.MANAGE | AgentPermission.INSPECT, public_base_url=public_base_url)
        if address:
            keyboard.append([{"label": "label.open_access" if not recipients else "label.choose_answerer", "url": address}])
    return WorkRequestPresentation(tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "protect_content": source.protected} for chunk in split_html(html)), tuple(tuple(row) for row in canonical_keyboard(keyboard)))
