"""Close file inputs refused before a Home operation can be admitted."""

from sqlalchemy import and_, or_, select

from app.telegram_bots.authority import AuthorityUnavailable
from app.telegram_bots.instance_file_authority import _SCOPE
from app.telegram_bots.instance_refusals import describe_send_refusal
from app.telegram_bots.policy import POLICIES, pending_discard
from app.telegram_bots.publication import fixed_source
from app.telegram_bots.turns import _Guard
from deerflow.agent_instances.contract import AgentDenied, ExecutionUnavailable
from deerflow.agent_instances.service import AgentInstances
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramInstancePolicyRow
from deerflow.persistence.user.access import DisabledIdentityRow
from deerflow.persistence.user.model import UserRow
from deerflow.utils.file_io import await_drained


class _RefusalGuard(_Guard):
    """The refusal needs current Use, but neither a Home nor ready execution."""

    async def prepare(self):
        await super().prepare()
        self.service = self.services.authority.app.state.agent_instances
        if type(self.service) is not AgentInstances or self.service.directory.session_factory.kw.get("bind") is not self.services.links.session_factory.kw.get("bind"):
            raise AuthorityUnavailable
        self.view = await self.services.authority.instance_view(self.captured_link)
        self.current()

    async def check(self, writer):
        link = await super().check(writer)
        bot, instance, grant, policy = TelegramBotLinkRow, AgentInstanceRow, AgentInstanceGrantRow, TelegramInstancePolicyRow
        facts = (
            await writer.execute(
                select(instance, grant.permissions, policy.policy, bot, UserRow, DisabledIdentityRow)
                .join(bot, bot.agent_ref == instance.id)
                .join(grant, and_(grant.instance_id == instance.id, grant.user_id == bot.owner_user_id))
                .join(UserRow, UserRow.id == bot.owner_user_id)
                .outerjoin(policy, policy.instance_id == instance.id)
                .outerjoin(DisabledIdentityRow, or_(DisabledIdentityRow.subject == UserRow.id, DisabledIdentityRow.subject == UserRow.oauth_id))
                .where(bot.id == self.captured_link.id)
                .execution_options(populate_existing=True)
            )
        ).all()
        if not facts:
            raise AgentDenied("The file input is no longer available")
        target, permissions, value, actual, user, _ = facts[0]
        self.live_owner.check_snapshot(user, [row[-1] for row in facts])
        value = "allowed" if target.custody == "personal" or value is None else value
        if (
            self.services.authority.app.state.agent_instances is not self.service
            or any(getattr(actual, key) != getattr(self.captured_link, key) for key in _SCOPE)
            or actual.state != "connected"
            or actual.health != "ok"
            or target.id != self.view.id
            or target.status != "active"
            or target.generation != self.view.generation
            or target.custody not in {"personal", "company"}
            or type(permissions) is not int
            or not 1 <= permissions <= 7
            or not permissions & 1
            or value not in POLICIES
            or value == "off"
            or pending_discard(actual) is not None
        ):
            raise AgentDenied("The file input is no longer available")
        self.current()
        return link


async def refuse_file_input(services, channel, link, origins, error, *, current):
    """No invented file receipt, download, resident admission or model turn."""

    async def perform():
        key, _identity = await services.authority.instance_lifecycle(link)
        if key in {"access.agent_off", "agent.paused", "agent.retired"}:
            guard = _Guard(services, channel, link, current, repair=True, input_closure=True)
            await services.outbox.inbox.close_unpublished_input(link, origins, guard=guard)
            await services.notice_instance_unavailable(link, file_input=True)
            return True
        if not isinstance(error, ExecutionUnavailable):
            return False
        failure = await describe_send_refusal(services.authority, link, error)
        source = await fixed_source(services, link, failure.text_key)
        source["values"].update(failure.values)
        await services.outbox.plan_input_outcome(
            link,
            source_id=f"chat-input:{link.chat_id}:{link.binding_generation}:{origins[0].message_id}",
            source_ref=source,
            keyboard=[],
            callbacks=services.callbacks,
            guard=_RefusalGuard(services, channel, link, current, repair=True),
            inbox_rows=origins,
        )
        return True

    return await await_drained(perform())
