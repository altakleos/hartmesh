"""One final authority snapshot inside the canonical Work effect transaction."""

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.authority import AccessOff, AuthorityUnavailable, CeilingRefused, _PreparedLiveOwner
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow
from deerflow.persistence.user.access import DisabledIdentityRow
from deerflow.persistence.user.model import UserRow


async def prepare_effect_admission(acts, link, act, *, attempt_current):
    # Provider/config preparation must finish before the domain opens its writer.
    prepared = await prepare_attention(acts, link)
    projection = _EffectProjection(acts, act, attempt_current)

    async def admission(writer):
        acts._writer_required(writer)
        if not attempt_current():
            raise AuthorityUnavailable()
        # The domain already owns instance/Work/request locks. Retain its
        # instance -> link -> Act order so cancellation and binding writers
        # cannot alter the admitted operation until this effect commits.
        await writer.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).with_for_update().execution_options(populate_existing=True))
        await writer.scalar(select(TelegramActRow).where(TelegramActRow.id == act.id).with_for_update().execution_options(populate_existing=True))
        held, _target = await prepared(session=writer, projection=projection)
        if "agents:write" not in held:
            raise CeilingRefused()
        if not attempt_current() or getattr(acts.authority.app.state, "agent_instances", None) is not acts.work.agents:
            raise AuthorityUnavailable()

    return admission


class _EffectProjection:
    def __init__(self, acts, act, attempt_current):
        from app.gateway.deps import bind_user_repo
        from deerflow.config.app_config import peek_loaded_app_config

        self.acts, self.act, self.attempt_current = acts, act, attempt_current
        self.config = peek_loaded_app_config()
        if self.config is None:
            raise AuthorityUnavailable()
        self.sign_on_only = self.config.auth.local.enabled is False
        self.recipient_id = act.body.get("recipient_id") if act.kind == "route" else None
        self.recipient = _PreparedLiveOwner(self.recipient_id, "", self.sign_on_only, bind_user_repo(acts.session_factory), acts.session_factory.kw["bind"].sync_engine)

    def extend(self, query):
        act, request, work, bot = TelegramActRow, HumanInputRequestRow, AgentWorkRow, TelegramBotLinkRow
        user, grant, disabled = aliased(UserRow), aliased(AgentInstanceGrantRow), aliased(DisabledIdentityRow)
        return (
            query.add_columns(act, request, work, user, grant.permissions, disabled)
            .join(act, act.id == self.act.id)
            .join(request, and_(request.id == self.act.request_id, request.instance_id == bot.agent_ref, request.work_id == self.act.work_id))
            .join(work, and_(work.id == self.act.work_id, work.instance_id == bot.agent_ref))
            .outerjoin(user, user.id == self.recipient_id)
            .outerjoin(grant, and_(grant.instance_id == bot.agent_ref, grant.user_id == user.id))
            .outerjoin(disabled, or_(disabled.subject == user.id, disabled.subject == user.oauth_id))
        )

    def check(self, rows):
        from deerflow.config.app_config import peek_loaded_app_config

        bot, _target, permissions, _policy = rows[0][:4]
        saved, request, _work, recipient, recipient_grant = rows[0][8:13]
        if not self.attempt_current() or peek_loaded_app_config() is not self.config or (self.config.auth.local.enabled is False) != self.sign_on_only:
            raise AuthorityUnavailable()
        if any(getattr(saved, name) != getattr(self.act, name) for name in _ACT_SCOPE) or not same_body(saved.body, self.act.body) or saved.state != "pending" or saved.attempts != self.act.attempts or saved.cancel_requested_at is not None:
            raise AgentDenied("The action admission is no longer current")
        if saved.kind != "respond" and permissions & 6 != 6:
            raise AgentDenied("Management access is unavailable")
        if saved.kind in {"decide", "accept"} and bot.acts_paused_since is not None:
            raise AgentDenied("Actions are temporarily paused", reason="actions-paused")
        if saved.kind == "route":
            required = 3 if request.purpose == "information" else 6
            if type(self.recipient_id) is not str or not self.recipient_id or recipient is None or type(recipient_grant) is not int or recipient_grant & required != required:
                raise AgentDenied("Recipient is not eligible", reason="recipient-ineligible")
            try:
                self.recipient.check_snapshot(recipient, [row[13] for row in rows])
            except AccessOff:
                raise AgentDenied("Recipient is not eligible", reason="recipient-ineligible") from None
        if not self.attempt_current() or peek_loaded_app_config() is not self.config or (self.config.auth.local.enabled is False) != self.sign_on_only:
            raise AuthorityUnavailable()
