"""Final joined account and grant facts for the complete routing roster."""

from datetime import datetime
from types import SimpleNamespace

from sqlalchemy import func, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.authority import AccessOff, _PreparedLiveOwner
from app.telegram_bots.config import load_config
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_request_source import _RequestProjection
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow
from deerflow.persistence.user.access import DisabledIdentityRow
from deerflow.persistence.user.model import UserRow


class RoutingCandidates:
    def __init__(self, sessions, link, source, routing):
        from deerflow.config.app_config import peek_loaded_app_config

        self.sessions, self.link, self.source, self.routing = sessions, link, source, routing
        self.config = peek_loaded_app_config()
        if self.config is None:
            raise ProductSourceRetired()
        self.sign_on_only = self.config.auth.local.enabled is False
        self.public_base_url = load_config(self.config).public_base_url

    def _mode_current(self):
        from deerflow.config.app_config import peek_loaded_app_config

        if (
            peek_loaded_app_config() is not self.config
            or (self.config.auth.local.enabled is False) != self.sign_on_only
            or load_config(self.config).public_base_url != self.public_base_url
            or self.routing["public_base_url"] != self.public_base_url
        ):
            raise ProductSourceRetired()

    def extend(self, query):
        # Compact correlated facts keep each complete delivery out of the
        # candidate/account-disable cross product. Password hashes never enter it.
        user, grant, disabled = aliased(UserRow), aliased(AgentInstanceGrantRow), aliased(DisabledIdentityRow)
        fields = {column.name: getattr(user, column.name) for column in UserRow.__table__.columns if column.name != "password_hash"}
        fields.update(permissions=grant.permissions, disabled_issuer=disabled.issuer, disabled_subject=disabled.subject, disabled_at=disabled.disabled_at)
        args = [value for pair in fields.items() for value in pair]
        sqlite = self.sessions.kw["bind"].dialect.name == "sqlite"
        aggregate = func.json_group_array(func.json_object(*args)) if sqlite else func.json_agg(func.json_build_object(*args))
        people = (
            select(aggregate)
            .select_from(grant)
            .join(user, user.id == grant.user_id)
            .outerjoin(disabled, or_(disabled.subject == user.id, disabled.subject == user.oauth_id))
            .where(grant.instance_id == TelegramBotLinkRow.agent_ref)
            .correlate(TelegramBotLinkRow)
            .scalar_subquery()
        )
        return query.add_columns(people)

    def check(self, rows):
        from app.gateway.deps import bind_user_repo

        self._mode_current()
        required = 3 if self.source.request["purpose"] == "information" else 6
        latest_wrong = max((row for row in self.source.responses if row["disposition"] == "wrong_recipient"), key=lambda row: (row["created_at"], row["id"]), default=None)
        excluded = latest_wrong["actor_id"] if latest_wrong else None
        users, facts = {}, {}
        for user in _RequestProjection.objects(rows[0][-1]):
            users[user.id] = user
            if user.disabled_issuer is not None:
                facts.setdefault(user.id, []).append(
                    SimpleNamespace(issuer=user.disabled_issuer, subject=user.disabled_subject, disabled_at=datetime.fromisoformat(user.disabled_at) if isinstance(user.disabled_at, str) else user.disabled_at)
                )
        repository = bind_user_repo(self.sessions)
        recipients, eligible = [], set()
        for user in users.values():
            if user.permissions & required != required:
                continue
            user.password_hash = None
            for key in ("last_sign_in_at", "released_at"):
                if isinstance(getattr(user, key), str):
                    setattr(user, key, datetime.fromisoformat(getattr(user, key)))
            owner = _PreparedLiveOwner(user.id, "", self.sign_on_only, repository, self.sessions.kw["bind"].sync_engine)
            try:
                owner.check_snapshot(user, facts.get(user.id, []))
            except AccessOff:
                continue
            eligible.add(user.id)
            if user.id != excluded:
                recipients.append({"user_id": user.id, "name": user.email, "permissions": user.permissions})
        recipients.sort(key=lambda row: (row["user_id"] != self.link.owner_user_id, row["name"].casefold(), row["user_id"]))
        if not self.routing["after_wrong"] and self.source.request["recipient_id"] in eligible:
            raise ProductSourceRetired()
        if recipients != self.routing["recipients"]:
            raise ProductSourceRetired()
        self._mode_current()
