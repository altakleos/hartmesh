"""Compact referenced-Space proof inside the final Work presentation SELECT."""

import json
from types import SimpleNamespace

from sqlalchemy import and_, false, func, select

from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.spaces.model import SpaceGrantRow, SpaceRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow
from deerflow.spaces.contract import Permission


class ReferencedSpaces:
    def __init__(self, reader, sessions, source):
        from app.telegram_bots.work_request_source import _references

        self.reader = reader
        self.sqlite = sessions.kw["bind"].dialect.name == "sqlite"
        self.identities = set(_references((source.request, source.work, source.responses)))

    def expression(self):
        space, grant, bot = SpaceRow, SpaceGrantRow, TelegramBotLinkRow
        fields = {column.name: column for column in space.__table__.columns if column.name not in {"created_at", "updated_at"}}
        args = [value for key, column in {**fields, "permissions": grant.permissions}.items() for value in (key, column)]
        aggregate = func.json_group_array(func.json_object(*args)) if self.sqlite else func.json_agg(func.json_build_object(*args))
        return (
            select(aggregate)
            .select_from(space)
            .outerjoin(grant, and_(grant.space_id == space.id, grant.principal_kind == "human", grant.subject_id == bot.owner_user_id))
            .where(space.id.in_(self.identities) if self.identities else false())
            .correlate(bot)
            .scalar_subquery()
        )

    def check(self, value):
        values = json.loads(value) if isinstance(value, str) else value or []
        seen = set()
        for fields in values:
            space = SimpleNamespace(**fields)
            if space.status not in {"active", "archived"} or space.permissions is None or not self.reader.work.agents.files.registry._view(space, space.permissions).permissions & Permission.READ:
                raise ProductSourceRetired()
            seen.add(space.id)
        if seen != self.identities:
            raise ProductSourceRetired()
