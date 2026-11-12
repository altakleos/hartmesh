"""Compose bounded Work source projections into one provider-final snapshot."""

from datetime import date, datetime
from types import SimpleNamespace

from sqlalchemy import JSON, Boolean, Date, DateTime, Float, Integer, String, and_, inspect, literal, select, true
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramInstancePolicyRow


def _member_base(link):
    # Account/identity/role rows belong to the outer authority read. Repeating
    # those one-to-many joins per member would multiply a ten-item digest.
    bot, instance, grant, policy = TelegramBotLinkRow, AgentInstanceRow, AgentInstanceGrantRow, TelegramInstancePolicyRow
    return (
        select(bot, instance, grant.permissions, policy.policy, *[literal(None) for _ in range(4)])
        .join(instance, instance.id == bot.agent_ref)
        .join(grant, and_(grant.instance_id == instance.id, grant.user_id == bot.owner_user_id))
        .outerjoin(policy, policy.instance_id == instance.id)
        .where(bot.id == link.id)
    )


class _EntityArray(FunctionElement):
    type = JSON()
    inherit_cache = True


@compiles(_EntityArray, "sqlite")
def _sqlite_entity(element, compiler, **kw):
    values = [compiler.process(value, **kw) for value in element.clauses]
    values = [f"json({sql})" if isinstance(value.type, JSON) else sql for value, sql in zip(element.clauses, values, strict=True)]
    return "json_array(" + ", ".join(values) + ")"


@compiles(_EntityArray, "postgresql")
def _postgres_entity(element, compiler, **kw):
    return "json_build_array(" + ", ".join(compiler.process(value, **kw) for value in element.clauses) + ")"


class _SourceFacts(SimpleNamespace):
    def to_dict(self, *, exclude=None):
        # Preserve the mapped-row read contract used by exact-output evidence.
        return {key: value for key, value in vars(self).items() if not exclude or key not in exclude}


def _entity_decoder(properties):
    def decode(value):
        if value is None:
            return None
        if type(value) is not list or len(value) != len(properties):
            raise ProductSourceRetired()
        if all(item is None for prop, item in zip(properties, value, strict=True) if prop.columns[0].primary_key):
            return None
        fields = {}
        for prop, item in zip(properties, value, strict=True):
            kind = prop.columns[0].type
            if item is not None:
                if isinstance(kind, DateTime):
                    item = datetime.fromisoformat(item)
                elif isinstance(kind, Date):
                    item = date.fromisoformat(item)
                elif isinstance(kind, Boolean):
                    if item not in (0, 1, False, True):
                        raise ProductSourceRetired()
                    item = bool(item)
            fields[prop.key] = item
        return _SourceFacts(**fields)

    return decode


def _source_columns(query, subquery):
    """Preserve ORM entities, scalar positions and distinct aliased source roles."""
    columns, decoders, offset = [], [], 0
    for index, description in enumerate(query.column_descriptions):
        expression = description["expr"]
        inspected = inspect(expression, raiseerr=False)
        if inspected is not None and (getattr(inspected, "is_mapper", False) or getattr(inspected, "is_aliased_class", False)):
            mapper = inspected.mapper if getattr(inspected, "is_aliased_class", False) else inspected
            properties = list(mapper.column_attrs)
            if len(properties) > 100 or any(len(prop.columns) != 1 or not isinstance(prop.columns[0].type, (JSON, Boolean, DateTime, Date, String, Integer, Float)) for prop in properties):
                raise ValueError("unsupported-digest-source-entity")
            # One SQL result column per entity avoids PostgreSQL's 1,664-column
            # limit at ten complex members. Positions preserve distinct aliases.
            value = _EntityArray(*list(subquery.c)[offset : offset + len(properties)])
            decoder = _entity_decoder(properties)
            offset += len(properties)
        else:
            value = subquery.c[offset]

            def decoder(value):
                return value

            offset += 1
        if index >= 8:
            columns.append(value)
            decoders.append(decoder)
    if offset != len(subquery.c) or not columns:
        raise ValueError("unsupported-digest-source-projection")
    return columns, decoders


class DigestProjection:
    def __init__(self, link, members):
        if not 1 <= len(members) <= 10:
            raise ValueError("invalid-digest-source-count")
        self.link, self.members = link, tuple(members)
        self.widths, self.offsets, self.decoders = [], [], []

    def extend(self, query):
        self.widths, self.offsets, self.decoders = [], [], []
        for index, projection in enumerate(self.members):
            source = projection.extend(_member_base(self.link))
            nested = source.subquery(f"digest_member_{index}")
            columns, decoders = _source_columns(source, nested)
            self.decoders.append(decoders)
            self.offsets.append(len(query.column_descriptions))
            self.widths.append(len(columns))
            query = query.add_columns(*columns).outerjoin(nested, true())
        return query

    def check_with_permissions(self, rows, held):
        for projection, offset, width, decoders in zip(self.members, self.offsets, self.widths, self.decoders, strict=True):
            projected = [(*row[:8], *(decode(value) for decode, value in zip(decoders, row[offset : offset + width], strict=True))) for row in rows]
            if not projected or all(value is None for value in projected[0][8:]):
                raise ProductSourceRetired()
            qualified = getattr(projection, "check_with_permissions", None)
            if qualified is not None:
                qualified(projected, held)
            else:
                projection.check(projected)
