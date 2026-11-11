"""Scoped read cursors over immutable creation keys, never authorization tokens."""

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import and_, or_


def utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Pagination requires an aware timestamp")
    return value.astimezone(UTC)


def stored_time(value):
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


@dataclass(frozen=True)
class PageCursor:
    actor: str
    instance: str
    view: str
    as_of: datetime
    after: tuple[datetime, str] | None = None

    @classmethod
    def read(cls, *, actor, instance, view, limit, cursor=None, as_of=None):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("Invalid page size")
        horizon = utc(as_of) if as_of is not None else None
        if cursor is None:
            return cls(actor.subject_id, instance, view, horizon or datetime.now(UTC))
        try:
            if type(cursor) is not str or not 1 <= len(cursor) <= 4096:
                raise ValueError
            value = json.loads(base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True))
            if type(value) is not dict or set(value) != {"v", "actor", "instance", "view", "as_of", "created_at", "id"} or type(value["v"]) is not int or value["v"] != 1:
                raise ValueError
            if (value["actor"], value["instance"], value["view"]) != (actor.subject_id, instance, view):
                raise ValueError
            fixed = utc(datetime.fromisoformat(value["as_of"]))
            created = utc(datetime.fromisoformat(value["created_at"]))
            identity = value["id"]
            if horizon is not None and horizon != fixed or created > fixed or type(identity) is not str or len(identity) != 32 or any(ch not in "0123456789abcdef" for ch in identity):
                raise ValueError
            return cls(actor.subject_id, instance, view, fixed, (created, identity))
        except (ValueError, TypeError, KeyError, RecursionError, OverflowError):
            raise ValueError("Invalid scoped page cursor") from None

    def where(self, row):
        conditions = [row.created_at <= self.as_of]
        if self.after is not None:
            created, identity = self.after
            conditions.append(or_(row.created_at > created, and_(row.created_at == created, row.id > identity)))
        return conditions

    def next(self, row):
        value = dict(v=1, actor=self.actor, instance=self.instance, view=self.view, as_of=self.as_of.isoformat(), created_at=stored_time(row.created_at).isoformat(), id=row.id)
        return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode().rstrip("=")
