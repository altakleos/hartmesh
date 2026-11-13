"""Pure copy over the Gateway route's owner-filtered capacity snapshot."""

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256

from app.telegram_bots.render import render

_KEY = re.compile(r"[0-9a-f]{32}\Z")
_FIELDS = {"instance_id", "instance_name", "work_id", "objective", "started_at", "requester_id"}


def date(value):
    if type(value) is not str:
        return None
    try:
        result = datetime.fromisoformat(value)
        return result.astimezone(UTC) if result.utcoffset() is not None else None
    except ValueError:
        return None


@dataclass(frozen=True)
class Occupant:
    instance_id: str
    instance_name: str
    work_id: str
    objective: str
    started_at: datetime | None
    requester_id: str | None

    @classmethod
    def parse(cls, value):
        if (
            type(value) is not dict
            or set(value) != _FIELDS
            or any(type(value[key]) is not str or not _KEY.fullmatch(value[key]) for key in ("instance_id", "work_id"))
            or type(value["instance_name"]) is not str
            or not 1 <= len(value["instance_name"]) <= 512
            or type(value["objective"]) is not str
            or not 1 <= len(value["objective"]) <= 4096
        ):
            return None
        requester = value["requester_id"]
        return cls(value["instance_id"], value["instance_name"], value["work_id"], value["objective"], date(value["started_at"]), requester if type(requester) is str and 1 <= len(requester) <= 128 else None)


@dataclass(frozen=True)
class Capacity:
    maximum: int
    in_use: int
    running: tuple[Occupant, ...]
    signature: str

    @property
    def available(self):
        return self.in_use < self.maximum

    @classmethod
    def parse(cls, value):
        if (
            type(value) is not dict
            or set(value) != {"max", "in_use", "running"}
            or type(value["max"]) is not int
            or value["max"] < 1
            or type(value["in_use"]) is not int
            or value["in_use"] < 0
            or type(value["running"]) is not list
            or len(value["running"]) > value["in_use"]
        ):
            raise ValueError("invalid-work-capacity")
        try:
            signature = sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()
        except (TypeError, ValueError):
            raise ValueError("invalid-work-capacity") from None
        occupants = tuple(found for item in value["running"] if (found := Occupant.parse(item)) is not None)
        return cls(value["max"], value["in_use"], occupants, signature)


def waiting_html(capacity, *, objective, locale, people, time):
    """Unknown task dates/people stay unknown; no assignment fallback exists."""
    if capacity.available:
        return None
    key = "busy.work_limit_over" if capacity.in_use > capacity.maximum else "busy.work_limit" if capacity.maximum == 1 else "busy.work_limit_many"
    text = render(key, locale, objective=objective, n=capacity.in_use, maximum=capacity.maximum)
    if capacity.running:
        occupant = capacity.running[0]
        person = people.get(occupant.requester_id) if occupant.requester_id is not None else None
        stamp = time(occupant.started_at) if occupant.started_at is not None else None
        key = "busy.work_limit_which" if person and stamp else "busy.work_limit_which_no_time" if person else "busy.work_limit_which_no_person" if stamp else "busy.work_limit_which_basic"
        text += "\n" + render(key, locale, other=occupant.instance_name, objective=occupant.objective, time=stamp, person=person)
    return text
