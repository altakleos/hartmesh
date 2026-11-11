"""Complete current-authority reads before notification/list presentation caps."""

from datetime import datetime

from app.telegram_bots.attention_authority import current_attention
from app.telegram_bots.authority import AuthorityUnavailable
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.paging import utc
from deerflow.agent_instances.work import UNRESOLVED
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained


def _time(value):
    return datetime.fromisoformat(value).timestamp()


class AttentionReader:
    """No stored list or chat entry point is installed by this reader."""

    def __init__(self, *, work, authority, links):
        self.work, self.authority, self.links = work, authority, links

    async def _current(self, captured):
        return await current_attention(self, captured)

    async def collect(self, link, *, as_of):
        return await await_drained(self._collect(link, as_of=utc(as_of)))

    async def _collect(self, link, *, as_of):
        basis = await self._current(link)
        held = basis[0]
        actor = PrincipalRef("human", link.owner_user_id)

        async def complete(service, **kwargs):
            cursor, seen, items = None, set(), {}
            while True:
                page = await service.list_page(actor=actor, instance_id=link.agent_ref, limit=100, cursor=cursor, as_of=as_of, **kwargs)
                for item in page["items"]:
                    items[item["id"]] = item
                cursor = page["next_cursor"]
                if cursor is None:
                    return list(items.values())
                if cursor in seen:
                    raise AuthorityUnavailable()
                seen.add(cursor)

        records = await complete(self.work)
        requests = await complete(HumanInput(self.work), view="all", can_write="agents:write" in held)
        if await self._current(link) != basis:
            raise AuthorityUnavailable()
        pending = [row for row in requests if row["state"] == "pending" and row["recipient_id"] == link.owner_user_id and not row["needs_routing"] and (row["can_respond"] if row["purpose"] == "information" else row["can_manage"])]
        routing = [row for row in requests if row["state"] != "closed" and row["needs_routing"] and row["can_manage"]]
        actionable = {row["work_id"] for row in pending + routing}
        answered = {row["work_id"] for row in requests if row["state"] == "answered" and row["recipient_id"] == link.owner_user_id and not row["needs_routing"]}

        def rank(row):
            attempt = row.get("attempt") or {}
            if row["id"] in actionable:
                priority = 0
            elif attempt.get("status") in UNRESOLVED:
                priority = 1
            elif row["status"] == "blocked" and row["id"] in answered:
                priority = 2
            elif row["status"] in {"open", "blocked", "submitted"}:
                priority = 3
            else:
                priority = 4
            return priority, -_time(row["updated_at"]) if priority == 4 else _time(row["created_at"]), row["id"]

        return {
            "as_of": as_of.isoformat(),
            "work": sorted(records, key=rank),
            "requests": requests,
            "pending": sorted(pending, key=lambda row: (_time(row["created_at"]), row["id"])),
            "routing": sorted(routing, key=lambda row: (_time(row["created_at"]), row["id"])),
        }
