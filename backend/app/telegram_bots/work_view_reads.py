"""Finite list pages join all displayed details in one final authority read."""

from copy import deepcopy
from dataclasses import asdict, dataclass

from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import current_attention, prepare_attention
from app.telegram_bots.authority import AuthorityUnavailable
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_details import WorkDetailProjection, WorkDetailsReader
from app.telegram_bots.work_lists import _snapshot
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class WorkPageRead:
    frame: dict
    details: tuple
    name: str
    protected: bool
    snapshot_signature: str
    signature: str


class WorkViewsReader(WorkDetailsReader):
    async def read_page(self, link, cursor):
        async def perform():
            frame = await self.page(link, cursor)
            details = tuple([await self.detail(link, record["id"]) for record in frame["work"]])
            snapshot = await self._list_snapshot(link, cursor)
            projection = WorkPageProjection(self, link, frame, details, snapshot)
            await current_attention(self, link, projection=projection)
            return projection.source

        return await await_drained(perform())

    async def prepare_page(self, link, source):
        admission = await prepare_attention(self, link)
        projection = WorkPageProjection(self, link, source.frame, source.details, source.snapshot_signature, expected=source)

        async def check(*, session=None):
            return await admission(session=session, projection=projection)

        return check


class WorkPageProjection:
    def __init__(self, reader, link, frame, details, snapshot, *, expected=None):
        self.reader, self.link = reader, link
        self.frame, self.snapshot, self.expected = deepcopy(frame), snapshot if type(snapshot) is str else snapshot_signature(snapshot), deepcopy(expected)
        self.details = {source.record["id"]: WorkDetailProjection(reader, link, source.record, source.request, reusable=source.can_reuse_conversation, expected=source) for source in details}
        self.source = None

    def extend(self, query):
        if self.details:
            first = next(iter(self.details.values()))
            query = first.extend(query, work_ids=list(self.details))
            for projection in self.details.values():
                projection.column = first.column
            self.work_column = first.column
        self.snapshot_column = len(query.column_descriptions)
        snapshot = aliased(TelegramCallbackRow)
        return query.add_columns(snapshot).join(snapshot, snapshot.id == self.frame["snapshot_id"])

    def check_with_permissions(self, rows, held):
        actual = _snapshot(rows[0][self.snapshot_column], self.link, self.frame["cursor"], self.reader.clock())
        if snapshot_signature(actual) != self.snapshot:
            raise AuthorityUnavailable()
        body, expires = actual[1], actual[3]
        position = body["cursors"][self.frame["cursor"]]
        slots = body["work_ids"][position * 10 : (position + 1) * 10]
        ids = [record["id"] for record in self.frame["work"]]
        tokens = {value: key for key, value in body["cursors"].items()}
        if (
            ids != [identity for identity in slots if identity in ids]
            or self.frame["positions"] != {identity: position * 10 + index + 1 for index, identity in enumerate(slots) if identity in ids}
            or (self.frame["page"], self.frame["pages"], self.frame["previous"], self.frame["next"], self.frame["expires_at"]) != (position + 1, len(tokens), tokens.get(position - 1), tokens.get(position + 1), expires.isoformat())
        ):
            raise AuthorityUnavailable()
        sources = {}
        for identity, projection in self.details.items():
            matching = [row for row in rows if row[self.work_column].id == identity]
            if not matching:
                raise AuthorityUnavailable()
            projection.check_with_permissions(matching, held)
            sources[identity] = projection.source
        instance, policy = rows[0][1], rows[0][3]
        frame = {**self.frame, "work": [sources[record["id"]].record for record in self.frame["work"]]}
        requests = [source.request for source in sources.values() if source.request is not None]
        frame.update(
            requests=requests,
            pending=[
                request
                for request in requests
                if request["state"] == "pending" and request["recipient_id"] == self.link.owner_user_id and not request["needs_routing"] and (request["can_respond"] if request["purpose"] == "information" else request["can_manage"])
            ],
            routing=[request for request in requests if request["needs_routing"] and request["can_manage"]],
        )
        details = tuple(sources[record["id"]] for record in frame["work"])
        source = WorkPageRead(frame, details, instance.name, instance.custody != "personal" and policy == "protected", self.snapshot, "")
        self.source = WorkPageRead(**{**asdict(source), "details": details, "signature": digest(asdict(source))})
        if self.expected is not None and not same_body(asdict(self.source), asdict(self.expected)):
            raise AuthorityUnavailable()


def snapshot_signature(snapshot):
    identity, body, horizon, expires = snapshot
    return digest([identity, body, horizon.isoformat(), expires.isoformat()])
