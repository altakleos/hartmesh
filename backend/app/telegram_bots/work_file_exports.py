"""Current-recipient exports of exact Work outcome references; no worker wiring."""

from copy import deepcopy
from dataclasses import dataclass

from sqlalchemy import and_

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.producer import DeliveryClaimRetired, ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from deerflow.agent_instances.work_contract import WorkOutcome, WorkSource
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.spaces.model import SpaceGrantRow, SpaceRow
from deerflow.spaces.contract import Permission, PrincipalRef, SpaceDenied
from deerflow.spaces.service import SpaceFiles
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class _WorkFileExport:
    reader: object
    link: object
    files: SpaceFiles
    work_id: str
    outcome_id: str
    evidence_revision: int
    assignment_revision: int
    source_index: int
    source: dict
    generation: int
    current: object

    def _current(self):
        if (
            self.current() is not True
            or getattr(self.reader.authority.app.state, "agent_instances", None) is not self.reader.work.agents
            or self.reader.work.agents.files is not self.files
            or getattr(self.reader.authority.app.state, "storage_spaces", None) is not self.files
        ):
            raise ProductSourceRetired()

    async def _admit(self, admission, writer, rows, *, check_delivery=None, protect_content=None, projection=None):
        self._current()
        admitted = rows.get(self.source["space_id"])
        if admitted is None or admitted[0].generation != self.generation:
            raise SpaceDenied("The original result file is unavailable")
        if check_delivery is not None:
            await check_delivery(writer)
        await admission(session=writer, projection=_WorkFileProjection(self, protect_content, projection))
        self._current()

    async def authorize(self, *, check_delivery=None, protect_content=None, projection=None):
        if protect_content is not None and type(protect_content) is not bool or check_delivery is not None and not callable(check_delivery):
            raise ValueError("work-file-dispatch-required")

        async def perform():
            self._current()
            admission = await prepare_attention(self.reader, self.link)
            async with self.files.registry.admitted(actor=PrincipalRef("human", self.link.owner_user_id), requests={self.source["space_id"]: (Permission.READ | Permission.EXPORT, self.generation)}) as (writer, rows):
                await self._admit(admission, writer, rows, check_delivery=check_delivery, protect_content=protect_content, projection=projection)
            self._current()

        return await await_drained(perform())

    async def read(self, *, max_bytes, check_delivery=None, projection=None):
        if type(max_bytes) is not int or not 0 < max_bytes <= 50_000_000 or check_delivery is not None and not callable(check_delivery):
            raise ValueError("work-file-export-limit")

        async def perform():
            self._current()
            admission = await prepare_attention(self.reader, self.link)

            async def admitted(writer, rows):
                await self._admit(admission, writer, rows, check_delivery=check_delivery, projection=projection)

            result = None
            try:
                result = await self.files.export_file(actor=PrincipalRef("human", self.link.owner_user_id), space_id=self.source["space_id"], path=self.source["path"], max_bytes=max_bytes, controller_admission=admitted)
                self._current()
                return result
            finally:
                result = None

        return await await_drained(perform())


class _WorkFileProjection:
    def __init__(self, export, protect_content=None, delivery=None):
        self.export, self.protect_content, self.delivery = export, protect_content, delivery

    def extend(self, query):
        value = self.export
        work, space, grant = AgentWorkRow, SpaceRow, SpaceGrantRow
        self.column = len(query.column_descriptions)
        result = (
            query.add_columns(work, space, grant.permissions)
            .join(work, and_(work.instance_id == value.link.agent_ref, work.id == value.work_id))
            .join(space, space.id == value.source["space_id"])
            .outerjoin(grant, and_(grant.space_id == space.id, grant.principal_kind == "human", grant.subject_id == value.link.owner_user_id))
        )
        return self.delivery.extend(result) if self.delivery is not None else result

    def check(self, rows):
        value = self.export
        value._current()
        work, space, permission = rows[0][self.column : self.column + 3]
        try:
            outcome = WorkOutcome.model_validate(work.outcome).model_dump()
        except ValueError:
            raise ProductSourceRetired() from None
        sources = outcome["sources"]
        if (
            work.status not in {"submitted", "completed"}
            or (work.outcome_id, work.assignment_revision, work.outcome_assignment_revision) != (value.outcome_id, value.assignment_revision, value.assignment_revision)
            or (outcome.get("id"), outcome.get("evidence_revision"), outcome.get("assignment_revision")) != (value.outcome_id, value.evidence_revision, value.assignment_revision)
            or type(outcome.get("evidence_revision")) is not int
            or value.source_index >= len(sources)
            or not same_body(sources[value.source_index], value.source)
        ):
            raise ProductSourceRetired()
        if (
            space.status not in {"active", "archived"}
            or space.generation != value.generation
            or permission is None
            or value.files.registry._view(space, permission).permissions & (Permission.READ | Permission.EXPORT) != Permission.READ | Permission.EXPORT
        ):
            raise SpaceDenied("The original result file is unavailable")
        target, policy = rows[0][1], rows[0][3]
        if target.custody == "company" and policy == "protected" and self.protect_content is False:
            raise DeliveryClaimRetired()
        if self.delivery is not None:
            self.delivery.check(rows)


async def prepare_work_file_export(reader, link, *, work_id, outcome_id, evidence_revision, source_index, current):
    if type(source_index) is not int or not 0 <= source_index < 16 or type(evidence_revision) is not int or evidence_revision < 1 or not callable(current):
        raise ValueError("work-file-source-required")

    async def perform():
        files = reader.work.agents.files
        if type(files) is not SpaceFiles or files.registry._sf.kw.get("bind") is not reader.links.session_factory.kw.get("bind") or getattr(reader.authority.app.state, "storage_spaces", None) is not files or current() is not True:
            raise ProductSourceRetired()
        admission = await prepare_attention(reader, link)
        await admission()
        record = await reader.work.get(actor=PrincipalRef("human", link.owner_user_id), instance_id=link.agent_ref, work_id=work_id)
        outcome = record.get("outcome") or {}
        if record.get("outcome_id") != outcome_id or outcome.get("evidence_revision") != evidence_revision or source_index >= len(outcome.get("sources") or []):
            raise ProductSourceRetired()
        supplied = outcome["sources"][source_index]
        if supplied.get("kind") != "space_file":
            raise SpaceDenied("The result file is unavailable")
        source = WorkSource.model_validate({key: val for key, val in supplied.items() if key != "current_contents"}).model_dump()
        space = await files.registry.get(actor=PrincipalRef("human", link.owner_user_id), space_id=source["space_id"], permission=Permission.READ | Permission.EXPORT)
        export = _WorkFileExport(reader, deepcopy(link), files, work_id, outcome_id, evidence_revision, record["assignment_revision"], source_index, source, space.generation, current)
        await admission(projection=_WorkFileProjection(export))
        return export

    return await await_drained(perform())
