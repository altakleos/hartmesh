"""Instance-scoped review of one immutable change to an adopted definition."""

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select

from deerflow.agent_instances.contract import AgentConflict, AgentDenied, AgentPermission, DefinitionSnapshot, ExecutionUnavailable
from deerflow.agent_instances.conversations import AgentExecution
from deerflow.agent_instances.lifecycle import InstanceLifecycle
from deerflow.agent_instances.paging import PageCursor, stored_time
from deerflow.config.work_policy import WorkPolicy
from deerflow.persistence.agent_instances.model import AgentLifecycleRow
from deerflow.persistence.agent_instances.proposals import AgentProposalRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.spaces.contract import Permission

FIELD = "work_policy.enabled"
REVIEW = AgentPermission.INSPECT | AgentPermission.MANAGE


def effective_policy(snapshot):
    return WorkPolicy.model_validate(snapshot.config.get("work_policy") or {})


def candidate_for(snapshot, value):
    if type(value) is not bool:
        raise ValueError("A tracked assignment proposal requires a Boolean")
    snapshot.validated()
    if value == effective_policy(snapshot).enabled:
        raise AgentConflict("Tracked assignments already have that setting")
    config = snapshot.config
    config["work_policy"] = {**(config.get("work_policy") or {}), "enabled": value}
    return DefinitionSnapshot.capture(owner_id=snapshot.owner_id, config=config, soul=snapshot.soul)


async def proposal_row(session, instance_id, proposal_id):
    row = await session.scalar(select(AgentProposalRow).where(AgentProposalRow.id == proposal_id, AgentProposalRow.instance_id == instance_id).with_for_update())
    if row is None:
        raise AgentDenied("Configuration proposal is unavailable")
    return row


def review_authority(view):
    if view.custody != "company" or view.permissions & REVIEW != REVIEW or view.status == "deleted":
        raise AgentDenied("Configuration review requires current Inspect and Manage")


async def verify_association(session, intent, view):
    """All lifecycle recovery paths use the same committed proposal and actor."""
    key = intent.request.get("proposal_id")
    if key is None:
        return
    review_authority(view)
    row = await proposal_row(session, view.id, key)
    if row.operation_id != intent.operation_id or row.base_generation != intent.generation or row.decided_by != intent.actor_id or row.candidate_digest != intent.request.get("definition_revision"):
        raise AgentConflict("Configuration proposal decision does not match lifecycle intent")


@dataclass(frozen=True)
class ReviewedProposal:
    id: str
    base_generation: int
    base_revision: str
    candidate_digest: str

    def matches(self, row):
        if (self.base_generation, self.base_revision, self.candidate_digest) != (row.base_generation, row.base_revision, row.candidate_digest):
            raise AgentConflict("The reviewed proposal changed")

    async def bind(self, session, agents, actor, current, operation_id, candidate, load_source):
        review_authority(current)
        row = await proposal_row(session, current.id, self.id)
        self.matches(row)
        if row.decided_at is not None or current.status != "active" or current.generation != row.base_generation or current.definition_revision != row.base_revision:
            raise AgentConflict("The employee or proposal changed. Review a fresh proposal.")
        base = await agents._stored_definition(session, current.definition_revision)
        if base.owner_id != actor.subject_id:
            raise AgentDenied("Fresh adoption requires the definition owner")
        if candidate_for(base, row.proposed_value) != candidate or candidate.revision != row.candidate_digest:
            raise AgentConflict("The reviewed definition changed")
        await load_source(actor, base.config["name"].lower())
        row.operation_id, row.decided_by, row.decided_at = operation_id, actor.subject_id, datetime.now(UTC)


class ConfigurationProposals:
    def __init__(self, agents, *, admission=None):
        self.agents = agents
        self.admission = admission
        self.lifecycle = InstanceLifecycle(agents, proposal_admission=admission)

    async def submit(self, execution, *, run_id, issuer_id, call_id, proposed_value, check_live=None):
        if (
            not isinstance(execution, AgentExecution)
            or execution.authority.instances is not self.agents
            or not execution.proposal_submission
            or not callable(execution.proposal_account)
            or not execution.execution_allowed
            or execution.work is not None
            or execution.instance.custody != "company"
        ):
            raise AgentDenied("Only an interactive employee turn can submit a proposal")
        if type(proposed_value) is not bool or any(type(key) is not str or not 1 <= len(key) <= maximum for key, maximum in ((run_id, 128), (issuer_id, 256), (call_id, 256))):
            raise ValueError("Invalid proposal issuance")
        await execution.authority.require_ready(execution)
        employee = execution.instance
        async with self.agents.files.registry.admitted(actor=employee.principal, requests={employee.home_id: (Permission.READ | Permission.WRITE, execution.home_generation)}) as (session, _homes):
            await execution.authority.context_admission_in_session(session, execution)
            await execution.proposal_account(session)
            row = await session.scalar(
                select(AgentProposalRow).where(
                    AgentProposalRow.instance_id == employee.id,
                    AgentProposalRow.thread_incarnation == execution.thread_incarnation,
                    AgentProposalRow.run_id == run_id,
                    AgentProposalRow.issuer_id == issuer_id,
                    AgentProposalRow.call_id == call_id,
                )
            )
            if row is not None:
                if (
                    row.proposed_value != proposed_value
                    or row.requester_id != execution.requester.subject_id
                    or row.thread_id != execution.thread_id
                    or row.base_revision != employee.definition_revision
                    or row.base_generation != employee.generation
                ):
                    raise AgentConflict("Proposal issuance already belongs to another request")
            else:
                base = await self.agents._stored_definition(session, employee.definition_revision)
                candidate = candidate_for(base, proposed_value)
                row = AgentProposalRow(
                    id=uuid4().hex,
                    instance_id=employee.id,
                    author_id=employee.principal.subject_id,
                    requester_id=execution.requester.subject_id,
                    thread_id=execution.thread_id,
                    thread_incarnation=execution.thread_incarnation,
                    run_id=run_id,
                    issuer_id=issuer_id,
                    call_id=call_id,
                    base_generation=employee.generation,
                    base_revision=base.revision,
                    field=FIELD,
                    proposed_value=proposed_value,
                    candidate_digest=candidate.revision,
                    created_at=datetime.now(UTC),
                )
                session.add(row)
            if check_live is not None:
                check_live()
            receipt = {"id": row.id, "field": FIELD, "proposed_value": proposed_value}
        return receipt

    @asynccontextmanager
    async def _review(self, actor, instance_id, *, writing=False):
        async with self.agents._sf() as session, session.begin():
            if writing:
                await self.agents._reserve_writer(session)
            _row, _grant, view = await self.lifecycle._parent(session, actor, instance_id)
            review_authority(view)
            if self.admission is not None:
                await self.admission(session)
            yield session, view

    async def _blocked(self, session, view, actor, base, load_source):
        if view.status != "active":
            return "employee-inactive"
        if base.owner_id != actor.subject_id:
            return "different-definition-owner"
        home = await session.get(SpaceRow, view.home_id)
        if home is None or home.status not in {"active", "archived"} or not await self.lifecycle._stored_file_grant(session, view.home_id, actor) & Permission.ADMIN:
            return "home-admin-required"
        try:
            await load_source(actor, base.config["name"].lower())
        except (FileNotFoundError, ValueError):
            return "definition-unavailable"
        return None

    async def _project(self, session, view, actor, row, load_source, cache):
        snapshots = cache.setdefault("snapshots", {})

        async def snapshot(revision):
            if revision not in snapshots:
                snapshots[revision] = await self.agents._stored_definition(session, revision)
            return snapshots[revision]

        base = await snapshot(row.base_revision)
        candidate = candidate_for(base, row.proposed_value)
        if row.field != FIELD or candidate.revision != row.candidate_digest:
            raise AgentConflict("Invalid recorded configuration proposal")
        current = await snapshot(view.definition_revision)
        stale = (view.generation, current.revision) != (row.base_generation, row.base_revision)
        status, resolved_by, blocked = "open", None, None
        if row.operation_id:
            intent = await session.get(AgentLifecycleRow, (view.id, row.operation_id))
            if intent is None or intent.request.get("proposal_id") != row.id:
                raise AgentConflict("Configuration decision history is unavailable")
            await verify_association(session, intent, view)
            status = {"pending": "applying", "complete": "adopted", "abandoned": "abandoned"}[intent.phase]
            resolved_by = intent.resolved_by
        elif row.decided_at is not None:
            status = "declined"
        elif stale:
            blocked = "stale"
        else:
            if "blocked" not in cache:
                cache["blocked"] = await self._blocked(session, view, actor, base, load_source)
            blocked = cache["blocked"]
        return dict(
            id=row.id,
            instance_id=view.id,
            employee_name=view.name,
            field=row.field,
            proposed_value=row.proposed_value,
            base_value=effective_policy(base).enabled,
            current_value=effective_policy(current).enabled,
            base_generation=row.base_generation,
            current_generation=view.generation,
            base_revision=base.revision,
            current_revision=current.revision,
            candidate_digest=candidate.revision,
            created_at=stored_time(row.created_at),
            decided_at=stored_time(row.decided_at) if row.decided_at else None,
            decided_by=row.decided_by,
            resolved_by=resolved_by,
            operation_id=row.operation_id,
            status=status,
            stale=stale,
            adopt_blocked_reason=blocked,
            effective_work_policy=effective_policy(candidate).model_dump(mode="json"),
        )

    async def _project_readiness(self, rows, actor, instance_id):
        # The deployment probe owns separate SQL reads, including on hosts with
        # one available connection. Release the review transaction first.
        eligible = [row for row in rows if row["status"] == "open" and row["adopt_blocked_reason"] is None]
        if eligible and self.agents.execution_probe is not None:
            readiness = await self.agents.execution_probe(instance_id)
            async with self._review(actor, instance_id):
                if readiness.execution == "unavailable":
                    for row in eligible:
                        row["adopt_blocked_reason"] = "lifecycle-unavailable"

    async def list(self, actor, instance_id, load_source, *, limit=20, cursor=None):
        page = PageCursor.read(actor=actor, instance=instance_id, view="proposals", limit=limit, cursor=cursor)
        async with self._review(actor, instance_id) as (session, view):
            rows = (await session.scalars(select(AgentProposalRow).where(AgentProposalRow.instance_id == instance_id, *page.where(AgentProposalRow)).order_by(AgentProposalRow.created_at, AgentProposalRow.id).limit(limit + 1))).all()
            cache = {}
            proposals = [await self._project(session, view, actor, row, load_source, cache) for row in rows[:limit]]
            following = page.next(rows[limit - 1]) if len(rows) > limit else None
        await self._project_readiness(proposals, actor, instance_id)
        return {"proposals": proposals, "next_cursor": following}

    async def detail(self, actor, instance_id, proposal_id, load_source):
        async with self._review(actor, instance_id) as (session, view):
            row = await proposal_row(session, instance_id, proposal_id)
            result = await self._project(session, view, actor, row, load_source, {})
        await self._project_readiness([result], actor, instance_id)
        return result

    async def decline(self, actor, instance_id, proposal_id, load_source):
        async with self._review(actor, instance_id, writing=True) as (session, _view):
            row = await proposal_row(session, instance_id, proposal_id)
            if row.operation_id:
                raise AgentConflict("The proposal already has an adoption decision")
            if row.decided_at is None:
                row.decided_at, row.decided_by = datetime.now(UTC), actor.subject_id
        return await self.detail(actor, instance_id, proposal_id, load_source)

    async def adopt(self, actor, instance_id, proposal_id, basis, load_source):
        reviewed = ReviewedProposal(id=proposal_id, **basis)
        async with self._review(actor, instance_id) as (session, view):
            row = await proposal_row(session, instance_id, proposal_id)
            reviewed.matches(row)
            if row.operation_id is None and row.decided_at is not None:
                raise AgentConflict("The proposal was declined")
            base = await self.agents._stored_definition(session, row.base_revision)
            if not row.operation_id and base.owner_id != actor.subject_id:
                raise AgentDenied("Fresh adoption requires the definition owner")
            candidate = candidate_for(base, row.proposed_value)
            # Private unpredictable operation identity until withdrawal commits.
            operation_id = row.operation_id or uuid4().hex
        await self.agents.files.registry.get(actor=actor, space_id=view.home_id, permission=Permission.ADMIN)
        if self.agents.execution_probe is not None:
            state = await self.agents.execution_probe(instance_id)
            if state.execution == "unavailable":
                raise ExecutionUnavailable(state.reason)
        await self.lifecycle.change(
            actor=actor,
            instance_id=instance_id,
            expected_generation=reviewed.base_generation,
            operation_id=operation_id,
            action="adopt",
            definition=candidate,
            proposal=reviewed,
            load_source=load_source,
            only_if_idle=True,
            runs_in_flight=(lambda: self.agents.residents.in_flight(instance_id)) if self.agents.residents is not None else None,
        )
        return await self.detail(actor, instance_id, proposal_id, load_source)
