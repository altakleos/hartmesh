"""Host-owned explicit Work attempts, fenced reports and terminal reconciliation."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import func, select

from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.work import DELEGATE, UNRESOLVED, AgentWork
from deerflow.agent_instances.work_contract import WorkSource
from deerflow.agent_instances.work_execution_contract import ActivateWork, ReportWork
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputRequestRow, HumanInputResponseRow
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentLifecycleRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.utils.file_io import await_drained


@dataclass
class WorkAttempt:
    """Transient host capability. Never deserialize it from messages or run config."""

    service: "WorkExecution" = field(repr=False)
    work_id: str
    attempt_id: str
    run_id: str | None = None
    retired: bool = False
    owned_cleanup_confirmed: bool = False

    async def validate(self, execution):
        if self.retired:
            raise AgentDenied("The Work attempt is no longer active")
        await self.service.validate(execution, self)

    async def bind(self, execution, record, incarnation):
        await self.service.bind(execution, self, record, incarnation)

    async def observe_run(self, execution, record):
        await self.service.observe_run(execution, self, record)

    async def report(self, execution, request):
        if self.retired:
            raise AgentDenied("The Work attempt is no longer active")
        return await self.service.report(execution, self, request)

    async def context(self, execution, *, response_offset=0):
        return await self.service.context(execution, self, response_offset=response_offset)

    async def finish(self, execution, record, *, settled):
        self.retired = True
        return await self.service.finish(execution, self, record, settled=settled)


class WorkExecution:
    def __init__(self, work: AgentWork):
        self.work = work
        self.agents = work.agents
        self._sf = work._sf

    async def _sources(self, session, row):
        sources = list(row.sources) + (row.blocker or {}).get("sources", [])
        sources += await HumanInput.decision_sources(session, row)
        attempt = await session.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == row.id, AgentWorkAttemptRow.status.in_(UNRESOLVED)))
        sources += (attempt.candidate or {}).get("sources", []) if attempt else []
        return sources

    async def _locked(self, session, execution, work_id, *, extra_sources=()):
        """Resource → thread → instance → Work; repeat the source snapshot under locks."""
        await self.agents._reserve_writer(session)
        initial = await self.work._row(session, execution.instance.id, work_id)
        sources = await self._sources(session, initial)
        # Home belongs to the same ordered resource lock domain as source Spaces.
        # Include it without manufacturing a file locator.
        from deerflow.persistence.spaces.model import SpaceRow

        for space_id in sorted({execution.instance.home_id} | {s["space_id"] for s in sources} | {s.space_id for s in extra_sources}):
            await session.execute(select(SpaceRow).where(SpaceRow.id == space_id).with_for_update())
        thread = (await session.execute(select(ThreadMetaRow).where(ThreadMetaRow.thread_id == execution.thread_id).with_for_update())).scalar_one_or_none()
        parent, _ = await self.work._parent(session, execution.requester, execution.instance.id, DELEGATE, lock=True)
        row = (await session.execute(select(AgentWorkRow).where(AgentWorkRow.id == work_id, AgentWorkRow.instance_id == parent.id).with_for_update().execution_options(populate_existing=True))).scalar_one_or_none()
        binding = await session.get(AgentConversationRow, execution.thread_id)
        if row is None or thread is None or thread.incarnation != execution.thread_incarnation or binding is None or binding.instance_id != parent.id:
            raise AgentDenied("Work conversation binding changed")
        home = await session.get(SpaceRow, execution.instance.home_id)
        if home is None or home.status != "active" or home.generation != execution.home_generation:
            raise AgentDenied("Work Home changed")
        await execution.authority.destination_audience(session, execution.instance, binding, memory=True)
        if sources != await self._sources(session, row):
            raise AgentConflict("Work sources changed; reload before acting")
        return parent, row

    async def _admit(self, session, execution, parent, row):
        policy = await self.work._policy(session, parent)
        if (
            parent.status != "active"
            or parent.generation != execution.instance.generation
            or parent.definition_revision != execution.definition.revision
            or row.definition_revision != parent.definition_revision
            or not policy
            or not policy.enabled
        ):
            raise AgentDenied("Work requires an active reconciled mandate")
        if await session.scalar(select(AgentLifecycleRow.operation_id).where(AgentLifecycleRow.instance_id == parent.id, AgentLifecycleRow.phase == "pending")):
            raise AgentDenied("Lifecycle containment is pending")
        if row.status not in {"open", "blocked"}:
            raise AgentConflict("Work requires Changes requested or Reopen before activation")
        await self.work._admit_sources(session, execution.requester, parent, [WorkSource.model_validate(s) for s in await self._sources(session, row)])
        return policy

    async def reserve(self, *, execution, work_id, request: ActivateWork):
        request = ActivateWork.model_validate(request.model_dump())
        if request.thread_id != execution.thread_id or not execution.execution_allowed:
            raise AgentDenied("Activation requires the admitted conversation")
        await execution.authority.validate(execution)
        # Shared Work remains protected after memory is disabled, restart or branching.
        await execution.authority.context_admission(execution, mark_work=True)

        async def perform():
            async with self._sf() as session, session.begin():
                parent, row = await self._locked(session, execution, work_id)
                attempt = await session.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == row.id, AgentWorkAttemptRow.activation_id == request.operation_id))
                if attempt is not None:
                    if attempt.requester_id != execution.requester.subject_id or attempt.request != request.model_dump(mode="json"):
                        raise AgentConflict("Activation identity belongs to another request")
                    return WorkAttempt(self, row.id, attempt.id, attempt.run_id, attempt.status not in UNRESOLVED)
                await self._admit(session, execution, parent, row)
                if (row.revision, row.assignment_revision) != (request.expected_revision, request.expected_assignment_revision):
                    raise AgentConflict("Work changed; reload before activation")
                if await session.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.work_id == row.id, AgentWorkAttemptRow.status.in_(UNRESOLVED))):
                    raise AgentConflict("Reconcile the prior attempt before activating")
                attempt = AgentWorkAttemptRow(
                    id=uuid4().hex,
                    work_id=row.id,
                    activation_id=request.operation_id,
                    requester_id=execution.requester.subject_id,
                    assignment_revision=row.assignment_revision,
                    instance_generation=parent.generation,
                    definition_revision=parent.definition_revision,
                    status="starting",
                    thread_id=execution.thread_id,
                    incarnation=execution.thread_incarnation,
                    request=request.model_dump(mode="json"),
                )
                session.add(attempt)
                self._advance(row)
                await self.work._event(session, execution.requester, row, request, "activate")
                return WorkAttempt(self, row.id, attempt.id)

        return await await_drained(perform())

    async def _current(self, session, execution, scope, *, starting=False, extra_sources=()):
        parent, row = await self._locked(session, execution, scope.work_id, extra_sources=extra_sources)
        attempt = await session.get(AgentWorkAttemptRow, scope.attempt_id)
        if (
            attempt is None
            or attempt.work_id != row.id
            or attempt.assignment_revision != row.assignment_revision
            or attempt.instance_generation != parent.generation
            or attempt.definition_revision != parent.definition_revision
            or attempt.requester_id != execution.requester.subject_id
            or attempt.thread_id != execution.thread_id
            or attempt.incarnation != execution.thread_incarnation
            or attempt.status not in (("starting", "running") if starting else ("running",))
            or not starting
            and (not scope.run_id or attempt.run_id != scope.run_id)
        ):
            raise AgentDenied("The current Work attempt changed")
        await self._admit(session, execution, parent, row)
        return parent, row, attempt

    async def validate(self, execution, scope):
        async with self._sf() as session, session.begin():
            await self._current(session, execution, scope)

    async def bind(self, execution, scope, record, incarnation):
        if record.thread_id != execution.thread_id or record.user_id != execution.requester.subject_id or incarnation != execution.thread_incarnation:
            raise AgentDenied("The admitted run does not match the Work attempt")
        # Record the host identity even if a concurrent cancellation/revocation
        # prevents dispatch. The worker can then settle that exact failed launch.
        await self.observe_run(execution, scope, record)

        async def perform():
            async with self._sf() as session, session.begin():
                _, row, attempt = await self._current(session, execution, scope, starting=True)
                if attempt.run_id and attempt.run_id != record.run_id:
                    raise AgentConflict("The attempt already owns another run")
                attempt.run_id, attempt.status = record.run_id, "running"
                scope.run_id = record.run_id

        await await_drained(perform())

    async def observe_run(self, execution, scope, record):
        if (
            record.thread_id != execution.thread_id
            or record.user_id != execution.requester.subject_id
            or record.idempotency_key != "agent-work:" + scope.attempt_id
            or (record.metadata or {}).get("agent_work_attempt_id") != scope.attempt_id
            or (record.metadata or {}).get("agent_work_id") != scope.work_id
        ):
            raise AgentDenied("The host run does not match the reserved attempt")

        async def perform():
            async with self._sf() as session, session.begin():
                await self.agents._reserve_writer(session)
                from deerflow.persistence.agent_instances.model import AgentInstanceRow

                await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.id == execution.instance.id).with_for_update())
                row = await self.work._row(session, execution.instance.id, scope.work_id, lock=True)
                attempt = await session.get(AgentWorkAttemptRow, scope.attempt_id)
                if (
                    attempt is None
                    or attempt.work_id != row.id
                    or attempt.thread_id != record.thread_id
                    or attempt.requester_id != record.user_id
                    or attempt.incarnation != execution.thread_incarnation
                    or attempt.run_id not in (None, record.run_id)
                ):
                    raise AgentDenied("The reserved attempt changed")
                attempt.run_id = scope.run_id = record.run_id
                status = getattr(record.status, "value", record.status)
                if attempt.status in UNRESOLVED and getattr(record, "idempotency_reused", False) and status in {"success", "error", "timeout", "interrupted"}:
                    # Durable terminal status alone cannot certify host cleanup.
                    attempt.status = "uncertain"

        await await_drained(perform())

    @staticmethod
    def _advance(row):
        if row.revision >= 2147483647:
            raise AgentConflict("Work revision exhausted")
        row.revision += 1
        row.updated_at = datetime.now(UTC)

    async def context(self, execution, scope, *, response_offset=0):
        if type(response_offset) is not int or not 0 <= response_offset <= 200:
            raise ValueError("Invalid response offset")
        async with self._sf() as session, session.begin():
            parent, row, attempt = await self._current(session, execution, scope)
            request = await session.scalar(select(HumanInputRequestRow).where(HumanInputRequestRow.work_id == row.id, HumanInputRequestRow.state != "closed"))
            replies = []
            if request:
                replies = (
                    await session.scalars(select(HumanInputResponseRow).where(HumanInputResponseRow.request_id == request.id).order_by(HumanInputResponseRow.created_at, HumanInputResponseRow.id).offset(response_offset).limit(9))
                ).all()
            # Bound each model projection; assessment still requires the full exact set.
            return {
                "work": await self.work._project(session, execution.requester, parent, self.work._snapshot(row)),
                "candidate": attempt.candidate,
                "request": {"id": request.id, "revision": request.revision, "request_revision": request.request_revision, "purpose": request.purpose} if request else None,
                "responses": [{"id": r.id, "actor_kind": "human", "actor_id": r.actor_id, "text": r.text, "choice": r.choice, "disposition": r.disposition, "sources": r.sources} for r in replies[:8]],
                "next_response_offset": response_offset + 8 if len(replies) > 8 else None,
            }

    async def report(self, execution, scope, request: ReportWork):
        request = ReportWork.model_validate(request.model_dump(exclude_unset=True))
        await execution.authority.validate(execution)

        async def perform():
            async with self._sf() as session, session.begin():
                parent, row, attempt = await self._current(session, execution, scope, extra_sources=request.sources)
                event = await session.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.instance_id == parent.id, AgentWorkEventRow.operation_id == request.operation_id))
                body = request.model_dump(mode="json", exclude_unset=True)
                if event:
                    if event.actor_kind != "nonhuman" or event.actor_id != parent.principal_id or event.work_id != row.id or event.request != {**body, "attempt_id": attempt.id}:
                        raise AgentConflict("Report operation belongs to another intent")
                    return await self.work._project(session, execution.requester, parent, event.result)
                if row.revision != request.expected_revision:
                    raise AgentConflict("Work changed; read the current context before reporting")
                if request.action == "progress":
                    row.progress = request.statement
                    if request.next_action is not None:
                        row.next_action = request.next_action
                elif request.action == "outcome":
                    if row.status != "open":
                        raise AgentConflict("Resolve the current blocker before reporting completion")
                    await self.work._admit_sources(session, execution.requester, parent, request.sources)
                    attempt.candidate = {
                        "id": uuid4().hex,
                        "attempt_id": attempt.id,
                        "assignment_revision": row.assignment_revision,
                        "statement": request.statement,
                        "evidence_revision": 1,
                        "sources": [s.model_dump() for s in request.sources],
                    }
                elif request.action in {"request_input", "revise_input"}:
                    await self.work._admit_sources(session, execution.requester, parent, request.sources)
                    if request.action == "revise_input":
                        await self._assess(session, parent, row, request, revise=True)
                    await self._request(session, parent, row, request, creator=parent.principal_id)
                    attempt.candidate = None
                elif request.action == "assess_input":
                    await self._assess(session, parent, row, request)
                elif request.action in {"suggest", "derive"}:
                    await self._followup(session, execution, parent, row, attempt, request)
                self._advance(row)
                await self._event(session, parent, row, request.operation_id, request.action, {**body, "attempt_id": attempt.id})
                return await self.work._project(session, execution.requester, parent, self.work._snapshot(row))

        return await await_drained(perform())

    async def _event(self, session, parent, row, operation_id, action, request):
        await session.flush()
        session.add(
            AgentWorkEventRow(
                id=uuid4().hex,
                instance_id=parent.id,
                work_id=row.id,
                actor_id=parent.principal_id,
                actor_kind="nonhuman",
                operation_id=operation_id,
                action=action,
                revision=row.revision,
                assignment_revision=row.assignment_revision,
                request=request,
                result=self.work._snapshot(row),
            )
        )
        await session.flush()

    async def _request(self, session, parent, row, request, *, creator, review=False):
        if await session.scalar(select(HumanInputRequestRow.id).where(HumanInputRequestRow.work_id == row.id, HumanInputRequestRow.state != "closed")):
            raise AgentConflict("Work already has a live request")
        if not review and row.status != "open":
            raise AgentConflict("Only open Work can raise a new blocker")
        purpose = "review" if review else request.purpose
        sources = (row.outcome or {}).get("sources", []) if review else [s.model_dump() for s in request.sources]
        basis_id = row.outcome_id if review else uuid4().hex
        question = "Review the AI employee's reported outcome." if review else request.statement
        if not review:
            row.blocker = {"id": basis_id, "revision": 1, "assignment_revision": row.assignment_revision, "kind": purpose, "question": question, "sources": sources, "resolution": None}
            row.status = "blocked"
            row.next_action = "Await an authorized human response, then explicitly resume."
        recipient = parent.supervisor_id
        human_input = HumanInput(self.work)
        from deerflow.agent_instances.work import MANAGE

        if not await human_input._eligible(session, parent, recipient, DELEGATE if purpose == "information" else MANAGE):
            recipient = None
        req = HumanInputRequestRow(
            id=uuid4().hex,
            source_kind="work",
            instance_id=parent.id,
            work_id=row.id,
            assignment_revision=row.assignment_revision,
            basis_id=basis_id,
            basis_revision=(row.outcome["evidence_revision"] if review else 1),
            creator_id=creator,
            creator_kind="nonhuman",
            recipient_id=recipient,
            revision=1,
            request_revision=1,
            purpose=purpose,
            question=question,
            reason="The adopted policy requires human review." if review else request.reason,
            expected_response="Review the exact outcome statement." if review else request.expected_response,
            choices=[] if review else request.choices,
            sources=sources,
            state="pending",
        )
        session.add(req)
        await session.flush()
        session.add(
            HumanInputEventRow(
                id=uuid4().hex,
                instance_id=parent.id,
                request_id=req.id,
                actor_id=creator,
                actor_kind="nonhuman",
                operation_id=uuid4().hex,
                action="create",
                revision=1,
                request={"question": question},
                receipt={"request_id": req.id, "actor_kind": "nonhuman", "actor_id": creator},
            )
        )

    async def _assess(self, session, parent, row, request, *, revise=False):
        basis = request.request_basis
        req = await session.get(HumanInputRequestRow, basis.id)
        human_input = HumanInput(self.work)
        if (
            not req
            or req.work_id != row.id
            or req.purpose != "information"
            or req.state not in (("pending", "answered") if revise else ("answered",))
            or not human_input._source_current(req, row)
            or (req.revision, req.request_revision) != (basis.revision, basis.request_revision)
        ):
            raise AgentConflict("Only exact answered factual requests may be assessed")
        ids = set((await session.scalars(select(HumanInputResponseRow.id).where(HumanInputResponseRow.request_id == req.id))).all())
        if set(basis.response_ids) != ids or len(basis.response_ids) != len(ids):
            raise AgentConflict("The response set changed; assess all current responses")
        row.status, row.blocker = "open", None
        row.progress = request.statement
        human_input._advance(req)
        req.state, req.closed_reason = "closed", "superseded" if revise else "resolved"
        session.add(
            HumanInputEventRow(
                id=uuid4().hex,
                instance_id=parent.id,
                request_id=req.id,
                actor_id=parent.principal_id,
                actor_kind="nonhuman",
                operation_id=uuid4().hex,
                action="assess",
                revision=req.revision,
                request=request.model_dump(mode="json"),
                receipt={"actor_kind": "nonhuman", "actor_id": parent.principal_id},
            )
        )

    async def _followup(self, session, execution, parent, row, attempt, request):
        # Suggestions are attributed, bounded events; derived Work uses policy defaults.
        if request.action == "suggest":
            row.next_action = "Suggested follow-up: " + request.statement[:4075]
            return
        policy = await self.work._policy(session, parent)
        if not policy.allow_derived:
            raise AgentDenied("The adopted mandate permits suggestions only")
        count = await session.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.work_id == row.id, AgentWorkEventRow.action == "derive", AgentWorkEventRow.request["attempt_id"].as_string() == attempt.id))
        if count >= policy.max_derived_per_activation:
            raise AgentConflict("Derived Work limit reached for this activation")
        self.work._responsibility(policy, request.responsibility)
        child = AgentWorkRow(
            id=uuid4().hex,
            instance_id=parent.id,
            creator_id=parent.principal_id,
            objective=request.statement,
            success_criteria=request.success_criteria,
            responsibility=request.responsibility,
            priority=policy.default_priority,
            definition_revision=parent.definition_revision,
            assignment_revision=1,
            revision=1,
            status="open",
            sources=[],
            review_required=policy.review_required,
            review_state="none",
        )
        session.add(child)
        await self._event(session, parent, child, uuid4().hex, "derived", {"source_work_id": row.id, "attempt_id": attempt.id})
        row.next_action = "Derived Work awaits explicit activation: " + child.id

    async def finish(self, execution, scope, record, *, settled):
        """Only the owning worker calls after host cleanup; crashes remain uncertain."""

        async def perform():
            async with self._sf() as session, session.begin():
                await self.agents._reserve_writer(session)
                from deerflow.persistence.agent_instances.model import AgentInstanceRow
                from deerflow.persistence.spaces.model import SpaceRow

                initial = await self.work._row(session, execution.instance.id, scope.work_id)
                sources = await self._sources(session, initial)
                for space_id in sorted({execution.instance.home_id} | {s["space_id"] for s in sources}):
                    await session.execute(select(SpaceRow).where(SpaceRow.id == space_id).with_for_update())
                thread = await session.scalar(select(ThreadMetaRow).where(ThreadMetaRow.thread_id == execution.thread_id).with_for_update())
                parent = await session.scalar(select(AgentInstanceRow).where(AgentInstanceRow.id == execution.instance.id).with_for_update())
                if parent is None:
                    return
                row = await self.work._row(session, parent.id, scope.work_id, lock=True)
                attempt = await session.get(AgentWorkAttemptRow, scope.attempt_id)
                if not attempt or attempt.work_id != row.id or attempt.run_id != record.run_id or attempt.thread_id != record.thread_id:
                    return
                if attempt.status not in UNRESOLVED:
                    return
                status = getattr(record.status, "value", record.status)
                if not settled or record.ownership_lost or status not in {"success", "error", "timeout", "interrupted"}:
                    attempt.status = "uncertain"
                else:
                    stale = row.assignment_revision != attempt.assignment_revision or parent.generation != attempt.instance_generation or parent.definition_revision != attempt.definition_revision
                    attempt.status = "cancelled" if stale or status == "interrupted" else "succeeded" if status == "success" else "failed"
                    attempt.settled_at = datetime.now(UTC)
                    promote = not stale and status == "success" and attempt.candidate and row.status == "open"
                    if promote:
                        try:
                            # Terminal bookkeeping survives revoked rights; publishing does not.
                            await self.work._parent(session, execution.requester, parent.id, DELEGATE)
                            home = await session.get(SpaceRow, execution.instance.home_id)
                            binding = await session.get(AgentConversationRow, execution.thread_id)
                            if (
                                home is None
                                or home.status != "active"
                                or home.generation != execution.home_generation
                                or thread is None
                                or thread.incarnation != execution.thread_incarnation
                                or binding is None
                                or binding.instance_id != parent.id
                            ):
                                raise AgentDenied("Work destination changed")
                            await execution.authority.destination_audience(session, execution.instance, binding, memory=True)
                            # Include the candidate explicitly: this attempt is now terminal.
                            if sources != list(row.sources) + (row.blocker or {}).get("sources", []) + await HumanInput.decision_sources(session, row) + (attempt.candidate or {}).get("sources", []):
                                raise AgentConflict("Work sources changed")
                            await self._admit(session, execution, parent, row)
                            await self.work._admit_sources(session, execution.requester, parent, [WorkSource.model_validate(s) for s in attempt.candidate.get("sources", [])])
                        except (AgentDenied, AgentConflict):
                            promote = False
                    if promote:
                        row.outcome = attempt.candidate
                        row.outcome_id = attempt.candidate["id"]
                        row.outcome_assignment_revision = row.assignment_revision
                        row.status = "submitted" if row.review_required else "completed"
                        row.review_state = "pending" if row.review_required else "reported"
                        row.next_action = "Human review required." if row.review_required else "AI employee reported complete."
                        if row.review_required:
                            await self._request(session, parent, row, None, creator=parent.principal_id, review=True)
                self._advance(row)
                await self._event(session, parent, row, uuid4().hex, "attempt_settled" if attempt.settled_at else "attempt_uncertain", {"attempt_id": attempt.id, "run_id": record.run_id, "status": attempt.status})

        return await await_drained(perform())
