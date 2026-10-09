"""Human-managed Work records, with no launch, worker or implicit completion path."""

from copy import deepcopy
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select

from deerflow.agent_instances.contract import AgentConflict, AgentDenied, AgentPermission
from deerflow.agent_instances.work_contract import AssignmentFields, DelegateWork, WorkBlocker, WorkCommand, WorkOutcome, WorkReview, WorkSource
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow, AgentLifecycleRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.spaces.model import SpaceGrantRow, SpaceRow
from deerflow.spaces.contract import Permission, PrincipalRef
from deerflow.utils.file_io import await_drained

INSPECT = AgentPermission.INSPECT
DELEGATE = AgentPermission.USE | INSPECT
MANAGE = AgentPermission.MANAGE | INSPECT
UNRESOLVED = ("starting", "running", "stopping", "uncertain")


def _date(value):
    return value.replace(tzinfo=UTC).isoformat() if value.tzinfo is None else value.astimezone(UTC).isoformat()


class AgentWork:
    def __init__(self, agents):
        self.agents = agents
        self._sf = agents._sf

    async def _parent(self, session, actor, instance_id, permission, *, lock=False):
        query = select(AgentInstanceRow).where(AgentInstanceRow.id == instance_id)
        row = (await session.execute(query.with_for_update() if lock else query)).scalar_one_or_none()
        await self.agents._human(actor)
        grant = await session.get(AgentInstanceGrantRow, (instance_id, actor.subject_id))
        if row is None or grant is None or row.status == "provisioning":
            raise AgentDenied("Work requires current instance visibility")
        view = self.agents._view(row, grant.permissions)
        if view.permissions & permission != permission:
            raise AgentDenied("Work requires Inspect and the relevant Use or Manage grant")
        return row, view

    async def _policy(self, session, parent):
        snapshot = await self.agents._stored_definition(session, parent.definition_revision)
        from deerflow.config.agents_config import AgentConfig

        return AgentConfig.model_validate(snapshot.config).work_policy

    @staticmethod
    def _snapshot(row):
        return {column.name: _date(value) if isinstance(value, datetime) else deepcopy(value) for column in row.__table__.columns if (value := getattr(row, column.name)) is not None} | {
            "due_at": _date(row.due_at) if row.due_at else None,
            "responsibility": row.responsibility,
            "blocker": deepcopy(row.blocker),
            "outcome": deepcopy(row.outcome),
            "review": deepcopy(row.review),
        }

    async def _can_read_source(self, session, actor, source):
        try:
            ref = WorkSource.model_validate(source)
            row = await session.get(SpaceRow, ref.space_id)
            grant = await session.get(SpaceGrantRow, (ref.space_id, "human", actor.subject_id))
            if row is None or row.status not in {"active", "archived"} or grant is None:
                return False
            return bool(self.agents.files.registry._view(row, grant.permissions).permissions & Permission.READ)
        except ValueError:
            return False

    async def _redact(self, session, actor, value):
        if isinstance(value, list):
            return [await self._redact(session, actor, item) for item in value]
        if isinstance(value, dict):
            if value.get("kind") == "space_file":
                if not await self._can_read_source(session, actor, value):
                    return {"kind": "unavailable", "current_contents": "not_checked"}
                return {**value, "current_contents": "not_checked"}
            return {key: await self._redact(session, actor, item) for key, item in value.items()}
        return value

    async def _validate_snapshot(self, session, snapshot):
        try:
            for name in ("objective", "success_criteria", "progress", "next_action"):
                if not isinstance(snapshot[name], str) or len(snapshot[name]) > 4096:
                    raise ValueError("Invalid text bound")
            if len(snapshot["sources"]) > 16:
                raise ValueError("Invalid sources bound")
            for source in snapshot["sources"]:
                WorkSource.model_validate(source)
            blocker = WorkBlocker.model_validate(snapshot["blocker"]) if snapshot.get("blocker") is not None else None
            if blocker and blocker.assignment_revision != snapshot["assignment_revision"]:
                raise ValueError("Blocker assignment changed")
            outcome = WorkOutcome.model_validate(snapshot["outcome"]) if snapshot.get("outcome") is not None else None
            review = WorkReview.model_validate(snapshot["review"]) if snapshot.get("review") is not None else None
            if outcome:
                if outcome.id != snapshot.get("outcome_id") or outcome.assignment_revision != snapshot["assignment_revision"] or outcome.assignment_revision != snapshot.get("outcome_assignment_revision"):
                    raise ValueError("Outcome identity changed")
                attempt = await session.get(AgentWorkAttemptRow, outcome.attempt_id)
                if attempt is None or attempt.work_id != snapshot["id"] or attempt.assignment_revision != outcome.assignment_revision or attempt.definition_revision != snapshot["definition_revision"] or attempt.status != "succeeded":
                    raise ValueError("Outcome lacks reconciled attempt")
            if review and (outcome is None or review.outcome_id != outcome.id or review.assignment_revision != outcome.assignment_revision or review.evidence_revision != outcome.evidence_revision):
                raise ValueError("Review identity changed")
        except (ValueError, TypeError, KeyError):
            raise AgentConflict("Stored Work bindings are invalid; preserve the record for host reconciliation") from None

    async def _project(self, session, actor, parent, snapshot, *, current_attempt=True):
        await self._validate_snapshot(session, snapshot)
        value = await self._redact(session, actor, snapshot)
        policy = await self._policy(session, parent)
        attempt = None
        if current_attempt:
            attempt = (await session.execute(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == snapshot["id"]).order_by(AgentWorkAttemptRow.created_at.desc(), AgentWorkAttemptRow.id.desc()).limit(1))).scalar_one_or_none()
        from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow

        live_request = await session.scalar(select(HumanInputRequestRow.id).where(HumanInputRequestRow.work_id == snapshot["id"], HumanInputRequestRow.state != "closed")) if current_attempt else None
        value.update(
            human_input_request_id=live_request,
            execution_available=False,
            availability="records_only",
            work_enabled=bool(policy and policy.enabled),
            needs_mandate_reconciliation=parent.definition_revision != snapshot["definition_revision"],
            current_contents="not_checked",
            attempt={"id": attempt.id, "status": attempt.status, "assignment_revision": attempt.assignment_revision} if attempt else None,
        )
        return value

    @staticmethod
    def _pagination(limit, offset):
        if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or not 0 <= offset <= 100000:
            raise ValueError("Invalid Work pagination")

    async def list(self, *, actor, instance_id=None, limit=50, offset=0):
        self._pagination(limit, offset)
        await self.agents._human(actor)
        async with self._sf() as session:
            if instance_id is not None:
                await self._parent(session, actor, instance_id, INSPECT)
            query = (
                select(AgentWorkRow, AgentInstanceRow)
                .join(AgentInstanceRow, AgentInstanceRow.id == AgentWorkRow.instance_id)
                .join(AgentInstanceGrantRow, AgentInstanceGrantRow.instance_id == AgentWorkRow.instance_id)
                .where(AgentInstanceGrantRow.user_id == actor.subject_id, AgentInstanceGrantRow.permissions.between(1, 7), AgentInstanceGrantRow.permissions.op("&")(int(INSPECT)) != 0)
                .order_by(AgentWorkRow.created_at.desc(), AgentWorkRow.id.desc())
                .limit(limit)
                .offset(offset)
            )
            if instance_id is not None:
                query = query.where(AgentWorkRow.instance_id == instance_id)
            results = (await session.execute(query)).all()
            return [await self._project(session, actor, parent, self._snapshot(row)) for row, parent in results]

    async def get(self, *, actor, instance_id, work_id):
        async with self._sf() as session:
            parent, _ = await self._parent(session, actor, instance_id, INSPECT)
            row = await self._row(session, instance_id, work_id)
            return await self._project(session, actor, parent, self._snapshot(row))

    async def history(self, *, actor, instance_id, work_id, limit=50, offset=0):
        self._pagination(limit, offset)
        async with self._sf() as session:
            parent, _ = await self._parent(session, actor, instance_id, INSPECT)
            await self._row(session, instance_id, work_id)
            events = (await session.execute(select(AgentWorkEventRow).where(AgentWorkEventRow.work_id == work_id).order_by(AgentWorkEventRow.revision.desc()).limit(limit).offset(offset))).scalars().all()
            return [
                {
                    "id": event.id,
                    "actor_id": event.actor_id,
                    "actor_kind": event.actor_kind,
                    "action": event.action,
                    "revision": event.revision,
                    "assignment_revision": event.assignment_revision,
                    "created_at": _date(event.created_at),
                    "note": event.request.get("note"),
                    "record": await self._project(session, actor, parent, event.result, current_attempt=False),
                }
                for event in events
            ]

    @staticmethod
    async def _row(session, instance_id, work_id, *, lock=False):
        query = select(AgentWorkRow).where(AgentWorkRow.id == work_id, AgentWorkRow.instance_id == instance_id)
        row = (await session.execute(query.with_for_update() if lock else query)).scalar_one_or_none()
        if row is None:
            raise AgentDenied("Work is unavailable")
        return row

    async def _receipt(self, session, actor, parent, request, work_id):
        event = (await session.execute(select(AgentWorkEventRow).where(AgentWorkEventRow.instance_id == parent.id, AgentWorkEventRow.operation_id == request.operation_id))).scalar_one_or_none()
        if event is None:
            return None
        if event.actor_kind != "human" or event.actor_id != actor.subject_id or event.request != request.model_dump(mode="json", exclude_unset=True) or (work_id is not None and event.work_id != work_id):
            raise AgentConflict("Operation identity belongs to another actor, Work or request")
        return await self._project(session, actor, parent, event.result)

    async def _lock_sources(self, session, sources):
        # Lock resources before the instance. Retry receipts are checked before
        # source admission, so revoked locators can be redacted without replay.
        for space_id in sorted({source.space_id for source in sources or []}):
            await session.execute(select(SpaceRow).where(SpaceRow.id == space_id).with_for_update())

    async def _admit_sources(self, session, actor, parent, sources):
        audience = (await session.execute(select(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == parent.id, AgentInstanceGrantRow.permissions.op("&")(int(INSPECT)) != 0))).scalars().all()
        for source in sources or []:
            payload = source.model_dump()
            if not await self._can_read_source(session, actor, payload):
                raise AgentDenied("Source is unavailable")
            for member in audience:
                self.agents._view(parent, member.permissions)
                principal = PrincipalRef("human", member.user_id)
                try:
                    await self.agents._human(principal)
                except AgentDenied:
                    continue
                if not await self._can_read_source(session, principal, payload):
                    raise AgentDenied("The Work audience cannot inspect this source; no content or locator was copied")

    @staticmethod
    def _responsibility(policy, key):
        if key is not None and (policy is None or key not in {item.key for item in policy.responsibilities}):
            raise AgentConflict("Responsibility is absent from the adopted mandate")

    async def _event(self, session, actor, row, request, action):
        await session.flush()
        snapshot = self._snapshot(row)
        session.add(
            AgentWorkEventRow(
                id=uuid4().hex,
                instance_id=row.instance_id,
                work_id=row.id,
                actor_id=actor.subject_id,
                actor_kind="human",
                operation_id=request.operation_id,
                action=action,
                revision=row.revision,
                assignment_revision=row.assignment_revision,
                request=request.model_dump(mode="json", exclude_unset=True),
                result=snapshot,
            )
        )
        await session.flush()
        return snapshot

    async def delegate(self, *, actor, instance_id, request: DelegateWork):
        request = DelegateWork.model_validate(request.model_dump(exclude_unset=True))
        await self.agents._human(actor)

        async def perform():
            async with self._sf() as session, session.begin():
                await self.agents._reserve_writer(session)
                await self._lock_sources(session, request.sources)
                parent, view = await self._parent(session, actor, instance_id, DELEGATE, lock=True)
                receipt = await self._receipt(session, actor, parent, request, None)
                if receipt is not None:
                    return receipt
                policy = await self._policy(session, parent)
                if parent.status != "active" or policy is None or not policy.enabled:
                    raise AgentConflict("New Work requires an active AI employee with an enabled adopted Work policy")
                if await session.scalar(select(AgentLifecycleRow.operation_id).where(AgentLifecycleRow.instance_id == instance_id, AgentLifecycleRow.phase == "pending")):
                    raise AgentConflict("Resolve the pending lifecycle operation before delegating")
                if request.model_fields_set & {"priority", "due_at", "review_required"} and not view.permissions & AgentPermission.MANAGE:
                    raise AgentDenied("Assignment overrides require Manage plus Inspect")
                if policy.review_required and request.review_required is False:
                    raise AgentConflict("The adopted policy requires review")
                self._responsibility(policy, request.responsibility)
                await self._admit_sources(session, actor, parent, request.sources)
                row = AgentWorkRow(
                    id=uuid4().hex,
                    instance_id=instance_id,
                    creator_id=actor.subject_id,
                    objective=request.objective,
                    success_criteria=request.success_criteria,
                    responsibility=request.responsibility,
                    priority=request.priority or policy.default_priority,
                    due_at=request.due_at,
                    definition_revision=parent.definition_revision,
                    assignment_revision=1,
                    revision=1,
                    status="open",
                    sources=[source.model_dump() for source in request.sources or []],
                    review_required=policy.review_required if request.review_required is None else request.review_required,
                    review_state="none",
                )
                session.add(row)
                snapshot = await self._event(session, actor, row, request, "delegate")
                return await self._project(session, actor, parent, snapshot)

        return await await_drained(perform())

    async def command(self, *, actor, instance_id, work_id, request: WorkCommand):
        request = WorkCommand.model_validate(request.model_dump(exclude_unset=True))
        await self.agents._human(actor)
        decision_sources = []
        if request.action == "decide":
            async with self._sf() as session:
                await self._parent(session, actor, instance_id, MANAGE)
                current = await self._row(session, instance_id, work_id)
                from deerflow.agent_instances.human_input import HumanInput

                decision_sources = [WorkSource.model_validate(source) for source in (current.blocker or {}).get("sources", []) + await HumanInput.decision_sources(session, current)]

        async def perform():
            async with self._sf() as session, session.begin():
                await self.agents._reserve_writer(session)
                await self._lock_sources(session, decision_sources if request.action == "decide" else request.sources)
                parent, _ = await self._parent(session, actor, instance_id, DELEGATE if request.action == "input" else MANAGE, lock=True)
                row = await self._row(session, instance_id, work_id, lock=True)
                receipt = await self._receipt(session, actor, parent, request, work_id)
                if receipt is not None:
                    return receipt
                if row.revision != request.expected_revision or row.assignment_revision != request.expected_assignment_revision or row.revision >= 2147483647:
                    raise AgentConflict("Work changed; reload before acting")
                if await session.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.work_id == row.id, AgentWorkAttemptRow.status.in_(UNRESOLVED))):
                    raise AgentConflict("An unresolved attempt requires host reconciliation")
                if request.action == "decide":
                    from deerflow.agent_instances.human_input import HumanInput

                    current_sources = (row.blocker or {}).get("sources", []) + await HumanInput.decision_sources(session, row)
                    if current_sources != [source.model_dump() for source in decision_sources]:
                        raise AgentConflict("Decision basis changed; reload before acting")
                    for source in current_sources:
                        if not await self._can_read_source(session, actor, source):
                            raise AgentDenied("Decision basis is unavailable")
                await self._validate_snapshot(session, self._snapshot(row))
                policy = await self._policy(session, parent)
                from deerflow.agent_instances.human_input import HumanInput

                await HumanInput(self).work_transition(session, actor, row, request)
                await self._apply(session, actor, parent, row, policy, request)
                row.revision += 1
                row.updated_at = datetime.now(UTC)
                snapshot = await self._event(session, actor, row, request, request.action)
                return await self._project(session, actor, parent, snapshot)

        return await await_drained(perform())

    async def _apply(self, session, actor, parent, row, policy, request):
        action = request.action
        if action in {"edit", "reconcile_mandate"}:
            if row.status not in {"open", "blocked"}:
                raise AgentConflict("Changes requested or Reopen is required before assignment edits")
            if action == "edit":
                if policy and policy.review_required and request.review_required is False:
                    raise AgentConflict("The adopted policy requires review")
                if "responsibility" in request.model_fields_set:
                    self._responsibility(policy, request.responsibility)
                await self._admit_sources(session, actor, parent, request.sources)
                for field in set(AssignmentFields.model_fields) & request.model_fields_set:
                    value = getattr(request, field)
                    setattr(row, field, [source.model_dump() for source in value] if field == "sources" else value)
            else:
                self._responsibility(policy, row.responsibility)
                row.definition_revision = parent.definition_revision
                row.review_required = row.review_required or bool(policy and policy.review_required)
            row.assignment_revision += 1
            row.blocker = None
            if row.status == "blocked":
                row.status = "open"
        elif action in {"cancel", "reopen", "changes_requested"}:
            valid = {"cancel": {"open", "blocked", "submitted"}, "reopen": {"completed", "cancelled"}, "changes_requested": {"submitted"}}
            if row.status not in valid[action]:
                raise AgentConflict("This transition is unavailable for the current Work state")
            row.status = "cancelled" if action == "cancel" else "open"
            row.assignment_revision += 1
            row.outcome_id = row.outcome_assignment_revision = row.outcome = row.review = row.blocker = None
            row.review_state = "none"
        elif action == "accept":
            if (
                row.status != "submitted"
                or row.review_state != "pending"
                or not row.outcome
                or row.outcome_id != request.outcome_id
                or row.outcome.get("evidence_revision") != request.evidence_revision
                or row.outcome_assignment_revision != row.assignment_revision
            ):
                raise AgentConflict("The exact submitted outcome is no longer available")
            row.review = {
                "actor_kind": "human",
                "actor_id": actor.subject_id,
                "assignment_revision": row.assignment_revision,
                "outcome_id": row.outcome_id,
                "evidence_revision": request.evidence_revision,
                "basis": request.basis,
                "current_contents": "not_checked",
                "note": request.note,
                "accepted_at": datetime.now(UTC).isoformat(),
            }
            row.status = "completed"
            row.review_state = "accepted"
        elif action in {"input", "decide"}:
            if row.status != "blocked" or not row.blocker or row.blocker.get("id") != request.blocker_id or row.blocker.get("revision") != request.blocker_revision:
                raise AgentConflict("The exact blocker is no longer current")
            if action == "decide":
                if row.blocker.get("kind") != "decision":
                    raise AgentConflict("Factual input is not a manager decision")
                # A decision cannot rely on an inaccessible source. Statement
                # review is deliberately a different, explicitly limited action.
                for source in row.blocker.get("sources", []):
                    if not await self._can_read_source(session, actor, source):
                        raise AgentDenied("Decision basis is unavailable")
                row.blocker = {**row.blocker, "resolution": {"actor_kind": "human", "actor_id": actor.subject_id, "statement": request.note}}
                row.status = "open"
            # Factual input is an attributed event only. It does not clear a
            # blocker, waive review or start a run; later execution assesses it.
