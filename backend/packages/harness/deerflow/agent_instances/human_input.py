"""Durable human requests. The only qualified source is a locked Work record.

No run admission or filesystem effects belong here. A reference never mounts data.
"""

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select

from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.human_input_contract import CreateRequest, RequestCommand, Respond
from deerflow.agent_instances.work import DELEGATE, INSPECT, MANAGE, UNRESOLVED, _date
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputReadRow, HumanInputRequestRow, HumanInputResponseRow
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow, AgentLifecycleRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained


class HumanInput:
    def __init__(self, work):
        self.work = work
        self.agents = work.agents
        self._sf = work._sf

    async def _eligible(self, session, parent, user_id, permission):
        if not user_id or parent.status in {"provisioning", "deleted", "archived"}:
            return False
        try:
            await self.agents._human(PrincipalRef("human", user_id))
        except AgentDenied:
            return False
        grant = await session.get(AgentInstanceGrantRow, (parent.id, user_id))
        return bool(grant and 1 <= grant.permissions <= 7 and grant.permissions & permission == permission)

    @staticmethod
    def _source_current(row, work):
        if row.assignment_revision != work.assignment_revision:
            return False
        if row.purpose == "review":
            return work.status == "submitted" and work.outcome_id == row.basis_id and (work.outcome or {}).get("evidence_revision") == row.basis_revision
        return work.status == "blocked" and (work.blocker or {}).get("id") == row.basis_id and (work.blocker or {}).get("revision") == row.basis_revision

    async def _project(self, session, actor, parent, work, row, *, can_write=True):
        eligible = await self._eligible(session, parent, row.recipient_id, DELEGATE if row.purpose == "information" else MANAGE)
        read = await session.get(HumanInputReadRow, (row.id, actor.subject_id))
        grant = await session.get(AgentInstanceGrantRow, (parent.id, actor.subject_id))
        current = row.state != "closed" and self._source_current(row, work)
        response_eligible = await self._eligible(session, parent, actor.subject_id, DELEGATE)
        values = {c.name: _date(v) if isinstance(v, datetime) else v for c in row.__table__.columns if (v := getattr(row, c.name)) is not None}
        return await self.work._redact(
            session,
            actor,
            {
                **values,
                "recipient_id": row.recipient_id,
                "closed_reason": row.closed_reason,
                "instance_name": parent.name,
                "work_objective": work.objective,
                "needs_routing": row.state != "closed" and not eligible,
                "read_revision": read.revision if read else 0,
                "can_recover_response": bool(can_write and response_eligible),
                "can_recover_management": bool(can_write and grant.permissions & MANAGE == MANAGE),
                "can_respond": bool(current and can_write and response_eligible),
                "can_manage": bool(current and can_write and grant.permissions & MANAGE == MANAGE),
                "execution_available": False,
                "assessment_available": False,
            },
        )

    async def _lookup(self, session, actor, request_id, permission=INSPECT, *, lock=False):
        candidate = await session.get(HumanInputRequestRow, request_id)
        if candidate is None:
            raise AgentDenied("Request is unavailable")
        parent, _ = await self.work._parent(session, actor, candidate.instance_id, permission, lock=lock)
        work = await self.work._row(session, parent.id, candidate.work_id, lock=lock)
        # The parent lock serializes all writes, including legacy Work commands.
        row = (await session.execute(select(HumanInputRequestRow).where(HumanInputRequestRow.id == request_id).execution_options(populate_existing=True))).scalar_one()
        return parent, work, row

    async def get(self, *, actor, request_id, can_write=True):
        async with self._sf() as session:
            parent, work, row = await self._lookup(session, actor, request_id)
            return await self._project(session, actor, parent, work, row, can_write=can_write)

    async def list(self, *, actor, view="pending", instance_id=None, work_id=None, limit=50, offset=0, can_write=True):
        self.work._pagination(limit, offset)
        if view not in {"pending", "answered", "routing", "all"}:
            raise ValueError("Unknown Attention view")
        await self.agents._human(actor)
        async with self._sf() as session:
            query = (
                select(HumanInputRequestRow, AgentInstanceRow, AgentWorkRow)
                .join(AgentInstanceRow, AgentInstanceRow.id == HumanInputRequestRow.instance_id)
                .join(AgentWorkRow, AgentWorkRow.id == HumanInputRequestRow.work_id)
                .join(AgentInstanceGrantRow, AgentInstanceGrantRow.instance_id == AgentInstanceRow.id)
                .where(AgentInstanceGrantRow.user_id == actor.subject_id, AgentInstanceGrantRow.permissions.between(1, 7), AgentInstanceGrantRow.permissions.op("&")(int(INSPECT)) != 0, AgentInstanceRow.status != "provisioning")
                .order_by(HumanInputRequestRow.updated_at.desc(), HumanInputRequestRow.id.desc())
            )
            if instance_id is not None:
                query = query.where(HumanInputRequestRow.instance_id == instance_id)
            if work_id is not None:
                query = query.where(HumanInputRequestRow.work_id == work_id)
            if view != "all":
                query = query.where(HumanInputRequestRow.state != "closed")
            counts = {"pending": 0, "routing": 0, "answered": 0}
            page, matched = [], 0
            # Identity directories need async checks. Stream SQL-visible candidates;
            # apply current response/recipient eligibility BEFORE counting/paging.
            stream = await session.stream(query.execution_options(yield_per=100))
            async for row, parent, work in stream:
                value = await self._project(session, actor, parent, work, row, can_write=can_write)
                addressed = row.recipient_id == actor.subject_id and not value["needs_routing"] and can_write
                personal = addressed and (value["can_respond"] if row.purpose == "information" else value["can_manage"])
                bucket = "routing" if value["needs_routing"] and value["can_manage"] else row.state if personal and row.state in {"pending", "answered"} else None
                if bucket:
                    counts[bucket] += 1
                if view == "all" or bucket == view:
                    if offset <= matched < offset + limit:
                        page.append(value)
                    matched += 1
            return {"requests": page, "counts": counts, "has_more": matched > offset + limit, "execution_available": False}

    async def responses(self, *, actor, request_id, limit=50, offset=0):
        self.work._pagination(limit, offset)
        async with self._sf() as session:
            await self._lookup(session, actor, request_id)
            rows = (
                (await session.execute(select(HumanInputResponseRow).where(HumanInputResponseRow.request_id == request_id).order_by(HumanInputResponseRow.created_at, HumanInputResponseRow.id).limit(limit + 1).offset(offset)))
                .scalars()
                .all()
            )
            values = [{c.name: _date(v) if isinstance(v, datetime) else v for c in row.__table__.columns if (v := getattr(row, c.name)) is not None} | {"actor_kind": "human"} for row in rows[:limit]]
            return {"responses": await self.work._redact(session, actor, values), "has_more": len(rows) > limit}

    async def history(self, *, actor, request_id, limit=50, offset=0):
        self.work._pagination(limit, offset)
        async with self._sf() as session:
            await self._lookup(session, actor, request_id)
            rows = (await session.execute(select(HumanInputEventRow).where(HumanInputEventRow.request_id == request_id).order_by(HumanInputEventRow.revision.desc()).limit(limit + 1).offset(offset))).scalars().all()
            return {
                "events": await self.work._redact(
                    session, actor, [{"id": r.id, "actor_id": r.actor_id, "actor_kind": r.actor_kind, "action": r.action, "revision": r.revision, "request": r.request, "created_at": _date(r.created_at)} for r in rows[:limit]]
                ),
                "has_more": len(rows) > limit,
            }

    async def _receipt(self, session, actor, instance_id, request, request_id=None, work_id=None):
        event = (await session.execute(select(HumanInputEventRow).where(HumanInputEventRow.instance_id == instance_id, HumanInputEventRow.operation_id == request.operation_id))).scalar_one_or_none()
        if event is None:
            if await session.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.instance_id == instance_id, AgentWorkEventRow.operation_id == request.operation_id)):
                raise AgentConflict("Operation identity belongs to another Work command")
            return None
        if event.actor_kind != "human" or event.actor_id != actor.subject_id or event.request != request.model_dump(mode="json", exclude_unset=True) or (request_id and event.request_id != request_id):
            raise AgentConflict("Operation identity belongs to another actor or request")
        parent, work, row = await self._lookup(session, actor, event.request_id)
        if work_id is not None and work.id != work_id:
            raise AgentConflict("Operation identity belongs to another Work")
        return {**await self._project(session, actor, parent, work, row), "receipt": event.receipt}

    async def _event(self, session, actor, row, request, action, response_id=None):
        receipt = {"operation_id": request.operation_id, "request_id": row.id, "revision": row.revision, "request_revision": row.request_revision, "actor_id": actor.subject_id, "actor_kind": "human", "response_id": response_id}
        session.add(
            HumanInputEventRow(
                id=uuid4().hex,
                instance_id=row.instance_id,
                request_id=row.id,
                actor_id=actor.subject_id,
                operation_id=request.operation_id,
                action=action,
                revision=row.revision,
                request=request.model_dump(mode="json", exclude_unset=True),
                receipt=receipt,
            )
        )
        await session.flush()
        return receipt

    @staticmethod
    def _advance(row):
        if row.revision >= 2147483647:
            raise AgentConflict("Request revision exhausted")
        row.revision += 1
        row.updated_at = datetime.now(UTC)

    async def _mutable_work(self, session, parent, work, *, responding=False):
        if not responding and await session.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.work_id == work.id, AgentWorkAttemptRow.status.in_(UNRESOLVED))):
            raise AgentConflict("An unresolved attempt requires host reconciliation")
        await self.work._validate_snapshot(session, self.work._snapshot(work))
        if work.revision >= 2147483647:
            raise AgentConflict("Work revision exhausted")

    async def create(self, *, actor, instance_id, work_id, request: CreateRequest):
        request = CreateRequest.model_validate(request.model_dump(exclude_unset=True))

        async def perform():
            async with self._sf() as session, session.begin():
                await self.agents._reserve_writer(session)
                await self.work._lock_sources(session, request.sources)
                parent, _ = await self.work._parent(session, actor, instance_id, MANAGE, lock=True)
                work = await self.work._row(session, instance_id, work_id, lock=True)
                receipt = await self._receipt(session, actor, instance_id, request, work_id=work_id)
                if receipt is not None:
                    return receipt
                await self._mutable_work(session, parent, work)
                policy = await self.work._policy(session, parent)
                if parent.status != "active" or not policy or not policy.enabled or work.definition_revision != parent.definition_revision:
                    raise AgentConflict("New requests require an active reconciled Work policy")
                if await session.scalar(select(AgentLifecycleRow.operation_id).where(AgentLifecycleRow.instance_id == instance_id, AgentLifecycleRow.phase == "pending")):
                    raise AgentConflict("Resolve the pending lifecycle operation first")
                if (work.revision, work.assignment_revision) != (request.expected_work_revision, request.expected_assignment_revision):
                    raise AgentConflict("Work changed; reload before requesting input")
                if await session.scalar(select(HumanInputRequestRow.id).where(HumanInputRequestRow.work_id == work.id, HumanInputRequestRow.state != "closed")):
                    raise AgentConflict("This Work already has a live request; withdraw or change its scope first")
                await self.work._admit_sources(session, actor, parent, request.sources)
                recipient = request.recipient_id if "recipient_id" in request.model_fields_set else parent.supervisor_id
                if not await self._eligible(session, parent, recipient, DELEGATE if request.purpose == "information" else MANAGE):
                    if request.recipient_id:
                        raise AgentDenied("Recipient is not eligible")
                    recipient = None
                if request.purpose == "review":
                    if work.status != "submitted" or work.review_state != "pending" or not work.outcome:
                        raise AgentConflict("Review requires a canonical submitted outcome")
                    basis_id, basis_revision = work.outcome_id, work.outcome["evidence_revision"]
                else:
                    if work.status != "open":
                        raise AgentConflict("A new blocking request requires open Work")
                    basis_id, basis_revision = uuid4().hex, 1
                    work.blocker = {
                        "id": basis_id,
                        "revision": 1,
                        "assignment_revision": work.assignment_revision,
                        "kind": request.purpose,
                        "question": request.question,
                        "sources": [s.model_dump() for s in request.sources],
                        "resolution": None,
                    }
                    work.status = "blocked"
                row = HumanInputRequestRow(
                    id=uuid4().hex,
                    source_kind="work",
                    instance_id=instance_id,
                    work_id=work_id,
                    assignment_revision=work.assignment_revision,
                    basis_id=basis_id,
                    basis_revision=basis_revision,
                    creator_id=actor.subject_id,
                    recipient_id=recipient,
                    revision=1,
                    request_revision=1,
                    purpose=request.purpose,
                    question=request.question,
                    reason=request.reason,
                    expected_response=request.expected_response,
                    choices=request.choices,
                    sources=[s.model_dump() for s in request.sources],
                    state="pending",
                )
                session.add(row)
                work.revision += 1
                work.updated_at = datetime.now(UTC)
                await self.work._event(session, actor, work, request, "request_input")
                receipt = await self._event(session, actor, row, request, "create")
                return {**await self._project(session, actor, parent, work, row), "receipt": receipt}

        return await await_drained(perform())

    async def respond(self, *, actor, request_id, request: Respond):
        request = Respond.model_validate(request.model_dump(exclude_unset=True))

        async def perform():
            async with self._sf() as session, session.begin():
                await self.agents._reserve_writer(session)
                await self.work._lock_sources(session, request.sources)
                parent, work, row = await self._lookup(session, actor, request_id, DELEGATE, lock=True)
                if not await self._eligible(session, parent, actor.subject_id, DELEGATE):
                    raise AgentDenied("This AI employee cannot receive human responses in its current lifecycle")
                receipt = await self._receipt(session, actor, parent.id, request, request_id)
                if receipt is not None:
                    return receipt
                if row.state == "closed" or not self._source_current(row, work) or (row.request_revision, row.assignment_revision) != (request.expected_request_revision, request.expected_assignment_revision):
                    raise AgentConflict("The request changed; reload before replying")
                await self._mutable_work(session, parent, work, responding=True)
                await self.work._admit_sources(session, actor, parent, request.sources)
                if request.choice is not None and request.choice not in row.choices:
                    raise AgentConflict("Choice is not offered by this request")
                ids = await self._response_ids(session, row.id)
                if len(ids) >= 200:
                    raise AgentConflict("Response limit reached; a manager must revise the request")
                response = HumanInputResponseRow(
                    id=uuid4().hex,
                    request_id=row.id,
                    actor_id=actor.subject_id,
                    request_revision=row.request_revision,
                    assignment_revision=row.assignment_revision,
                    disposition=request.disposition,
                    text=request.text,
                    choice=request.choice,
                    sources=[s.model_dump() for s in request.sources],
                )
                session.add(response)
                if row.purpose == "information" and request.disposition == "supplied":
                    row.state = "answered"
                self._advance(row)
                work.revision += 1
                work.updated_at = datetime.now(UTC)
                await self.work._event(session, actor, work, request, "human_response")
                receipt = await self._event(session, actor, row, request, "respond", response.id)
                return {**await self._project(session, actor, parent, work, row), "receipt": receipt}

        return await await_drained(perform())

    @staticmethod
    async def _response_ids(session, request_id):
        return list((await session.scalars(select(HumanInputResponseRow.id).where(HumanInputResponseRow.request_id == request_id).order_by(HumanInputResponseRow.id))).all())

    async def command(self, *, actor, request_id, request: RequestCommand):
        request = RequestCommand.model_validate(request.model_dump(exclude_unset=True))

        async def perform():
            async with self._sf() as session, session.begin():
                await self.agents._reserve_writer(session)
                parent, work, row = await self._lookup(session, actor, request_id, MANAGE, lock=True)
                receipt = await self._receipt(session, actor, parent.id, request, request_id)
                if receipt is not None:
                    return receipt
                if row.state == "closed" or not self._source_current(row, work) or (row.revision, row.request_revision) != (request.expected_revision, request.expected_request_revision):
                    raise AgentConflict("The request changed; reload before acting")
                await self._mutable_work(session, parent, work)
                if request.action == "route":
                    if request.recipient_id and not await self._eligible(session, parent, request.recipient_id, DELEGATE if row.purpose == "information" else MANAGE):
                        raise AgentDenied("Recipient is not eligible")
                    row.recipient_id = request.recipient_id
                    row.request_revision += 1
                else:
                    if sorted(request.response_ids) != await self._response_ids(session, row.id):
                        raise AgentConflict("Responses changed; review them before withdrawal")
                    if row.purpose == "review":
                        raise AgentConflict("Use Changes requested or Cancel on the submitted Work")
                    row.state, row.closed_reason = "closed", "withdrawn"
                    work.blocker, work.status = None, "open"
                self._advance(row)
                work.revision += 1
                work.updated_at = datetime.now(UTC)
                await self.work._event(session, actor, work, request, "request_" + request.action)
                receipt = await self._event(session, actor, row, request, request.action)
                return {**await self._project(session, actor, parent, work, row), "receipt": receipt}

        return await await_drained(perform())

    async def mark_read(self, *, actor, request_id, revision):
        if type(revision) is not int or not 1 <= revision <= 2147483647:
            raise ValueError("Invalid read revision")
        async with self._sf() as session, session.begin():
            await self.agents._reserve_writer(session)
            _, _, row = await self._lookup(session, actor, request_id, lock=True)
            if revision > row.revision:
                raise AgentConflict("Cannot mark unseen future content read")
            read = await session.get(HumanInputReadRow, (row.id, actor.subject_id))
            if read:
                read.revision = max(read.revision, revision)
            else:
                session.add(HumanInputReadRow(request_id=row.id, actor_id=actor.subject_id, revision=revision))
        return {"request_id": request_id, "read_revision": max(read.revision if read else 0, revision)}

    @staticmethod
    async def decision_sources(session, work):
        request_id = await session.scalar(select(HumanInputRequestRow.id).where(HumanInputRequestRow.work_id == work.id, HumanInputRequestRow.state != "closed"))
        if not request_id:
            return []
        rows = (await session.scalars(select(HumanInputResponseRow).where(HumanInputResponseRow.request_id == request_id).order_by(HumanInputResponseRow.id))).all()
        return [source for row in rows for source in row.sources]

    async def work_transition(self, session, actor, work, command):
        """Called inside every canonical Work command before applying its transition."""
        row = await session.scalar(select(HumanInputRequestRow).where(HumanInputRequestRow.work_id == work.id, HumanInputRequestRow.state != "closed"))
        if row is None:
            if command.request_basis is not None:
                raise AgentConflict("Linked request is no longer current")
            return
        if command.action == "input":
            raise AgentConflict("Reply through the linked human-input request")
        if command.action in {"decide", "accept"}:
            basis = command.request_basis
            if (
                not basis
                or (basis.id, basis.revision, basis.request_revision) != (row.id, row.revision, row.request_revision)
                or sorted(basis.response_ids) != await self._response_ids(session, row.id)
                or not self._source_current(row, work)
            ):
                raise AgentConflict("Review the current request and all responses before deciding")
            if (command.action, row.purpose) not in {("decide", "decision"), ("accept", "review")}:
                raise AgentConflict("This request requires a different canonical action")
            reason = "resolved"
        elif command.action in {"edit", "reconcile_mandate", "reopen", "changes_requested"}:
            reason = "superseded"
        elif command.action == "cancel":
            reason = "withdrawn"
        else:
            return
        row.state, row.closed_reason = "closed", reason
        self._advance(row)
        await self._event(session, actor, row, command, command.action)
