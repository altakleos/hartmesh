"""Orphaned AI employee recovery, bound to one immutable host intent."""

import hashlib
import json
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import func, select

from app.gateway.agent_operator_actions import empty_closure
from app.gateway.agent_operator_contract import CommandRefused
from app.gateway.auth.mode import account_refusal
from deerflow.agent_instances.contract import Refusal
from deerflow.agent_instances.host_operations import human, require_orphaned
from deerflow.agent_instances.lifecycle import InstanceLifecycle
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow, AgentLifecycleRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.attachments import SpaceAttachments
from deerflow.spaces.contract import Permission, PrincipalRef
from deerflow.spaces.recovery import SpaceRecovery
from deerflow.utils.file_io import await_drained

_INSTANCE = ("id", "principal_id", "home_id", "custody", "owner_id", "definition_revision", "supervisor_id", "generation", "status")
_HOME = ("id", "backing_handle", "custody_kind", "custodian_kind", "custodian_id", "mode", "status", "feature_namespace", "feature_controller", "feature_metadata_version")
_BACKING = ("backing_handle", "space_id", "slot_id", "filesystem_uuid", "root_inode", "control_inode", "max_bytes", "max_inodes")
_DETAILS = frozenset(("operator_host_id", "target_id", "instance", "home", "backing", "instance_grants", "space_grants", "lifecycle_operation_id"))
_HANDOFF = frozenset(("intent_generation", "intent_sha256", "blocking_operation_id", "instance"))


def _facts(row, fields):
    return {field: getattr(row, field) for field in fields}


def _intent_digest(event):
    return hashlib.sha256(json.dumps(event.details, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


async def _grants(session, instance_id, home_id):
    instance = [dict(user_id=row.user_id, permissions=row.permissions) for row in (await session.scalars(select(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == instance_id).order_by(AgentInstanceGrantRow.user_id))).all()]
    space = [
        dict(principal_kind=row.principal_kind, subject_id=row.subject_id, permissions=row.permissions)
        for row in (await session.scalars(select(SpaceGrantRow).where(SpaceGrantRow.space_id == home_id).order_by(SpaceGrantRow.principal_kind, SpaceGrantRow.subject_id))).all()
    ]
    return instance, space


def _with_grant(baseline, target, permissions, *, space=False):
    key = (lambda row: (row["principal_kind"], row["subject_id"])) if space else (lambda row: row["user_id"])
    identity = ("human", target) if space else target
    changed = {key(row): dict(row) for row in baseline}
    changed[identity] = dict(principal_kind="human", subject_id=target, permissions=permissions) if space else dict(user_id=target, permissions=permissions)
    return [changed[identifier] for identifier in sorted(changed)]


class OperatorSupervision:
    @staticmethod
    def _recovery_room(instance, home, target_grant):
        # The newly admitted manager can start another product lifecycle in
        # the grant gap. Reserve a terminal handoff even before one is pending.
        required = 2 + int(target_grant is None or target_grant.permissions != 27)
        if instance.generation >= 2**31 - 1 or home.generation + required > 2**31 - 1:
            raise RuntimeError("The AI employee or its Home lacks generation room for supervision recovery")

    async def _supervisor_target(self, *, email, issuer, subject):
        if email is not None:
            async with self.files.registry._sf() as session:
                identities = list((await session.scalars(select(UserRow.id).where(func.lower(UserRow.email) == email.lower()))).all())
            if len(identities) != 1:
                raise CommandRefused(f"no account, or more than one, holds the address {email!r}")
            target = await self.repository.get_user_by_id(identities[0])
        else:
            people = await self.repository.list_users_by_identity(issuer, subject)
            if not people:
                raise CommandRefused(f"no account exists for subject {subject!r} at issuer {issuer!r}")
            if len(people) != 1:
                raise CommandRefused(f"{len(people)} accounts have subject {subject!r} at this issuer; address one of them by --email")
            target = people[0]
        if target is None or account_refusal(target) is not None:
            raise CommandRefused(f"the account of subject {(subject or email)!r} is turned off; a supervisor must be able to sign in")
        return target

    async def _recovery_eligible(self, session, instance, *, intent):
        try:
            if intent is None:
                await require_orphaned(self.agents, session, instance)
                return
            # The caller first proves this exact intent and its permitted grant
            # deltas. Only that intent's target may now be an active manager.
            target = intent.details["target_id"]
            grants = (await session.scalars(select(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == instance.id))).all()
            managers = {instance.supervisor_id} | {row.user_id for row in grants if row.permissions & 4 and row.user_id != target}
            for identifier in sorted(managers):
                if await human(self.agents, PrincipalRef("human", identifier), session) is not None:
                    raise Refusal("A customer manager can sign in", reason="managers-active")
        except Refusal:
            raise CommandRefused(f"{instance.id} is an AI employee with a manager who can sign in: a manager changes its supervisor in the product") from None

    async def _handoff_event(self, session, event):
        for generation in (event.generation + 1, event.generation + 2):
            handoff = await session.get(SpaceEventRow, (event.space_id, generation))
            if handoff is None or handoff.action != "operator-supervise-handoff":
                continue
            details = handoff.details
            if (
                not isinstance(details, dict)
                or set(details) != _HANDOFF
                or details["intent_generation"] != event.generation
                or details["intent_sha256"] != _intent_digest(event)
                or (handoff.actor_kind, handoff.actor_id) != (event.actor_kind, event.actor_id)
                or not isinstance(details["instance"], dict)
                or set(details["instance"]) != set(_INSTANCE)
            ):
                raise RuntimeError("The terminal supervision handoff changed; preserve its evidence")
            self.files._operation_id(details["blocking_operation_id"])
            return handoff
        return None

    async def _recovery_intent(self, session, instance, target, *, replay=False):
        event = await session.scalar(select(SpaceEventRow).where(SpaceEventRow.space_id == instance.home_id, SpaceEventRow.action == "operator-supervise-intent").order_by(SpaceEventRow.generation.desc()).limit(1))
        if event is None:
            return None
        details = event.details
        if not isinstance(details, dict) or set(details) != _DETAILS:
            raise RuntimeError("The operator supervision intent is invalid; preserve its evidence")
        if (event.actor_kind, event.actor_id) != ("human", details["target_id"]) or details["instance"]["id"] != instance.id or details["instance"]["home_id"] != event.space_id:
            raise RuntimeError("The operator supervision intent has inconsistent ownership")
        handoff = await self._handoff_event(session, event)
        if handoff is not None:
            if replay and (details["target_id"], details["operator_host_id"]) == (target, self.adapter.host_id) and _facts(instance, _INSTANCE) == handoff.details["instance"]:
                pending = await session.get(AgentLifecycleRow, (instance.id, handoff.details["blocking_operation_id"]))
                instance_grants, space_grants = await _grants(session, instance.id, instance.home_id)
                home = await session.get(SpaceRow, instance.home_id)
                binding = await session.get(SpaceBackingRow, home.backing_handle) if home else None
                if (
                    pending is not None
                    and pending.phase == "pending"
                    and home is not None
                    and home.generation == handoff.generation
                    and _facts(home, _HOME) == details["home"]
                    and binding is not None
                    and _facts(binding, _BACKING) == details["backing"]
                    and instance_grants == _with_grant(details["instance_grants"], target, 7)
                    and space_grants == _with_grant(details["space_grants"], target, 27, space=True)
                ):
                    return event
            return None  # A fresh recovery must prove every manager orphaned.
        operation = await session.get(AgentLifecycleRow, (instance.id, details["lifecycle_operation_id"]))
        if operation is not None and operation.phase in ("complete", "abandoned"):
            if (operation.actor_id, operation.home_id, operation.generation, operation.request, operation.prior_status) != (
                details["target_id"],
                event.space_id,
                details["instance"]["generation"],
                {"action": "supervise", "supervisor_id": details["target_id"]},
                details["instance"]["status"],
            ):
                raise RuntimeError("The terminal recovery lifecycle receipt changed")
            return None  # A later orphan recovery must independently qualify.
        baseline_home_grant = any(row == {"principal_kind": "human", "subject_id": details["target_id"], "permissions": 27} for row in details["space_grants"])
        granted = await session.get(SpaceEventRow, (event.space_id, event.generation + 1))
        expected_grant = {"subject_kind": "human", "subject_id": details["target_id"], "permissions": 27, "by": "host", "operator_host_id": details["operator_host_id"]}
        authority_handed_off = baseline_home_grant or (granted is not None and (granted.action, granted.actor_kind, granted.actor_id, granted.details) == ("grant", "human", details["target_id"], expected_grant))
        if authority_handed_off:
            try:
                await require_orphaned(self.agents, session, instance)
            except Refusal:
                pass
            else:
                # The authority pair completed before a later product change or
                # lost handoff reply. This is NEW recovery, with no old-target
                # exemption. Every mutating caller repeats this under Home→Agent.
                # Preserve any old pending lifecycle for the new human manager.
                return None
        if details["target_id"] != target or details["operator_host_id"] != self.adapter.host_id or (event.actor_kind, event.actor_id) != ("human", target):
            raise RuntimeError("Another operator supervision intent is unfinished; repeat its exact host and target")
        return event

    async def _handoff_recovery(self, instance, event, target):
        async with self.files.registry.host_locked([instance.home_id]) as (session, homes):
            current = await session.scalar(select(AgentInstanceRow).where(AgentInstanceRow.id == instance.id).with_for_update())
            home = homes.get(instance.home_id)
            actual = await session.get(SpaceEventRow, (instance.home_id, event.generation))
            if current is None or home is None or actual is None or actual.details != event.details:
                raise RuntimeError("The supervision intent changed before handoff")
            pending = await session.scalar(
                select(AgentLifecycleRow)
                .where(AgentLifecycleRow.instance_id == instance.id, AgentLifecycleRow.phase == "pending", AgentLifecycleRow.operation_id != event.details["lifecycle_operation_id"])
                .order_by(AgentLifecycleRow.operation_id)
                .limit(1)
            )
            if pending is None:
                return False
            await self._check_recovery_intent(session, current, home, actual, target, pending_handoff=pending)
            await SpaceRecovery._require_fenced(session, home.id)
            instance_grants, space_grants = await _grants(session, instance.id, home.id)
            if instance_grants != _with_grant(event.details["instance_grants"], target, 7) or space_grants != _with_grant(event.details["space_grants"], target, 27, space=True):
                raise RuntimeError("The exact recovery grants changed before handoff")
            if home.generation >= 2**31 - 1:
                raise RuntimeError("The Home lacks generation room for supervision handoff")
            home.generation += 1
            home.updated_at = datetime.now(UTC)
            session.add(
                SpaceEventRow(
                    space_id=home.id,
                    generation=home.generation,
                    action="operator-supervise-handoff",
                    actor_kind="human",
                    actor_id=target,
                    details=dict(intent_generation=event.generation, intent_sha256=_intent_digest(event), blocking_operation_id=pending.operation_id, instance=_facts(current, _INSTANCE)),
                )
            )
            return True

    async def _check_recovery_intent(self, session, instance, home, event, target, *, pending_handoff=None):
        details = event.details
        if set(details) != _DETAILS or details["operator_host_id"] != self.adapter.host_id or details["target_id"] != target or event.space_id != home.id or (event.actor_kind, event.actor_id) != ("human", target):
            raise RuntimeError("The exact operator supervision intent changed")
        self.files._operation_id(details["lifecycle_operation_id"])
        binding = await session.get(SpaceBackingRow, home.backing_handle)
        if _facts(home, _HOME) != details["home"] or binding is None or _facts(binding, _BACKING) != details["backing"]:
            raise RuntimeError("The recovery Home or backing changed")
        expected = dict(details["instance"])
        operation = await session.get(AgentLifecycleRow, (instance.id, details["lifecycle_operation_id"]))
        if operation is not None:
            if (
                operation.actor_id != target
                or operation.home_id != home.id
                or operation.generation != expected["generation"]
                or operation.request != {"action": "supervise", "supervisor_id": target}
                or operation.phase not in ("pending", "complete")
            ):
                raise RuntimeError("The recovery lifecycle receipt changed")
            expected["generation"] += 1
            expected["status"] = "suspended" if operation.phase == "pending" else details["instance"]["status"]
            if operation.phase == "complete":
                expected["supervisor_id"] = target
        elif pending_handoff is not None and _facts(instance, _INSTANCE) != expected:
            if (pending_handoff.actor_id, pending_handoff.home_id, pending_handoff.generation, pending_handoff.prior_status, pending_handoff.phase) != (target, home.id, expected["generation"], expected["status"], "pending"):
                raise RuntimeError("The exact product lifecycle withdrawal changed before handoff")
            expected["generation"] += 1
            expected["status"] = "suspended"
        if _facts(instance, _INSTANCE) != expected:
            raise RuntimeError("The recovery AI employee changed")
        current_instance, current_space = await _grants(session, instance.id, home.id)
        granted_instance = _with_grant(details["instance_grants"], target, 7)
        if current_instance not in (details["instance_grants"], granted_instance):
            raise RuntimeError("The recovery AI employee's grants changed")
        granted_space = _with_grant(details["space_grants"], target, 27, space=True)
        if home.generation == event.generation:
            if current_space != details["space_grants"]:
                raise RuntimeError("The recovery Home grants changed")
        elif home.generation == event.generation + 1:
            grant = await session.get(SpaceEventRow, (home.id, home.generation))
            expected_grant = {"subject_kind": "human", "subject_id": target, "permissions": 27, "by": "host", "operator_host_id": self.adapter.host_id}
            if grant is None or (grant.action, grant.actor_kind, grant.actor_id, grant.details) != ("grant", "human", target, expected_grant) or current_space != granted_space:
                raise RuntimeError("The exact operator Home grant changed")
        else:
            raise RuntimeError("The recovery Home generation changed")
        await self.agents._human(PrincipalRef("human", target), session=session)
        await self._recovery_eligible(session, instance, intent=event)

    async def _supervise(self, *, instance_id, email=None, issuer=None, subject=None, end_running_work=False, wait_seconds=120):
        instance = await self._agent(instance_id)
        if instance.custody != "company":
            raise CommandRefused(
                f"{instance_id} is an AI assistant: it stops with its owner's account and cannot be handed to another person; "
                "its owner can make an AI employee with the same brief, and release-seat or delete removes it once the owner's account is turned off"
            )
        if instance.status == "deleted":
            raise CommandRefused(f"{instance_id} is deleted; supervise acts on an AI employee that is active, paused or retired")
        if instance.status == "provisioning":
            raise CommandRefused(f"{instance_id} is not set up yet (status provisioning)")
        target = await self._supervisor_target(email=email, issuer=issuer, subject=subject)
        identifier = str(target.id)
        await self._require_daemon()
        resume_handoff = False
        async with self.files.registry._sf() as session:
            target_grant = await session.get(AgentInstanceGrantRow, (instance_id, identifier))
            if instance.supervisor_id == identifier and target_grant is not None and target_grant.permissions & 4:
                home_grant = await session.get(SpaceGrantRow, (instance.home_id, "human", identifier))
                return self._supervision_answer(instance, target, target, verdict="unchanged", closure=empty_closure(), permissions={"instance": target_grant.permissions, "space": home_grant.permissions if home_grant else 0})
            event = await self._recovery_intent(session, instance, identifier, replay=True)
            home = await session.get(SpaceRow, instance.home_id)
            if home is None:
                raise RuntimeError("The AI employee's Home is unavailable")
            if event is not None and await self._handoff_event(session, event) is not None:
                previous = await self.repository.get_user_by_id(event.details["instance"]["supervisor_id"], session=session)
                return self._supervision_answer(instance, target, previous, verdict="manager_added", closure=empty_closure(), previous_id=event.details["instance"]["supervisor_id"])
            if event is not None:
                home_grant = await session.get(SpaceGrantRow, (home.id, "human", identifier))
                other = await session.scalar(
                    select(AgentLifecycleRow).where(AgentLifecycleRow.instance_id == instance_id, AgentLifecycleRow.phase == "pending", AgentLifecycleRow.operation_id != event.details["lifecycle_operation_id"]).limit(1)
                )
                resume_handoff = other is not None and target_grant is not None and target_grant.permissions == 7 and home_grant is not None and home_grant.permissions == 27
                await self._check_recovery_intent(session, instance, home, event, identifier, pending_handoff=other if resume_handoff else None)
            else:
                await self._recovery_eligible(session, instance, intent=None)
                self._recovery_room(instance, home, await session.get(SpaceGrantRow, (home.id, "human", identifier)))
        previous = await self.repository.get_user_by_id(instance.supervisor_id)
        if resume_handoff:
            if await self._handoff_recovery(instance, event, identifier):
                return self._supervision_answer(instance, target, previous, verdict="manager_added", closure=empty_closure(), previous_id=event.details["instance"]["supervisor_id"])
            raise RuntimeError("The product lifecycle changed before supervision handoff; run the command again")
        effects = {"closed": False, "granted": False}

        async def closure_admission(session):
            current = await session.scalar(select(AgentInstanceRow).where(AgentInstanceRow.id == instance_id).with_for_update().execution_options(populate_existing=True))
            home = await session.get(SpaceRow, instance.home_id, populate_existing=True)
            if current is None or home is None or current.home_id != instance.home_id:
                raise RuntimeError("The recovery AI employee changed before containment")
            intent = await self._recovery_intent(session, current, identifier)
            if intent is None:
                await self._recovery_eligible(session, current, intent=None)
                self._recovery_room(current, home, await session.get(SpaceGrantRow, (home.id, "human", identifier)))
            else:
                await self._check_recovery_intent(session, current, home, intent, identifier)
            await self.agents._human(PrincipalRef("human", identifier), session=session)

        async def perform():
            closure = await self._close(instance, end_running_work=end_running_work, wait_seconds=wait_seconds, wait_if_closed=True, domain_admission=closure_admission)
            effects["closed"] = bool(closure["attachments"] or closure["attachments_unconfirmed"])
            if closure["attachments_unconfirmed"]:
                async with self.files.registry._sf() as session:
                    record = await session.get(SpaceAttachmentRow, closure["attachments_unconfirmed"][0])
                name = record.container_id[:12] if record and record.container_id else closure["attachments_unconfirmed"][0]
                raise CommandRefused(f"the Docker daemon did not confirm that container {name} is gone; nothing was granted; run the command again")
            async with self.files.registry.host_locked([instance.home_id]) as (session, homes):
                home = homes.get(instance.home_id)
                current = await session.scalar(select(AgentInstanceRow).where(AgentInstanceRow.id == instance_id).with_for_update())
                if current is None or home is None or current.home_id != instance.home_id or current.status not in ("active", "suspended", "archived"):
                    raise RuntimeError("The AI employee changed before supervision recovery")
                intent = await self._recovery_intent(session, current, identifier)
                await SpaceRecovery._require_fenced(session, home.id)
                await self.agents._human(PrincipalRef("human", identifier), session=session)
                if intent is None:
                    await self._recovery_eligible(session, current, intent=None)
                    self._recovery_room(current, home, await session.get(SpaceGrantRow, (home.id, "human", identifier)))
                    binding = await session.get(SpaceBackingRow, home.backing_handle)
                    if binding is None or binding.space_id != home.id or home.generation >= 2**31 - 1:
                        raise RuntimeError("The AI employee's Home cannot record recovery")
                    instance_grants, space_grants = await _grants(session, instance_id, home.id)
                    details = dict(
                        operator_host_id=self.adapter.host_id,
                        target_id=identifier,
                        instance=_facts(current, _INSTANCE),
                        home=_facts(home, _HOME),
                        backing=_facts(binding, _BACKING),
                        instance_grants=instance_grants,
                        space_grants=space_grants,
                        lifecycle_operation_id=uuid4().hex,
                    )
                    home.generation += 1
                    home.updated_at = datetime.now(UTC)
                    intent = SpaceEventRow(space_id=home.id, generation=home.generation, action="operator-supervise-intent", actor_kind="human", actor_id=identifier, details=details)
                    session.add(intent)
                else:
                    await self._check_recovery_intent(session, current, home, intent, identifier)
                grant = await session.get(AgentInstanceGrantRow, (instance_id, identifier))
                if grant is None:
                    session.add(AgentInstanceGrantRow(instance_id=instance_id, user_id=identifier, permissions=7))
                else:
                    grant.permissions = 7
                details, sequence = intent.details, intent.generation
            effects["granted"] = True

            async def admission(session):
                home = await session.get(SpaceRow, instance.home_id, populate_existing=True)
                current = await session.scalar(select(AgentInstanceRow).where(AgentInstanceRow.id == instance_id).with_for_update().execution_options(populate_existing=True))
                actual = await session.get(SpaceEventRow, (instance.home_id, sequence))
                if actual is None or actual.details != details:
                    raise RuntimeError("The supervision intent changed before its next phase")
                await self._check_recovery_intent(session, current, home, actual, identifier)
                await self.agents.space_audience_admission(instance.home_id, PrincipalRef("human", identifier))(session)

            await self.files.registry.host_grant(instance.home_id, PrincipalRef("human", identifier), Permission(27), host_id=self.adapter.host_id, admission=admission)
            async with self.files.registry._sf() as session:
                other_pending = await session.scalar(
                    select(AgentLifecycleRow.operation_id).where(AgentLifecycleRow.instance_id == instance_id, AgentLifecycleRow.phase == "pending", AgentLifecycleRow.operation_id != details["lifecycle_operation_id"]).limit(1)
                )
            if other_pending is not None and await self._handoff_recovery(instance, intent, identifier):
                current = await self._agent(instance_id)
                return self._supervision_answer(current, target, previous, verdict="manager_added", closure=closure, previous_id=details["instance"]["supervisor_id"])
            if self.attachments is None:
                if self._attachment_factory is not None:
                    await self._attachment_factory()
                else:
                    self.attachments = SpaceAttachments(self.files, self.adapter)

            async def lifecycle_admission(session):
                await admission(session)

            result = await InstanceLifecycle(self.agents).change(
                actor=PrincipalRef("human", identifier),
                instance_id=instance_id,
                expected_generation=details["instance"]["generation"],
                operation_id=details["lifecycle_operation_id"],
                action="supervise",
                supervisor=PrincipalRef("human", identifier),
                host_admission=lifecycle_admission,
            )
            return self._supervision_answer(
                result["instance"],
                target,
                previous,
                verdict="changed" if result["complete"] else "manager_added",
                closure=closure,
                operation_id=details["lifecycle_operation_id"] if result["complete"] else None,
                previous_id=details["instance"]["supervisor_id"],
            )

        async def report():
            try:
                return await perform()
            except CommandRefused as exc:
                if effects["closed"] or effects["granted"]:
                    # Recovery may already have closed the workspace or
                    # committed its first grant. Retain its exact retry intent.
                    raise RuntimeError(str(exc)) from exc
                raise

        return await await_drained(report())

    def _supervision_answer(self, instance, target, previous, *, verdict, closure, operation_id=None, previous_id=None, permissions=None):
        code = 2 if verdict == "manager_added" or closure["runs_unconfirmed"] or closure["attempts_unconfirmed"] else 0
        note = f"supervisor of {instance.id} unchanged" if verdict == "unchanged" else f"supervisor of {instance.id} changed; the new supervisor holds Use, Inspect and Manage and can administer the files"
        if verdict == "manager_added":
            note = f"the person holds Manage on {instance.id}; a lifecycle operation is pending, so the supervisor is unchanged; the person resolves it in the product"
        return self._answer(
            "supervise",
            note,
            returncode=code,
            instance_id=instance.id,
            verdict=verdict,
            previous={"id": previous_id or (str(previous.id) if previous else instance.supervisor_id), "email": previous.email if previous else None, "live": previous is not None and account_refusal(previous) is None},
            supervisor={"id": str(target.id), "email": str(target.email), "issuer": target.oauth_issuer, "subject": target.oauth_id},
            grants=permissions or {"instance": 7, "space": 27},
            generation=instance.generation,
            lifecycle_operation_id=operation_id,
            workspace_closed=bool(closure["attachments"]),
            **closure,
        )
