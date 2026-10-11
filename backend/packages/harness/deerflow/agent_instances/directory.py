"""Trusted non-login directory; a browser identifier never binds an actor."""

from contextlib import contextmanager
from contextvars import ContextVar

from sqlalchemy import select

from deerflow.agent_instances.contract import InstanceIdentity
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.spaces.contract import PrincipalRef, ResolvedPrincipal
from deerflow.spaces.principals import Lookup

_provisioning = ContextVar("agent_principal_provisioning", default=None)


class InstanceDirectory:
    def __init__(self, session_factory, *, human: Lookup):
        self.session_factory = session_factory
        self.human = human

    @contextmanager
    def provisioning(self, reference: PrincipalRef):
        """Only the host creation transaction may validate a pending grant target."""
        token = _provisioning.set((self, reference))
        try:
            yield
        finally:
            _provisioning.reset(token)

    async def lookup(self, reference: PrincipalRef) -> ResolvedPrincipal | None:
        async with self.session_factory() as session, session.begin():
            return await self.lookup_in_session(reference, session)

    async def lookup_in_session(self, reference, session):
        if not isinstance(reference, PrincipalRef) or reference.kind != "nonhuman":
            return None
        if not session.in_transaction() or not session.is_active:
            raise ValueError("Borrowed instance lookup requires an active transaction")
        row = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.principal_id == reference.subject_id).execution_options(populate_existing=True))).scalar_one_or_none()
        if row is None:
            return None
        try:
            identity = InstanceIdentity.from_row(row)
        except (TypeError, ValueError):
            return None
        if identity.principal != reference or identity.home_id is None:
            return None
        allowed_pending = row.status == "provisioning" and _provisioning.get() == (self, reference)
        if row.status != "active" and not allowed_pending:
            return None
        if row.custody == "personal":
            if row.owner_id is None:
                return None
            owner = PrincipalRef("human", row.owner_id)
            borrowed = getattr(self.human, "in_session", None)
            resolved = await borrowed(owner, session) if callable(borrowed) else await self.human(owner)
            if resolved is None:
                return None
        elif row.custody != "company" or row.owner_id is not None:
            return None
        # Provisioning company resources belongs to the authenticated human,
        # never the resident principal or the creator's historical admin role.
        return ResolvedPrincipal(reference)
