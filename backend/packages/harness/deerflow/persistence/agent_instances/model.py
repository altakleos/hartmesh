"""Company custody has no creator/owner user foreign key or deletion cascade."""

from datetime import UTC, datetime

from sqlalchemy import JSON, CheckConstraint, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class AgentDefinitionRevisionRow(Base):
    __tablename__ = "agent_definition_revisions"
    revision: Mapped[str] = mapped_column(String(64), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(128))
    config: Mapped[dict] = mapped_column(JSON)
    soul: Mapped[str] = mapped_column(Text)


class AgentInstanceRow(Base):
    __tablename__ = "agent_instances"
    __table_args__ = (
        UniqueConstraint("principal_id", name="uq_agent_instances_principal"),
        UniqueConstraint("home_id", name="uq_agent_instances_home"),
        UniqueConstraint("creator_id", "creation_id", name="uq_agent_instances_creation"),
        CheckConstraint("principal_id = 'agent:' || id", name="ck_agent_instances_principal"),
        CheckConstraint("(custody = 'personal' AND owner_id IS NOT NULL) OR (custody = 'company' AND owner_id IS NULL)", name="ck_agent_instances_custody"),
        CheckConstraint("generation >= 1 AND generation <= 2147483647", name="ck_agent_instances_generation"),
        CheckConstraint("status IN ('provisioning', 'active', 'suspended', 'archived', 'deleted')", name="ck_agent_instances_status"),
        CheckConstraint("status = 'provisioning' OR home_id IS NOT NULL", name="ck_agent_instances_home"),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    principal_id: Mapped[str] = mapped_column(String(128))
    name: Mapped[str] = mapped_column(String(128))
    custody: Mapped[str] = mapped_column(String(16))
    owner_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    creator_id: Mapped[str] = mapped_column(String(128))
    supervisor_id: Mapped[str] = mapped_column(String(128))
    definition_revision: Mapped[str] = mapped_column(String(64), ForeignKey("agent_definition_revisions.revision"))
    home_id: Mapped[str | None] = mapped_column(String(32), ForeignKey("storage_spaces.id"), nullable=True)
    status: Mapped[str] = mapped_column(String(16))
    generation: Mapped[int] = mapped_column(Integer)
    creation_id: Mapped[str] = mapped_column(String(32))
    creation_request: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class AgentInstanceGrantRow(Base):
    __tablename__ = "agent_instance_grants"
    __table_args__ = (CheckConstraint("permissions >= 1 AND permissions <= 7", name="ck_agent_instance_grants_permissions"),)
    instance_id: Mapped[str] = mapped_column(String(32), ForeignKey("agent_instances.id"), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    permissions: Mapped[int] = mapped_column(Integer)


class AgentConversationRow(Base):
    """Keep the binding after chat deletion so legacy missing-row access cannot win."""

    __tablename__ = "agent_conversations"
    __table_args__ = (UniqueConstraint("requester_id", "creation_id", name="uq_agent_conversations_creation"),)
    thread_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    instance_id: Mapped[str] = mapped_column(String(32), ForeignKey("agent_instances.id"), index=True)
    requester_id: Mapped[str] = mapped_column(String(128))
    creation_id: Mapped[str] = mapped_column(String(32))


class AgentMemoryRow(Base):
    """Platform memory belongs to an instance, including every summary field."""

    __tablename__ = "agent_instance_memory"
    __table_args__ = (CheckConstraint("epoch >= 1 AND epoch <= 2147483647", name="ck_agent_memory_epoch"),)
    instance_id: Mapped[str] = mapped_column(String(32), ForeignKey("agent_instances.id"), primary_key=True)
    epoch: Mapped[int] = mapped_column(Integer)
    document: Mapped[dict] = mapped_column(JSON)


class AgentProtectedContextRow(Base):
    """Memory/cross-requester context needs Inspect, including after restart."""

    __tablename__ = "agent_protected_contexts"
    thread_id: Mapped[str] = mapped_column(String(64), ForeignKey("agent_conversations.thread_id"), primary_key=True)


class AgentLifecycleRow(Base):
    """Exact containment scope survives cancellation, adapter loss and restart."""

    __tablename__ = "agent_lifecycle_operations"
    __table_args__ = (
        CheckConstraint("generation >= 1 AND generation < 2147483647", name="ck_agent_lifecycle_generation"),
        CheckConstraint("phase IN ('pending', 'complete', 'abandoned')", name="ck_agent_lifecycle_phase"),
        CheckConstraint("prior_status IN ('active', 'suspended', 'archived', 'deleted')", name="ck_agent_lifecycle_prior_status"),
    )
    instance_id: Mapped[str] = mapped_column(String(32), ForeignKey("agent_instances.id"), primary_key=True)
    operation_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    actor_id: Mapped[str] = mapped_column(String(128))
    generation: Mapped[int] = mapped_column(Integer)
    home_generation: Mapped[int] = mapped_column(Integer)
    home_id: Mapped[str] = mapped_column(String(32), ForeignKey("storage_spaces.id"))
    request: Mapped[dict] = mapped_column(JSON)
    attachment_ids: Mapped[list] = mapped_column(JSON)
    phase: Mapped[str] = mapped_column(String(16))
    prior_status: Mapped[str] = mapped_column(String(16))
    resolved_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
