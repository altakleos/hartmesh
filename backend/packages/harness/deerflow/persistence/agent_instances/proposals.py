"""Durable employee proposals survive deletion of their source conversation."""

from datetime import datetime

from sqlalchemy import Boolean, CheckConstraint, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class AgentProposalRow(Base):
    __tablename__ = "agent_configuration_proposals"
    __table_args__ = (
        UniqueConstraint("instance_id", "thread_incarnation", "run_id", "issuer_id", "call_id", name="uq_agent_proposal_issuance"),
        UniqueConstraint("instance_id", "operation_id", name="uq_agent_proposal_operation"),
        Index("ix_agent_proposal_page", "instance_id", "created_at", "id"),
        CheckConstraint("base_generation >= 1 AND base_generation <= 2147483647", name="ck_agent_proposal_generation"),
        CheckConstraint("field = 'work_policy.enabled'", name="ck_agent_proposal_field"),
        CheckConstraint("(decided_at IS NULL AND decided_by IS NULL AND operation_id IS NULL) OR (decided_at IS NOT NULL AND decided_by IS NOT NULL)", name="ck_agent_proposal_decision"),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    instance_id: Mapped[str] = mapped_column(String(32), ForeignKey("agent_instances.id"))
    author_id: Mapped[str] = mapped_column(String(128))
    requester_id: Mapped[str] = mapped_column(String(128))
    thread_id: Mapped[str] = mapped_column(String(64))
    thread_incarnation: Mapped[str] = mapped_column(String(64))
    run_id: Mapped[str] = mapped_column(String(128))
    issuer_id: Mapped[str] = mapped_column(String(256))
    call_id: Mapped[str] = mapped_column(String(256))
    base_generation: Mapped[int] = mapped_column(Integer)
    base_revision: Mapped[str] = mapped_column(String(64), ForeignKey("agent_definition_revisions.revision"))
    field: Mapped[str] = mapped_column(String(64))
    proposed_value: Mapped[bool] = mapped_column(Boolean)
    candidate_digest: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    decided_by: Mapped[str | None] = mapped_column(String(128))
    operation_id: Mapped[str | None] = mapped_column(String(32))
