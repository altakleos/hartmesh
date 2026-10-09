"""Durable generic obligations; no user/chat cascade and no execution queue."""

from datetime import UTC, datetime

from sqlalchemy import JSON, Boolean, CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class AgentWorkRow(Base):
    __tablename__ = "agent_work"
    __table_args__ = (
        CheckConstraint("revision >= assignment_revision AND assignment_revision >= 1 AND revision <= 2147483647", name="ck_agent_work_revision"),
        CheckConstraint("priority IN ('low', 'normal', 'high', 'urgent')", name="ck_agent_work_priority"),
        CheckConstraint("status IN ('open', 'blocked', 'submitted', 'completed', 'cancelled')", name="ck_agent_work_status"),
        CheckConstraint(
            "(status IN ('open', 'blocked', 'cancelled') AND outcome_id IS NULL AND outcome_assignment_revision IS NULL AND outcome IS NULL AND review IS NULL AND review_state = 'none') OR "
            "(status = 'submitted' AND review_required AND review_state = 'pending' AND review IS NULL AND outcome_id IS NOT NULL AND outcome_assignment_revision IS NOT NULL AND "
            "outcome_assignment_revision = assignment_revision AND outcome IS NOT NULL) OR (status = 'completed' AND outcome_id IS NOT NULL AND outcome_assignment_revision IS NOT NULL AND "
            "outcome_assignment_revision = assignment_revision AND outcome IS NOT NULL AND ((review_required AND review_state = 'accepted' AND review IS NOT NULL) OR (NOT review_required AND "
            "review_state = 'reported' AND review IS NULL)))",
            name="ck_agent_work_review",
        ),
        Index("ix_agent_work_instance_created", "instance_id", "created_at", "id"),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    instance_id: Mapped[str] = mapped_column(String(32), ForeignKey("agent_instances.id"))
    creator_id: Mapped[str] = mapped_column(String(128))
    objective: Mapped[str] = mapped_column(Text)
    success_criteria: Mapped[str] = mapped_column(Text)
    responsibility: Mapped[str | None] = mapped_column(String(64), nullable=True)
    priority: Mapped[str] = mapped_column(String(16))
    due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    definition_revision: Mapped[str] = mapped_column(String(64), ForeignKey("agent_definition_revisions.revision"))
    assignment_revision: Mapped[int] = mapped_column(Integer)
    revision: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(16))
    progress: Mapped[str] = mapped_column(Text, default="")
    next_action: Mapped[str] = mapped_column(Text, default="")
    sources: Mapped[list] = mapped_column(JSON, default=list)
    blocker: Mapped[dict | None] = mapped_column(JSON(none_as_null=True), nullable=True)
    review_required: Mapped[bool] = mapped_column(Boolean)
    review_state: Mapped[str] = mapped_column(String(16))
    outcome_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    outcome_assignment_revision: Mapped[int | None] = mapped_column(Integer, nullable=True)
    outcome: Mapped[dict | None] = mapped_column(JSON(none_as_null=True), nullable=True)
    review: Mapped[dict | None] = mapped_column(JSON(none_as_null=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class AgentWorkAttemptRow(Base):
    """Reserved for host activation; public records commands cannot create attempts."""

    __tablename__ = "agent_work_attempts"
    __table_args__ = (
        UniqueConstraint("work_id", "activation_id", name="uq_agent_work_attempt_activation"),
        CheckConstraint("assignment_revision >= 1 AND assignment_revision <= 2147483647 AND instance_generation >= 1 AND instance_generation <= 2147483647", name="ck_agent_work_attempt_revision"),
        CheckConstraint("status IN ('starting', 'running', 'stopping', 'uncertain', 'succeeded', 'failed', 'cancelled')", name="ck_agent_work_attempt_status"),
        Index("uq_agent_work_attempt_unresolved", "work_id", unique=True, sqlite_where=text("status IN ('starting', 'running', 'stopping', 'uncertain')"), postgresql_where=text("status IN ('starting', 'running', 'stopping', 'uncertain')")),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    work_id: Mapped[str] = mapped_column(String(32), ForeignKey("agent_work.id"))
    activation_id: Mapped[str] = mapped_column(String(32))
    requester_id: Mapped[str] = mapped_column(String(128))
    assignment_revision: Mapped[int] = mapped_column(Integer)
    instance_generation: Mapped[int] = mapped_column(Integer)
    definition_revision: Mapped[str] = mapped_column(String(64), ForeignKey("agent_definition_revisions.revision"))
    status: Mapped[str] = mapped_column(String(16))
    thread_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    incarnation: Mapped[str | None] = mapped_column(String(64), nullable=True)
    request: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class AgentWorkEventRow(Base):
    __tablename__ = "agent_work_events"
    __table_args__ = (
        UniqueConstraint("instance_id", "operation_id", name="uq_agent_work_event_operation"),
        CheckConstraint("actor_kind IN ('human', 'nonhuman')", name="ck_agent_work_event_actor"),
        UniqueConstraint("work_id", "revision", name="uq_agent_work_event_revision"),
        CheckConstraint("revision >= assignment_revision AND assignment_revision >= 1 AND revision <= 2147483647", name="ck_agent_work_event_revision"),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    instance_id: Mapped[str] = mapped_column(String(32), ForeignKey("agent_instances.id"))
    work_id: Mapped[str] = mapped_column(String(32), ForeignKey("agent_work.id"))
    actor_id: Mapped[str] = mapped_column(String(128))
    actor_kind: Mapped[str] = mapped_column(String(16))
    operation_id: Mapped[str] = mapped_column(String(32))
    action: Mapped[str] = mapped_column(String(32))
    revision: Mapped[int] = mapped_column(Integer)
    assignment_revision: Mapped[int] = mapped_column(Integer)
    request: Mapped[dict] = mapped_column(JSON)
    result: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
