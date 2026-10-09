"""Work-backed requests, appended human responses, receipts and independent read state."""

from datetime import UTC, datetime

from sqlalchemy import JSON, CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class HumanInputRequestRow(Base):
    __tablename__ = "human_input_requests"
    __table_args__ = (
        CheckConstraint("source_kind = 'work'", name="ck_human_input_source"),
        CheckConstraint("purpose IN ('information', 'decision', 'review')", name="ck_human_input_purpose"),
        CheckConstraint("state IN ('pending', 'answered', 'closed') AND (state != 'answered' OR purpose = 'information')", name="ck_human_input_state"),
        CheckConstraint("(state = 'closed' AND closed_reason IS NOT NULL AND closed_reason IN ('resolved', 'superseded', 'withdrawn')) OR (state != 'closed' AND closed_reason IS NULL)", name="ck_human_input_closed"),
        CheckConstraint("revision >= request_revision AND request_revision >= 1 AND revision <= 2147483647 AND assignment_revision >= 1 AND basis_revision >= 1", name="ck_human_input_revision"),
        Index("uq_human_input_live_work", "work_id", unique=True, sqlite_where=text("state != 'closed'"), postgresql_where=text("state != 'closed'")),
        Index("ix_human_input_recipient", "recipient_id", "state", "updated_at", "id"),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    source_kind: Mapped[str] = mapped_column(String(16))
    instance_id: Mapped[str] = mapped_column(String(32), ForeignKey("agent_instances.id"))
    work_id: Mapped[str] = mapped_column(String(32), ForeignKey("agent_work.id"))
    assignment_revision: Mapped[int] = mapped_column(Integer)
    basis_id: Mapped[str] = mapped_column(String(32))
    basis_revision: Mapped[int] = mapped_column(Integer)
    creator_id: Mapped[str] = mapped_column(String(128))
    creator_kind: Mapped[str] = mapped_column(String(16), default="human", server_default="human")
    recipient_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    revision: Mapped[int] = mapped_column(Integer)
    request_revision: Mapped[int] = mapped_column(Integer)
    purpose: Mapped[str] = mapped_column(String(16))
    question: Mapped[str] = mapped_column(Text)
    reason: Mapped[str] = mapped_column(Text)
    expected_response: Mapped[str] = mapped_column(Text)
    choices: Mapped[list] = mapped_column(JSON)
    sources: Mapped[list] = mapped_column(JSON)
    state: Mapped[str] = mapped_column(String(16))
    closed_reason: Mapped[str | None] = mapped_column(String(16), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class HumanInputResponseRow(Base):
    __tablename__ = "human_input_responses"
    __table_args__ = (
        CheckConstraint("disposition IN ('supplied', 'cannot_provide', 'wrong_recipient')", name="ck_human_response_disposition"),
        CheckConstraint("request_revision >= 1 AND assignment_revision >= 1", name="ck_human_response_revision"),
        Index("ix_human_response_request", "request_id", "created_at", "id"),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    request_id: Mapped[str] = mapped_column(String(32), ForeignKey("human_input_requests.id"))
    actor_id: Mapped[str] = mapped_column(String(128))
    request_revision: Mapped[int] = mapped_column(Integer)
    assignment_revision: Mapped[int] = mapped_column(Integer)
    disposition: Mapped[str] = mapped_column(String(32))
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    choice: Mapped[str | None] = mapped_column(Text, nullable=True)
    sources: Mapped[list] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class HumanInputEventRow(Base):
    __tablename__ = "human_input_events"
    __table_args__ = (
        UniqueConstraint("instance_id", "operation_id", name="uq_human_input_operation"),
        UniqueConstraint("request_id", "revision", name="uq_human_input_event_revision"),
    )
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    instance_id: Mapped[str] = mapped_column(String(32), ForeignKey("agent_instances.id"))
    request_id: Mapped[str] = mapped_column(String(32), ForeignKey("human_input_requests.id"))
    actor_id: Mapped[str] = mapped_column(String(128))
    actor_kind: Mapped[str] = mapped_column(String(16), default="human", server_default="human")
    operation_id: Mapped[str] = mapped_column(String(32))
    action: Mapped[str] = mapped_column(String(32))
    revision: Mapped[int] = mapped_column(Integer)
    request: Mapped[dict] = mapped_column(JSON)
    receipt: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class HumanInputReadRow(Base):
    __tablename__ = "human_input_reads"
    __table_args__ = (CheckConstraint("revision >= 1", name="ck_human_input_read_revision"),)
    request_id: Mapped[str] = mapped_column(String(32), ForeignKey("human_input_requests.id"), primary_key=True)
    actor_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer)
