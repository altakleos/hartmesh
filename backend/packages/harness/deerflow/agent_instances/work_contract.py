"""Bounded human commands. Execution and outcome reporting are host-only later work."""

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StringConstraints, field_validator, model_validator

from deerflow.config.work_policy import WorkPriority

Key = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{32}$")]
Revision = Annotated[int, Field(strict=True, ge=1, le=2147483647)]
Statement = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=4096)]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class WorkSource(StrictModel):
    kind: Literal["space_file"] = "space_file"
    space_id: Key
    path: str = Field(min_length=1, max_length=1024)

    @field_validator("path")
    @classmethod
    def relative_path(cls, value):
        if value.startswith("/") or "\\" in value or any(ord(c) < 32 or ord(c) == 127 for c in value) or any(part in {"", ".", ".."} for part in value.split("/")):
            raise ValueError("A canonical relative file path is required")
        return value


class WorkResolution(StrictModel):
    actor_kind: Literal["human"]
    actor_id: str = Field(min_length=1, max_length=128)
    statement: Statement


class WorkBlocker(StrictModel):
    id: Key
    revision: Revision
    assignment_revision: Revision
    kind: Literal["information", "decision"]
    question: Statement
    sources: list[WorkSource] = Field(default_factory=list, max_length=16)
    resolution: WorkResolution | None = None


class WorkOutcome(StrictModel):
    id: Key
    attempt_id: Key
    assignment_revision: Revision
    statement: Statement
    evidence_revision: Revision
    sources: list[WorkSource] = Field(default_factory=list, max_length=16)


class WorkReview(StrictModel):
    actor_kind: Literal["human"]
    actor_id: str = Field(min_length=1, max_length=128)
    assignment_revision: Revision
    outcome_id: Key
    evidence_revision: Revision
    basis: Literal["outcome_statement"]
    current_contents: Literal["not_checked"]
    note: Statement | None = None
    accepted_at: datetime


class AssignmentFields(StrictModel):
    objective: Statement | None = None
    success_criteria: Statement | None = None
    responsibility: str | None = Field(default=None, min_length=1, max_length=64)
    priority: WorkPriority | None = None
    due_at: datetime | None = None
    review_required: StrictBool | None = None
    sources: list[WorkSource] | None = Field(default=None, max_length=16)

    @field_validator("due_at")
    @classmethod
    def aware_date(cls, value):
        if value is not None:
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("Due targets require a timezone")
            return value.astimezone(UTC)
        return value


class DelegateWork(AssignmentFields):
    operation_id: Key
    objective: Statement
    success_criteria: Statement


class RequestBasis(StrictModel):
    id: Key
    revision: Revision
    request_revision: Revision
    response_ids: list[Key] = Field(default_factory=list, max_length=200)


class WorkCommand(AssignmentFields):
    operation_id: Key
    expected_revision: Revision
    expected_assignment_revision: Revision
    action: Literal["edit", "cancel", "reopen", "changes_requested", "reconcile_mandate", "accept", "input", "decide"]
    note: Statement | None = None
    outcome_id: Key | None = None
    evidence_revision: Revision | None = None
    basis: Literal["outcome_statement"] | None = None
    acknowledge_unchecked_sources: StrictBool = False
    blocker_id: Key | None = None
    blocker_revision: Revision | None = None
    request_basis: RequestBasis | None = None

    @model_validator(mode="after")
    def action_fields(self):
        assignment = set(AssignmentFields.model_fields)
        review = {"outcome_id", "evidence_revision", "basis", "acknowledge_unchecked_sources"}
        blocker = {"blocker_id", "blocker_revision"}
        supplied = self.model_fields_set
        allowed = {"operation_id", "expected_revision", "expected_assignment_revision", "action", "note"}
        if self.action in {"decide", "accept"}:
            allowed.add("request_basis")
        if self.action == "edit":
            allowed |= assignment
            if not supplied & assignment:
                raise ValueError("An assignment change is required")
            for key in ("objective", "success_criteria", "priority", "review_required", "sources"):
                if key in supplied and getattr(self, key) is None:
                    raise ValueError(f"{key} cannot be null")
        elif self.action == "accept":
            allowed |= review
            if not self.outcome_id or self.evidence_revision is None or self.basis is None or not self.acknowledge_unchecked_sources:
                raise ValueError("Acceptance requires the exact outcome and explicit unchecked-content acknowledgement")
        elif self.action in {"input", "decide"}:
            allowed |= blocker
            if not self.blocker_id or self.blocker_revision is None or not self.note:
                raise ValueError("Input requires the exact blocker and a statement")
        if supplied - allowed:
            raise ValueError("Fields do not belong to this command")
        if self.action in {"reopen", "changes_requested", "reconcile_mandate"} and not self.note:
            raise ValueError("This transition requires a note")
        return self
