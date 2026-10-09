"""Bounded explicit activation and current-attempt reports; no caller authority."""

from typing import Literal

from pydantic import Field, field_validator, model_validator

from deerflow.agent_instances.work_contract import Key, RequestBasis, Revision, Statement, StrictModel, WorkSource
from deerflow.utils.thread_id import validate_thread_id


class ActivateWork(StrictModel):
    operation_id: Key
    expected_revision: Revision
    expected_assignment_revision: Revision
    thread_id: str = Field(min_length=1, max_length=64)

    @field_validator("thread_id")
    @classmethod
    def valid_thread(cls, value):
        validate_thread_id(value)
        return value


class ReportWork(StrictModel):
    operation_id: Key
    expected_revision: Revision
    action: Literal["progress", "outcome", "suggest", "derive", "request_input", "assess_input", "revise_input"]
    statement: Statement
    next_action: Statement | None = None
    sources: list[WorkSource] = Field(default_factory=list, max_length=16)
    purpose: Literal["information", "decision"] | None = None
    reason: Statement | None = None
    expected_response: Statement | None = None
    choices: list[Statement] = Field(default_factory=list, max_length=16)
    request_basis: RequestBasis | None = None
    success_criteria: Statement | None = None
    responsibility: str | None = Field(default=None, min_length=1, max_length=64)

    @model_validator(mode="after")
    def action_fields(self):
        allowed = {"operation_id", "expected_revision", "action", "statement"}
        if self.action == "progress":
            allowed.add("next_action")
        elif self.action == "outcome":
            allowed.add("sources")
        elif self.action in {"request_input", "revise_input"}:
            allowed |= {"purpose", "reason", "expected_response", "choices", "sources"}
            if not self.purpose or not self.reason or not self.expected_response:
                raise ValueError("A request needs purpose, reason and expected response")
            if len(set(self.choices)) != len(self.choices):
                raise ValueError("Choices must be unique")
            if self.action == "revise_input":
                allowed.add("request_basis")
                if self.request_basis is None or self.purpose != "information":
                    raise ValueError("Revision requires the exact factual request basis")
        elif self.action == "assess_input":
            allowed.add("request_basis")
            if self.request_basis is None:
                raise ValueError("Assessment requires the exact request and response set")
        elif self.action == "derive":
            allowed |= {"success_criteria", "responsibility"}
            if not self.success_criteria:
                raise ValueError("Derived Work needs success criteria")
        if self.model_fields_set - allowed:
            raise ValueError("Fields do not belong to this report")
        return self
