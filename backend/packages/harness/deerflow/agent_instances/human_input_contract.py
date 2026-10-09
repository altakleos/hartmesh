"""Bounded authenticated human commands; trusted source bindings are host-derived."""

from typing import Literal

from pydantic import Field, model_validator

from deerflow.agent_instances.work_contract import Key, Revision, Statement, StrictModel, WorkSource


class CreateRequest(StrictModel):
    operation_id: Key
    expected_work_revision: Revision
    expected_assignment_revision: Revision
    purpose: Literal["information", "decision", "review"]
    question: Statement
    reason: Statement
    expected_response: Statement
    choices: list[Statement] = Field(default_factory=list, max_length=16)
    sources: list[WorkSource] = Field(default_factory=list, max_length=16)
    recipient_id: str | None = Field(default=None, min_length=1, max_length=128)

    @model_validator(mode="after")
    def unique_choices(self):
        if len(set(self.choices)) != len(self.choices):
            raise ValueError("Choices must be unique")
        return self


class Respond(StrictModel):
    operation_id: Key
    expected_request_revision: Revision
    expected_assignment_revision: Revision
    disposition: Literal["supplied", "cannot_provide", "wrong_recipient"] = "supplied"
    text: Statement | None = None
    choice: Statement | None = None
    sources: list[WorkSource] = Field(default_factory=list, max_length=16)

    @model_validator(mode="after")
    def content(self):
        if not self.text and not self.choice and not self.sources:
            raise ValueError("A response is required")
        return self


class RequestCommand(StrictModel):
    operation_id: Key
    expected_revision: Revision
    expected_request_revision: Revision
    action: Literal["route", "withdraw"]
    recipient_id: str | None = Field(default=None, min_length=1, max_length=128)
    note: Statement | None = None
    response_ids: list[Key] = Field(default_factory=list, max_length=200)

    @model_validator(mode="after")
    def fields_for_action(self):
        if self.action == "withdraw" and (not self.note or self.recipient_id is not None):
            raise ValueError("Withdrawal needs a reason and cannot route")
        return self
