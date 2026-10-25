"""Pure, closed projections of admitted channel runs and generic output proof."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from deerflow.runtime.runs.output import OutputEvidenceError, OutputReceipt, OutputUnavailableReason, TextOutputReceipt, canonical_text_digest, validate_receipt_status

from .channel_execution import ChannelExecutionReceipt


class RunOutputSource(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, revalidate_instances="always")

    text: str = Field(min_length=1)


class RunOutputProjection(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, revalidate_instances="always")

    version: Literal[1]
    owner_user_id: str = Field(min_length=1, max_length=128)
    thread_id: str = Field(min_length=1, max_length=64)
    run_id: str = Field(min_length=1, max_length=128)
    run_status: Literal["pending", "running", "success", "error", "timeout", "interrupted"]
    state: Literal["pending", "complete", "unavailable"]
    channel_execution: ChannelExecutionReceipt
    output_evidence: Annotated[OutputReceipt, Field(discriminator="kind")] | None
    source: RunOutputSource | None
    reason: OutputUnavailableReason | Literal["source_missing", "source_changed"] | None

    @field_validator("version", mode="before")
    @classmethod
    def literal_integer_version(cls, value):
        if type(value) is not int or value != 1:
            raise ValueError("Unsupported output projection version")
        return value

    @field_validator("channel_execution", mode="before")
    @classmethod
    def revalidate_admission(cls, value):
        # The older admission type does not revalidate constructed instances.
        # Detach its primitives so this projection validates its own full proof.
        if isinstance(value, ChannelExecutionReceipt):
            return value.model_dump(mode="python")
        return value

    @model_validator(mode="after")
    def joined_authority(self):
        channel = self.channel_execution
        if (channel.owner_user_id, channel.thread_id, channel.assistant_id) != (self.owner_user_id, self.thread_id, "lead_agent"):
            raise ValueError("Output admission scope differs")
        receipt = self.output_evidence
        if self.state == "pending":
            if self.run_status not in {"pending", "running"} or receipt is not None or self.source is not None or self.reason is not None:
                raise ValueError("Invalid pending output")
            return self
        if receipt is None or (receipt.owner_user_id, receipt.thread_id, receipt.run_id) != (self.owner_user_id, self.thread_id, self.run_id):
            raise ValueError("Output receipt scope differs")
        try:
            validate_receipt_status(receipt, self.run_status)
        except OutputEvidenceError:
            raise ValueError("Output status differs") from None
        if isinstance(receipt, TextOutputReceipt):
            if receipt.input_message_id is None:
                raise ValueError("Channel output needs its actual human input")
            if self.state == "complete":
                if self.source is None or self.reason is not None or canonical_text_digest(self.source.text) != receipt.text_sha256:
                    raise ValueError("Output source differs")
            elif self.source is not None or self.reason not in {"source_missing", "source_changed"}:
                raise ValueError("Invalid missing output source")
        elif self.state != "unavailable" or self.source is not None or self.reason != receipt.reason:
            raise ValueError("Invalid unavailable output")
        return self
