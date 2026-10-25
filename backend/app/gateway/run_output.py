"""Pure, closed projections of admitted channel runs and generic output proof."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from deerflow.runtime.runs.output import OutputEvidenceError, OutputReceipt, OutputUnavailableReason, TextOutputReceipt, canonical_text_digest, validate_receipt_status
from deerflow.runtime.runs.output_inventory import (
    EventInventoryEvidence,
    InventoryDocument,
    InventoryEvidence,
    InventoryUnavailableReason,
    UnavailableInventoryEvidence,
    inventory_document_digest,
    validate_inventory_document,
    validate_inventory_evidence,
    validate_inventory_pair,
)

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


# Inventory negotiation is separate from the existing text-only projection.
OUTPUT_INVENTORY_REQUIRE_HEADER = "X-DeerFlow-Require-Output-Inventory"
OUTPUT_INVENTORY_VERSION_HEADER = "X-DeerFlow-Output-Inventory-Version"
OUTPUT_INVENTORY_VERSION = "1"


class InventoryProjection(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, revalidate_instances="always")

    state: Literal["pending", "matched", "unavailable"]
    evidence: Annotated[InventoryEvidence, Field(discriminator="kind")] | None
    document: Annotated[InventoryDocument, Field(discriminator="coverage")] | None
    reason: InventoryUnavailableReason | Literal["source_missing", "source_changed"] | None

    @field_validator("evidence", mode="before")
    @classmethod
    def revalidate_evidence(cls, value):
        return validate_inventory_evidence(value) if value is not None else None

    @field_validator("document", mode="before")
    @classmethod
    def revalidate_document(cls, value):
        return validate_inventory_document(value) if value is not None else None

    @model_validator(mode="after")
    def joined_inventory(self):
        evidence, document = self.evidence, self.document
        if self.state == "pending":
            if evidence is not None or document is not None or self.reason is not None:
                raise ValueError("Invalid pending inventory")
        elif isinstance(evidence, EventInventoryEvidence):
            if self.state == "matched":
                if document is None or self.reason is not None or inventory_document_digest(document) != evidence.inventory_sha256:
                    raise ValueError("Inventory source differs")
                if (document.owner_user_id, document.thread_id, document.run_id, document.input_message_id, document.output_receipt_sha256) != (
                    evidence.owner_user_id,
                    evidence.thread_id,
                    evidence.run_id,
                    evidence.input_message_id,
                    evidence.output_receipt_sha256,
                ):
                    raise ValueError("Inventory scope differs")
            elif document is not None or self.reason not in {"source_missing", "source_changed"}:
                raise ValueError("Invalid missing inventory")
        elif not isinstance(evidence, UnavailableInventoryEvidence) or self.state != "unavailable" or document is not None or self.reason != evidence.reason:
            raise ValueError("Invalid unavailable inventory")
        return self


class RunOutputProjectionV2(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, revalidate_instances="always")

    version: Literal[2]
    output: RunOutputProjection
    inventory: InventoryProjection

    @field_validator("version", mode="before")
    @classmethod
    def literal_integer_version(cls, value):
        if type(value) is not int or value != 2:
            raise ValueError("Unsupported inventory projection version")
        return value

    @field_validator("output", "inventory", mode="before")
    @classmethod
    def revalidate_nested(cls, value):
        return value.model_dump(mode="python") if isinstance(value, BaseModel) else value

    @model_validator(mode="after")
    def joined_pair(self):
        output, inventory = self.output, self.inventory
        if output.state == "pending":
            if inventory.state != "pending":
                raise ValueError("Pending inventory differs")
        else:
            if output.output_evidence is None or inventory.evidence is None or inventory.state == "pending":
                raise ValueError("Missing inventory pair")
            validate_inventory_pair(output.output_evidence, inventory.evidence, status=output.run_status)
        return self
