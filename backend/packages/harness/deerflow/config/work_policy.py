"""Optional adopted Work policy; business meaning remains in the mandate."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StringConstraints, model_validator

ResponsibilityKey = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=64)]
WorkPriority = Literal["low", "normal", "high", "urgent"]


class WorkResponsibility(BaseModel):
    model_config = ConfigDict(extra="forbid")
    key: ResponsibilityKey
    label: str = Field(min_length=1, max_length=128)


class WorkPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: StrictBool = False
    default_priority: WorkPriority = "normal"
    review_required: StrictBool = True
    allow_derived: StrictBool = False
    max_derived_per_activation: int = Field(default=3, ge=1, le=20, strict=True)
    responsibilities: list[WorkResponsibility] = Field(default_factory=list, max_length=32)

    @model_validator(mode="after")
    def unique_keys(self):
        if len({item.key for item in self.responsibilities}) != len(self.responsibilities):
            raise ValueError("Responsibility keys must be unique")
        return self
