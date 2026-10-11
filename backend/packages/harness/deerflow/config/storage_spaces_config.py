"""Startup-only provider inventory; ordinary APIs cannot mount/format volumes."""

from pathlib import Path
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StoragePlan(BaseModel):
    """Operator counts and capacities; preparation and admission own availability."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    users: int = Field(ge=1, le=100)
    user_gib: int = Field(ge=1, le=1024)
    ai_assistants_per_user: int = Field(ge=0, le=10)
    ai_assistant_gib: int = Field(ge=1, le=1024)
    ai_employees: int = Field(ge=0, le=100)
    ai_employee_gib: int = Field(ge=1, le=1024)
    company_gib: int = Field(ge=1, le=1024)


class StorageSpacesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    enabled: bool = False
    inventory_path: str | None = None
    user_spaces: bool = True
    plan: StoragePlan | None = None

    @model_validator(mode="after")
    def qualified_inventory(self) -> Self:
        if self.inventory_path is not None and (not self.inventory_path or not Path(self.inventory_path).is_absolute()):
            raise ValueError("Storage inventory must name an absolute provider path")
        if self.enabled and self.inventory_path is None:
            raise ValueError("Enabled storage spaces require an explicit provider inventory")
        return self
