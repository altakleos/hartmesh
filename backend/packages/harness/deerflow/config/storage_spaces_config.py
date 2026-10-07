"""Startup-only provider inventory; ordinary APIs cannot mount/format volumes."""

from pathlib import Path
from typing import Self

from pydantic import BaseModel, ConfigDict, model_validator


class StorageSpacesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    enabled: bool = False
    inventory_path: str | None = None

    @model_validator(mode="after")
    def qualified_inventory(self) -> Self:
        if self.inventory_path is not None and (not self.inventory_path or not Path(self.inventory_path).is_absolute()):
            raise ValueError("Storage inventory must name an absolute provider path")
        if self.enabled and self.inventory_path is None:
            raise ValueError("Enabled storage spaces require an explicit provider inventory")
        return self
