"""Host-independent health contract for authoritative capabilities."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

CapabilityHealthStatus = Literal["healthy", "unhealthy"]
CapabilityHealthProbe = Callable[[], Awaitable["CapabilityHealthResult"]]

_DIAGNOSTIC_CODE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$", re.ASCII)


@dataclass(frozen=True)
class CapabilityHealthResult:
    """One bounded health result without exception text or private details."""

    status: CapabilityHealthStatus
    diagnostic_code: str | None = None

    def __post_init__(self) -> None:
        if self.status not in ("healthy", "unhealthy"):
            raise ValueError("capability health status must be 'healthy' or 'unhealthy'")
        code = self.diagnostic_code
        if code is not None and (not isinstance(code, str) or _DIAGNOSTIC_CODE.fullmatch(code) is None):
            raise ValueError("capability health diagnostic_code must be a 1-64 character ASCII code")


__all__ = [
    "CapabilityHealthProbe",
    "CapabilityHealthResult",
    "CapabilityHealthStatus",
]
