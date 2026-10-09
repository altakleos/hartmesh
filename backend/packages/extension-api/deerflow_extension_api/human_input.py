"""Optional v1 host facade for authenticated human plugin actions.

Only the host creates a bound handle. IDs select records; they grant no access.
Calls use the same bounded JSON command contracts as the host HTTP API. Missing
or unsupported handles raise NotImplementedError, never return empty success.
"""

from collections.abc import Mapping
from typing import Any


class HumanInputActions:
    api_version = 1

    async def call(self, operation: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        """Operations: list/get/responses/history/create/respond/command/work_command.

        payload contains the corresponding canonical API arguments and command
        body. The host rechecks the current human, plugin, ceilings and grants.
        No operation starts execution, uploads files or supplies model authority.
        """
        raise NotImplementedError("Human input actions are unavailable")
