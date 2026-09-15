"""A host-local provider that declares immutable accepted material; tests only.

The host-local ``LocalSandboxProvider`` answers ``empty_only`` for accepted
material, because files on the Gateway's own filesystem cannot be proved
immutable to the commands a sandbox runs, so it refuses a nonempty skill
snapshot under every deployment profile. The tenant profile's provider (the
AIO provider over the local container backend) declares
``IMMUTABLE_READ_ONLY`` for the accepted-only sandboxes it creates, which
needs Docker. This subclass makes that one declaration on the host-local
provider so the scripted-Gateway regression can drive the route, admission,
worker decision and projection binding without Docker. It proves nothing
about immutability and must never be configured outside tests.
"""

from __future__ import annotations

import sys

from deerflow.sandbox.accepted_material import AcceptedMaterialCapability
from deerflow.sandbox.local.local_sandbox_provider import LocalSandboxProvider


class ProjectionProvider(LocalSandboxProvider):
    def __init__(self, *args: object, **kwargs: object) -> None:
        if "pytest" not in sys.modules:
            raise RuntimeError("ProjectionProvider is test-only and must never be configured")
        super().__init__(*args, **kwargs)

    def accepted_skill_material_capability(self, sandbox_id: str) -> AcceptedMaterialCapability:
        if self.has_accepted_skill_isolation(sandbox_id):
            return AcceptedMaterialCapability.IMMUTABLE_READ_ONLY
        return AcceptedMaterialCapability.EMPTY_ONLY
