"""Private retained native containment; eligibility belongs to the host caller."""

from deerflow.sandbox.lease import run_sync_lifecycle_operation
from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.utils.file_io import await_drained


class NativeRetirementScope:
    def __init__(self, provider, gate, reservation, bridge):
        self.provider, self.gate = provider, gate
        self.reservation, self.bridge = reservation, bridge

    def assert_current(self):
        if not self.gate.current(self.reservation) or self.provider._native_creation is not self.bridge or not self.provider._pool_runtime_current():
            raise StorageHostUnqualified("Native containment no longer owns its exact owner, slot and runtime")

    async def prove_absent(self):
        """Keep the scope alive through actual worker drainage, including Stop."""
        self.assert_current()
        await await_drained(run_sync_lifecycle_operation(self.provider._prove_native_retirement_absent, self))
        self.assert_current()
