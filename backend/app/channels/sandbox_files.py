"""Shared lifecycle-safe synchronization for inbound channel attachments."""

from __future__ import annotations

import logging

from deerflow.sandbox.lease import acquire_sandbox_client_lease
from deerflow.sandbox.session import SandboxSessionConflict

logger = logging.getLogger(__name__)


async def sync_file_to_thread_sandbox(
    sandbox_provider,
    *,
    thread_id: str,
    user_id: str,
    virtual_path: str,
    content: bytes,
    owner_prefix: str,
) -> bool:
    """Copy one attachment while holding a non-releasing sandbox client lease.

    Thread-data mount providers already see the persisted upload. Other
    providers need a unique holder so a parallel run cannot close their client
    during ``update_file``. The blocking transport worker is drained even when
    the channel handler is repeatedly cancelled, and only then is the holder
    released.
    """
    if getattr(sandbox_provider, "uses_thread_data_mounts", False):
        return True

    try:
        lease = await acquire_sandbox_client_lease(
            sandbox_provider,
            thread_id,
            user_id=user_id,
            owner_prefix=owner_prefix,
            release_on_last=False,
        )
    except SandboxSessionConflict as exc:
        # An accepted run holds this thread's container; the attachment stays
        # in thread storage and the refusal is recorded on that run.
        from app.gateway.authz import record_refused_sandbox_acquire

        logger.info("Channel attachment sync skipped: %s (requester=%s)", exc, owner_prefix)
        record_refused_sandbox_acquire(exc, requester=owner_prefix)
        return False
    try:
        if lease.sandbox_id == "local" or lease.sandbox_id.startswith("local:"):
            return True
        if lease.sandbox is None:
            return False
        await lease.run_sync(lease.sandbox.update_file, virtual_path, content)
        return True
    finally:
        await lease.release()
