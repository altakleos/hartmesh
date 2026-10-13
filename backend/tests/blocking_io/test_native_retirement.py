"""Actual retained containment keeps journal and Docker work off the loop."""

import os
from unittest.mock import Mock

import pytest

from deerflow.files.store import open_directory_source
from deerflow.utils.file_io import run_file_io

pytestmark = pytest.mark.asyncio


def prepare(tmp_path):
    from test_native_create_local_backend import backend
    from test_native_create_provider import fixture, install

    generator = fixture.__wrapped__(tmp_path)
    provider, owner = next(generator)
    try:
        # Import the real descriptor capability before strict instrumentation;
        # its OS-function identity check describes the underlying platform.
        descriptor = open_directory_source(owner.pool)
        os.close(descriptor)
        provider._backend = backend(None)
        install(provider, owner)
        provider._backend._slot_containers = Mock(return_value=())
        return provider, generator
    except BaseException:
        generator.close()
        raise


async def test_retained_scope_and_final_proof_offload_actual_journal(tmp_path):
    from deerflow.spaces.docker import StorageHostUnqualified

    provider, generator = await run_file_io(prepare, tmp_path)
    try:
        async with provider._native_retirement_scope("alice", "a" * 32) as scope:
            scope.assert_current()
            with pytest.raises(StorageHostUnqualified):
                with provider._owner_admission_scope("alice"):
                    pytest.fail("Retirement lost its memory reservation")
            await scope.prove_absent()
        assert provider._backend._slot_containers.call_count >= 3
    finally:
        await run_file_io(generator.close)


async def test_inline_retirement_journal_check_trips_strict_detector(tmp_path):
    from blockbuster import BlockingError

    from deerflow.community.aio_sandbox.native_retirement import NativeRetirementScope

    provider, generator = await run_file_io(prepare, tmp_path)
    gate = provider._owner_admission_gate()
    reservation = gate.reserve("alice", "a" * 32)
    scope = NativeRetirementScope(provider, gate, reservation, provider._native_creation)
    try:
        with pytest.raises(BlockingError):
            provider._native_retirement_check(scope)
    finally:
        gate.end_retirement(reservation)
        await run_file_io(generator.close)
